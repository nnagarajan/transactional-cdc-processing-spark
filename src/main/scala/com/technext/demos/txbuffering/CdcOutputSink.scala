/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import scala.util.Try

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, lit, to_date}
import org.apache.spark.sql.types.StructType

import com.technext.demos.txbuffering.model.spec.{JobModel, Schemas, StreamTransform}
import com.technext.demos.txbuffering.processor.{SequencerOutput, TransactionOrderingProcessor}

/** Settings for the dropped-events table (`job.output.dropped-events`). */
final case class DroppedEventsSettings(enabled: Boolean, tableName: String, summarizeReplays: Boolean)

object DroppedEventsSettings {
  def apply(config: Config): DroppedEventsSettings = {
    val path = "job.output.dropped-events"
    val detail = config.getString(s"$path.replay-detail").trim.toLowerCase
    if (detail != "full" && detail != "summary")
      throw new IllegalArgumentException(s"""Invalid $path.replay-detail: '$detail'. Expected "full" or "summary".""")
    DroppedEventsSettings(config.getBoolean(s"$path.enabled"), config.getString(s"$path.table-name"), detail == "summary")
  }
}

/**
 * Writes one micro-batch of sequencer output: the emitted transactions, shaped into stream records
 * of the job's model (and passed through its transform, if any), to the stream table; then drop
 * records, tagged with the job name, to the dropped-events table.
 *
 * Both writes are idempotent per batch: Delta's `txnAppId`/`txnVersion` options make a re-run of
 * an already committed batch a no-op. `txnAppId` is derived from the query id stored in the
 * checkpoint, so it is stable across restarts and different for every checkpoint (batch ids
 * restart at 0 on a new checkpoint, which a fixed id would wrongly skip).
 *
 * The stream table must already exist with the model's schema ([[CdcOutputSink.requireTableSchema]]
 * runs before the query starts); the sink never creates or alters it.
 */
final class CdcOutputSink(model: JobModel, dropped: DroppedEventsSettings, txnAppIdPrefix: () => String)
    extends Serializable with Logging {

  private val shaper = new StreamShaper(model)
  private val streamSchema: StructType = Schemas.streamSchema(model)
  @transient private lazy val transform: Option[StreamTransform] = model.transform.map(CdcOutputSink.loadTransform)

  def writeBatch(batch: Dataset[SequencerOutput], batchId: Long): Unit = {
    batch.persist()
    try {
      val prefix = txnAppIdPrefix()

      val transactions = TransactionOrderingProcessor.transactionsOf(batch)
      if (!transactions.isEmpty) {
        shaper.report(transactions, batchId)
        val records = applyTransform(shaper.shape(transactions), batchId)
        records.write.format("delta").mode("append")
          .option("txnAppId", s"$prefix-${CdcOutputSink.tableSuffix(model.streamTable)}")
          .option("txnVersion", batchId)
          .saveAsTable(model.streamTable)
      }

      if (dropped.enabled) {
        val drops = TransactionOrderingProcessor.droppedOf(batch)
        if (!drops.isEmpty) {
          drops.toDF()
            .withColumn("jobName", lit(model.name))
            .withColumn("batchId", lit(batchId))
            .withColumn("droppedTs", current_timestamp())
            .withColumn("droppedDate", to_date(current_timestamp()))
            .write.format("delta").mode("append")
            .option("txnAppId", s"$prefix-${CdcOutputSink.tableSuffix(dropped.tableName)}")
            .option("txnVersion", batchId)
            .saveAsTable(dropped.tableName)
        }
      }
    } finally batch.unpersist()
  }

  /** The job's transform applied to shaped records; fails the batch, before any write, if it changes the schema. */
  private def applyTransform(records: DataFrame, batchId: Long): DataFrame = transform match {
    case None => records
    case Some(t) =>
      val out = t(records, model)
      val diff = Schemas.differences(streamSchema, out.schema)
      if (diff.nonEmpty)
        throw new IllegalStateException(s"Batch $batchId: transform ${model.transform.get} returned records that " +
          s"don't match the stream table ${model.streamTable}: ${diff.mkString("; ")}. Nothing was written.")
      out
  }
}

object CdcOutputSink {

  /** The columns `dropped_events` must have for this version. */
  val DroppedEventsRequiredColumns: Seq[String] = Seq("jobName", "bufferedCounts")

  private def tableSuffix(table: String): String = table.split('.').last

  def loadTransform(className: String): StreamTransform =
    Class.forName(className).getConstructor().newInstance().asInstanceOf[StreamTransform]

  /**
   * Reads the streaming query id from `<checkpoint>/metadata`, which Spark writes when the query
   * is first started on a checkpoint and keeps for the checkpoint's lifetime.
   */
  def queryIdFromCheckpoint(spark: SparkSession, checkpointLocation: String): String = {
    val path = s"${checkpointLocation.stripSuffix("/")}/metadata"
    val text = spark.read.text(path).collect().map(_.getString(0)).mkString
    Option(new ObjectMapper().readTree(text).get("id")).map(_.asText()).filter(_.nonEmpty).getOrElse(
      throw new IllegalStateException(s"No query id in $path"))
  }

  /**
   * Fails before a query starts unless `table` exists with exactly `expected`'s names, types,
   * nesting and field order (nullability and comments are ignored). Jobs never create or alter
   * their tables: create them with the DDL written from the job config.
   */
  def requireTableSchema(spark: SparkSession, table: String, expected: StructType, job: String): Unit = {
    if (!Try(spark.catalog.tableExists(table)).getOrElse(false))
      throw new IllegalStateException(
        s"Table $table does not exist. Write its DDL with `scripts/cdc-model.sh ddl` for job $job and run it with SeedJob.")
    val diff = Schemas.differences(expected, spark.table(table).schema)
    if (diff.nonEmpty)
      throw new IllegalStateException(
        s"Table $table does not match the model of job $job: ${diff.mkString("; ")}. " +
          "Recreate it from the DDL written from the job config, or fix the job config.")
  }

  /** Fails at startup if the dropped-events table is enabled but missing, or lacks this version's columns. */
  def checkDroppedEventsTable(spark: SparkSession, settings: DroppedEventsSettings): Unit = {
    if (!settings.enabled) return
    if (!Try(spark.catalog.tableExists(settings.tableName)).getOrElse(false))
      throw new IllegalStateException(
        s"Dropped-events table ${settings.tableName} does not exist. Create it with " +
          "db/create_dropped_events_table.deltalake.sql (SeedJob), or set job.output.dropped-events.enabled = false.")
    val columns = spark.table(settings.tableName).schema.fieldNames.toSet
    val missing = DroppedEventsRequiredColumns.filterNot(columns.contains)
    if (missing.nonEmpty)
      throw new IllegalStateException(
        s"Dropped-events table ${settings.tableName} lacks column(s) ${missing.mkString(", ")}. " +
          "Add them with db/alter_dropped_events_add_job.sql.")
  }
}
