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
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, lit, to_date}

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
 * Writes one micro-batch of sequencer output: order records to the order table, then drop
 * records to the dropped-events table.
 *
 * Both writes are idempotent per batch: Delta's `txnAppId`/`txnVersion` options make a re-run of
 * an already committed batch a no-op. `txnAppId` is derived from the query id stored in the
 * checkpoint, so it is stable across restarts and different for every checkpoint (batch ids
 * restart at 0 on a new checkpoint, which a fixed id would wrongly skip).
 */
final class CdcOutputSink(ordersTable: String, dropped: DroppedEventsSettings, txnAppIdPrefix: () => String)
    extends Serializable with Logging {

  def writeBatch(batch: Dataset[SequencerOutput], batchId: Long): Unit = {
    batch.persist()
    try {
      val prefix = txnAppIdPrefix()

      val orders = TransactionOrderingProcessor.ordersOf(batch)
      if (!orders.isEmpty)
        orders.write.format("delta").mode("append")
          .option("txnAppId", s"$prefix-${CdcOutputSink.tableSuffix(ordersTable)}")
          .option("txnVersion", batchId)
          .saveAsTable(ordersTable)

      if (dropped.enabled) {
        val drops = TransactionOrderingProcessor.droppedOf(batch)
        if (!drops.isEmpty) {
          drops.toDF()
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
}

object CdcOutputSink {

  private def tableSuffix(table: String): String = table.split('.').last

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

  /** Fails at startup if the dropped-events table is enabled but missing. */
  def checkDroppedEventsTable(spark: SparkSession, settings: DroppedEventsSettings): Unit =
    if (settings.enabled && !Try(spark.catalog.tableExists(settings.tableName)).getOrElse(false))
      throw new IllegalStateException(
        s"Dropped-events table ${settings.tableName} does not exist. Create it with " +
          "db/create_dropped_events_table.deltalake.sql (SeedJob), or set job.output.dropped-events.enabled = false.")
}
