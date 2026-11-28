/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import scala.collection.mutable

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DateType, StringType, TimestampType}

import com.technext.demos.txbuffering.model.spec.{EntityModel, JobModel, ModelColumn, Schemas}

/**
 * Shapes emitted transactions (xid, csn, processedTs, events) into stage 1 stream records of a
 * job's model, as one DataFrame projection (design D4):
 *
 *  1. explode the events with their index, keeping those with an after image (deletes count
 *     toward completion but produce no element)
 *  2. build each entity's element from the after image, each column converted to its declared
 *     type, plus a `before` struct from the before image (null when there is none)
 *  3. compute the root key, one part per root key column, from the root's key columns or the
 *     child's mapped columns, each converted to the root key column's type
 *  4. group by transaction and root key, one array per entity in buffered order (empty when the
 *     entity has no events for the key)
 *
 * A value that fails to convert becomes null; [[report]] logs how many did per table and column,
 * and warns once per run about source columns the model doesn't declare.
 */
final class StreamShaper(model: JobModel) extends Serializable with Logging {

  import StreamShaper._

  private val streamSchema = Schemas.streamSchema(model)
  private val keyColumns: Seq[ModelColumn] = model.rootKeyColumns

  /** Undeclared (table, column) pairs already warned about in this run. */
  @transient private lazy val warned = mutable.Set[(String, String)]()

  /** Stream records for the transactions, with exactly the stream table's columns. */
  def shape(transactions: DataFrame): DataFrame = {
    val exploded = explodeEvents(transactions)
    val withKey = exploded.select(
      Seq(col("xid"), col("csn"), col("processedTs")) ++
        keyColumns.zipWithIndex.map { case (k, i) => keyPart(i, k).as(k.name) } ++
        model.entities.map(e => when(col(Table) === lit(e.table), struct(col(Idx), element(e).as("elem"))).as(e.name)): _*)

    val arrays = model.entities.map(e =>
      transform(array_sort(collect_list(col(e.name))), x => x.getField("elem")).as(e.name))
    withKey
      .groupBy(Seq(col("xid"), col("csn"), col("processedTs")) ++ keyColumns.map(k => col(k.name)): _*)
      .agg(arrays.head, arrays.tail: _*)
      .withColumnRenamed("processedTs", "dwhProcessedTs")
      .select(streamSchema.fieldNames.map(col).toIndexedSeq: _*)
  }

  /**
   * Values that were present but failed to convert, per (table, column), over after and before
   * images. Columns of type string never fail.
   */
  def castFailures(transactions: DataFrame): Map[(String, String), Long] = {
    val exploded = explodeAll(transactions)
    val checks = for {
      e <- model.entities
      c <- e.columns if c.source.isDefined && c.dataType != StringType
      image <- Seq(After, BeforeImage)
    } yield {
      val raw = col(image).getItem(c.source.get)
      (e.table, c.source.get) ->
        when(col(Table) === lit(e.table) && raw.isNotNull && convert(raw, c.dataType).isNull, 1L).otherwise(0L)
    }
    if (checks.isEmpty) return Map.empty
    val grouped = checks.groupBy(_._1).toSeq.sortBy(_._1)
    val sums = grouped.map { case (_, cs) => cs.map(_._2).reduce(_ + _) }.map(sum)
    val row = exploded.agg(sums.head, sums.tail: _*).head()
    grouped.map(_._1).zipWithIndex
      .map { case (k, i) => k -> (if (row.isNullAt(i)) 0L else row.getLong(i)) }
      .filter(_._2 > 0).toMap
  }

  /** Source columns present in after images that the entity doesn't declare, per table. */
  def undeclaredColumns(transactions: DataFrame): Map[String, Seq[String]] = {
    val seen = explodeAll(transactions)
      .where(col(After).isNotNull)
      .select(col(Table), explode(map_keys(col(After))).as("column"))
      .distinct().collect()
      .map(r => r.getString(0) -> r.getString(1))
    seen.groupBy(_._1).flatMap { case (table, cols) =>
      val declared = model.entityForTable(table).map(_.sourceColumns.toSet).getOrElse(Set.empty)
      val extra = cols.map(_._2).filterNot(declared.contains).sorted.toSeq
      if (extra.isEmpty) None else Some(table -> extra)
    }
  }

  /** Logs the batch's cast failures, and warns once per run about each undeclared column. */
  def report(transactions: DataFrame, batchId: Long): Unit = {
    val failures = castFailures(transactions)
    if (failures.nonEmpty)
      logger.warn(s"Batch $batchId: values that could not be converted to their column type were written as null: " +
        failures.toSeq.sortBy(_._1).map { case ((t, c), n) => s"$t.$c: $n" }.mkString(", "))
    undeclaredColumns(transactions).toSeq.sortBy(_._1).foreach { case (table, cols) =>
      val fresh = cols.filterNot(c => warned.contains(table -> c))
      if (fresh.nonEmpty) {
        fresh.foreach(c => warned += (table -> c))
        logger.warn(s"Job ${model.name}: source columns not declared in the job config are left out: " +
          fresh.map(c => s"$table.$c").mkString(", "))
      }
    }
  }

  // ---- expressions ----

  private def explodeAll(transactions: DataFrame): DataFrame =
    transactions
      .select(posexplode(col("events")).as(Seq(Idx, "ev")))
      .select(col(Idx), tableName(col("ev.table")).as(Table), col("ev.after").as(After), col("ev.before").as(BeforeImage))

  private def explodeEvents(transactions: DataFrame): DataFrame =
    transactions
      .select(col("xid"), col("csn"), col("processedTs"), posexplode(col("events")).as(Seq(Idx, "ev")))
      .where(col("ev.after").isNotNull)
      .select(col("xid"), col("csn"), col("processedTs"), col(Idx), tableName(col("ev.table")).as(Table),
        col("ev.after").as(After), col("ev.before").as(BeforeImage))

  private def columnsOf(e: EntityModel, image: Column): Seq[Column] =
    e.columns.map(c => c.source match {
      case Some(src) => convert(image.getItem(src), c.dataType).as(c.name)
      case None => lit(null).cast(c.dataType).as(c.name)
    })

  private def element(e: EntityModel): Column = {
    val cols = columnsOf(e, col(After))
    if (!e.before) struct(cols: _*)
    else struct(cols :+ when(col(BeforeImage).isNotNull, struct(columnsOf(e, col(BeforeImage)): _*)).as(Schemas.Before): _*)
  }

  /** Part `i` of the root key: read from the column each entity maps it to, as the root key column's type. */
  private def keyPart(i: Int, keyColumn: ModelColumn): Column = {
    val branches = model.entities.map(e => (e.table, e.rootKey(i)._2))
    branches.tail.foldLeft(when(col(Table) === lit(branches.head._1), convert(col(After).getItem(branches.head._2), keyColumn.dataType))) {
      case (acc, (table, src)) => acc.when(col(Table) === lit(table), convert(col(After).getItem(src), keyColumn.dataType))
    }
  }

  /** A source string converted to a column type; null when it doesn't convert. */
  private def convert(raw: Column, t: DataType): Column = t match {
    case StringType => raw
    case TimestampType => try_to_timestamp(raw, lit(model.timestampFormat))
    case DateType => to_date(try_to_timestamp(raw, lit(model.timestampFormat)))
    case other => raw.try_cast(other)
  }
}

object StreamShaper {
  private val Idx = "_idx"
  private val Table = "_table"
  private val After = "_after"
  private val BeforeImage = "_before"

  /** The table name without schema, as [[com.technext.demos.txbuffering.model.generic.DataChangeEvent.tableName]]. */
  private def tableName(table: Column): Column = regexp_replace(table, "^[^.]*\\.", "")
}
