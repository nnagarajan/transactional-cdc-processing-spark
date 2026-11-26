package com.technext.demos.txbuffering.e2e

import java.time.Instant

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, size}

import com.technext.demos.txbuffering.SparkSessionCreator

/** One entry of an `order_stream` array: field → value, and the before image if any. */
final case class Entity(after: Map[String, Any], before: Option[Map[String, Any]])

/** One `order_stream` row with the version of the Delta commit that wrote it. */
final case class StreamRow(
    orderId: Long,
    xid: String,
    csn: BigDecimal,
    dwhProcessedTs: Instant,
    commitVersion: Long,
    orders: Vector[Entity],
    details: Vector[Entity],
    items: Vector[Entity])

/** The columns of an `order_stream` row the performance and latency runs need. */
final case class StreamKey(
    orderId: Long,
    xid: String,
    csn: BigDecimal,
    dwhProcessedTs: Instant,
    commitVersion: Long,
    nOrders: Int,
    nDetails: Int,
    nItems: Int)

/** One `orders_current` row: flattened order attributes, the detail struct and the line items. */
final case class CurrentRow(
    orderId: Long,
    xid: String,
    csn: BigDecimal,
    dwhProcessedTs: Instant,
    commitVersion: Long,
    order: Map[String, Any],
    detail: Option[Entity],
    items: Vector[Entity])

/**
 * Reads the pipeline's Unity Catalog tables, limited to a run's orders (design.md D5, D6). Each
 * row carries `_metadata.row_commit_version`; [[commitTimes]] maps versions to their in-commit
 * timestamps from `DESCRIBE HISTORY`.
 */
final class UcReader(val spark: SparkSession, val tables: TableNames) {

  import spark.implicits._

  private val MetaColumns = Set("xid", "csn", "dwhProcessedTs", "orderBefore", "orderDetails", "lineItems", "commitVersion")

  def idFrame(ids: Iterable[Long]): DataFrame = ids.toSeq.distinct.map(_.toDouble).toDF("orderId")

  private def withVersion(table: String, ids: Iterable[Long]): DataFrame =
    spark.table(table)
      .select(col("*"), col("_metadata.row_commit_version").as("commitVersion"))
      .join(broadcast(idFrame(ids)), "orderId")

  def streamRows(ids: Iterable[Long]): Vector[StreamRow] =
    withVersion(tables.orderStreamFqn, ids).collect().toVector.map { r =>
      StreamRow(
        r.getAs[Double]("orderId").toLong,
        r.getAs[String]("xid"),
        BigDecimal(r.getAs[String]("csn")),
        Instant.parse(r.getAs[String]("dwhProcessedTs")),
        r.getAs[Long]("commitVersion"),
        entities(r, "orders"),
        entities(r, "orderDetails"),
        entities(r, "lineItems"))
    }

  def streamKeys(ids: Iterable[Long]): Vector[StreamKey] =
    withVersion(tables.orderStreamFqn, ids)
      .select(col("orderId"), col("xid"), col("csn"), col("dwhProcessedTs"), col("commitVersion"),
        size(col("orders")), size(col("orderDetails")), size(col("lineItems")))
      .collect().toVector.map { r =>
        StreamKey(r.getDouble(0).toLong, r.getString(1), BigDecimal(r.getString(2)), Instant.parse(r.getString(3)),
          r.getLong(4), r.getInt(5), r.getInt(6), r.getInt(7))
      }

  def currentRows(ids: Iterable[Long]): Vector[CurrentRow] =
    withVersion(tables.ordersCurrentFqn, ids).collect().toVector.map { r =>
      val fields = r.schema.fieldNames.toVector
      CurrentRow(
        r.getAs[Double]("orderId").toLong,
        r.getAs[String]("xid"),
        BigDecimal(r.getAs[String]("csn")),
        Instant.parse(r.getAs[String]("dwhProcessedTs")),
        r.getAs[Long]("commitVersion"),
        fields.filterNot(MetaColumns).map(f => f -> r.getAs[Any](f)).toMap,
        Option(r.getAs[Row]("orderDetails")).map(entity),
        entities(r, "lineItems"))
    }

  /** `(orderId, csn, commitVersion)` of `orders_current`, for catch-up polling. */
  def currentKeys(ids: Iterable[Long]): Vector[(Long, BigDecimal, Long)] =
    withVersion(tables.ordersCurrentFqn, ids).select("orderId", "csn", "commitVersion").collect().toVector
      .map(r => (r.getDouble(0).toLong, BigDecimal(r.getString(1)), r.getLong(2)))

  /** `dropped_events` rows whose CSN is one of `csns`: reason, item kind, xid, csn, table. */
  def droppedFor(csns: Iterable[BigDecimal]): Vector[Map[String, Any]] = {
    val wanted = csns.map(_.toString).toSeq.distinct.toDF("csn")
    spark.table(tables.droppedEventsFqn).join(broadcast(wanted), "csn")
      .select("csn", "xid", "reason", "itemKind", "tableName", "batchId").collect().toVector
      .map(r => r.schema.fieldNames.zip(r.toSeq).toMap)
  }

  /** In-commit timestamp of every version from `fromVersion` on. */
  def commitTimes(table: String, fromVersion: Long = 0L): Map[Long, Instant] =
    spark.sql(s"DESCRIBE HISTORY $table").where(col("version") >= fromVersion).select("version", "timestamp")
      .collect().map(r => r.getLong(0) -> r.getTimestamp(1).toInstant).toMap

  def latestVersion(table: String): Long =
    spark.sql(s"DESCRIBE HISTORY $table LIMIT 1").select("version").head().getLong(0)

  def rowTrackingEnabled(table: String): Boolean =
    spark.sql(s"SHOW TBLPROPERTIES $table").collect()
      .exists(r => r.getString(0) == "delta.enableRowTracking" && r.getString(1).equalsIgnoreCase("true"))

  private def entities(r: Row, field: String): Vector[Entity] =
    Option(r.getAs[scala.collection.Seq[Row]](field)).map(_.toVector.map(entity)).getOrElse(Vector.empty)

  private def entity(s: Row): Entity = {
    val names = s.schema.fieldNames
    val after = names.filterNot(_ == "before").map(n => n -> s.getAs[Any](n)).toMap
    val before = if (names.contains("before")) Option(s.getAs[Row]("before")).map(b => b.schema.fieldNames.map(n => n -> b.getAs[Any](n)).toMap) else None
    Entity(after, before)
  }
}

object UcReader {

  /** A local Spark session on the Unity Catalog of `reference.conf` (no UI), via SparkSessionCreator. */
  def connect(config: E2eConfig): UcReader = {
    System.setProperty("spark.ui.enabled", "false")
    val spark = SparkSessionCreator.getSpark(config.raw)
    spark.sparkContext.setLogLevel("WARN")
    new UcReader(spark, config.tables)
  }
}
