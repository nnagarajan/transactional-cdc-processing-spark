package com.technext.demos.txbuffering.e2e

import java.nio.file.Files
import java.time.{Instant, LocalDateTime}

import org.apache.spark.sql.SparkSession
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

/** UcReader on local Delta tables with row tracking, standing in for the UC managed tables. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UcReaderTest {

  private var spark: SparkSession = _
  private var reader: UcReader = _
  private val tables = TableNames("spark_catalog", "default", "os_t", "oc_t", "de_t")

  private val Props = "TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableInCommitTimestamps' = 'true')"
  private val Item = "STRUCT<lineItemId: DOUBLE, orderId: DOUBLE, version: DOUBLE, before: STRUCT<lineItemId: DOUBLE, version: DOUBLE>>"

  @BeforeAll
  def start(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("uc-reader-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("uc-reader-warehouse").toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    reader = new UcReader(spark, tables)

    spark.sql(s"""CREATE TABLE os_t (xid STRING, csn STRING, dwhProcessedTs STRING, orderId DOUBLE,
                 |  orders ARRAY<STRUCT<orderId: DOUBLE, version: DOUBLE, orderTs: STRING, before: STRUCT<orderId: DOUBLE, version: DOUBLE>>>,
                 |  orderDetails ARRAY<STRUCT<orderId: DOUBLE, version: DOUBLE, before: STRUCT<orderId: DOUBLE, version: DOUBLE>>>,
                 |  lineItems ARRAY<$Item>) USING DELTA $Props""".stripMargin)
    // version 1: order 1 inserted; version 2: order 1 updated (line item only) and order 2 inserted
    spark.sql("""INSERT INTO os_t VALUES ('9.1.2.3', '100', '2026-11-25T10:00:01.123456789Z', 1.0,
                |  array(named_struct('orderId', 1.0D, 'version', 1.0D, 'orderTs', '2026-02-17 10:00:05.000000000', 'before', CAST(NULL AS STRUCT<orderId: DOUBLE, version: DOUBLE>))),
                |  array(named_struct('orderId', 1.0D, 'version', 1.0D, 'before', CAST(NULL AS STRUCT<orderId: DOUBLE, version: DOUBLE>))),
                |  array(named_struct('lineItemId', 10.0D, 'orderId', 1.0D, 'version', 1.0D, 'before', CAST(NULL AS STRUCT<lineItemId: DOUBLE, version: DOUBLE>))))""".stripMargin)
    spark.sql("""INSERT INTO os_t VALUES
                |  ('9.1.2.4', '200', '2026-11-25T10:00:02Z', 1.0, array(), array(),
                |   array(named_struct('lineItemId', 10.0D, 'orderId', 1.0D, 'version', 2.0D, 'before', named_struct('lineItemId', 10.0D, 'version', 1.0D)))),
                |  ('9.1.2.5', '300', '2026-11-25T10:00:03Z', 2.0, array(), array(), array())""".stripMargin)

    spark.sql(s"""CREATE TABLE oc_t (xid STRING, csn STRING, dwhProcessedTs STRING, orderId DOUBLE, version DOUBLE, orderStatus STRING,
                 |  orderBefore STRUCT<orderId: DOUBLE, version: DOUBLE>,
                 |  orderDetails STRUCT<orderId: DOUBLE, version: DOUBLE, before: STRUCT<orderId: DOUBLE, version: DOUBLE>>,
                 |  lineItems ARRAY<$Item>) USING DELTA $Props""".stripMargin)
    spark.sql("INSERT INTO oc_t VALUES ('9.1.2.3', '100', '2026-11-25T10:00:01Z', 1.0, 1.0, 'PENDING', NULL, NULL, array())")
    spark.sql("INSERT INTO oc_t VALUES ('9.1.2.5', '300', '2026-11-25T10:00:03Z', 2.0, 1.0, 'PENDING', NULL, NULL, array())")
    spark.sql("""MERGE INTO oc_t t USING (SELECT 1.0D AS orderId, '400' AS csn) s ON t.orderId = s.orderId
                |WHEN MATCHED THEN UPDATE SET t.csn = s.csn, t.version = 2.0, t.orderStatus = 'CONFIRMED'""".stripMargin)
  }

  @AfterAll
  def stop(): Unit = if (spark != null) spark.stop()

  @Test
  def streamRowsCarryTheVersionOfTheAppendThatWroteThem(): Unit = {
    val rows = reader.streamRows(Seq(1L, 2L)).sortBy(_.csn)

    assertThat(rows.map(r => (r.orderId, r.csn.toString, r.commitVersion))).isEqualTo(Vector((1L, "100", 1L), (1L, "200", 2L), (2L, "300", 2L)))
    val first = rows.head
    assertThat(first.dwhProcessedTs).isEqualTo(Instant.parse("2026-11-25T10:00:01.123456789Z"))
    assertThat(first.orders.head.after("version")).isEqualTo(1.0)
    assertThat(first.orders.head.before.isEmpty).isTrue
    assertThat(rows(1).items.head.before.get("version")).isEqualTo(1.0)
    assertThat(reader.streamRows(Seq(2L)).size).isEqualTo(1)

    val keys = reader.streamKeys(Seq(1L)).sortBy(_.csn)
    assertThat(keys.map(k => (k.nOrders, k.nDetails, k.nItems))).isEqualTo(Vector((1, 1, 1), (0, 0, 1)))
  }

  @Test
  def mergedRowReportsTheMergeVersion(): Unit = {
    val current = reader.currentRows(Seq(1L, 2L)).sortBy(_.orderId)

    assertThat(current.map(r => (r.orderId, r.csn.toString, r.commitVersion))).isEqualTo(Vector((1L, "400", 3L), (2L, "300", 2L)))
    assertThat(current.head.order("orderStatus")).isEqualTo("CONFIRMED")
    assertThat(current.head.order.contains("csn")).isFalse
    assertThat(current.head.detail.isEmpty).isTrue
    assertThat(reader.currentKeys(Seq(1L))).isEqualTo(Vector((1L, BigDecimal(400), 3L)))
  }

  @Test
  def commitTimesComeFromHistory(): Unit = {
    val times = reader.commitTimes(tables.ordersCurrentFqn)

    assertThat(times.keySet).isEqualTo(Set(0L, 1L, 2L, 3L))
    assertThat(times(3L).isAfter(times(2L)) || times(3L) == times(2L)).isTrue
    assertThat(reader.latestVersion(tables.ordersCurrentFqn)).isEqualTo(3L)
    assertThat(reader.commitTimes(tables.ordersCurrentFqn, fromVersion = 2L).keySet).isEqualTo(Set(2L, 3L))
    assertThat(reader.rowTrackingEnabled(tables.orderStreamFqn)).isTrue
  }

  @Test
  def normalizerTreatsGoldenGateAndOracleValuesAsEqual(): Unit = {
    assertThat(Values.same("2026-02-17 10:00:05.000000000", java.sql.Timestamp.valueOf("2026-02-17 10:00:05"))).isTrue
    assertThat(Values.same("2026-02-17 10:00:05.123456000", LocalDateTime.parse("2026-02-17T10:00:05.123456"))).isTrue
    assertThat(Values.same("2026-02-17 00:00:00", java.sql.Timestamp.valueOf("2026-02-17 00:00:00"))).isTrue
    assertThat(Values.same(336.6666, new java.math.BigDecimal("336.6666"))).isTrue
    assertThat(Values.same(8605.1983, new java.math.BigDecimal("8605.19830000"))).isTrue
    assertThat(Values.same("USD", "USD")).isTrue
    assertThat(Values.same(25.55, new java.math.BigDecimal("25.56"))).isFalse
    assertThat(Values.same("2026-02-17 10:00:05.000000000", java.sql.Timestamp.valueOf("2026-02-17 10:00:06"))).isFalse
    assertThat(Values.camel("LINE_ITEM_ID")).isEqualTo("lineItemId")
    assertThat(Values.camel("ESTIMATED_DELIVERY_DATE")).isEqualTo("estimatedDeliveryDate")
  }
}
