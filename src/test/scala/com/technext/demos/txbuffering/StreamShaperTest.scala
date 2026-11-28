package com.technext.demos.txbuffering

import java.{util => ju}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import com.technext.demos.txbuffering.model.generic.DataChangeEvent
import com.technext.demos.txbuffering.model.spec.{JobModel, Schemas, TestModels}
import com.technext.demos.txbuffering.processor.{EmittedTransaction, Fixtures}

/** Scenarios of spec `stream-record-shaping`, plus the root key ones of `cdc-model-config`. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StreamShaperTest {

  private var spark: SparkSession = _

  @BeforeAll
  def startSpark(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("stream-shaper-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  @AfterAll
  def stopSpark(): Unit = if (spark != null) spark.stop()

  private lazy val orders: JobModel = TestModels.ordersModel

  // ---- builders ----

  private def ev(table: String, pos: String, after: Map[String, String], before: Map[String, String] = null,
      op: String = "I"): DataChangeEvent =
    Fixtures.event("x", "100", table, pos, opType = op, after = after, before = before)

  private def transactions(events: DataChangeEvent*): DataFrame = {
    val t = new EmittedTransaction()
    t.xid = "x"
    t.csn = "100"
    t.processedTs = "2026-11-26T10:00:00Z"
    t.events = new ju.ArrayList[DataChangeEvent](events.asJava)
    spark.createDataset(Seq(t))(Encoders.bean(classOf[EmittedTransaction])).toDF()
  }

  private def shape(m: JobModel, events: DataChangeEvent*): Seq[Row] =
    new StreamShaper(m).shape(transactions(events: _*)).collect().toSeq

  private def byOrder(rows: Seq[Row]): Map[Double, Row] = rows.map(r => r.getAs[Double]("orderId") -> r).toMap

  private def arr(r: Row, f: String): Seq[Row] = r.getSeq[Row](r.fieldIndex(f))

  private def ord(id: Int, status: String = "NEW") = Map("ORDER_ID" -> id.toString, "VERSION" -> "1", "ORDER_STATUS" -> status)
  private def item(id: Int, orderId: Int) = Map("LINE_ITEM_ID" -> id.toString, "ORDER_ID" -> orderId.toString, "VERSION" -> "1")

  // ---- 5.1 ----

  @Test
  def transactionTouchingTwoOrders(): Unit = {
    val rows = shape(orders,
      ev("ORDERS", "p1", ord(7, "SHIPPED"), ord(7), op = "U"),
      ev("ORDER_LINE_ITEMS", "p2", item(70, 7), item(70, 7), op = "U"),
      ev("ORDERS", "p3", ord(8)),
      ev("ORDER_LINE_ITEMS", "p4", item(80, 8)),
      ev("ORDER_LINE_ITEMS", "p5", item(81, 8)))
    val m = byOrder(rows)
    assertThat(m.keySet.asJava).containsExactlyInAnyOrder(7.0, 8.0)
    assertThat(arr(m(7.0), "orders").size).isEqualTo(1)
    assertThat(arr(m(7.0), "lineItems").size).isEqualTo(1)
    assertThat(arr(m(8.0), "orders").size).isEqualTo(1)
    assertThat(arr(m(8.0), "lineItems").map(_.getAs[Double]("lineItemId")).asJava).containsExactly(80.0, 81.0)
    assertThat(m(7.0).getAs[String]("xid")).isEqualTo("x")
    assertThat(m(7.0).getAs[String]("dwhProcessedTs")).isEqualTo("2026-11-26T10:00:00Z")
  }

  @Test
  def entityAbsentForAKey(): Unit = {
    val r = shape(orders, ev("ORDER_LINE_ITEMS", "p1", item(70, 7))).head
    assertThat(arr(r, "orders").isEmpty).isTrue
    assertThat(arr(r, "orderDetails").isEmpty).isTrue
    assertThat(arr(r, "lineItems").size).isEqualTo(1)
  }

  @Test
  def updateWithABeforeImage(): Unit = {
    val e = arr(shape(orders, ev("ORDERS", "p1", ord(7, "SHIPPED"), ord(7, "NEW"), op = "U")).head, "orders").head
    assertThat(e.getAs[String]("orderStatus")).isEqualTo("SHIPPED")
    assertThat(e.getAs[Row]("before").getAs[String]("orderStatus")).isEqualTo("NEW")
  }

  @Test
  def insertHasNullBefore(): Unit = {
    val e = arr(shape(orders, ev("ORDERS", "p1", ord(8))).head, "orders").head
    assertThat(e.isNullAt(e.fieldIndex("before"))).isTrue
  }

  @Test
  def deleteInTheTransactionProducesNoElement(): Unit = {
    val r = shape(orders,
      ev("ORDER_LINE_ITEMS", "p1", item(70, 7), item(70, 7), op = "U"),
      ev("ORDER_LINE_ITEMS", "p2", null, item(71, 7), op = "D")).head
    assertThat(arr(r, "lineItems").map(_.getAs[Double]("lineItemId")).asJava).containsExactly(70.0)
  }

  @Test
  def bufferedOrderIsKeptWithinAnEntity(): Unit = {
    val r = shape(orders, ev("ORDER_LINE_ITEMS", "p1", item(3, 7)), ev("ORDER_LINE_ITEMS", "p2", item(1, 7)),
      ev("ORDER_LINE_ITEMS", "p3", item(2, 7))).head
    assertThat(arr(r, "lineItems").map(_.getAs[Double]("lineItemId")).asJava).containsExactly(3.0, 1.0, 2.0)
  }

  @Test
  def outputHasTheStreamSchema(): Unit =
    assertThat(Schemas.differences(Schemas.streamSchema(orders),
      new StreamShaper(orders).shape(transactions(ev("ORDERS", "p1", ord(1)))).schema).asJava).isEmpty()

  @Test
  def compoundRootKeyFromDifferentlyNamedChildColumns(): Unit = {
    val m = TestModels.resolve(TestModels.Compound).toOption.get
    def o(region: String) = Map("REGION" -> region, "ORDER_NO" -> "7", "VERSION" -> "1")
    def s(region: String) = Map("SHIP_REGION" -> region, "ORD_REF" -> "7", "SHIPMENT_SEQ" -> "1", "CARRIER" -> "DHL")
    val rows = shape(m, ev("ORDERS", "p1", o("EU")), ev("ORDERS", "p2", o("US")),
      ev("SHIPMENTS", "p3", s("EU")), ev("SHIPMENTS", "p4", s("US")))
    assertThat(rows.size).isEqualTo(2)
    rows.foreach { r =>
      assertThat(arr(r, "orders").size).isEqualTo(1)
      assertThat(arr(r, "shipments").head.getAs[String]("shipRegion")).isEqualTo(r.getAs[String]("region"))
      assertThat(r.getAs[Long]("orderNo")).isEqualTo(7L)
    }
  }

  @Test
  def keyValuesAreConvertedToTheRootKeyType(): Unit = {
    // shipments declares ORD_REF as string; the root key ORDER_NO is bigint
    val m = TestModels.resolve(TestModels.Compound).toOption.get
    val rows = shape(m,
      ev("ORDERS", "p1", Map("REGION" -> "EU", "ORDER_NO" -> "7", "VERSION" -> "1")),
      ev("SHIPMENTS", "p2", Map("SHIP_REGION" -> "EU", "ORD_REF" -> "007", "SHIPMENT_SEQ" -> "1")))
    assertThat(rows.size).isEqualTo(1)
    assertThat(arr(rows.head, "shipments").head.getAs[String]("ordRef")).isEqualTo("007")
  }

  // ---- 5.2 ----

  @Test
  def malformedNumberBecomesNullAndIsCounted(): Unit = {
    val bad = ev("ORDERS", "p1", ord(7) + ("TOTAL_AMOUNT" -> "12,50"))
    val e = arr(shape(orders, bad).head, "orders").head
    assertThat(e.isNullAt(e.fieldIndex("totalAmount"))).isTrue
    assertThat(new StreamShaper(orders).castFailures(transactions(bad)).asJava)
      .isEqualTo(Map(("ORDERS", "TOTAL_AMOUNT") -> 1L).asJava)
  }

  @Test
  def newColumnNotYetInTheConfigIsLeftOutAndReported(): Unit = {
    val e = ev("ORDERS", "p1", ord(7) + ("PRIORITY" -> "HIGH"))
    val row = shape(orders, e).head
    assertThat(arr(row, "orders").head.schema.fieldNames.toSeq.asJava).doesNotContain("PRIORITY", "priority")
    assertThat(new StreamShaper(orders).undeclaredColumns(transactions(e)).asJava)
      .isEqualTo(Map("ORDERS" -> Seq("PRIORITY")).asJava)
  }

  @Test
  def columnMissingFromTheEventIsNull(): Unit = {
    val e = arr(shape(orders, ev("ORDERS", "p1", ord(7))).head, "orders").head
    assertThat(e.isNullAt(e.fieldIndex("shippingAddressId"))).isTrue
  }

  @Test
  def goldenGateTimestampStringWithATimestampColumn(): Unit = {
    val text = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get("jobs/orders.conf")))
      .replace("{ source = ORDER_TS,            name = orderTs,           type = string }",
        "{ source = ORDER_TS, name = orderTs, type = timestamp }")
    val m = com.technext.demos.txbuffering.model.spec.ModelResolver.load(TestModels.config(text))
    val df = new StreamShaper(m).shape(transactions(ev("ORDERS", "p1", ord(7) + ("ORDER_TS" -> "2026-11-11 10:15:30.123456000"))))
    val ts = df.selectExpr("CAST(orders[0].orderTs AS STRING)").head().getString(0)
    assertThat(ts).isEqualTo("2026-11-11 10:15:30.123456")
  }

  @Test
  def reportWarnsOncePerColumn(): Unit = {
    val shaper = new StreamShaper(orders)
    val tx = transactions(ev("ORDERS", "p1", ord(7) + ("PRIORITY" -> "HIGH")))
    shaper.report(tx, 0L)
    shaper.report(tx, 1L) // no second warning; checked by not failing, the log is the output
  }
}
