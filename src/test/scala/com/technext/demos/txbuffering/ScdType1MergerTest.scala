package com.technext.demos.txbuffering

import java.nio.file.Files
import java.{util => ju}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, Test, TestInstance}

import com.technext.demos.txbuffering.model.order.{Order, OrderLineItem, OrderStream}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScdType1MergerTest {

  private var spark: SparkSession = _
  private var table: String = _
  private var tableSeq = 0

  @BeforeAll
  def startSpark(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("scd1-merge-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("scd1-warehouse").toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  @AfterAll
  def stopSpark(): Unit = if (spark != null) spark.stop()

  @BeforeEach
  def newTable(): Unit = {
    tableSeq += 1
    table = s"orders_current_t$tableSeq"
  }

  // ---- builders ----

  private def order(orderId: Int, version: Int, status: String = "NEW"): Order = {
    val o = new Order()
    o.orderId = orderId.toDouble
    o.version = version.toDouble
    o.orderStatus = status
    o
  }

  private def item(lineItemId: Int, orderId: Int, version: Int): OrderLineItem = {
    val li = new OrderLineItem()
    li.lineItemId = lineItemId.toDouble
    li.orderId = orderId.toDouble
    li.version = version.toDouble
    li
  }

  private def stream(orderId: Int, xid: String, csn: String, orders: Seq[Order], items: Seq[OrderLineItem]): OrderStream = {
    val os = new OrderStream()
    os.orderId = orderId.toDouble
    os.xid = xid
    os.csn = csn
    os.dwhProcessedTs = s"ts-$csn"
    os.orders = new ju.ArrayList[Order](orders.asJava)
    os.orderDetails = new ju.ArrayList()
    os.lineItems = new ju.ArrayList[OrderLineItem](items.asJava)
    os
  }

  private def merge(rows: OrderStream*): Unit = {
    val df = spark.createDataset(rows.asJava)(Encoders.bean(classOf[OrderStream])).toDF()
    ScdType1Merger.mergeScdType1(spark, df, table)
  }

  private def current(orderId: Int): Row =
    spark.table(table).where(s"orderId = $orderId").collect().head

  private def lineItemVersions(row: Row): Map[Double, Double] =
    row.getSeq[Row](row.fieldIndex("lineItems"))
      .map(li => li.getAs[Double]("lineItemId") -> li.getAs[Double]("version")).toMap

  // ---- tests ----

  @Test
  def childOnlyChangeForUnknownOrderIsKeptAsPlaceholderThenFilledByParent(): Unit = {
    merge(stream(1, "x1", "100", Seq(order(1, 1)), Seq(item(1, 1, 1)))) // creates the table

    merge(stream(7, "x2", "200", Seq.empty, Seq(item(70, 7, 2))))
    val placeholder = current(7)
    assertThat(placeholder.isNullAt(placeholder.fieldIndex("version"))).isTrue
    assertThat(lineItemVersions(placeholder).asJava).containsEntry(70.0, 2.0)

    merge(stream(7, "x3", "300", Seq(order(7, 1, "SHIPPED")), Seq(item(70, 7, 1))))
    val filled = current(7)
    assertThat(filled.getAs[Double]("version")).isEqualTo(1.0)
    assertThat(filled.getAs[String]("orderStatus")).isEqualTo("SHIPPED")
    assertThat(lineItemVersions(filled).asJava).containsEntry(70.0, 2.0)
  }

  @Test
  def olderCsnDoesNotOverwriteTransactionContext(): Unit = {
    merge(stream(1, "x9", "900", Seq(order(1, 1)), Seq(item(1, 1, 1))))

    merge(stream(1, "x8", "800", Seq.empty, Seq(item(1, 1, 2))))

    val row = current(1)
    assertThat(row.getAs[String]("csn")).isEqualTo("900")
    assertThat(row.getAs[String]("xid")).isEqualTo("x9")
    assertThat(row.getAs[String]("dwhProcessedTs")).isEqualTo("ts-900")
    assertThat(lineItemVersions(row).asJava).containsEntry(1.0, 2.0)
  }

  @Test
  def csnIsComparedNumerically(): Unit = {
    merge(stream(1, "x99", "99", Seq(order(1, 1)), Seq(item(1, 1, 1))))

    merge(stream(1, "x100", "100", Seq.empty, Seq(item(1, 1, 2))))

    val row = current(1)
    assertThat(row.getAs[String]("csn")).isEqualTo("100")
    assertThat(row.getAs[String]("xid")).isEqualTo("x100")
  }

  @Test
  def sameOrderVersionInOneBatchResolvedByHighestCsn(): Unit = {
    merge(
      stream(7, "a", "100", Seq(order(7, 4, "PACKED")), Seq.empty),
      stream(7, "b", "120", Seq(order(7, 4, "SHIPPED")), Seq.empty))

    val row = current(7)
    assertThat(row.getAs[String]("orderStatus")).isEqualTo("SHIPPED")
    assertThat(row.getAs[String]("csn")).isEqualTo("120")
  }
}
