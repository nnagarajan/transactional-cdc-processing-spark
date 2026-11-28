package com.technext.demos.txbuffering

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, Test, TestInstance}

import com.technext.demos.txbuffering.model.spec.{Ddl, JobModel, Schemas, TestModels}

/** The generated stage 2 merge ([[CurrentStateMerger]]) for the orders job, and for a compound, versionless child. */
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

  /** The model with this test's current table, which is created from the written DDL. */
  private def withTable(m: JobModel): JobModel = {
    val mm = m.copy(currentTable = table)
    TestModels.runDdl(spark, Ddl.currentDdl(mm), "default")
    mm
  }

  private lazy val orders: JobModel = TestModels.ordersModel

  // ---- builders (stream rows as JSON in the stream schema) ----

  private def order(orderId: Int, version: Int, status: String = "NEW"): String =
    s"""{"orderId":$orderId,"version":$version,"orderStatus":"$status"}"""

  private def item(lineItemId: Int, orderId: Int, version: Int): String =
    s"""{"lineItemId":$lineItemId,"orderId":$orderId,"version":$version}"""

  private def stream(orderId: Int, xid: String, csn: String, orders: Seq[String], items: Seq[String]): String =
    s"""{"xid":"$xid","csn":"$csn","dwhProcessedTs":"ts-$csn","orderId":$orderId,""" +
      s""""orders":[${orders.mkString(",")}],"orderDetails":[],"lineItems":[${items.mkString(",")}]}"""

  private def merge(m: JobModel, rows: String*): Unit = {
    val df = spark.read.schema(Schemas.streamSchema(m)).json(spark.createDataset(rows.asJava)(Encoders.STRING))
    new CurrentStateMerger(m).merge(spark, df)
  }

  private def current(where: String): Row = spark.table(table).where(where).collect().head

  private def current(orderId: Int): Row = current(s"orderId = $orderId")

  private def lineItemVersions(row: Row): Map[Double, Double] =
    row.getSeq[Row](row.fieldIndex("lineItems"))
      .map(li => li.getAs[Double]("lineItemId") -> li.getAs[Double]("version")).toMap

  // ---- orders job ----

  @Test
  def childOnlyChangeForUnknownOrderIsKeptAsPlaceholderThenFilledByParent(): Unit = {
    val m = withTable(orders)
    merge(m, stream(1, "x1", "100", Seq(order(1, 1)), Seq(item(1, 1, 1))))

    merge(m, stream(7, "x2", "200", Seq.empty, Seq(item(70, 7, 2))))
    val placeholder = current(7)
    assertThat(placeholder.isNullAt(placeholder.fieldIndex("version"))).isTrue
    assertThat(lineItemVersions(placeholder).asJava).containsEntry(70.0, 2.0)

    merge(m, stream(7, "x3", "300", Seq(order(7, 1, "SHIPPED")), Seq(item(70, 7, 1))))
    val filled = current(7)
    assertThat(filled.getAs[Double]("version")).isEqualTo(1.0)
    assertThat(filled.getAs[String]("orderStatus")).isEqualTo("SHIPPED")
    assertThat(lineItemVersions(filled).asJava).containsEntry(70.0, 2.0)
  }

  @Test
  def olderCsnDoesNotOverwriteTransactionContext(): Unit = {
    val m = withTable(orders)
    merge(m, stream(1, "x9", "900", Seq(order(1, 1)), Seq(item(1, 1, 1))))

    merge(m, stream(1, "x8", "800", Seq.empty, Seq(item(1, 1, 2))))

    val row = current(1)
    assertThat(row.getAs[String]("csn")).isEqualTo("900")
    assertThat(row.getAs[String]("xid")).isEqualTo("x9")
    assertThat(row.getAs[String]("dwhProcessedTs")).isEqualTo("ts-900")
    assertThat(lineItemVersions(row).asJava).containsEntry(1.0, 2.0)
  }

  @Test
  def csnIsComparedNumerically(): Unit = {
    val m = withTable(orders)
    merge(m, stream(1, "x99", "99", Seq(order(1, 1)), Seq(item(1, 1, 1))))

    merge(m, stream(1, "x100", "100", Seq.empty, Seq(item(1, 1, 2))))

    val row = current(1)
    assertThat(row.getAs[String]("csn")).isEqualTo("100")
    assertThat(row.getAs[String]("xid")).isEqualTo("x100")
  }

  @Test
  def sameOrderVersionInOneBatchResolvedByHighestCsn(): Unit = {
    val m = withTable(orders)
    merge(m,
      stream(7, "a", "100", Seq(order(7, 4, "PACKED")), Seq.empty),
      stream(7, "b", "120", Seq(order(7, 4, "SHIPPED")), Seq.empty))

    val row = current(7)
    assertThat(row.getAs[String]("orderStatus")).isEqualTo("SHIPPED")
    assertThat(row.getAs[String]("csn")).isEqualTo("120")
  }

  /** Found by CorrectnessOrderingIT: a whole workload in one batch left 40 orders at an older CSN. */
  @Test
  def childOnlyChangeAfterOrderUpdateInOneBatchMovesTransactionContext(): Unit = {
    val m = withTable(orders)
    merge(m, stream(1, "x0", "50", Seq(order(1, 1)), Seq(item(1, 1, 1))))

    merge(m,
      stream(1, "a", "100", Seq(order(1, 2, "PACKED")), Seq.empty),
      stream(1, "b", "120", Seq.empty, Seq(item(1, 1, 2))))

    val row = current(1)
    assertThat(row.getAs[String]("orderStatus")).isEqualTo("PACKED")
    assertThat(row.getAs[Double]("version")).isEqualTo(2.0)
    assertThat(row.getAs[String]("csn")).isEqualTo("120")
    assertThat(row.getAs[String]("xid")).isEqualTo("b")
    assertThat(row.getAs[String]("dwhProcessedTs")).isEqualTo("ts-120")
    assertThat(lineItemVersions(row).asJava).containsEntry(1.0, 2.0)
  }

  @Test
  def firstBatchTakesTransactionContextFromHighestCsn(): Unit = {
    val m = withTable(orders)
    merge(m,
      stream(3, "a", "100", Seq(order(3, 1)), Seq(item(30, 3, 1))),
      stream(3, "b", "130", Seq.empty, Seq(item(30, 3, 2))))

    val row = current(3)
    assertThat(row.getAs[Double]("version")).isEqualTo(1.0)
    assertThat(row.getAs[String]("csn")).isEqualTo("130")
    assertThat(row.getAs[String]("xid")).isEqualTo("b")
    assertThat(lineItemVersions(row).asJava).containsEntry(30.0, 2.0)
  }

  @Test
  def olderLineItemVersionDoesNotReplaceANewerOne(): Unit = {
    val m = withTable(orders)
    merge(m, stream(1, "a", "100", Seq(order(1, 1)), Seq(item(1, 1, 3))))
    merge(m, stream(1, "b", "200", Seq.empty, Seq(item(1, 1, 2))))
    assertThat(lineItemVersions(current(1)).asJava).containsEntry(1.0, 3.0)
  }

  // ---- compound root key, versionless one-to-many child (TestModels.Compound) ----

  private lazy val regional: JobModel = TestModels.resolve(TestModels.Compound).toOption.get

  private def regionalRow(region: String, orderNo: Int, xid: String, csn: String, orderVersion: Option[Int],
      shipments: Seq[(String, String, Int, String)]): String = {
    val o = orderVersion.map(v => s"""{"region":"$region","orderNo":$orderNo,"version":$v,"status":"S$v"}""").toSeq
    val s = shipments.map { case (r, ref, seq, carrier) =>
      s"""{"shipRegion":"$r","ordRef":"$ref","shipmentSeq":$seq,"carrier":"$carrier"}""" }
    s"""{"xid":"$xid","csn":"$csn","dwhProcessedTs":"ts-$csn","region":"$region","orderNo":$orderNo,""" +
      s""""orders":[${o.mkString(",")}],"shipments":[${s.mkString(",")}]}"""
  }

  private def shipments(row: Row): Map[(String, String, Int), (String, String)] =
    row.getSeq[Row](row.fieldIndex("shipments")).map(s =>
      (s.getAs[String]("shipRegion"), s.getAs[String]("ordRef"), s.getAs[Int]("shipmentSeq")) ->
        (s.getAs[String]("carrier"), s.getAs[String]("changeCsn"))).toMap

  @Test
  def compoundRootKeyKeepsOneRowPerKey(): Unit = {
    val m = withTable(regional)
    merge(m,
      regionalRow("EU", 7, "a", "100", Some(1), Seq(("EU", "7", 1, "DHL"))),
      regionalRow("US", 7, "b", "110", Some(1), Seq(("US", "7", 1, "UPS"))))
    merge(m, regionalRow("EU", 7, "c", "120", Some(2), Nil))

    assertThat(spark.table(table).count()).isEqualTo(2L)
    val eu = current("region = 'EU' AND orderNo = 7")
    val us = current("region = 'US' AND orderNo = 7")
    assertThat(eu.getAs[String]("status")).isEqualTo("S2")
    assertThat(eu.getAs[String]("csn")).isEqualTo("120")
    assertThat(us.getAs[String]("status")).isEqualTo("S1")
    assertThat(us.getAs[String]("csn")).isEqualTo("110")
  }

  @Test
  def childWithoutVersionKeepsTheChangeFromTheHigherCsn(): Unit = {
    val m = withTable(regional)
    merge(m, regionalRow("EU", 7, "a", "500", Some(1), Seq(("EU", "7", 1, "DHL"))))
    // A replayed older change to the same shipment
    merge(m, regionalRow("EU", 7, "b", "400", None, Seq(("EU", "7", 1, "UPS"))))

    assertThat(shipments(current("region = 'EU'")).asJava)
      .isEqualTo(Map(("EU", "7", 1) -> ("DHL", "500")).asJava)
  }

  @Test
  def childWithoutVersionTakesTheNewerChangeAndDedupsByItsWholeKey(): Unit = {
    val m = withTable(regional)
    merge(m, regionalRow("EU", 7, "a", "100", Some(1), Seq(("EU", "7", 1, "DHL"), ("EU", "7", 2, "DHL"))))
    // In one batch: shipment 1 changes twice (csn 200 then 300); shipment 2 is untouched
    merge(m,
      regionalRow("EU", 7, "b", "200", None, Seq(("EU", "7", 1, "UPS"))),
      regionalRow("EU", 7, "c", "300", None, Seq(("EU", "7", 1, "FEDEX"))))

    assertThat(shipments(current("region = 'EU'")).asJava).isEqualTo(Map(
      ("EU", "7", 1) -> ("FEDEX", "300"),
      ("EU", "7", 2) -> ("DHL", "100")).asJava)
  }

  @Test
  def laterChangeInTheSameTransactionWinsForAVersionlessChild(): Unit = {
    val m = withTable(regional)
    merge(m, regionalRow("EU", 7, "a", "100", Some(1), Seq(("EU", "7", 1, "DHL"), ("EU", "7", 1, "UPS"))))
    assertThat(shipments(current("region = 'EU'")).asJava).isEqualTo(Map(("EU", "7", 1) -> ("UPS", "100")).asJava)
  }
}
