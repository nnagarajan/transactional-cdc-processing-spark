package com.technext.demos.txbuffering

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import com.technext.demos.txbuffering.model.generic.DataChangeEvent
import com.technext.demos.txbuffering.model.spec.{JobModel, TestModels}
import com.technext.demos.txbuffering.processor._
import com.technext.demos.txbuffering.processor.Fixtures.marker

/**
 * Compatibility baseline for the configurable-model refactoring: a fixed set of orders
 * transactions goes through the sequencer, the stage 1 sink and the stage 2 merger into tables
 * created from the shipped DDL, and the resulting `order_stream` and `orders_current` rows and
 * table schemas must equal the files under `src/test/resources/golden/`, recorded from the
 * pipeline before job models (beans, OrderJoiner, ScdType1Merger)
 * (`dwhProcessedTs` excluded, since it is a wall-clock time).
 *
 * Record the files again with `-Dgolden.record=true`; only do that when the output is meant to change.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GoldenOutputTest {

  import GoldenOutputTest._

  private var spark: SparkSession = _

  @BeforeAll
  def startSpark(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("golden-output-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("golden-warehouse").toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE SCHEMA IF NOT EXISTS golden")
    Seq("db/orders_stream.deltalake.sql", "db/orders_current.deltalake.sql").foreach(runDdl)
    runPipeline()
  }

  @AfterAll
  def stopSpark(): Unit = if (spark != null) spark.stop()

  /**
   * Names, types, nesting and field order, as the startup check compares them: the recorded
   * schemas carry the column comments of the original hand-written DDL, the written DDL has none.
   */
  @Test
  def orderStreamSchemaMatches(): Unit = checkSchema("order_stream.schema.json", spark.table(StreamTable).schema)

  @Test
  def ordersCurrentSchemaMatches(): Unit = checkSchema("orders_current.schema.json", spark.table(CurrentTable).schema)

  private def checkSchema(file: String, actual: StructType): Unit =
    if (Record) check(file, actual.prettyJson)
    else assertThat(com.technext.demos.txbuffering.model.spec.Schemas.differences(goldenSchema(file), actual).asJava).isEmpty()

  @Test
  def orderStreamRowsMatch(): Unit =
    check("order_stream.rows.json", rowsJson(spark.table(StreamTable), "CAST(csn AS DECIMAL(38,0))", "orderId"))

  @Test
  def ordersCurrentRowsMatch(): Unit =
    check("orders_current.rows.json", rowsJson(spark.table(CurrentTable), "orderId"))

  // ---- pipeline ----

  /** Runs every input batch through the sequencer, then stage 1 and stage 2, as the apps do per micro-batch. */
  private def runPipeline(): Unit = {
    val store = new InMemorySequencerStore
    val sequencer = new TransactionSequencer(store, Model.trackedTables, Long.MaxValue, () => 0L)
    val sink = new CdcOutputSink(Model, DroppedEventsSettings(enabled = false, "unused", summarizeReplays = false),
      () => "golden")
    val merger = new CurrentStateMerger(Model)
    Batches.zipWithIndex.foreach { case (inputs, batchId) =>
      val result = sequencer.process(inputs.iterator)
      val output = stage1Output(result)
      sink.writeBatch(spark.createDataset(output)(Encoders.bean(classOf[SequencerOutput])), batchId.toLong)
      // Stage 2 gets what stage 1 appended in this batch, as ScdType1MergeApp's stream would.
      val csns = EmittedCsnsByBatch(batchId)
      val appended = spark.table(StreamTable).where(org.apache.spark.sql.functions.col("csn").isin(csns: _*))
      merger.merge(spark, appended)
    }
  }

  /** The sequencer output a batch writes: emitted transactions as stage 1 records. */
  private def stage1Output(result: SequencerResult): Seq[SequencerOutput] =
    result.emitted.map(s => SequencerOutput.ofTransaction(EmittedTransaction(s, "2026-11-26T00:00:00Z")))

  private def runDdl(path: String): Unit = {
    val text = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
      .replace("{{CATALOG_SCHEMA}}", "golden")
    text.split(";").map(_.trim).filter(_.nonEmpty)
      .map(stripComments)
      .filter(s => s.nonEmpty && !s.toUpperCase.startsWith("DESCRIBE"))
      // Local Delta tables are not catalog-managed; the feature property doesn't change the schema
      .map(_.replaceAll("(?s)TBLPROPERTIES\\s*\\('delta.feature.catalogManaged'\\s*=\\s*'supported'\\)", ""))
      .foreach(spark.sql)
  }

  private def stripComments(sql: String): String =
    sql.linesIterator.filterNot(_.trim.startsWith("--")).mkString("\n").trim

  // ---- golden files ----

  private def rowsJson(df: DataFrame, orderBy: String*): String =
    df.drop("dwhProcessedTs").orderBy(orderBy.map(org.apache.spark.sql.functions.expr): _*)
      .toJSON.collect().mkString("", "\n", "\n")

  private def check(file: String, actual: String): Unit = {
    val path = GoldenDir.resolve(file)
    if (Record) {
      Files.createDirectories(GoldenDir)
      Files.write(path, actual.getBytes(StandardCharsets.UTF_8))
    } else {
      if (!Files.exists(path)) throw new AssertionError(s"Missing golden file $path (record with -Dgolden.record=true)")
      assertThat(actual).isEqualTo(new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
    }
  }
}

object GoldenOutputTest {

  val StreamTable = "golden.order_stream"
  val CurrentTable = "golden.orders_current"
  /** The shipped orders job, writing to this test's tables. */
  lazy val Model: JobModel = TestModels.ordersModel.copy(streamTable = StreamTable, currentTable = CurrentTable)
  val GoldenDir: Path = Paths.get("src/test/resources/golden")
  val Record: Boolean = java.lang.Boolean.getBoolean("golden.record")

  /** The golden schemas, parsed from the recorded files (for model-derivation tests). */
  def goldenSchema(file: String): StructType =
    DataType.fromJson(new String(Files.readAllBytes(GoldenDir.resolve(file)), StandardCharsets.UTF_8)).asInstanceOf[StructType]

  // ---- input ----

  private var offset = 0L

  private def mk(xid: String, csn: String, orders: Int = 0, details: Int = 0, items: Int = 0): MarkerInput = {
    offset += 1
    MarkerInput(marker(xid, csn, orders, details, items), 0, offset)
  }

  private def ev(xid: String, csn: String, table: String, pos: String, op: String,
      after: Map[String, String], before: Map[String, String] = null): DataInput = {
    val e = new DataChangeEvent()
    e.xid = xid
    e.csn = csn
    e.table = s"APPUSER.$table"
    e.pos = pos
    e.opType = op
    e.opTs = "2026-11-11 10:15:30.000000"
    e.currentTs = "2026-11-11 10:15:31.000000"
    e.after = if (after == null) null else new java.util.HashMap[String, String](after.asJava)
    e.before = if (before == null) null else new java.util.HashMap[String, String](before.asJava)
    DataInput(e)
  }

  private def order(id: Int, version: Int, status: String, amount: String): Map[String, String] = Map(
    "ORDER_ID" -> id.toString, "ORDER_REF" -> s"REF-$id", "VERSION" -> version.toString,
    "ORDER_DATE" -> "2026-11-11 00:00:00", "ORDER_TS" -> "2026-11-11 10:15:30.123456000",
    "ORDER_STATUS" -> status, "ORDER_TYPE" -> "WEB", "TOTAL_AMOUNT" -> amount, "CURRENCY" -> "EUR",
    "CUSTOMER_ID" -> s"C-$id", "SHIPPING_ADDRESS_ID" -> s"A-$id", "CREATED_TS" -> "2026-11-11 10:15:29.000000000")

  private def detail(orderId: Int, version: Int, status: String): Map[String, String] = Map(
    "ORDER_ID" -> orderId.toString, "VERSION" -> version.toString, "SHIPPING_METHOD" -> "STANDARD",
    "TRACKING_NUMBER" -> s"TRK-$orderId-$version", "SHIPPED_TS" -> "2026-11-12 08:00:00.000000000",
    "ESTIMATED_DELIVERY_DATE" -> "2026-11-15 00:00:00", "CARRIER" -> "DHL", "DELIVERY_STATUS" -> status)

  private def item(id: Int, orderId: Int, version: Int, qty: String, price: String): Map[String, String] = Map(
    "LINE_ITEM_ID" -> id.toString, "ORDER_ID" -> orderId.toString, "VERSION" -> version.toString,
    "PRODUCT_ID" -> s"P-$id", "ITEM_QTY" -> qty, "ITEM_PRICE" -> price,
    "ITEM_AMOUNT" -> (BigDecimal(qty) * BigDecimal(price)).toString, "ITEM_CURRENCY" -> "EUR")

  /**
   * Three micro-batches. t1 inserts two orders in one transaction; t2 updates order 1001 and a
   * line item (before images); t3 adds a line item to 1002 and deletes one of 1001's; t4 updates
   * 1002's details, one event missing a column and one carrying a column the model doesn't
   * know; t5 changes a line item of order 2000, which was never inserted (placeholder row).
   * t2 is split across batches and its marker arrives first.
   */
  val Batches: Seq[Seq[SequencerInput]] = Seq(
    Seq(
      mk("t1", "100", orders = 2, details = 2, items = 3),
      ev("t1", "100", "ORDERS", "p01", "I", order(1001, 1, "NEW", "250.75")),
      ev("t1", "100", "ORDER_DETAILS", "p02", "I", detail(1001, 1, "PENDING")),
      ev("t1", "100", "ORDER_LINE_ITEMS", "p03", "I", item(1, 1001, 1, "2", "100.25")),
      ev("t1", "100", "ORDER_LINE_ITEMS", "p04", "I", item(2, 1001, 1, "1", "50.25")),
      ev("t1", "100", "ORDERS", "p05", "I", order(1002, 1, "NEW", "19.99")),
      ev("t1", "100", "ORDER_DETAILS", "p06", "I", detail(1002, 1, "PENDING")),
      ev("t1", "100", "ORDER_LINE_ITEMS", "p07", "I", item(3, 1002, 1, "1", "19.99")),
      mk("t2", "200", orders = 1, items = 1),
      ev("t2", "200", "ORDERS", "p08", "U", order(1001, 2, "PACKED", "250.75"), order(1001, 1, "NEW", "250.75"))),
    Seq(
      ev("t2", "200", "ORDER_LINE_ITEMS", "p09", "U", item(1, 1001, 2, "3", "100.25"), item(1, 1001, 1, "2", "100.25")),
      ev("t3", "300", "ORDER_LINE_ITEMS", "p10", "I", item(4, 1002, 1, "5", "1.50")),
      ev("t3", "300", "ORDER_LINE_ITEMS", "p11", "D", null, item(2, 1001, 1, "1", "50.25")),
      mk("t3", "300", items = 2)),
    Seq(
      mk("t4", "400", orders = 1, details = 1),
      ev("t4", "400", "ORDER_DETAILS", "p12", "U", detail(1002, 2, "SHIPPED"), detail(1002, 1, "PENDING")),
      ev("t4", "400", "ORDERS", "p13", "U",
        order(1002, 2, "SHIPPED", "19.99") - "SHIPPING_ADDRESS_ID" + ("PRIORITY" -> "HIGH"),
        order(1002, 1, "NEW", "19.99")),
      mk("t5", "500", items = 1),
      ev("t5", "500", "ORDER_LINE_ITEMS", "p14", "U", item(9, 2000, 2, "1", "5"), item(9, 2000, 1, "1", "4"))))

  /** CSNs of the transactions each batch emits (t2 completes in batch 1, with t3). */
  val EmittedCsnsByBatch: Seq[Seq[String]] = Seq(Seq("100"), Seq("200", "300"), Seq("400", "500"))
}
