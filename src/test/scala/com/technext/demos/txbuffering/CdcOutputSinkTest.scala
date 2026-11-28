package com.technext.demos.txbuffering

import java.nio.file.{Files, Path}
import java.{util => ju}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, Test, TestInstance}

import com.technext.demos.txbuffering.model.generic.{DataChangeEvent, DroppedEvent, KafkaSource}
import com.technext.demos.txbuffering.model.generic.DroppedEvent.Reason
import com.technext.demos.txbuffering.model.spec.{Ddl, JobModel, Schemas, TestModels}
import com.technext.demos.txbuffering.processor.{EmittedTransaction, Fixtures, SequencerOutput}

/** The two-table micro-batch sink, against local Delta tables. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CdcOutputSinkTest {

  private var spark: SparkSession = _
  private val Stream = "orders_out"
  private val Dropped = "dropped_out"

  @BeforeAll
  def startSpark(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("cdc-output-sink-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("sink-warehouse").toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  @AfterAll
  def stopSpark(): Unit = if (spark != null) spark.stop()

  @BeforeEach
  def recreateTables(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $Dropped")
    createStreamTable(model)
  }

  private def model: JobModel = TestModels.ordersModel.copy(streamTable = Stream)

  private def createStreamTable(m: JobModel): Unit =
    TestModels.runDdl(spark, Ddl.streamDdl(m), "default")

  // ---- builders ----

  private def transaction(xid: String, csn: String, events: DataChangeEvent*): SequencerOutput = {
    val t = new EmittedTransaction()
    t.xid = xid
    t.csn = csn
    t.processedTs = "2026-11-26T10:00:00Z"
    t.events = new ju.ArrayList[DataChangeEvent](events.asJava)
    SequencerOutput.ofTransaction(t)
  }

  private def order(xid: String, csn: String, orderId: Int = 1): SequencerOutput =
    transaction(xid, csn, Fixtures.order(xid, csn, "p1", orderId = orderId))

  private def drop(xid: String, csn: String): SequencerOutput = {
    val d = new DroppedEvent()
    d.itemKind = DroppedEvent.ItemKind.DataEvent
    d.reason = Reason.ReplayBelowCursor
    d.xid = xid
    d.csn = csn
    d.lastEmittedCsn = "900"
    KafkaSource("dev.appuser.orders.json", 0, 42L).applyTo(d)
    SequencerOutput.ofDropped(d)
  }

  private def batch(rows: SequencerOutput*): Dataset[SequencerOutput] =
    spark.createDataset(rows)(Encoders.bean(classOf[SequencerOutput]))

  private def sink(enabled: Boolean, prefix: String = "query-1", m: JobModel = model): CdcOutputSink =
    new CdcOutputSink(m, DroppedEventsSettings(enabled, Dropped, summarizeReplays = false), () => prefix)

  private def count(table: String): Long = spark.table(table).count()

  // ---- writes ----

  @Test
  def ordersAndDropsGoToTheirTablesOnce(): Unit = {
    val b = batch(order("a", "100"), order("b", "200", orderId = 2), drop("z", "50"))
    val s = sink(enabled = true)

    s.writeBatch(b, 0L)
    s.writeBatch(b, 0L) // retried batch: both writes are skipped

    assertThat(count(Stream)).isEqualTo(2L)
    assertThat(count(Dropped)).isEqualTo(1L)
    // Timestamps are checked in SQL: decoding them in the test JVM needs extra --add-opens.
    val row = spark.table(Dropped)
      .selectExpr("batchId", "reason", "kafkaOffset", "jobName", "droppedDate IS NOT NULL AS hasDate",
        "droppedTs IS NOT NULL AS hasTs").collect().head
    assertThat(row.getAs[Long]("batchId")).isEqualTo(0L)
    assertThat(row.getAs[String]("reason")).isEqualTo(Reason.ReplayBelowCursor)
    assertThat(row.getAs[Long]("kafkaOffset")).isEqualTo(42L)
    assertThat(row.getAs[String]("jobName")).isEqualTo("orders")
    assertThat(row.getAs[Boolean]("hasDate")).isTrue
    assertThat(row.getAs[Boolean]("hasTs")).isTrue

    s.writeBatch(batch(order("c", "300", orderId = 3)), 1L) // next batch is written
    assertThat(count(Stream)).isEqualTo(3L)
  }

  @Test
  def streamRecordsAreShapedFromTheModel(): Unit = {
    sink(enabled = false).writeBatch(batch(order("a", "100", orderId = 7)), 0L)
    val row = spark.table(Stream).collect().head
    assertThat(row.getAs[Double]("orderId")).isEqualTo(7.0)
    assertThat(row.getAs[String]("dwhProcessedTs")).isEqualTo("2026-11-26T10:00:00Z")
    assertThat(row.getSeq[Row](row.fieldIndex("orders")).head.getAs[Double]("version")).isEqualTo(1.0)
    assertThat(row.getSeq[Row](row.fieldIndex("lineItems")).isEmpty).isTrue
  }

  @Test
  def retryAfterTheDropWriteFailedWritesOnlyTheDrops(): Unit = {
    val b = batch(order("a", "100"), drop("z", "50"))
    // First attempt: orders committed, the dropped-events write never happened (simulated by
    // writing with recording disabled).
    sink(enabled = false).writeBatch(b, 0L)
    assertThat(spark.catalog.tableExists(Dropped)).isFalse

    sink(enabled = true).writeBatch(b, 0L)

    assertThat(count(Stream)).isEqualTo(1L)
    assertThat(count(Dropped)).isEqualTo(1L)
  }

  @Test
  def disabledWritesOnlyOrders(): Unit = {
    sink(enabled = false).writeBatch(batch(order("a", "100"), drop("z", "50")), 0L)

    assertThat(count(Stream)).isEqualTo(1L)
    assertThat(spark.catalog.tableExists(Dropped)).isFalse
  }

  @Test
  def aNewCheckpointIsNotSkipped(): Unit = {
    sink(enabled = true, prefix = "query-1").writeBatch(batch(order("a", "100")), 0L)
    // A new checkpoint has a new query id and restarts batch ids at 0.
    sink(enabled = true, prefix = "query-2").writeBatch(batch(order("b", "200")), 0L)

    assertThat(count(Stream)).isEqualTo(2L)
  }

  @Test
  def queryIdIsReadFromTheCheckpoint(): Unit = {
    val dir: Path = Files.createTempDirectory("ckpt")
    Files.writeString(dir.resolve("metadata"), """{"id":"0e62ffd8-1111-2222-3333-444455556666"}""" + "\n")

    assertThat(CdcOutputSink.queryIdFromCheckpoint(spark, dir.toUri.toString))
      .isEqualTo("0e62ffd8-1111-2222-3333-444455556666")
  }

  // ---- 5.3 transform ----

  @Test
  def derivedColumnIsFilledByTheTransform(): Unit = {
    val m = TestModels.ordersWithDerivedColumn("com.technext.demos.txbuffering.transforms.TotalWithTaxTransform")
      .copy(streamTable = "orders_taxed")
    createStreamTable(m)
    val e = Fixtures.event("a", "100", "ORDERS", "p1", after = Map("ORDER_ID" -> "1", "VERSION" -> "1", "TOTAL_AMOUNT" -> "10"))
    sink(enabled = false, m = m).writeBatch(batch(transaction("a", "100", e)), 0L)
    val written = spark.table("orders_taxed").collect().head
    val element = written.getSeq[Row](written.fieldIndex("orders")).head
    assertThat(element.getAs[Double]("totalWithTax")).isEqualTo(12.0)
  }

  @Test
  def transformThatChangesTheSchemaFailsTheBatchAndWritesNothing(): Unit = {
    val m = TestModels.ordersWithDerivedColumn("com.technext.demos.txbuffering.transforms.DropStatusTransform")
      .copy(streamTable = "orders_dropped_status")
    createStreamTable(m)
    assertThatThrownBy(() => sink(enabled = false, m = m).writeBatch(batch(order("a", "100")), 0L))
      .hasMessageContaining("orderStatus")
      .hasMessageContaining("Nothing was written")
    assertThat(count("orders_dropped_status")).isEqualTo(0L)
  }

  // ---- 5.5 startup schema check ----

  @Test
  def tableCreatedFromStaleDdlIsRefusedNamingTheColumn(): Unit = {
    // The table was created before the model gained column priority
    val stale = StructType(Schemas.streamSchema(model).fields.filterNot(_.name == "lineItems"))
    spark.sql("DROP TABLE IF EXISTS stale_stream")
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], stale).write.format("delta").saveAsTable("stale_stream")
    assertThatThrownBy(() => CdcOutputSink.requireTableSchema(spark, "stale_stream", Schemas.streamSchema(model), "orders"))
      .hasMessageContaining("stale_stream")
      .hasMessageContaining("lineItems: missing")
  }

  @Test
  def missingTableIsRefused(): Unit =
    assertThatThrownBy(() => CdcOutputSink.requireTableSchema(spark, "no_such_table", Schemas.streamSchema(model), "orders"))
      .hasMessageContaining("no_such_table does not exist")

  @Test
  def matchingTableIsAccepted(): Unit =
    CdcOutputSink.requireTableSchema(spark, Stream, Schemas.streamSchema(model), "orders")

  // ---- 4.4 dropped-events columns ----

  @Test
  def droppedEventsTableWithoutTheNewColumnsIsRefused(): Unit = {
    spark.sql(s"CREATE TABLE $Dropped (xid STRING, reason STRING) USING DELTA")
    assertThatThrownBy(() => CdcOutputSink.checkDroppedEventsTable(spark, DroppedEventsSettings(enabled = true, Dropped, summarizeReplays = false)))
      .hasMessageContaining("jobName, bufferedCounts")
      .hasMessageContaining("db/alter_dropped_events_add_job.sql")
  }

  @Test
  def droppedEventsTableFromTheShippedDdlIsAccepted(): Unit = {
    val ddl = new String(Files.readAllBytes(java.nio.file.Paths.get("db/create_dropped_events_table.deltalake.sql")))
      .replace("dropped_events", Dropped)
    TestModels.runDdl(spark, ddl.replaceAll("(?s)TBLPROPERTIES \\(.*?\\);", ";"), "default")
    CdcOutputSink.checkDroppedEventsTable(spark, DroppedEventsSettings(enabled = true, Dropped, summarizeReplays = false))
    // The shipped DDL holds every column the sequencer writes
    val columns = spark.table(Dropped).schema.fieldNames.toSet
    Encoders.bean(classOf[DroppedEvent]).schema.fieldNames.foreach(f => assertThat(columns.contains(f)).as(f).isTrue)
  }
}
