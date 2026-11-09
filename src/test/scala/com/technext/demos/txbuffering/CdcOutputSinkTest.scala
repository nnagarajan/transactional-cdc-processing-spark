package com.technext.demos.txbuffering

import java.nio.file.{Files, Path}

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, Test, TestInstance}

import com.technext.demos.txbuffering.model.generic.{DroppedEvent, KafkaSource}
import com.technext.demos.txbuffering.model.generic.DroppedEvent.Reason
import com.technext.demos.txbuffering.model.order.OrderStream
import com.technext.demos.txbuffering.processor.SequencerOutput

/** The two-table micro-batch sink, against local Delta tables. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CdcOutputSinkTest {

  private var spark: SparkSession = _

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
  def dropTables(): Unit = Seq("orders_out", "dropped_out").foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  private def order(xid: String, csn: String): SequencerOutput = {
    val o = new OrderStream()
    o.xid = xid
    o.csn = csn
    o.orderId = 1.0
    SequencerOutput.ofOrder(o)
  }

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

  private def sink(enabled: Boolean, prefix: String = "query-1"): CdcOutputSink =
    new CdcOutputSink("orders_out", DroppedEventsSettings(enabled, "dropped_out", summarizeReplays = false), () => prefix)

  private def count(table: String): Long = spark.table(table).count()

  @Test
  def ordersAndDropsGoToTheirTablesOnce(): Unit = {
    val b = batch(order("a", "100"), order("b", "200"), drop("z", "50"))
    val s = sink(enabled = true)

    s.writeBatch(b, 0L)
    s.writeBatch(b, 0L) // retried batch: both writes are skipped

    assertThat(count("orders_out")).isEqualTo(2L)
    assertThat(count("dropped_out")).isEqualTo(1L)
    // Timestamps are checked in SQL: decoding them in the test JVM needs extra --add-opens.
    val row = spark.table("dropped_out")
      .selectExpr("batchId", "reason", "kafkaOffset", "droppedDate IS NOT NULL AS hasDate",
        "droppedTs IS NOT NULL AS hasTs").collect().head
    assertThat(row.getAs[Long]("batchId")).isEqualTo(0L)
    assertThat(row.getAs[String]("reason")).isEqualTo(Reason.ReplayBelowCursor)
    assertThat(row.getAs[Long]("kafkaOffset")).isEqualTo(42L)
    assertThat(row.getAs[Boolean]("hasDate")).isTrue
    assertThat(row.getAs[Boolean]("hasTs")).isTrue

    s.writeBatch(batch(order("c", "300")), 1L) // next batch is written
    assertThat(count("orders_out")).isEqualTo(3L)
  }

  @Test
  def retryAfterTheDropWriteFailedWritesOnlyTheDrops(): Unit = {
    val b = batch(order("a", "100"), drop("z", "50"))
    // First attempt: orders committed, the dropped-events write never happened (simulated by
    // writing with recording disabled).
    sink(enabled = false).writeBatch(b, 0L)
    assertThat(spark.catalog.tableExists("dropped_out")).isFalse

    sink(enabled = true).writeBatch(b, 0L)

    assertThat(count("orders_out")).isEqualTo(1L)
    assertThat(count("dropped_out")).isEqualTo(1L)
  }

  @Test
  def disabledWritesOnlyOrders(): Unit = {
    sink(enabled = false).writeBatch(batch(order("a", "100"), drop("z", "50")), 0L)

    assertThat(count("orders_out")).isEqualTo(1L)
    assertThat(spark.catalog.tableExists("dropped_out")).isFalse
  }

  @Test
  def aNewCheckpointIsNotSkipped(): Unit = {
    sink(enabled = true, prefix = "query-1").writeBatch(batch(order("a", "100")), 0L)
    // A new checkpoint has a new query id and restarts batch ids at 0.
    sink(enabled = true, prefix = "query-2").writeBatch(batch(order("b", "200")), 0L)

    assertThat(count("orders_out")).isEqualTo(2L)
  }

  @Test
  def queryIdIsReadFromTheCheckpoint(): Unit = {
    val dir: Path = Files.createTempDirectory("ckpt")
    Files.writeString(dir.resolve("metadata"), """{"id":"0e62ffd8-1111-2222-3333-444455556666"}""" + "\n")

    assertThat(CdcOutputSink.queryIdFromCheckpoint(spark, dir.toUri.toString))
      .isEqualTo("0e62ffd8-1111-2222-3333-444455556666")
  }
}
