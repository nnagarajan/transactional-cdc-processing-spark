package com.technext.demos.txbuffering.processor

import java.nio.file.Files
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.connector.read.streaming.Offset
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, Test, TestInstance}

import com.technext.demos.txbuffering.model.order.OrderStream

/** Same column layout as the unioned CDC stream built by TransactionalCdcProcessingApp. */
case class TestDataCollection(data_collection: String, event_count: Int)

case class TestCdcRow(
    xid: String,
    csn: String,
    table: String,
    op_type: String,
    op_ts: String,
    current_ts: String,
    pos: String,
    before: Map[String, String],
    after: Map[String, String],
    event_type: String,
    expected_count: Option[Int],
    data_collections: Seq[TestDataCollection],
    marker_partition: Option[Int],
    marker_offset: Option[Long])

/** Output collected per micro-batch, in batch order: (batchId, xid:csn of each emitted row). */
object CollectedBatches {
  val batches = new ConcurrentLinkedQueue[(Long, Seq[String])]()
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransactionOrderingProcessorIntegrationTest {

  private var spark: SparkSession = _
  private var offset = 0L

  @BeforeAll
  def startSpark(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("ordering-it")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  @AfterAll
  def stopSpark(): Unit = if (spark != null) spark.stop()

  @BeforeEach
  def reset(): Unit = CollectedBatches.batches.clear()

  // ---- row builders ----

  private def markerRow(xid: String, csn: String, lineItems: Int, partition: Int = 0): TestCdcRow = {
    offset += 1
    TestCdcRow(xid, csn, null, null, null, null, null, null, null, "METADATA", Some(lineItems),
      Seq(TestDataCollection("ORDERS", 0), TestDataCollection("ORDER_DETAILS", 0),
        TestDataCollection("ORDER_LINE_ITEMS", lineItems)),
      Some(partition), Some(offset))
  }

  private def lineItemRow(xid: String, csn: String, pos: String, lineItemId: Int, orderId: Int): TestCdcRow =
    TestCdcRow(xid, csn, "APPUSER.ORDER_LINE_ITEMS", "I", "2025-07-07 23:33:54", "2025-07-07 23:36:00", pos,
      null, Map("LINE_ITEM_ID" -> lineItemId.toString, "ORDER_ID" -> orderId.toString, "VERSION" -> "1"),
      "ORDER_LINE_ITEMS", None, null, None, None)

  // ---- query plumbing ----

  private def newStream(): MemoryStream[TestCdcRow] = {
    implicit val session: SparkSession = spark
    implicit val encoder: Encoder[TestCdcRow] = Encoders.product[TestCdcRow]
    MemoryStream[TestCdcRow]
  }

  private def start(input: MemoryStream[TestCdcRow], checkpoint: String): StreamingQuery = {
    val ordered = TransactionOrderingProcessor.ordersOf(
      TransactionOrderingProcessor.orderTransactions(input.toDF(), 10 * 60 * 1000L, 60 * 1000L))
    val collect: (Dataset[OrderStream], Long) => Unit = (ds, batchId) => {
      val keys = ds.collect().toSeq.map(o => s"${o.xid}:${o.csn}")
      if (keys.nonEmpty) CollectedBatches.batches.add((batchId, keys))
    }
    ordered.writeStream
      .option("checkpointLocation", checkpoint)
      .foreachBatch(collect)
      .trigger(Trigger.ProcessingTime(100L))
      .start()
  }

  /**
   * processAllAvailable() never returns here: in processing-time mode transformWithState always
   * requests another batch, so the stream is never idle. Wait for the batch that consumed `upTo`.
   */
  private def awaitProcessed(query: StreamingQuery, upTo: Offset): Unit = {
    val deadline = System.currentTimeMillis() + 60000L
    def done = query.recentProgress.exists(p => p.sources.exists(_.endOffset == upTo.json))
    while (!done) {
      query.exception.foreach(e => throw e)
      if (System.currentTimeMillis() > deadline) throw new AssertionError(s"Query did not reach offset ${upTo.json}")
      Thread.sleep(50L)
    }
  }

  /** Waits for the query to fail and returns its error. */
  private def awaitFailure(query: StreamingQuery): Throwable = {
    val deadline = System.currentTimeMillis() + 60000L
    while (query.exception.isEmpty) {
      if (System.currentTimeMillis() > deadline) throw new AssertionError("Query did not fail")
      Thread.sleep(50L)
    }
    query.exception.get
  }

  private def newCheckpoint(): String = Files.createTempDirectory("ordering-it").toString

  /** Emitted transaction keys, in the order their batches committed. */
  private def emittedInOrder: Seq[String] =
    CollectedBatches.batches.asScala.toSeq.sortBy(_._1).flatMap(_._2.distinct)

  // ---- tests ----

  @Test
  def outOfOrderCompletionIsEmittedInMarkerOrderAcrossBatches(): Unit = {
    val input = newStream()
    val query = start(input, newCheckpoint())
    try {
      // Batch 1: T2 is complete, T1 is still missing its line item.
      awaitProcessed(query, input.addData(markerRow("t1", "100", 1), markerRow("t2", "200", 1), lineItemRow("t2", "200", "p20", 20, 2)))
      assertThat(emittedInOrder.asJava).isEmpty()

      // Batch 2: T1 completes; both are emitted, T1 first.
      awaitProcessed(query, input.addData(lineItemRow("t1", "100", "p10", 10, 1)))
      assertThat(emittedInOrder.asJava).containsExactly("t1:100", "t2:200")

      // Batch 3: T3 is emitted in a later commit than T1 and T2.
      awaitProcessed(query, input.addData(markerRow("t3", "300", 1), lineItemRow("t3", "300", "p30", 30, 3)))
      assertThat(emittedInOrder.asJava).containsExactly("t1:100", "t2:200", "t3:300")
      val batches = CollectedBatches.batches.asScala.toSeq.sortBy(_._1)
      assertThat(batches.last._2.asJava).containsExactly("t3:300")
    } finally query.stop()
  }

  @Test
  def markerFromSecondPartitionFailsTheQuery(): Unit = {
    val input = newStream()
    val query = start(input, newCheckpoint())
    try {
      input.addData(markerRow("t1", "100", 0, partition = 1))
      assertThat(rootMessage(awaitFailure(query))).contains("exactly one partition")
    } finally query.stop()
  }

  @Test
  def orderingAndReplayDetectionSurviveRestart(): Unit = {
    val input = newStream()
    val checkpoint = newCheckpoint()

    val q1 = start(input, checkpoint)
    try {
      // T0 is emitted; T1 blocks (1 of 2 line items); T2 is complete behind it.
      awaitProcessed(q1, input.addData(
        markerRow("t0", "50", 1), lineItemRow("t0", "50", "p0", 1, 9),
        markerRow("t1", "100", 2), lineItemRow("t1", "100", "p10", 10, 1),
        markerRow("t2", "200", 1), lineItemRow("t2", "200", "p20", 20, 2)))
      assertThat(emittedInOrder.asJava).containsExactly("t0:50")
    } finally q1.stop()

    val q2 = start(input, checkpoint)
    try {
      // Replay T0's marker and event (as after a replicat restart): must not be re-emitted.
      awaitProcessed(q2, input.addData(markerRow("t0", "50", 1), lineItemRow("t0", "50", "p0", 1, 9)))
      assertThat(emittedInOrder.asJava).containsExactly("t0:50")

      // T1 completes after the restart: T1 then T2, never T2 first.
      awaitProcessed(q2, input.addData(lineItemRow("t1", "100", "p11", 11, 1)))
      assertThat(emittedInOrder.asJava).containsExactly("t0:50", "t1:100", "t2:200")
    } finally q2.stop()
  }

  private def rootMessage(t: Throwable): String = {
    var c = t
    val sb = new StringBuilder
    while (c != null) { sb.append(c.getMessage).append('\n'); c = c.getCause }
    sb.toString
  }
}
