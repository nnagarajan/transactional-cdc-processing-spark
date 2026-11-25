package com.technext.demos.txbuffering.processor

import java.nio.file.Files

import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.streaming.{ExpiredTimerInfo, MapState, OutputMode, StatefulProcessor, TimeMode, TimerValues, TTLConfig, Trigger, ValueState}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

/**
 * Writes state in the layout used before per-event buffering: one `buffers` map of whole
 * `TransactionState` values, plus `queue` and `cursor`.
 */
class LegacyLayoutProcessor(queued: Boolean) extends StatefulProcessor[String, Row, SequencerOutput] {
  @transient private var buffers: MapState[String, TransactionState] = _
  @transient private var queue: MapState[Long, String] = _
  @transient private var cursor: ValueState[Cursor] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    buffers = getHandle.getMapState("buffers", Encoders.STRING, Encoders.bean(classOf[TransactionState]), TTLConfig.NONE)
    queue = getHandle.getMapState("queue", Encoders.scalaLong, Encoders.STRING, TTLConfig.NONE)
    cursor = getHandle.getValueState("cursor", Encoders.bean(classOf[Cursor]), TTLConfig.NONE)
  }

  override def handleInputRows(key: String, rows: Iterator[Row], timerValues: TimerValues): Iterator[SequencerOutput] = {
    rows.foreach(_ => ())
    val s = new TransactionState()
    s.xid = "old"
    s.csn = "100"
    s.setMetadata(Fixtures.marker("old", "100", lineItems = 2))
    s.addEvent(Fixtures.lineItem("old", "100", "p1", 1))
    buffers.updateValue("old:100", s)
    val c = new Cursor()
    if (queued) {
      queue.updateValue(0L, "old:100")
      c.nextSeqToAssign = 1L
    }
    cursor.update(c)
    Iterator.empty
  }

  override def handleExpiredTimer(key: String, timerValues: TimerValues, info: ExpiredTimerInfo): Iterator[SequencerOutput] =
    Iterator.empty
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LegacyCheckpointTest {

  private var spark: SparkSession = _

  @BeforeAll
  def startSpark(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("legacy-checkpoint")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  @AfterAll
  def stopSpark(): Unit = if (spark != null) spark.stop()

  private def runOnce(input: MemoryStream[TestCdcRow], checkpoint: String, processor: StatefulProcessor[String, Row, SequencerOutput]): Throwable = {
    spark.conf.set("spark.sql.streaming.stateStore.providerClass",
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    implicit val k: Encoder[String] = Encoders.STRING
    implicit val o: Encoder[SequencerOutput] = Encoders.bean(classOf[SequencerOutput])
    val grouped = input.toDF().groupByKey((_: Row) => TransactionOrderingProcessor.GlobalKey)
      .asInstanceOf[org.apache.spark.sql.classic.KeyValueGroupedDataset[String, Row]]
    val q = grouped.transformWithState(processor, TimeMode.ProcessingTime(), OutputMode.Append())
      .writeStream.option("checkpointLocation", checkpoint).format("noop")
      .trigger(Trigger.ProcessingTime(100L)).start()
    try {
      val deadline = System.currentTimeMillis() + 60000L
      while (q.exception.isEmpty && !q.recentProgress.exists(_.numInputRows > 0) &&
        System.currentTimeMillis() < deadline) Thread.sleep(50L)
      Thread.sleep(500L)
      q.exception.orNull
    } finally q.stop()
  }

  private def row(xid: String): TestCdcRow =
    TestCdcRow(xid, "100", "APPUSER.ORDER_LINE_ITEMS", "I", null, null, "p9", null,
      Map("LINE_ITEM_ID" -> "9", "ORDER_ID" -> "1", "VERSION" -> "1"), "ORDER_LINE_ITEMS", None, null, None, None)

  @Test
  def oldCheckpointWithAQueuedTransactionIsRefused(): Unit = assertRefused(queued = true)

  @Test
  def oldCheckpointWithOnlyUnmarkedBuffersIsRefused(): Unit = assertRefused(queued = false)

  @Test
  def newCheckpointIsAccepted(): Unit = {
    implicit val session: SparkSession = spark
    implicit val enc: Encoder[TestCdcRow] = Encoders.product[TestCdcRow]
    val input = MemoryStream[TestCdcRow]
    input.addData(row("new"))
    assertThat(runOnce(input, Files.createTempDirectory("new-ckpt").toString,
      new TransactionOrderingProcessor(600000L, 60000L))).isNull()
  }

  private def assertRefused(queued: Boolean): Unit = {
    implicit val session: SparkSession = spark
    implicit val enc: Encoder[TestCdcRow] = Encoders.product[TestCdcRow]
    val input = MemoryStream[TestCdcRow]
    val checkpoint = Files.createTempDirectory("legacy-ckpt").toString

    input.addData(row("old"))
    assertThat(runOnce(input, checkpoint, new LegacyLayoutProcessor(queued))).isNull()

    input.addData(row("new"))
    val error = runOnce(input, checkpoint, new TransactionOrderingProcessor(600000L, 60000L))
    assertThat(error).isNotNull
    assertThat(messages(error)).contains("Migrating to per-event buffering state")
  }

  private def messages(t: Throwable): String =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).map(e => s"${e.getClass.getSimpleName}: ${e.getMessage}").mkString("\n  caused by ")
}
