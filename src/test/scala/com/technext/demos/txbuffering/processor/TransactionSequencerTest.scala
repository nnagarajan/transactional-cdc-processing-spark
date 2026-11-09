package com.technext.demos.txbuffering.processor

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

import com.technext.demos.txbuffering.model.generic.{DroppedEvent, KafkaSource}
import com.technext.demos.txbuffering.model.generic.DroppedEvent.{ItemKind, Reason}
import com.technext.demos.txbuffering.processor.Fixtures._

class TransactionSequencerTest {

  private val ThresholdMs = 10 * 60 * 1000L

  private var nowMs = 0L
  private val store = new InMemorySequencerStore
  private val seq = new TransactionSequencer(store, ThresholdMs, () => nowMs)

  private var nextOffset = 0L

  private def mk(xid: String, csn: String, lineItems: Int, partition: Int = 0, offset: Long = -1): MarkerInput = {
    val off = if (offset >= 0) offset else { nextOffset += 1; nextOffset }
    MarkerInput(marker(xid, csn, lineItems = lineItems), partition, off)
  }

  private def li(xid: String, csn: String, pos: String): DataInput =
    DataInput(lineItem(xid, csn, pos, lineItemId = pos.hashCode))

  private def run(inputs: SequencerInput*): SequencerResult = seq.process(inputs.iterator)

  private def keys(r: SequencerResult): Seq[String] = r.emitted.map(s => s"${s.xid}:${s.csn}")

  // ---- markers (3.2) ----

  @Test
  def markerFromOtherPartitionFails(): Unit = {
    val e = assertThrows(classOf[IllegalStateException], () => { run(mk("a", "100", 0, partition = 1)); () })
    assertThat(e.getMessage).contains("exactly one partition")
  }

  @Test
  def markersAreQueuedInOffsetOrderNotArrivalOrder(): Unit = {
    // Both complete in one batch, delivered out of offset order.
    val r = run(mk("b", "200", 0, offset = 2), mk("a", "100", 0, offset = 1))

    assertThat(keys(r).toArray).containsExactly("a:100", "b:200")
  }

  @Test
  def markersSharingAMessageKeepTheirOrderWithinIt(): Unit = {
    // GROUPTRANSOPS: one Kafka message (offset 5) carries three markers in commit order,
    // between single-marker messages at offsets 4 and 6. Rows arrive shuffled.
    def at(xid: String, csn: String, offset: Long, index: Int) =
      MarkerInput(marker(xid, csn, lineItems = 1), 0, offset, index)
    val r1 = run(
      at("g3", "700", 5, 2), at("after", "800", 6, 0), at("g1", "500", 5, 0),
      at("before", "400", 4, 0), at("g2", "600", 5, 1))

    assertThat(r1.emitted.toArray).isEmpty()

    // Complete them in reverse order: nothing is emitted until the head ("before") completes.
    val r2 = run(li("after", "800", "p8"), li("g3", "700", "p7"), li("g2", "600", "p6"), li("g1", "500", "p5"))
    assertThat(r2.emitted.toArray).isEmpty()

    val r3 = run(li("before", "400", "p4"))
    assertThat(keys(r3).toArray).containsExactly("before:400", "g1:500", "g2:600", "g3:700", "after:800")
  }

  @Test
  def duplicateMarkerWhileBufferedIsIgnored(): Unit = {
    run(mk("a", "100", 2), li("a", "100", "p1"))
    run(mk("a", "100", 2))

    assertThat(store.getCursor.queueDepth).isEqualTo(1L)
    assertThat(keys(run(li("a", "100", "p2"))).toArray).containsExactly("a:100")
    assertThat(store.getCursor.queueDepth).isEqualTo(0L)
  }

  @Test
  def duplicateMarkerAfterEmissionIsIgnored(): Unit = {
    run(mk("a", "100", 1), li("a", "100", "p1"))

    val r = run(mk("a", "100", 1))

    assertThat(r.emitted.isEmpty).isTrue
    assertThat(store.buffers.isEmpty).isTrue
    assertThat(store.getCursor.queueDepth).isEqualTo(0L)
  }

  @Test
  def markerBelowLastEmittedCsnIsDropped(): Unit = {
    run(mk("b", "200", 0))

    run(mk("a", "100", 0))

    assertThat(store.buffers.isEmpty).isTrue
    assertThat(store.getCursor.queueDepth).isEqualTo(0L)
  }

  @Test
  def twoTransactionsAtTheSameCsnAreSeparate(): Unit = {
    val r = run(mk("a", "500", 1), mk("b", "500", 1), li("a", "500", "p1"), li("b", "500", "p2"))

    assertThat(keys(r).toArray).containsExactly("a:500", "b:500")
    assertThat(r.emitted.map(_.orderLineItemEvents.size).toArray).containsExactly(1, 1)
  }

  @Test
  def secondTransactionAtSameCsnIsAcceptedAfterFirstEmitted(): Unit = {
    assertThat(keys(run(mk("a", "500", 0))).toArray).containsExactly("a:500")

    assertThat(keys(run(mk("b", "500", 1), li("b", "500", "p1"))).toArray).containsExactly("b:500")
  }

  @Test
  def csnComparedNumerically(): Unit = {
    run(mk("a", "99", 0))

    assertThat(keys(run(mk("b", "100", 0))).toArray).containsExactly("b:100")
  }

  // ---- data events (3.3) ----

  @Test
  def dataBeforeMarkerIsBufferedButNotQueued(): Unit = {
    val r = run(li("a", "100", "p1"))

    assertThat(r.emitted.isEmpty).isTrue
    assertThat(store.buffers.contains("a:100")).isTrue
    assertThat(store.getCursor.queueDepth).isEqualTo(0L)

    assertThat(keys(run(mk("a", "100", 1))).toArray).containsExactly("a:100")
  }

  @Test
  def replayedDataAfterEmissionCreatesNoBuffer(): Unit = {
    run(mk("a", "100", 1), li("a", "100", "p1"))

    run(li("a", "100", "p1"))

    assertThat(store.buffers.isEmpty).isTrue
  }

  @Test
  def dataForLowerCsnThanEmittedIsDropped(): Unit = {
    run(mk("b", "200", 0))

    run(li("a", "100", "p1"))

    assertThat(store.buffers.isEmpty).isTrue
  }

  // ---- emission walk (3.4) ----

  @Test
  def laterTransactionIsHeldUntilEarlierCompletes(): Unit = {
    val r1 = run(mk("t1", "100", 2), mk("t2", "200", 1), li("t2", "200", "p9"), li("t1", "100", "p1"))
    assertThat(r1.emitted.isEmpty).isTrue

    val r2 = run(li("t1", "100", "p2"))
    assertThat(keys(r2).toArray).containsExactly("t1:100", "t2:200")
  }

  @Test
  def severalCompleteTransactionsEmitInOneStepInQueueOrder(): Unit = {
    val r = run(mk("t1", "100", 1), mk("t2", "200", 1), mk("t3", "300", 1),
      li("t3", "300", "p3"), li("t1", "100", "p1"), li("t2", "200", "p2"))

    assertThat(keys(r).toArray).containsExactly("t1:100", "t2:200", "t3:300")
  }

  @Test
  def incompleteHeadBlocksEverythingBehindIt(): Unit = {
    run(mk("t1", "100", 1), mk("t2", "200", 0), mk("t3", "300", 0))

    assertThat(run().emitted.isEmpty).isTrue
    assertThat(store.getCursor.queueDepth).isEqualTo(3L)
    assertThat(store.buffers.keySet.toSeq.sorted.toArray).containsExactly("t1:100", "t2:200", "t3:300")
  }

  // ---- blocked tracking (3.5) ----

  @Test
  def noAlertBeforeThreshold(): Unit = {
    run(mk("t1", "100", 1), mk("t2", "200", 0))
    nowMs += ThresholdMs

    assertThat(seq.onTimer().alert.isEmpty).isTrue
  }

  @Test
  def alertAfterThresholdWithNoNewInput(): Unit = {
    run(mk("t1", "100", 2), mk("t2", "200", 0), li("t1", "100", "p1"))
    nowMs += 11 * 60 * 1000L

    val alert = seq.onTimer().alert

    assertThat(alert.isDefined).isTrue
    assertThat(alert.get.headTxKey).isEqualTo("t1:100")
    assertThat(alert.get.queuedBehind).isEqualTo(1L)
    assertThat(alert.get.progress).contains("LineItems: 1/2")
  }

  @Test
  def blockedMeasurementResetsAfterHeadEmits(): Unit = {
    run(mk("t1", "100", 1), mk("t2", "200", 1))
    nowMs += 11 * 60 * 1000L
    assertThat(seq.onTimer().alert.isDefined).isTrue

    run(li("t1", "100", "p1"))
    assertThat(store.getCursor.blockedSinceMs).isEqualTo(-1L)

    nowMs += 1000L
    assertThat(seq.onTimer().alert.isEmpty).isTrue
  }

  // ---- orphan-buffer purge (buffer-retention) ----

  @Test
  def historyWithoutMarkersIsRemovedOnceTheCursorPassesIt(): Unit = {
    run((100 until 1100).map(c => li(s"h$c", c.toString, "p1")): _*)
    assertThat(store.buffers.size).isEqualTo(1000)

    assertThat(keys(run(mk("t", "2000", 0))).toArray).containsExactly("t:2000")
    assertThat(store.buffers.isEmpty).isTrue
  }

  @Test
  def dataAheadOfItsMarkerIsKept(): Unit = {
    run(li("f", "600", "p1"), mk("e", "500", 0))
    assertThat(store.buffers.contains("f:600")).isTrue

    assertThat(keys(run(mk("f", "600", 1))).toArray).containsExactly("f:600")
  }

  @Test
  def bufferAtTheLastEmittedCsnIsKept(): Unit = {
    run(li("b", "500", "p1"), mk("a", "500", 0))
    assertThat(store.buffers.contains("b:500")).isTrue

    assertThat(keys(run(mk("b", "500", 1))).toArray).containsExactly("b:500")
  }

  @Test
  def nothingIsRemovedBeforeTheFirstEmission(): Unit = {
    run(li("a", "100", "p1"), li("b", "50", "p1"), mk("h", "200", 1))

    assertThat(store.buffers.keySet.toArray).containsExactlyInAnyOrder("a:100", "b:50", "h:200")
  }

  @Test
  def incompleteHeadStaysQueuedWhileALowerOrphanIsRemoved(): Unit = {
    run(li("o", "300", "p1"))

    run(mk("e", "500", 0), mk("h", "700", 1), mk("z", "800", 0))

    assertThat(store.buffers.contains("o:300")).isFalse
    assertThat(store.buffers.keySet.toArray).containsExactlyInAnyOrder("h:700", "z:800")
    assertThat(store.getCursor.queueDepth).isEqualTo(2L)
    assertThat(run().emitted.isEmpty).isTrue
  }

  @Test
  def largeHistoryIsRemovedOverSeveralSteps(): Unit = {
    val total = TransactionSequencer.MaxPurgePerStep + 5000
    (0 until total).foreach { i =>
      val s = new TransactionState()
      s.xid = s"h$i"
      s.csn = i.toString
      store.buffers(s"h$i:$i") = s
    }

    run(mk("t1", "1000000", 0))
    assertThat(store.buffers.size).isEqualTo(5000)

    run(mk("t2", "1000001", 0))
    assertThat(store.buffers.isEmpty).isTrue
  }

  // ---- drop records (dropped-event-audit) ----

  private def only(r: SequencerResult): DroppedEvent = {
    assertThat(r.dropped.size).isEqualTo(1)
    r.dropped.head
  }

  @Test
  def replayedEventBelowTheCursorIsRecorded(): Unit = {
    run(mk("e", "500", 0))
    val src = KafkaSource("dev.appuser.order_line_items.json", 0, 371L)

    val d = only(run(DataInput(lineItem("a", "400", "p1", lineItemId = 1), src)))

    assertThat(d.itemKind).isEqualTo(ItemKind.DataEvent)
    assertThat(d.reason).isEqualTo(Reason.ReplayBelowCursor)
    assertThat(d.xid).isEqualTo("a")
    assertThat(d.csn).isEqualTo("400")
    assertThat(d.lastEmittedCsn).isEqualTo("500")
    assertThat(d.kafkaTopic).isEqualTo("dev.appuser.order_line_items.json")
    assertThat(d.kafkaOffset).isEqualTo(371L)
    assertThat(d.tableName).isEqualTo("ORDER_LINE_ITEMS")
  }

  @Test
  def replayedEventAtTheCursorForAnEmittedTransactionIsRecorded(): Unit = {
    run(mk("a", "500", 1), li("a", "500", "p1"))

    val d = only(run(li("a", "500", "p1")))

    assertThat(d.reason).isEqualTo(Reason.ReplayAlreadyEmitted)
    assertThat(d.lastEmittedCsn).isEqualTo("500")
  }

  @Test
  def replayedMarkerIsRecordedWithItsSource(): Unit = {
    run(mk("e", "500", 0))

    val d = only(run(MarkerInput(marker("a", "400", lineItems = 2), 0, 7L, 1, "dev.transaction_metadata_json")))

    assertThat(d.itemKind).isEqualTo(ItemKind.Marker)
    assertThat(d.reason).isEqualTo(Reason.ReplayBelowCursor)
    assertThat(d.kafkaOffset).isEqualTo(7L)
    assertThat(d.markerIndex).isEqualTo(1)
    assertThat(d.markerEventCount).isEqualTo(2)
  }

  @Test
  def duplicateMarkerIsRecordedWithItsCounts(): Unit = {
    run(mk("a", "100", 2))

    val d = only(run(mk("a", "100", 2)))

    assertThat(d.itemKind).isEqualTo(ItemKind.Marker)
    assertThat(d.reason).isEqualTo(Reason.DuplicateMarker)
    assertThat(d.markerEventCount).isEqualTo(2)
    assertThat(d.dataCollections.size).isGreaterThan(0)
  }

  @Test
  def duplicateEventIsRecorded(): Unit = {
    run(li("a", "100", "p1"))

    assertThat(only(run(li("a", "100", "p1"))).reason).isEqualTo(Reason.DuplicateEvent)
  }

  @Test
  def eventForAnUntrackedTableIsRecorded(): Unit = {
    val d = only(run(DataInput(event("a", "100", "CUSTOMERS", "p1"))))

    assertThat(d.reason).isEqualTo(Reason.UnknownTable)
    assertThat(d.tableName).isEqualTo("CUSTOMERS")
  }

  @Test
  def purgedBufferIsOneSummaryRowWithCounts(): Unit = {
    run(DataInput(order("h", "100", "p1")), DataInput(event("h", "100", "ORDER_DETAILS", "p2")),
      li("h", "100", "p3"), li("h", "100", "p4"), li("h", "100", "p5"))

    val d = only(run(mk("t", "200", 0)))

    assertThat(d.itemKind).isEqualTo(ItemKind.PurgedBuffer)
    assertThat(d.reason).isEqualTo(Reason.NoMarkerBelowCursor)
    assertThat(d.xid).isEqualTo("h")
    assertThat(d.csn).isEqualTo("100")
    assertThat(d.bufferedOrders).isEqualTo(1)
    assertThat(d.bufferedDetails).isEqualTo(1)
    assertThat(d.bufferedLineItems).isEqualTo(3)
    assertThat(d.lastEmittedCsn).isEqualTo("200")
  }

  @Test
  def acceptedInputProducesNoDropRecords(): Unit = {
    val r = run(mk("a", "100", 1), li("a", "100", "p1"))

    assertThat(keys(r).toArray).containsExactly("a:100")
    assertThat(r.dropped.isEmpty).isTrue
  }

  @Test
  def summaryModeFoldsReplaysOfOneTransaction(): Unit = {
    val summarizing = new TransactionSequencer(store, ThresholdMs, () => nowMs, summarizeReplays = true)
    summarizing.process(Iterator(mk("e", "500", 0)))

    val inputs = (1 to 350).map(i => li("a", "400", s"p$i")) ++
      Seq(DataInput(event("b", "600", "CUSTOMERS", "x1")), DataInput(event("b", "600", "CUSTOMERS", "x2")))
    val r = summarizing.process(inputs.iterator)

    val replay = r.dropped.filter(_.reason == Reason.ReplayBelowCursor)
    assertThat(replay.size).isEqualTo(1)
    assertThat(replay.head.droppedCount).isEqualTo(350)
    assertThat(replay.head.xid).isEqualTo("a")
    assertThat(replay.head.pos).isNull()
    assertThat(r.dropped.count(_.reason == Reason.UnknownTable)).isEqualTo(2)
    assertThat(r.dropped.filter(_.reason == Reason.UnknownTable).map(_.droppedCount).toArray).containsOnly(1)
  }

  @Test
  def rawDropIsRecordedAndLeavesStateUnchanged(): Unit = {
    run(mk("e", "500", 0), li("a", "600", "p1"))
    val cursorBefore = store.getCursor.lastEmittedCsn
    val buffersBefore = store.buffers.keySet.toSet
    val queueBefore = store.queue.toMap

    val src = KafkaSource("dev.transaction_metadata_json", 0, 12L)
    val d = only(run(RawDropInput(Reason.Unparseable, "{\"xid\":", null, null, src)))

    assertThat(d.itemKind).isEqualTo(ItemKind.RawMessage)
    assertThat(d.reason).isEqualTo(Reason.Unparseable)
    assertThat(d.rawValue).isEqualTo("{\"xid\":")
    assertThat(d.kafkaOffset).isEqualTo(12L)
    assertThat(d.lastEmittedCsn).isEqualTo("500")
    assertThat(store.getCursor.lastEmittedCsn).isEqualTo(cursorBefore)
    assertThat(store.buffers.keySet.toSet == buffersBefore).isTrue
    assertThat(store.queue.toMap == queueBefore).isTrue
  }
}
