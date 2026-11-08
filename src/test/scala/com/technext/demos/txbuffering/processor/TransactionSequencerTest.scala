package com.technext.demos.txbuffering.processor

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

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
}
