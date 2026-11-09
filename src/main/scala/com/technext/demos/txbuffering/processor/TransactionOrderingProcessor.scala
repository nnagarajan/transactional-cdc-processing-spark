/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.processor

import org.apache.logging.log4j.scala.Logger
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{ExpiredTimerInfo, MapState, OutputMode, StatefulProcessor, TimeMode, TimerValues, TTLConfig, ValueState}

import com.technext.demos.txbuffering.model.generic.DroppedEvent
import com.technext.demos.txbuffering.model.order.OrderStream

/**
 * Spark adapter for [[TransactionSequencer]]. All rows share one key, so this processor sees
 * every in-flight transaction and can emit them in commit order.
 *
 * State (see design D3): per-transaction buffers, an emission queue keyed by sequence number,
 * and a cursor. Only entries touched in a batch are written.
 *
 * A single processing-time timer is kept registered while transactions are queued, so a
 * blocked head is re-checked and alerted on even when no new input arrives.
 */
class TransactionOrderingProcessor(blockedAlertThresholdMs: Long, checkIntervalMs: Long,
    summarizeReplays: Boolean = false) extends StatefulProcessor[String, Row, SequencerOutput] {

  @transient private var buffers: MapState[String, TransactionState] = _
  @transient private var queue: MapState[Long, String] = _
  @transient private var cursor: ValueState[Cursor] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    val handle = getHandle
    buffers = handle.getMapState("buffers", Encoders.STRING, Encoders.bean(classOf[TransactionState]), TTLConfig.NONE)
    queue = handle.getMapState("queue", Encoders.scalaLong, Encoders.STRING, TTLConfig.NONE)
    cursor = handle.getValueState("cursor", Encoders.bean(classOf[Cursor]), TTLConfig.NONE)
  }

  override def handleInputRows(key: String, rows: Iterator[Row], timerValues: TimerValues): Iterator[SequencerOutput] =
    step(timerValues, expiredTimerMs = -1L)(_.process(rows.map(CdcRows.toSequencerInput)))

  override def handleExpiredTimer(key: String, timerValues: TimerValues, expiredTimerInfo: ExpiredTimerInfo): Iterator[SequencerOutput] =
    step(timerValues, expiredTimerInfo.getExpiryTimeInMs())(_.onTimer())

  private def step(timerValues: TimerValues, expiredTimerMs: Long)(
      run: TransactionSequencer => SequencerResult): Iterator[SequencerOutput] = {
    val now = timerValues.getCurrentProcessingTimeInMs()
    val store = new StateHandleStore
    val result = run(new TransactionSequencer(store, blockedAlertThresholdMs, () => now, summarizeReplays))

    result.emitted.foreach(s =>
      TransactionOrderingProcessor.log.info(s"Emitting transaction ${s.xid}:${s.csn}. Progress: ${s.progress}"))

    rescheduleTimer(store.getCursor, store, now, expiredTimerMs)
    result.emitted.iterator.flatMap(OrderJoiner.joinTransaction).map(SequencerOutput.ofOrder) ++
      result.dropped.iterator.map(SequencerOutput.ofDropped)
  }

  /** Keeps exactly one timer registered while the queue is non-empty, none otherwise. */
  private def rescheduleTimer(c: Cursor, store: StateHandleStore, now: Long, expiredTimerMs: Long): Unit = {
    if (c.timerMs >= 0 && c.timerMs != expiredTimerMs) getHandle.deleteTimer(c.timerMs)
    if (c.queueDepth > 0) {
      c.timerMs = now + checkIntervalMs
      getHandle.registerTimer(c.timerMs)
    } else c.timerMs = -1L
    store.putCursor(c)
  }

  /** [[SequencerStore]] over this processor's state handles. */
  private class StateHandleStore extends SequencerStore {
    override def getBuffer(txKey: String): Option[TransactionState] =
      if (buffers.containsKey(txKey)) Some(buffers.getValue(txKey)) else None
    override def putBuffer(txKey: String, state: TransactionState): Unit = buffers.updateValue(txKey, state)
    override def removeBuffer(txKey: String): Unit = buffers.removeKey(txKey)
    override def bufferKeys: Iterator[String] = buffers.keys()

    override def queueGet(seq: Long): Option[String] =
      if (queue.containsKey(seq)) Some(queue.getValue(seq)) else None
    override def queuePut(seq: Long, txKey: String): Unit = queue.updateValue(seq, txKey)
    override def queueRemove(seq: Long): Unit = queue.removeKey(seq)

    override def getCursor: Cursor = if (cursor.exists()) cursor.get() else new Cursor()
    override def putCursor(c: Cursor): Unit = cursor.update(c)
  }
}

object TransactionOrderingProcessor {

  private val log: Logger = Logger(classOf[TransactionOrderingProcessor])

  /** Grouping key shared by every row: the sequencer needs a global view. */
  val GlobalKey = "all"

  private val RocksDbProvider = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"

  /**
   * Applies commit-order transaction assembly to the unioned CDC stream.
   * Also sets the RocksDB state store provider, which transformWithState requires,
   * so it holds on the spark-submit path as well as the dev path.
   */
  def orderTransactions(allEvents: Dataset[Row], blockedAlertThresholdMs: Long, checkIntervalMs: Long,
      summarizeReplays: Boolean = false): Dataset[SequencerOutput] = {
    allEvents.sparkSession.conf.set("spark.sql.streaming.stateStore.providerClass", RocksDbProvider)

    implicit val keyEncoder: Encoder[String] = Encoders.STRING
    implicit val outputEncoder: Encoder[SequencerOutput] = Encoders.bean(classOf[SequencerOutput])

    // Spark 4.0 exposes the Scala transformWithState API only on the classic (non-Connect)
    // implementation; the base KeyValueGroupedDataset declares it private[sql].
    val grouped = allEvents.groupByKey((_: Row) => GlobalKey) match {
      case classicGrouped: org.apache.spark.sql.classic.KeyValueGroupedDataset[String @unchecked, Row @unchecked] =>
        classicGrouped
      case other =>
        throw new UnsupportedOperationException(
          s"transformWithState requires a classic Spark session, got ${other.getClass.getName}")
    }
    grouped
      .transformWithState(
        new TransactionOrderingProcessor(blockedAlertThresholdMs, checkIntervalMs, summarizeReplays),
        TimeMode.ProcessingTime(),
        OutputMode.Append())
  }

  /** The emitted order records of a sequencer output (streaming or batch). */
  def ordersOf(output: Dataset[SequencerOutput]): Dataset[OrderStream] =
    output.filter(col("kind") === SequencerOutput.OrderKind).select("order.*")
      .as(Encoders.bean(classOf[OrderStream]))

  /** The drop records of a sequencer output (streaming or batch). */
  def droppedOf(output: Dataset[SequencerOutput]): Dataset[DroppedEvent] =
    output.filter(col("kind") === SequencerOutput.DroppedKind).select("dropped.*")
      .as(Encoders.bean(classOf[DroppedEvent]))
}
