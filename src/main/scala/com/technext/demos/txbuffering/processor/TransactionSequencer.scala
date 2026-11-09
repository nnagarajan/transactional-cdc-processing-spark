/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.processor

import java.math.BigDecimal

import scala.collection.mutable

import org.apache.logging.log4j.scala.Logging

import com.technext.demos.txbuffering.model.generic.{DataChangeEvent, DroppedEvent, KafkaSource, TransactionMetadata}
import com.technext.demos.txbuffering.model.generic.DroppedEvent.Reason

/** One input row for the sequencer. */
sealed trait SequencerInput

/**
 * A transaction marker read from the metadata topic at the given Kafka partition and offset.
 * `index` is the marker's position within its Kafka message: one message carries several markers,
 * in commit order, when GoldenGate's GROUPTRANSOPS delivered several source transactions together.
 */
final case class MarkerInput(metadata: TransactionMetadata, partition: Int, offset: Long, index: Int = 0,
    topic: String = null) extends SequencerInput {
  def source: KafkaSource = KafkaSource(topic, partition, offset, index)
}

/** A CDC data event from one of the table topics, with where it was read from. */
final case class DataInput(event: DataChangeEvent, source: KafkaSource = KafkaSource.Unknown) extends SequencerInput

/**
 * A Kafka message that couldn't be used (not valid JSON, or no xid/csn). It never touches state:
 * the sequencer only records it as dropped.
 */
final case class RawDropInput(reason: String, rawValue: String, xid: String, csn: String, source: KafkaSource)
    extends SequencerInput

/** The head-of-line transaction has blocked emission for longer than the configured threshold. */
final case class BlockedAlert(headTxKey: String, progress: String, queuedBehind: Long, blockedForMs: Long)

/**
 * Transactions emitted by one sequencer step, in emission order, plus any blocked alert, plus a
 * record of every input the step dropped.
 */
final case class SequencerResult(emitted: Seq[TransactionState], alert: Option[BlockedAlert],
    dropped: Seq[DroppedEvent] = Seq.empty)

/**
 * Emits complete transactions strictly in the order their markers arrive on the
 * single-partition metadata topic, which is source commit order.
 *
 * Rules (C = highest CSN emitted so far, compared numerically):
 *  - A marker from a partition other than 0 fails processing.
 *  - A marker or data event is a replay, and is dropped, if its CSN is below C, or equal to C
 *    with an xid already emitted at C.
 *  - A marker for a transaction that already has one is a duplicate and is dropped.
 *  - Accepted markers are queued in Kafka offset order; data events go into their
 *    transaction's buffer, deduplicated on `pos`.
 *  - After each step, transactions are emitted from the queue head while the head is complete.
 *    An incomplete head blocks everything behind it; it is never skipped.
 *  - When C advances, buffers with a CSN below C are removed: they have no marker (a queued
 *    transaction never has a CSN below C), and any marker for them would now be dropped as a replay.
 *    This clears data that never gets a marker, such as history from before markers existed.
 */
class TransactionSequencer(store: SequencerStore, blockedAlertThresholdMs: Long, clock: () => Long,
    summarizeReplays: Boolean = false) extends Logging {

  /** Applies a micro-batch of inputs, then emits whatever has become emittable. */
  def process(inputs: Iterator[SequencerInput]): SequencerResult = {
    val cursor = store.getCursor
    val dropped = mutable.ArrayBuffer[DroppedEvent]()
    val markers = mutable.ArrayBuffer[MarkerInput]()

    inputs.foreach {
      case m: MarkerInput =>
        if (m.partition != 0)
          throw new IllegalStateException(
            s"Transaction metadata topic must have exactly one partition, but a marker for " +
              s"${m.metadata.xid}:${m.metadata.csn} was read from partition ${m.partition}. " +
              "Recreate the topic with a single partition (see gg-configs/README.md).")
        markers += m
      case d: DataInput => acceptData(cursor, d, dropped)
      case r: RawDropInput =>
        logger.warn(s"Dropping unusable message at ${r.source.topic}:${r.source.partition}:${r.source.offset} (${r.reason})")
        dropped += DroppedEvent.ofRawMessage(r.reason, r.rawValue, r.xid, r.csn, cursor.lastEmittedCsn, r.source)
    }

    // Kafka delivers one partition in offset order, but rows reach this task through a shuffle;
    // sort so queue order never depends on shuffle delivery order. Markers sharing a message
    // (GROUPTRANSOPS grouping) keep their order within it.
    markers.sortBy(m => (m.offset, m.index)).foreach(acceptMarker(cursor, _, dropped))

    val csnBefore = cursor.lastEmittedCsn
    val emitted = emitReady(cursor)
    if (cursor.lastEmittedCsn != null && cursor.lastEmittedCsn != csnBefore) purgeBelow(cursor.lastEmittedCsn, dropped)
    val alert = trackBlocked(cursor, emitted.nonEmpty)
    store.putCursor(cursor)
    SequencerResult(emitted, alert, if (summarizeReplays) summarize(dropped.toSeq) else dropped.toSeq)
  }

  /** Re-evaluates emission and blocking with no new input (timer expiry). */
  def onTimer(): SequencerResult = process(Iterator.empty)

  private def acceptMarker(cursor: Cursor, m: MarkerInput, dropped: mutable.Buffer[DroppedEvent]): Unit = {
    val md = m.metadata
    val key = TransactionSequencer.txKey(md.xid, md.csn)

    if (isReplay(cursor, md.xid, md.csn)) {
      val reason = replayReason(cursor, md.csn)
      if (reason == Reason.ReplayBelowCursor)
        logger.warn(s"Dropping marker $key at offset ${m.offset}: CSN is below last emitted CSN " +
          s"${cursor.lastEmittedCsn}. Expected only after a replicat restart; otherwise markers are out of commit order.")
      else
        logger.info(s"Dropping replayed marker $key at offset ${m.offset}: already emitted")
      dropped += DroppedEvent.ofMarker(md, reason, cursor.lastEmittedCsn, m.source)
      return
    }

    val existing = store.getBuffer(key)
    if (existing.exists(_.getMetadata != null)) {
      logger.info(s"Dropping duplicate marker $key at offset ${m.offset}")
      dropped += DroppedEvent.ofMarker(md, Reason.DuplicateMarker, cursor.lastEmittedCsn, m.source)
      return
    }

    val state = existing.getOrElse(newState(md.xid, md.csn))
    state.setMetadata(md)
    store.putBuffer(key, state)
    store.queuePut(cursor.nextSeqToAssign, key)
    cursor.nextSeqToAssign += 1
  }

  private def acceptData(cursor: Cursor, d: DataInput, dropped: mutable.Buffer[DroppedEvent]): Unit = {
    val e = d.event
    if (isReplay(cursor, e.xid, e.csn)) {
      logger.debug(s"Dropping event pos=${e.pos} for already emitted transaction ${e.xid}:${e.csn}")
      dropped += DroppedEvent.ofEvent(e, replayReason(cursor, e.csn), cursor.lastEmittedCsn, d.source)
      return
    }
    val key = TransactionSequencer.txKey(e.xid, e.csn)
    val state = store.getBuffer(key).getOrElse(newState(e.xid, e.csn))
    state.addEvent(e) match {
      case TransactionState.Added => store.putBuffer(key, state)
      case TransactionState.DuplicatePos =>
        logger.debug(s"Ignoring duplicate event pos=${e.pos} for $key")
        dropped += DroppedEvent.ofEvent(e, Reason.DuplicateEvent, cursor.lastEmittedCsn, d.source)
      case TransactionState.UnknownTable =>
        logger.debug(s"Ignoring event pos=${e.pos} for $key: table ${e.table} is not tracked")
        dropped += DroppedEvent.ofEvent(e, Reason.UnknownTable, cursor.lastEmittedCsn, d.source)
    }
  }

  private def emitReady(cursor: Cursor): Seq[TransactionState] = {
    val out = mutable.ArrayBuffer[TransactionState]()
    var blocked = false
    while (!blocked && cursor.queueDepth > 0) {
      val seq = cursor.nextSeqToEmit
      val key = store.queueGet(seq).getOrElse(
        throw new IllegalStateException(s"Emission queue is missing sequence $seq"))
      val state = store.getBuffer(key).getOrElse(
        throw new IllegalStateException(s"Queued transaction $key has no buffer"))

      if (state.isComplete) {
        out += state
        store.removeBuffer(key)
        store.queueRemove(seq)
        cursor.nextSeqToEmit += 1
        recordEmitted(cursor, state.xid, state.csn)
      } else blocked = true
    }
    out.toSeq
  }

  /**
   * Removes buffers whose CSN is below `bound`, at most [[TransactionSequencer.MaxPurgePerStep]]
   * per step; the rest go in later steps where the cursor advances. Keys are collected first and
   * removed after the scan, so nothing is removed while the state iterator is open.
   */
  private def purgeBelow(bound: String, dropped: mutable.Buffer[DroppedEvent]): Unit = {
    val limit = new BigDecimal(bound)
    val toRemove = store.bufferKeys
      .filter(k => TransactionSequencer.csnOf(k).exists(_.compareTo(limit) < 0))
      .take(TransactionSequencer.MaxPurgePerStep)
      .toVector
    if (toRemove.nonEmpty) {
      toRemove.foreach { key =>
        store.getBuffer(key).foreach { s =>
          dropped += DroppedEvent.ofPurgedBuffer(s.xid, s.csn, s.orderEvents.size, s.orderDetailEvents.size,
            s.orderLineItemEvents.size, bound)
        }
        store.removeBuffer(key)
      }
      logger.info(s"Removed ${toRemove.size} buffered transaction(s) without a marker below CSN $bound" +
        (if (toRemove.size == TransactionSequencer.MaxPurgePerStep) "; more may remain for later steps" else ""))
    }
  }

  private def trackBlocked(cursor: Cursor, emittedAny: Boolean): Option[BlockedAlert] = {
    if (emittedAny || cursor.queueDepth == 0) {
      cursor.blockedSinceMs = -1L
      return None
    }
    val now = clock()
    if (cursor.blockedSinceMs < 0) cursor.blockedSinceMs = now
    val blockedFor = now - cursor.blockedSinceMs
    if (blockedFor <= blockedAlertThresholdMs) return None

    val headKey = store.queueGet(cursor.nextSeqToEmit).get
    val progress = store.getBuffer(headKey).map(_.progress).getOrElse("unknown")
    val alert = BlockedAlert(headKey, progress, cursor.queueDepth - 1, blockedFor)
    logger.error(s"Transaction $headKey has blocked emission for ${blockedFor / 1000}s; " +
      s"${alert.queuedBehind} transaction(s) queued behind it. Progress: $progress")
    Some(alert)
  }

  private def recordEmitted(cursor: Cursor, xid: String, csn: String): Unit =
    if (cursor.lastEmittedCsn == null || compareCsn(csn, cursor.lastEmittedCsn) > 0) {
      cursor.lastEmittedCsn = csn
      cursor.emittedAtLastCsn = new java.util.ArrayList[String]()
      cursor.emittedAtLastCsn.add(xid)
    } else cursor.emittedAtLastCsn.add(xid)

  /**
   * Folds replayed items of one transaction (same xid, csn, reason and kind) into one row whose
   * `droppedCount` is the number folded. The row keeps the first item's Kafka coordinates; fields
   * describing one item (pos, images, marker counts) are cleared. Other reasons are kept as they are.
   */
  private def summarize(dropped: Seq[DroppedEvent]): Seq[DroppedEvent] = {
    val (replays, others) = dropped.partition(d => Reason.isReplay(d.reason))
    val folded = replays
      .groupBy(d => (d.xid, d.csn, d.reason, d.itemKind))
      .values
      .map { group =>
        val row = new DroppedEvent()
        val first = group.head
        row.itemKind = first.itemKind
        row.reason = first.reason
        row.lastEmittedCsn = first.lastEmittedCsn
        row.xid = first.xid
        row.csn = first.csn
        row.kafkaTopic = first.kafkaTopic
        row.kafkaPartition = first.kafkaPartition
        row.kafkaOffset = first.kafkaOffset
        row.markerIndex = first.markerIndex
        row.droppedCount = group.size
        row
      }
    others ++ folded.toSeq.sortBy(d => (d.csn, d.xid))
  }

  /** Why a replayed item is dropped: below the cursor, or at it for an xid already emitted. */
  private def replayReason(cursor: Cursor, csn: String): String =
    if (compareCsn(csn, cursor.lastEmittedCsn) < 0) Reason.ReplayBelowCursor else Reason.ReplayAlreadyEmitted

  private def isReplay(cursor: Cursor, xid: String, csn: String): Boolean =
    cursor.lastEmittedCsn != null && {
      val cmp = compareCsn(csn, cursor.lastEmittedCsn)
      cmp < 0 || (cmp == 0 && cursor.emittedAtLastCsn.contains(xid))
    }

  private def compareCsn(a: String, b: String): Int = new BigDecimal(a).compareTo(new BigDecimal(b))

  private def newState(xid: String, csn: String): TransactionState = {
    val s = new TransactionState()
    s.xid = xid
    s.csn = csn
    s
  }
}

object TransactionSequencer {
  /** Most buffers removed by one purge, bounding memory and batch time. */
  val MaxPurgePerStep = 100000

  def txKey(xid: String, csn: String): String = s"$xid:$csn"

  /** The CSN part of a buffer key (text after the last ':'), or None if it isn't a number. */
  def csnOf(txKey: String): Option[BigDecimal] = {
    val i = txKey.lastIndexOf(':')
    if (i < 0) None else scala.util.Try(new BigDecimal(txKey.substring(i + 1))).toOption
  }
}
