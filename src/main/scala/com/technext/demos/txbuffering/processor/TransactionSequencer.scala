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

import com.technext.demos.txbuffering.model.generic.{DataChangeEvent, TransactionMetadata}

/** One input row for the sequencer. */
sealed trait SequencerInput

/** A transaction marker read from the metadata topic at the given Kafka partition and offset. */
final case class MarkerInput(metadata: TransactionMetadata, partition: Int, offset: Long) extends SequencerInput

/** A CDC data event from one of the table topics. */
final case class DataInput(event: DataChangeEvent) extends SequencerInput

/** The head-of-line transaction has blocked emission for longer than the configured threshold. */
final case class BlockedAlert(headTxKey: String, progress: String, queuedBehind: Long, blockedForMs: Long)

/** Transactions emitted by one sequencer step, in emission order, plus any blocked alert. */
final case class SequencerResult(emitted: Seq[TransactionState], alert: Option[BlockedAlert])

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
 */
class TransactionSequencer(store: SequencerStore, blockedAlertThresholdMs: Long, clock: () => Long)
    extends Logging {

  /** Applies a micro-batch of inputs, then emits whatever has become emittable. */
  def process(inputs: Iterator[SequencerInput]): SequencerResult = {
    val cursor = store.getCursor
    val markers = mutable.ArrayBuffer[MarkerInput]()

    inputs.foreach {
      case m: MarkerInput =>
        if (m.partition != 0)
          throw new IllegalStateException(
            s"Transaction metadata topic must have exactly one partition, but a marker for " +
              s"${m.metadata.xid}:${m.metadata.csn} was read from partition ${m.partition}. " +
              "Recreate the topic with a single partition (see gg-configs/README.md).")
        markers += m
      case DataInput(e) => acceptData(cursor, e)
    }

    // Kafka delivers one partition in offset order, but rows reach this task through a shuffle;
    // sort so queue order never depends on shuffle delivery order.
    markers.sortBy(_.offset).foreach(acceptMarker(cursor, _))

    val emitted = emitReady(cursor)
    val alert = trackBlocked(cursor, emitted.nonEmpty)
    store.putCursor(cursor)
    SequencerResult(emitted, alert)
  }

  /** Re-evaluates emission and blocking with no new input (timer expiry). */
  def onTimer(): SequencerResult = process(Iterator.empty)

  private def acceptMarker(cursor: Cursor, m: MarkerInput): Unit = {
    val md = m.metadata
    val key = TransactionSequencer.txKey(md.xid, md.csn)

    if (isReplay(cursor, md.xid, md.csn)) {
      if (compareCsn(md.csn, cursor.lastEmittedCsn) < 0)
        logger.warn(s"Dropping marker $key at offset ${m.offset}: CSN is below last emitted CSN " +
          s"${cursor.lastEmittedCsn}. Expected only after a replicat restart; otherwise markers are out of commit order.")
      else
        logger.info(s"Dropping replayed marker $key at offset ${m.offset}: already emitted")
      return
    }

    val existing = store.getBuffer(key)
    if (existing.exists(_.getMetadata != null)) {
      logger.info(s"Dropping duplicate marker $key at offset ${m.offset}")
      return
    }

    val state = existing.getOrElse(newState(md.xid, md.csn))
    state.setMetadata(md)
    store.putBuffer(key, state)
    store.queuePut(cursor.nextSeqToAssign, key)
    cursor.nextSeqToAssign += 1
  }

  private def acceptData(cursor: Cursor, e: DataChangeEvent): Unit = {
    if (isReplay(cursor, e.xid, e.csn)) {
      logger.debug(s"Dropping event pos=${e.pos} for already emitted transaction ${e.xid}:${e.csn}")
      return
    }
    val key = TransactionSequencer.txKey(e.xid, e.csn)
    val state = store.getBuffer(key).getOrElse(newState(e.xid, e.csn))
    if (state.addEvent(e)) store.putBuffer(key, state)
    else logger.debug(s"Ignoring duplicate or unknown-table event pos=${e.pos} for $key")
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
  def txKey(xid: String, csn: String): String = s"$xid:$csn"
}
