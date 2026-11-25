/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.processor

import java.{util => ju}

import scala.beans.BeanProperty

import com.technext.demos.txbuffering.model.generic.DataChangeEvent

/**
 * Storage the [[TransactionSequencer]] needs. Implemented over Spark state handles in production
 * and in memory in tests, so the sequencing logic can be tested without a Spark runtime.
 *
 *  - summaries: one [[TransactionSummary]] per buffered transaction, keyed by `xid:csn`
 *  - events:    each buffered event once, keyed by transaction and its index in buffered order
 *  - positions: the `(table, pos)` pairs buffered per transaction, for duplicate detection
 *  - queue:     emission queue keyed by a sequence number assigned in marker order
 *  - cursor:    queue bounds plus the record of what has already been emitted
 *
 * Every operation reads or writes one small entry, so the cost of buffering an event doesn't
 * depend on how many events its transaction already holds. Implementations build keys with the
 * helpers in the [[TransactionSequencer]] companion.
 */
trait SequencerStore {
  def getSummary(txKey: String): Option[TransactionSummary]
  def putSummary(txKey: String, summary: TransactionSummary): Unit
  def removeSummary(txKey: String): Unit
  /** Keys (`xid:csn`) of every buffered transaction. */
  def summaryKeys: Iterator[String]

  def getEvent(txKey: String, index: Int): Option[DataChangeEvent]
  def putEvent(txKey: String, index: Int, event: DataChangeEvent): Unit
  def removeEvent(txKey: String, index: Int): Unit

  def positionExists(txKey: String, table: String, pos: String): Boolean
  def putPosition(txKey: String, table: String, pos: String, index: Int): Unit
  def removePosition(txKey: String, table: String, pos: String): Unit

  def queueGet(seq: Long): Option[String]
  def queuePut(seq: Long, txKey: String): Unit
  def queueRemove(seq: Long): Unit

  def getCursor: Cursor
  def putCursor(cursor: Cursor): Unit
}

/**
 * Sequencer bookkeeping. Bean-shaped so Spark can encode it as a single state value.
 *
 * Queue entries live at sequence numbers `[nextSeqToEmit, nextSeqToAssign)`.
 * `lastEmittedCsn` is the highest CSN emitted so far (decimal string) and `emittedAtLastCsn`
 * holds the xids emitted at exactly that CSN; together they identify every emitted transaction.
 */
class Cursor extends Serializable {
  @BeanProperty var nextSeqToEmit: Long = 0L
  @BeanProperty var nextSeqToAssign: Long = 0L
  @BeanProperty var lastEmittedCsn: String = _
  @BeanProperty var emittedAtLastCsn: ju.List[String] = new ju.ArrayList[String]()
  @BeanProperty var blockedSinceMs: Long = -1L
  @BeanProperty var timerMs: Long = -1L

  def queueDepth: Long = nextSeqToAssign - nextSeqToEmit
}
