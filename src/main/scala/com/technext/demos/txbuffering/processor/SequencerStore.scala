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

/**
 * Storage the [[TransactionSequencer]] needs. Implemented over Spark state handles in production
 * and in memory in tests, so the sequencing logic can be tested without a Spark runtime.
 *
 *  - buffers: per-transaction state keyed by `xid:csn`
 *  - queue:   emission queue keyed by a sequence number assigned in marker order
 *  - cursor:  queue bounds plus the record of what has already been emitted
 */
trait SequencerStore {
  def getBuffer(txKey: String): Option[TransactionState]
  def putBuffer(txKey: String, state: TransactionState): Unit
  def removeBuffer(txKey: String): Unit

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
