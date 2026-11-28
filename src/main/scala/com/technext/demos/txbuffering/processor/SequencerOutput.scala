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
import scala.jdk.CollectionConverters._

import com.technext.demos.txbuffering.model.generic.{DataChangeEvent, DroppedEvent}

/**
 * A transaction the sequencer emitted, whole: its events in tracked-table order, then buffered
 * order, and the time it was emitted. The same for every job model, so the operator's output
 * (and checkpoint) never depends on one; the sink shapes it into stream records.
 */
class EmittedTransaction extends Serializable {
  @BeanProperty var xid: String = _
  @BeanProperty var csn: String = _
  @BeanProperty var processedTs: String = _
  @BeanProperty var events: ju.List[DataChangeEvent] = _
}

object EmittedTransaction {
  def apply(state: TransactionState, processedTs: String): EmittedTransaction = {
    val t = new EmittedTransaction()
    t.xid = state.xid
    t.csn = state.csn
    t.processedTs = processedTs
    t.events = new ju.ArrayList[DataChangeEvent](state.events.asJava)
    t
  }
}

/**
 * One output row of the sequencer: an emitted transaction, or a record of a dropped input.
 * Encoders.bean can't encode a sealed trait, so this is a wrapper with one non-null side.
 */
class SequencerOutput extends Serializable {
  @BeanProperty var kind: String = _
  @BeanProperty var transaction: EmittedTransaction = _
  @BeanProperty var dropped: DroppedEvent = _
}

object SequencerOutput {
  val TransactionKind = "TRANSACTION"
  val DroppedKind = "DROPPED"

  def ofTransaction(t: EmittedTransaction): SequencerOutput = {
    val s = new SequencerOutput()
    s.kind = TransactionKind
    s.transaction = t
    s
  }

  def ofDropped(d: DroppedEvent): SequencerOutput = {
    val s = new SequencerOutput()
    s.kind = DroppedKind
    s.dropped = d
    s
  }
}
