/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.processor

import java.{util => ju}

import scala.jdk.CollectionConverters._

import com.technext.demos.txbuffering.model.generic.{DataChangeEvent, TransactionMetadata}

/**
 * A transaction's events grouped by tracked table, each table's in buffered order. The sequencer
 * assembles one when it emits a transaction; while a transaction is buffered, state holds a
 * [[TransactionSummary]] and one entry per event instead.
 */
class TransactionState(val tracked: Seq[String]) extends Serializable {

  var xid: String = _
  var csn: String = _

  private val byTable: Map[String, ju.List[DataChangeEvent]] =
    tracked.map(t => t -> (new ju.ArrayList[DataChangeEvent](): ju.List[DataChangeEvent])).toMap

  private var _metadata: TransactionMetadata = _
  private var _expected: ju.Map[String, Integer] = new ju.HashMap[String, Integer]()

  def getMetadata: TransactionMetadata = _metadata

  def setMetadata(metadata: TransactionMetadata): Unit = {
    _metadata = metadata
    _expected = TransactionCompletion.expectedBy(metadata, tracked)
  }

  /** A tracked table's events (name without schema), in buffered order. */
  def eventsOf(table: String): Seq[DataChangeEvent] = byTable.get(table).map(_.asScala.toSeq).getOrElse(Nil)

  /** Every event, in tracked-table order and then buffered order. */
  def events: Seq[DataChangeEvent] = tracked.flatMap(eventsOf)

  /**
   * Buffers an event unless an event with the same GoldenGate trail position (`pos`)
   * is already buffered for the same table. Re-sent events therefore never count twice.
   *
   * @return Added if the event was buffered; DuplicatePos or UnknownTable if it was ignored
   */
  def addEvent(event: DataChangeEvent): TransactionState.AddResult =
    byTable.get(event.tableName) match {
      case None => TransactionState.UnknownTable
      case Some(target) if event.pos != null && target.asScala.exists(_.pos == event.pos) => TransactionState.DuplicatePos
      case Some(target) =>
        target.add(event)
        TransactionState.Added
    }

  /** Appends an event already accepted (and de-duplicated) by the sequencer, without a scan. */
  private[processor] def appendAccepted(event: DataChangeEvent): Unit =
    byTable.getOrElse(event.tableName,
      throw new IllegalStateException(s"Buffered event pos=${event.pos} of $xid:$csn has untracked table ${event.table}"))
      .add(event)

  private def received: ju.Map[String, Integer] = {
    val m = new ju.HashMap[String, Integer]()
    byTable.foreach { case (t, l) => m.put(t, l.size) }
    m
  }

  /** See [[TransactionCompletion.isComplete]]. */
  def isComplete: Boolean = TransactionCompletion.isComplete(xid, csn, _metadata != null, _expected, received, tracked)

  def progress: String = TransactionCompletion.progress(_expected, received, tracked)
}

object TransactionState {
  /** Outcome of [[TransactionState.addEvent]]. */
  sealed trait AddResult
  case object Added extends AddResult
  case object DuplicatePos extends AddResult
  case object UnknownTable extends AddResult
}
