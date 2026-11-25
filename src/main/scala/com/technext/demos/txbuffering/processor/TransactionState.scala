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

import com.technext.demos.txbuffering.model.generic.{DataChangeEvent, TransactionMetadata}

/**
 * A transaction's events grouped by table, in buffered order. The sequencer assembles one when it
 * emits a transaction and hands it to [[OrderJoiner]]; while a transaction is buffered, state holds
 * a [[TransactionSummary]] and one entry per event instead.
 */
class TransactionState extends Serializable {

  @BeanProperty var xid: String = _
  @BeanProperty var csn: String = _

  @BeanProperty var orderEvents: ju.List[DataChangeEvent] = new ju.ArrayList[DataChangeEvent]()
  @BeanProperty var orderDetailEvents: ju.List[DataChangeEvent] = new ju.ArrayList[DataChangeEvent]()
  @BeanProperty var orderLineItemEvents: ju.List[DataChangeEvent] = new ju.ArrayList[DataChangeEvent]()

  private var _metadata: TransactionMetadata = _
  private var _expected: TableCounts = TableCounts.Zero

  def getMetadata: TransactionMetadata = _metadata

  def setMetadata(metadata: TransactionMetadata): Unit = {
    _metadata = metadata
    if (metadata != null) _expected = TableCounts.expectedBy(metadata)
  }

  def getExpectedOrderCount: Int = _expected.orders
  def getExpectedOrderDetailCount: Int = _expected.details
  def getExpectedOrderLineItemCount: Int = _expected.lineItems

  /**
   * Buffers an event unless an event with the same GoldenGate trail position (`pos`)
   * is already buffered for the same table. Re-sent events therefore never count twice.
   *
   * @return Added if the event was buffered; DuplicatePos or UnknownTable if it was ignored
   */
  def addEvent(event: DataChangeEvent): TransactionState.AddResult = {
    val target = listFor(event)
    if (target == null) TransactionState.UnknownTable
    else if (containsPos(target, event.pos)) TransactionState.DuplicatePos
    else {
      target.add(event)
      TransactionState.Added
    }
  }

  /** Appends an event already accepted (and de-duplicated) by the sequencer, without a scan. */
  private[processor] def appendAccepted(event: DataChangeEvent): Unit = {
    val target = listFor(event)
    if (target == null)
      throw new IllegalStateException(s"Buffered event pos=${event.pos} of $xid:$csn has untracked table ${event.table}")
    target.add(event)
  }

  private def listFor(event: DataChangeEvent): ju.List[DataChangeEvent] = event.tableName match {
    case TransactionCompletion.Orders         => orderEvents
    case TransactionCompletion.OrderDetails   => orderDetailEvents
    case TransactionCompletion.OrderLineItems => orderLineItemEvents
    case _                                    => null
  }

  private def containsPos(events: ju.List[DataChangeEvent], pos: String): Boolean =
    pos != null && events.stream().anyMatch(e => pos == e.pos)

  private def received: TableCounts =
    TableCounts(orderEvents.size, orderDetailEvents.size, orderLineItemEvents.size)

  /** See [[TransactionCompletion.isComplete]]. */
  def isComplete: Boolean = TransactionCompletion.isComplete(xid, csn, _metadata != null, _expected, received)

  def progress: String = TransactionCompletion.progress(_expected, received)
}

object TransactionState {
  /** Outcome of [[TransactionState.addEvent]]. */
  sealed trait AddResult
  case object Added extends AddResult
  case object DuplicatePos extends AddResult
  case object UnknownTable extends AddResult
}
