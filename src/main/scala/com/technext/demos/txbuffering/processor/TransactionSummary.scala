/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.processor

import scala.beans.BeanProperty

import com.technext.demos.txbuffering.model.generic.TransactionMetadata

/**
 * What state keeps per buffered transaction besides its events: the marker (once it arrives),
 * the number of distinct events received per tracked table, and `eventCount`, the number of
 * events buffered so far. Event `i` (0 until `eventCount`) is stored under
 * [[TransactionSequencer.eventKey]], in buffered order.
 *
 * It is small and fixed in size, so writing it per event keeps buffering cost constant per
 * event, and completeness is decided without reading any events. Bean-shaped for `Encoders.bean`.
 */
class TransactionSummary extends Serializable {

  @BeanProperty var xid: String = _
  @BeanProperty var csn: String = _

  @BeanProperty var receivedOrders: Int = 0
  @BeanProperty var receivedOrderDetails: Int = 0
  @BeanProperty var receivedOrderLineItems: Int = 0
  @BeanProperty var eventCount: Int = 0

  private var _metadata: TransactionMetadata = _
  private var _expected: TableCounts = TableCounts.Zero

  def getMetadata: TransactionMetadata = _metadata

  def setMetadata(metadata: TransactionMetadata): Unit = {
    _metadata = metadata
    if (metadata != null) _expected = TableCounts.expectedBy(metadata)
  }

  def hasMarker: Boolean = _metadata != null

  def expected: TableCounts = _expected

  // Read-only bean properties, so the expected counts appear in the state store (see docs/STATE-STORE-DEBUG.md).
  def getExpectedOrders: Int = _expected.orders
  def getExpectedOrderDetails: Int = _expected.details
  def getExpectedOrderLineItems: Int = _expected.lineItems

  def received: TableCounts = TableCounts(receivedOrders, receivedOrderDetails, receivedOrderLineItems)

  /**
   * Counts one more accepted event of the given table (name without schema) and returns its
   * index in buffered order.
   */
  def recordEvent(tableName: String): Int = {
    tableName match {
      case TransactionCompletion.Orders         => receivedOrders += 1
      case TransactionCompletion.OrderDetails   => receivedOrderDetails += 1
      case TransactionCompletion.OrderLineItems => receivedOrderLineItems += 1
      case other                                => throw new IllegalArgumentException(s"Table $other is not tracked")
    }
    eventCount += 1
    eventCount - 1
  }

  /** See [[TransactionCompletion.isComplete]]. */
  def isComplete: Boolean = TransactionCompletion.isComplete(xid, csn, hasMarker, _expected, received)

  def progress: String = TransactionCompletion.progress(_expected, received)
}

object TransactionSummary {
  def apply(xid: String, csn: String): TransactionSummary = {
    val s = new TransactionSummary()
    s.xid = xid
    s.csn = csn
    s
  }
}
