/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.processor

import org.apache.logging.log4j.scala.Logger

import com.technext.demos.txbuffering.model.generic.TransactionMetadata

/** Received or expected event counts for the tracked tables. */
final case class TableCounts(orders: Int, details: Int, lineItems: Int)

object TableCounts {
  val Zero: TableCounts = TableCounts(0, 0, 0)

  /** The counts a marker declares for the tracked tables (zero when there is no marker). */
  def expectedBy(metadata: TransactionMetadata): TableCounts =
    if (metadata == null) Zero
    else TableCounts(
      metadata.getEventCountFor(TransactionCompletion.Orders),
      metadata.getEventCountFor(TransactionCompletion.OrderDetails),
      metadata.getEventCountFor(TransactionCompletion.OrderLineItems))
}

/**
 * Completion rules shared by the buffered summary ([[TransactionSummary]]) and the assembled
 * transaction ([[TransactionState]]), so both decide and report progress identically.
 */
object TransactionCompletion {
  private val log: Logger = Logger(classOf[TransactionState])

  val Orders = "ORDERS"
  val OrderDetails = "ORDER_DETAILS"
  val OrderLineItems = "ORDER_LINE_ITEMS"

  /** Whether events of this table (name without schema) are tracked. */
  def isTracked(tableName: String): Boolean =
    tableName == Orders || tableName == OrderDetails || tableName == OrderLineItems

  /**
   * Complete when the marker has arrived and each table has exactly the declared number of
   * distinct events. More events than declared means the marker and data disagree: this is
   * logged and treated as incomplete, so it surfaces as a blocked transaction instead of
   * being emitted with the wrong contents.
   */
  def isComplete(xid: String, csn: String, hasMarker: Boolean, expected: TableCounts, received: TableCounts): Boolean = {
    if (!hasMarker) return false
    if (isOverfilled(expected, received)) {
      log.error(s"Transaction $xid:$csn has more events than its marker declares. ${progress(expected, received)}")
      return false
    }
    received == expected
  }

  def isOverfilled(expected: TableCounts, received: TableCounts): Boolean =
    received.orders > expected.orders ||
      received.details > expected.details ||
      received.lineItems > expected.lineItems

  def progress(expected: TableCounts, received: TableCounts): String =
    s"Orders: ${received.orders}/${expected.orders}, " +
      s"Details: ${received.details}/${expected.details}, " +
      s"LineItems: ${received.lineItems}/${expected.lineItems}"
}
