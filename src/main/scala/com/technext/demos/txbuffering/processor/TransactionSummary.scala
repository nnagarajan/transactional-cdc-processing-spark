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

import com.technext.demos.txbuffering.model.generic.TransactionMetadata

/**
 * What state keeps per buffered transaction besides its events: the marker (once it arrives),
 * the counts it declares for the tracked tables (`expected`), the number of distinct events
 * received per tracked table (`received`), and `eventCount`, the number of events buffered so far.
 * Event `i` (0 until `eventCount`) is stored under [[TransactionSequencer.eventKey]], in buffered
 * order. Tables are named without schema.
 *
 * It is small (one count per tracked table), so writing it per event keeps buffering cost constant
 * per event, and completeness is decided without reading any events. Bean-shaped for `Encoders.bean`.
 */
class TransactionSummary extends Serializable {

  @BeanProperty var xid: String = _
  @BeanProperty var csn: String = _

  @BeanProperty var expected: ju.Map[String, Integer] = new ju.HashMap[String, Integer]()
  @BeanProperty var received: ju.Map[String, Integer] = new ju.HashMap[String, Integer]()
  @BeanProperty var eventCount: Int = 0

  private var _metadata: TransactionMetadata = _

  def getMetadata: TransactionMetadata = _metadata

  /** Bean setter; use [[acceptMarker]] to record a marker. */
  def setMetadata(metadata: TransactionMetadata): Unit = _metadata = metadata

  /** Records the transaction's marker and the counts it declares for the tracked tables. */
  def acceptMarker(metadata: TransactionMetadata, tracked: Seq[String]): Unit = {
    _metadata = metadata
    expected = TransactionCompletion.expectedBy(metadata, tracked)
  }

  def hasMarker: Boolean = _metadata != null

  /**
   * Counts one more accepted event of the given tracked table (name without schema) and returns
   * its index in buffered order.
   */
  def recordEvent(tableName: String): Int = {
    // A summary read back from Spark state holds an unmodifiable map, so update a copy (one entry
    // per tracked table)
    val counts = if (received == null) new ju.HashMap[String, Integer]() else new ju.HashMap[String, Integer](received)
    counts.put(tableName, TransactionCompletion.count(received, tableName) + 1)
    received = counts
    eventCount += 1
    eventCount - 1
  }

  /** See [[TransactionCompletion.isComplete]]. */
  def isComplete(tracked: Seq[String]): Boolean =
    TransactionCompletion.isComplete(xid, csn, hasMarker, expected, received, tracked)

  def progress(tracked: Seq[String]): String = TransactionCompletion.progress(expected, received, tracked)
}

object TransactionSummary {
  def apply(xid: String, csn: String): TransactionSummary = {
    val s = new TransactionSummary()
    s.xid = xid
    s.csn = csn
    s
  }
}
