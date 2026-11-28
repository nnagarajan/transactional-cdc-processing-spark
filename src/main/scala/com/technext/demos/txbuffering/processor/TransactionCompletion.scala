/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.processor

import java.{util => ju}

import org.apache.logging.log4j.scala.Logger

import com.technext.demos.txbuffering.model.generic.TransactionMetadata

/**
 * Completion rules shared by the buffered summary ([[TransactionSummary]]) and the assembled
 * transaction ([[TransactionState]]), so both decide and report progress identically. Counts are
 * per tracked table (name without schema); a table missing from a count map counts as zero.
 */
object TransactionCompletion {
  private val log: Logger = Logger(classOf[TransactionState])

  /** The counts a marker declares for the tracked tables; counts for other tables are ignored. */
  def expectedBy(metadata: TransactionMetadata, tracked: Seq[String]): ju.Map[String, Integer] = {
    val m = new ju.LinkedHashMap[String, Integer]()
    if (metadata != null) tracked.foreach(t => m.put(t, metadata.getEventCountFor(t)))
    m
  }

  /**
   * Complete when the marker has arrived and each tracked table has exactly the declared number of
   * distinct events. More events than declared means the marker and data disagree: this is
   * logged and treated as incomplete, so it surfaces as a blocked transaction instead of
   * being emitted with the wrong contents.
   */
  def isComplete(xid: String, csn: String, hasMarker: Boolean, expected: ju.Map[String, Integer],
      received: ju.Map[String, Integer], tracked: Seq[String]): Boolean = {
    if (!hasMarker) return false
    if (isOverfilled(expected, received, tracked)) {
      log.error(s"Transaction $xid:$csn has more events than its marker declares. ${progress(expected, received, tracked)}")
      return false
    }
    tracked.forall(t => count(received, t) == count(expected, t))
  }

  def isOverfilled(expected: ju.Map[String, Integer], received: ju.Map[String, Integer], tracked: Seq[String]): Boolean =
    tracked.exists(t => count(received, t) > count(expected, t))

  /** For example `ORDERS: 1/1, ORDER_DETAILS: 0/1, ORDER_LINE_ITEMS: 1/3` (received/expected). */
  def progress(expected: ju.Map[String, Integer], received: ju.Map[String, Integer], tracked: Seq[String]): String =
    tracked.map(t => s"$t: ${count(received, t)}/${count(expected, t)}").mkString(", ")

  def count(counts: ju.Map[String, Integer], table: String): Int =
    if (counts == null) 0 else Option(counts.get(table)).map(_.intValue).getOrElse(0)

  /** Per-table counts in tracked order. */
  def counts(counts: ju.Map[String, Integer], tracked: Seq[String]): Seq[(String, Int)] =
    tracked.map(t => t -> count(counts, t))
}
