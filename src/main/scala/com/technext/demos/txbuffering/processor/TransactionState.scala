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

import org.apache.logging.log4j.scala.Logger

import com.technext.demos.txbuffering.model.generic.{DataChangeEvent, TransactionMetadata}

/**
 * State object tracking events for a single transaction.
 * Buffers all CDC events until transaction completion is detected.
 */
class TransactionState extends Serializable {

  @BeanProperty var xid: String = _
  @BeanProperty var csn: String = _

  @BeanProperty var orderEvents: ju.List[DataChangeEvent] = new ju.ArrayList[DataChangeEvent]()
  @BeanProperty var orderDetailEvents: ju.List[DataChangeEvent] = new ju.ArrayList[DataChangeEvent]()
  @BeanProperty var orderLineItemEvents: ju.List[DataChangeEvent] = new ju.ArrayList[DataChangeEvent]()

  private var _metadata: TransactionMetadata = _
  private var _expectedOrderCount: Int = 0
  private var _expectedOrderDetailCount: Int = 0
  private var _expectedOrderLineItemCount: Int = 0

  def getMetadata: TransactionMetadata = _metadata

  def setMetadata(metadata: TransactionMetadata): Unit = {
    _metadata = metadata
    if (metadata != null) {
      _expectedOrderCount = metadata.getEventCountFor("ORDERS")
      _expectedOrderDetailCount = metadata.getEventCountFor("ORDER_DETAILS")
      _expectedOrderLineItemCount = metadata.getEventCountFor("ORDER_LINE_ITEMS")
    }
  }

  def getExpectedOrderCount: Int = _expectedOrderCount
  def getExpectedOrderDetailCount: Int = _expectedOrderDetailCount
  def getExpectedOrderLineItemCount: Int = _expectedOrderLineItemCount

  /**
   * Buffers an event unless an event with the same GoldenGate trail position (`pos`)
   * is already buffered for the same table. Re-sent events therefore never count twice.
   *
   * @return true if the event was buffered, false if it was ignored
   */
  def addEvent(event: DataChangeEvent): Boolean = {
    val target = event.tableName match {
      case "ORDERS"           => orderEvents
      case "ORDER_DETAILS"    => orderDetailEvents
      case "ORDER_LINE_ITEMS" => orderLineItemEvents
      case _                  => null // Unknown table, ignore
    }
    if (target == null || containsPos(target, event.pos)) false
    else {
      target.add(event)
      true
    }
  }

  private def containsPos(events: ju.List[DataChangeEvent], pos: String): Boolean =
    pos != null && events.stream().anyMatch(e => pos == e.pos)

  /**
   * Complete when the marker has arrived and each table has exactly the declared number of
   * distinct events. More events than declared means the marker and data disagree: this is
   * logged and treated as incomplete, so it surfaces as a blocked transaction instead of
   * being emitted with the wrong contents.
   */
  def isComplete: Boolean = {
    if (_metadata == null) return false
    if (isOverfilled) {
      TransactionState.log.error(s"Transaction $xid:$csn has more events than its marker declares. $progress")
      return false
    }
    orderEvents.size == _expectedOrderCount &&
    orderDetailEvents.size == _expectedOrderDetailCount &&
    orderLineItemEvents.size == _expectedOrderLineItemCount
  }

  private def isOverfilled: Boolean =
    orderEvents.size > _expectedOrderCount ||
      orderDetailEvents.size > _expectedOrderDetailCount ||
      orderLineItemEvents.size > _expectedOrderLineItemCount

  def progress: String =
    s"Orders: ${orderEvents.size}/${_expectedOrderCount}, " +
      s"Details: ${orderDetailEvents.size}/${_expectedOrderDetailCount}, " +
      s"LineItems: ${orderLineItemEvents.size}/${_expectedOrderLineItemCount}"
}

object TransactionState {
  private val log: Logger = Logger(classOf[TransactionState])
}
