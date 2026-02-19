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

  def addEvent(event: DataChangeEvent): Unit = {
    event.tableName match {
      case "ORDERS"           => orderEvents.add(event)
      case "ORDER_DETAILS"    => orderDetailEvents.add(event)
      case "ORDER_LINE_ITEMS" => orderLineItemEvents.add(event)
      case _                  => // Unknown table, ignore
    }
  }

  def isComplete: Boolean =
    _metadata != null &&
      orderEvents.size >= _expectedOrderCount &&
      orderDetailEvents.size >= _expectedOrderDetailCount &&
      orderLineItemEvents.size >= _expectedOrderLineItemCount

  def progress: String =
    s"Orders: ${orderEvents.size}/${_expectedOrderCount}, " +
      s"Details: ${orderDetailEvents.size}/${_expectedOrderDetailCount}, " +
      s"LineItems: ${orderLineItemEvents.size}/${_expectedOrderLineItemCount}"
}
