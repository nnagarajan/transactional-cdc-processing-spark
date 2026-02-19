/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.processor

import java.time.Instant
import java.{util => ju}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

import com.technext.demos.txbuffering.model.order.{Order, OrderBefore, OrderDetail, OrderDetailBefore, OrderLineItem, OrderLineItemBefore, OrderStream}

/**
 * Joins order events with their related details and line items
 * into denormalized records.
 */
object OrderJoiner {

  private val objectMapper = new ObjectMapper()

  def joinTransaction(state: TransactionState): Seq[OrderStream] = {
    val orderMap = mutable.HashMap[java.lang.Double, OrderStream]()
    val processingTimestamp = Instant.now().toString

    // Process order events — build Order with before image, add to orders array
    for (event <- state.orderEvents.asScala) {
      val afterData = event.after
      val beforeData = event.before

      if (afterData != null) {
        val order = convert(afterData, classOf[Order])
        val orderBefore =
          if (beforeData != null) convert(beforeData, classOf[OrderBefore])
          else null
        order.before = orderBefore
        val orderId = order.orderId

        val os = orderMap.getOrElseUpdate(orderId, newOrderStream(orderId, state, processingTimestamp))
        os.orders.add(order)
      }
    }

    // Process order detail events
    for (event <- state.orderDetailEvents.asScala) {
      val afterData = event.after
      val beforeData = event.before

      if (afterData != null) {
        val detail = convert(afterData, classOf[OrderDetail])
        val detailBefore =
          if (beforeData != null) convert(beforeData, classOf[OrderDetailBefore])
          else null
        detail.before = detailBefore
        val orderId = detail.orderId

        val os = orderMap.getOrElseUpdate(orderId, newOrderStream(orderId, state, processingTimestamp))
        os.orderDetails.add(detail)
      }
    }

    // Process order line item events
    for (event <- state.orderLineItemEvents.asScala) {
      val afterData = event.after
      val beforeData = event.before

      if (afterData != null) {
        val lineItem = convert(afterData, classOf[OrderLineItem])
        val lineItemBefore =
          if (beforeData != null) convert(beforeData, classOf[OrderLineItemBefore])
          else null
        lineItem.before = lineItemBefore
        val orderId = lineItem.orderId

        val os = orderMap.getOrElseUpdate(orderId, newOrderStream(orderId, state, processingTimestamp))
        os.lineItems.add(lineItem)
      }
    }

    orderMap.values.toSeq
  }

  /** Converts a Map[String, String] to a target bean type via JsonNode.
   *  Jackson coerces string values to the target field types (e.g. "100.50" → Double). */
  private def convert[T](data: ju.Map[String, String], clazz: Class[T]): T = {
    val node: ObjectNode = objectMapper.createObjectNode()
    data.forEach { (key, value) =>
      if (value != null) node.put(key, value)
    }
    objectMapper.treeToValue(node, clazz)
  }

  private def newOrderStream(orderId: java.lang.Double, state: TransactionState, processingTimestamp: String): OrderStream = {
    val os = new OrderStream()
    os.orderId = orderId
    os.xid = state.xid
    os.csn = state.csn
    os.dwhProcessedTs = processingTimestamp
    os.orders = new ju.ArrayList[Order]()
    os.orderDetails = new ju.ArrayList[OrderDetail]()
    os.lineItems = new ju.ArrayList[OrderLineItem]()
    os
  }
}
