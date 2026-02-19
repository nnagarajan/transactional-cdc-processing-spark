/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.model.order

import java.{util => ju}

import scala.beans.BeanProperty

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Denormalized order record with nested order attributes and related details/line items.
 * Top-level: xid, csn, dwhProcessedTs, orderId.
 * All entity attributes stored as arrays of structs with before images.
 */
class OrderStream extends Serializable {

  // Transaction identifiers
  @BeanProperty @JsonProperty("xid") var xid: String = _
  @BeanProperty @JsonProperty("csn") var csn: String = _

  // Metadata
  @BeanProperty @JsonProperty("dwh_processed_ts") var dwhProcessedTs: String = _

  // Order identifier
  @BeanProperty @JsonProperty("ORDER_ID") var orderId: java.lang.Double = _

  // Order attributes as nested array (with before images inside each element)
  @BeanProperty @JsonProperty("orders") var orders: ju.List[Order] = _

  // Related entities with their before images
  @BeanProperty @JsonProperty("order_details") var orderDetails: ju.List[OrderDetail] = _
  @BeanProperty @JsonProperty("line_items") var lineItems: ju.List[OrderLineItem] = _
}
