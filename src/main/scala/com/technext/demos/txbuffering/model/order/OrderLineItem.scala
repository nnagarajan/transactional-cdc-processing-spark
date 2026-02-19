/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.model.order

import scala.beans.BeanProperty

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}

@JsonIgnoreProperties(ignoreUnknown = true)
class OrderLineItem extends Serializable {

  @BeanProperty @JsonProperty("LINE_ITEM_ID") var lineItemId: java.lang.Double = _
  @BeanProperty @JsonProperty("ORDER_ID") var orderId: java.lang.Double = _
  @BeanProperty @JsonProperty("VERSION") var version: java.lang.Double = _
  @BeanProperty @JsonProperty("PRODUCT_ID") var productId: String = _
  @BeanProperty @JsonProperty("ITEM_QTY") var itemQty: java.lang.Double = _
  @BeanProperty @JsonProperty("ITEM_PRICE") var itemPrice: java.lang.Double = _
  @BeanProperty @JsonProperty("ITEM_AMOUNT") var itemAmount: java.lang.Double = _
  @BeanProperty @JsonProperty("ITEM_CURRENCY") var itemCurrency: String = _
  @BeanProperty @JsonProperty("before") var before: OrderLineItemBefore = _
}
