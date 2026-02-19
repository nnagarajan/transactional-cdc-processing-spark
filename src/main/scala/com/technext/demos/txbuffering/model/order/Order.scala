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
class Order extends Serializable {

  @BeanProperty @JsonProperty("ORDER_ID") var orderId: java.lang.Double = _
  @BeanProperty @JsonProperty("ORDER_REF") var orderRef: String = _
  @BeanProperty @JsonProperty("VERSION") var version: java.lang.Double = _
  @BeanProperty @JsonProperty("ORDER_DATE") var orderDate: String = _
  @BeanProperty @JsonProperty("ORDER_TS") var orderTs: String = _
  @BeanProperty @JsonProperty("ORDER_STATUS") var orderStatus: String = _
  @BeanProperty @JsonProperty("ORDER_TYPE") var orderType: String = _
  @BeanProperty @JsonProperty("TOTAL_AMOUNT") var totalAmount: java.lang.Double = _
  @BeanProperty @JsonProperty("CURRENCY") var currency: String = _
  @BeanProperty @JsonProperty("CUSTOMER_ID") var customerId: String = _
  @BeanProperty @JsonProperty("SHIPPING_ADDRESS_ID") var shippingAddressId: String = _
  @BeanProperty @JsonProperty("CREATED_TS") var createdTs: String = _

  @BeanProperty @JsonProperty("before") var before: OrderBefore = _
}
