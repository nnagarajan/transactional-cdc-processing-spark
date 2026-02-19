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
class OrderDetailBefore extends Serializable {

  @BeanProperty @JsonProperty("ORDER_ID") var orderId: java.lang.Double = _
  @BeanProperty @JsonProperty("VERSION") var version: java.lang.Double = _
  @BeanProperty @JsonProperty("SHIPPING_METHOD") var shippingMethod: String = _
  @BeanProperty @JsonProperty("TRACKING_NUMBER") var trackingNumber: String = _
  @BeanProperty @JsonProperty("SHIPPED_TS") var shippedTs: String = _
  @BeanProperty @JsonProperty("ESTIMATED_DELIVERY_DATE") var estimatedDeliveryDate: String = _
  @BeanProperty @JsonProperty("CARRIER") var carrier: String = _
  @BeanProperty @JsonProperty("DELIVERY_STATUS") var deliveryStatus: String = _
}
