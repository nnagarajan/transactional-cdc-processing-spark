/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.model.generic

import java.{util => ju}

import scala.beans.BeanProperty
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}

/**
 * Transaction metadata event from the transaction topic.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class TransactionMetadata extends Serializable {

  @BeanProperty var xid: String = _
  @BeanProperty var csn: String = _

  @BeanProperty
  @JsonProperty("tx_ts")
  var txTs: String = _

  @BeanProperty
  @JsonProperty("event_count")
  var eventCount: Int = 0

  @BeanProperty
  @JsonProperty("data_collections")
  var dataCollections: ju.List[DataCollectionCount] = _

  def getEventCountFor(collectionName: String): Int = {
    if (dataCollections == null) return 0
    dataCollections.asScala
      .filter(_.dataCollection == collectionName)
      .map(_.eventCount)
      .sum
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class DataCollectionCount extends Serializable {

  @BeanProperty
  @JsonProperty("data_collection")
  var dataCollection: String = _

  @BeanProperty
  @JsonProperty("event_count")
  var eventCount: Int = 0

  def this(dataCollection: String, eventCount: Int) = {
    this()
    this.dataCollection = dataCollection
    this.eventCount = eventCount
  }
}
