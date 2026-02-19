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

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}

/**
 * Generic CDC event structure matching the Oracle GoldenGate format.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class DataChangeEvent extends Serializable {

  @BeanProperty var table: String = _

  @BeanProperty
  @JsonProperty("op_type")
  var opType: String = _

  @BeanProperty
  @JsonProperty("op_ts")
  var opTs: String = _

  @BeanProperty
  @JsonProperty("current_ts")
  var currentTs: String = _

  @BeanProperty var pos: String = _
  @BeanProperty var csn: String = _
  @BeanProperty var xid: String = _

  @BeanProperty var before: ju.Map[String, String] = _
  @BeanProperty var after: ju.Map[String, String] = _

  def tableName: String =
    if (table != null && table.contains(".")) table.substring(table.indexOf('.') + 1)
    else table

  def isDelete: Boolean = "D" == opType
  def isInsert: Boolean = "I" == opType
  def isUpdate: Boolean = "U" == opType
}
