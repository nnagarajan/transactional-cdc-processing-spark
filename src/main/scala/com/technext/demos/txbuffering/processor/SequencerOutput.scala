/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.processor

import scala.beans.BeanProperty

import com.technext.demos.txbuffering.model.generic.DroppedEvent
import com.technext.demos.txbuffering.model.order.OrderStream

/**
 * One output row of the sequencer: an emitted order record, or a record of a dropped input.
 * Encoders.bean can't encode a sealed trait, so this is a wrapper with one non-null side.
 */
class SequencerOutput extends Serializable {
  @BeanProperty var kind: String = _
  @BeanProperty var order: OrderStream = _
  @BeanProperty var dropped: DroppedEvent = _
}

object SequencerOutput {
  val OrderKind = "ORDER"
  val DroppedKind = "DROPPED"

  def ofOrder(o: OrderStream): SequencerOutput = {
    val s = new SequencerOutput()
    s.kind = OrderKind
    s.order = o
    s
  }

  def ofDropped(d: DroppedEvent): SequencerOutput = {
    val s = new SequencerOutput()
    s.kind = DroppedKind
    s.dropped = d
    s
  }
}
