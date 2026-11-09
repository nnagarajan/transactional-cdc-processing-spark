/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.model.generic

import java.{lang => jl, util => ju}

import scala.beans.BeanProperty

/**
 * One input the pipeline discarded, with the reason. Mirrors the `dropped_events` table
 * (db/create_dropped_events_table.deltalake.sql), except `droppedTs`, `droppedDate` and `batchId`,
 * which the sink adds when it writes the micro-batch.
 *
 * Nullable numeric columns use boxed types so an unset value is written as NULL, not 0.
 */
class DroppedEvent extends Serializable {
  @BeanProperty var itemKind: String = _
  @BeanProperty var reason: String = _
  @BeanProperty var droppedCount: Int = 1
  @BeanProperty var lastEmittedCsn: String = _

  @BeanProperty var xid: String = _
  @BeanProperty var csn: String = _

  @BeanProperty var kafkaTopic: String = _
  @BeanProperty var kafkaPartition: jl.Integer = _
  @BeanProperty var kafkaOffset: jl.Long = _
  @BeanProperty var markerIndex: jl.Integer = _

  @BeanProperty var tableName: String = _
  @BeanProperty var opType: String = _
  @BeanProperty var opTs: String = _
  @BeanProperty var pos: String = _
  @BeanProperty var before: ju.Map[String, String] = _
  @BeanProperty var after: ju.Map[String, String] = _

  @BeanProperty var markerEventCount: jl.Integer = _
  @BeanProperty var markerTxTs: String = _
  @BeanProperty var dataCollections: ju.List[DataCollectionCount] = _

  @BeanProperty var bufferedOrders: jl.Integer = _
  @BeanProperty var bufferedDetails: jl.Integer = _
  @BeanProperty var bufferedLineItems: jl.Integer = _

  @BeanProperty var rawValue: String = _
}

object DroppedEvent {

  object ItemKind {
    val DataEvent = "DATA_EVENT"
    val Marker = "MARKER"
    val PurgedBuffer = "PURGED_BUFFER"
    val RawMessage = "RAW_MESSAGE"
  }

  object Reason {
    val ReplayBelowCursor = "REPLAY_BELOW_CURSOR"
    val ReplayAlreadyEmitted = "REPLAY_ALREADY_EMITTED"
    val DuplicateMarker = "DUPLICATE_MARKER"
    val DuplicateEvent = "DUPLICATE_EVENT"
    val UnknownTable = "UNKNOWN_TABLE"
    val NoMarkerBelowCursor = "NO_MARKER_BELOW_CURSOR"
    val MissingIds = "MISSING_IDS"
    val Unparseable = "UNPARSEABLE"

    def isReplay(reason: String): Boolean = reason == ReplayBelowCursor || reason == ReplayAlreadyEmitted
  }

  private def base(kind: String, reason: String, lastEmittedCsn: String, xid: String, csn: String): DroppedEvent = {
    val d = new DroppedEvent()
    d.itemKind = kind
    d.reason = reason
    d.lastEmittedCsn = lastEmittedCsn
    d.xid = xid
    d.csn = csn
    d
  }

  /** A data event that was dropped. */
  def ofEvent(e: DataChangeEvent, reason: String, lastEmittedCsn: String, source: KafkaSource): DroppedEvent = {
    val d = base(ItemKind.DataEvent, reason, lastEmittedCsn, e.xid, e.csn)
    source.applyTo(d)
    d.tableName = e.tableName
    d.opType = e.opType
    d.opTs = e.opTs
    d.pos = e.pos
    d.before = e.before
    d.after = e.after
    d
  }

  /** A marker that was dropped. */
  def ofMarker(m: TransactionMetadata, reason: String, lastEmittedCsn: String, source: KafkaSource): DroppedEvent = {
    val d = base(ItemKind.Marker, reason, lastEmittedCsn, m.xid, m.csn)
    source.applyTo(d)
    d.markerEventCount = m.eventCount
    d.markerTxTs = m.txTs
    d.dataCollections = m.dataCollections
    d
  }

  /** A buffered transaction without a marker, removed by the purge. */
  def ofPurgedBuffer(xid: String, csn: String, orders: Int, details: Int, lineItems: Int, lastEmittedCsn: String): DroppedEvent = {
    val d = base(ItemKind.PurgedBuffer, Reason.NoMarkerBelowCursor, lastEmittedCsn, xid, csn)
    d.bufferedOrders = orders
    d.bufferedDetails = details
    d.bufferedLineItems = lineItems
    d
  }

  /** A Kafka message that could not be used. */
  def ofRawMessage(reason: String, rawValue: String, xid: String, csn: String, lastEmittedCsn: String,
      source: KafkaSource): DroppedEvent = {
    val d = base(ItemKind.RawMessage, reason, lastEmittedCsn, xid, csn)
    source.applyTo(d)
    d.rawValue = rawValue
    d
  }
}

/** Where an input row was read from in Kafka. Any field may be null when unknown. */
final case class KafkaSource(topic: String, partition: jl.Integer, offset: jl.Long, markerIndex: jl.Integer = null) {
  def applyTo(d: DroppedEvent): Unit = {
    d.kafkaTopic = topic
    d.kafkaPartition = partition
    d.kafkaOffset = offset
    d.markerIndex = markerIndex
  }
}

object KafkaSource {
  val Unknown: KafkaSource = KafkaSource(null, null, null)
}
