package com.technext.demos.txbuffering.processor

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import com.technext.demos.txbuffering.model.generic.DroppedEvent

class SequencerOutputEncodingTest {

  /** Columns of dropped_events that the sequencer fills (the sink adds droppedTs, droppedDate, batchId). */
  private val TableColumns = Seq(
    "itemKind", "reason", "droppedCount", "lastEmittedCsn", "xid", "csn",
    "kafkaTopic", "kafkaPartition", "kafkaOffset", "markerIndex",
    "tableName", "opType", "opTs", "pos", "before", "after",
    "markerEventCount", "markerTxTs", "dataCollections",
    "bufferedOrders", "bufferedDetails", "bufferedLineItems", "rawValue")

  @Test
  def droppedEventBeanMatchesTheTableColumns(): Unit = {
    val schema = Encoders.bean(classOf[DroppedEvent]).schema
    assertThat(schema.fieldNames.sorted.toArray).containsExactly(TableColumns.sorted: _*)
    assertThat(schema("kafkaOffset").dataType.typeName).isEqualTo("long")
    assertThat(schema("kafkaPartition").nullable).isTrue
  }

  @Test
  def envelopeHasBothSides(): Unit = {
    val schema = Encoders.bean(classOf[SequencerOutput]).schema
    assertThat(schema.fieldNames.sorted.toArray).containsExactly("dropped", "kind", "order")
    assertThat(schema("order").dataType.isInstanceOf[StructType]).isTrue
    assertThat(schema("dropped").dataType.isInstanceOf[StructType]).isTrue
  }
}
