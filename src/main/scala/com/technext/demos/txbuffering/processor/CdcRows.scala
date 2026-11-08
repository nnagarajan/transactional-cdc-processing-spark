/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.processor

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Row

import com.technext.demos.txbuffering.model.generic.{DataChangeEvent, DataCollectionCount, TransactionMetadata}

/** Converts rows of the unioned CDC stream into model objects and sequencer inputs. */
object CdcRows {

  val MetadataEventType = "METADATA"

  def toSequencerInput(row: Row): SequencerInput =
    if (MetadataEventType == row.getAs[String]("event_type"))
      MarkerInput(toMetadata(row), row.getAs[Int]("marker_partition"), row.getAs[Long]("marker_offset"),
        markerIndex(row))
    else DataInput(toDataChangeEvent(row))

  /** Position of the marker within its Kafka message; 0 when the row has no `marker_index`. */
  private def markerIndex(row: Row): Int = {
    if (!row.schema.fieldNames.contains("marker_index")) return 0
    val i = row.fieldIndex("marker_index")
    if (row.isNullAt(i)) 0 else row.getInt(i)
  }

  /** Transaction metadata event: expected per-table counts. */
  def toMetadata(row: Row): TransactionMetadata = {
    val metadata = new TransactionMetadata()
    metadata.xid = row.getAs[String]("xid")
    metadata.csn = row.getAs[String]("csn")
    metadata.eventCount = row.getAs[Int]("expected_count")

    // Parse data collections
    val dcRows = row.getList[Row](row.fieldIndex("data_collections"))
    if (dcRows != null) {
      val dataCollections = mutable.ListBuffer[DataCollectionCount]()
      dcRows.asScala.foreach { dcRow =>
        dataCollections += new DataCollectionCount(
          dcRow.getAs[String]("data_collection"),
          dcRow.getAs[Int]("event_count"))
      }
      metadata.dataCollections = dataCollections.asJava
    }
    metadata
  }

  /** CDC data event. */
  def toDataChangeEvent(row: Row): DataChangeEvent = {
    val cdcEvent = new DataChangeEvent()
    cdcEvent.table = row.getAs[String]("table")
    cdcEvent.opType = row.getAs[String]("op_type")
    cdcEvent.opTs = row.getAs[String]("op_ts")
    cdcEvent.currentTs = row.getAs[String]("current_ts")
    cdcEvent.pos = row.getAs[String]("pos")
    cdcEvent.csn = row.getAs[String]("csn")
    cdcEvent.xid = row.getAs[String]("xid")

    // Convert Spark scala.collection.Map to java.util.Map
    val beforeMap = row.getAs[scala.collection.Map[String, String]]("before")
    val afterMap = row.getAs[scala.collection.Map[String, String]]("after")

    if (beforeMap != null) {
      cdcEvent.before = beforeMap.asJava
    }
    if (afterMap != null) {
      cdcEvent.after = afterMap.asJava
    }
    cdcEvent
  }
}
