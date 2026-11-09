/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._

/**
 * Parses the GoldenGate JSON topics into the unified row shape the sequencer consumes.
 *
 * Every row carries where it was read from (`source_topic`, `source_partition`, `source_offset`).
 * A message that can't be used becomes a row with `event_type = RAW_DROP`, a `drop_reason`
 * (`UNPARSEABLE` when it isn't valid JSON, `MISSING_IDS` when it has no xid or csn), and its
 * payload in `raw_value`, so it is recorded instead of disappearing.
 */
object CdcJsonParsing {

  val RawDropEventType: String = processor.CdcRows.RawDropEventType

  private val DataEventSchema =
    "struct<table:string,op_type:string,op_ts:string,current_ts:string,pos:string,csn:string,xid:string," +
      "before:map<string,string>,after:map<string,string>>"

  /** Reason a message can't be used, or null; `ids` is true when both xid and csn are present. */
  private def dropReason(ids: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    when(ids, lit(null).cast("string"))
      .when(get_json_object(col("json"), "$").isNull, lit("UNPARSEABLE"))
      .otherwise(lit("MISSING_IDS"))

  def parseDataChangeEvents(rawStream: Dataset[Row], eventType: String): Dataset[Row] = {
    val ids = col("data.xid").isNotNull && col("data.csn").isNotNull
    rawStream
      .selectExpr("CAST(value AS STRING) as json", "topic", "partition", "offset")
      .withColumn("data", from_json(col("json"), DataEventSchema, Map.empty[String, String]))
      .select(
        col("data.xid").as("xid"),
        col("data.csn").as("csn"),
        col("data.table").as("table"),
        col("data.op_type").as("op_type"),
        col("data.op_ts").as("op_ts"),
        col("data.current_ts").as("current_ts"),
        col("data.pos").as("pos"),
        col("data.before").as("before"),
        col("data.after").as("after"),
        when(ids, lit(eventType)).otherwise(lit(RawDropEventType)).as("event_type"),
        lit(null).cast("int").as("expected_count"),
        lit(null).cast("array<struct<data_collection:string,event_count:int>>").as("data_collections"),
        lit(null).cast("int").as("marker_partition"),
        lit(null).cast("long").as("marker_offset"),
        lit(null).cast("int").as("marker_index"),
        col("topic").as("source_topic"),
        col("partition").as("source_partition"),
        col("offset").as("source_offset"),
        dropReason(ids).as("drop_reason"),
        when(ids, lit(null).cast("string")).otherwise(col("json")).as("raw_value"))
  }

  private val MarkerArraySchema =
    "array<struct<xid:string,csn:string,tx_ts:string,event_count:int," +
      "data_collections:array<struct<data_collection:string,event_count:int>>>>"

  /**
   * Parses the transaction metadata topic. A message holds either one marker (a JSON object) or,
   * when GoldenGate's GROUPTRANSOPS delivered several source transactions together, a JSON array
   * of markers in commit order. Each marker keeps its Kafka partition and offset and its position
   * in the message: (offset, index) defines emission order.
   *
   * A message that doesn't parse becomes one RAW_DROP row holding the whole message. A marker
   * without xid or csn inside an otherwise valid message becomes a MISSING_IDS row holding that marker.
   */
  def parseTransactionMetadata(rawStream: Dataset[Row]): Dataset[Row] = {
    val ids = col("data.xid").isNotNull && col("data.csn").isNotNull
    rawStream
      .selectExpr("CAST(value AS STRING) as json", "topic", "partition", "offset")
      .withColumn("markers", from_json(col("json"), MarkerArraySchema, Map.empty[String, String]))
      // posexplode_outer keeps a row (with null data) for a message that didn't parse
      .selectExpr("json", "topic", "partition", "offset", "posexplode_outer(markers) as (marker_index, data)")
      .select(
        col("data.xid").as("xid"),
        col("data.csn").as("csn"),
        lit(null).cast("string").as("table"),
        lit(null).cast("string").as("op_type"),
        lit(null).cast("string").as("op_ts"),
        lit(null).cast("string").as("current_ts"),
        lit(null).cast("string").as("pos"),
        lit(null).cast("map<string,string>").as("before"),
        lit(null).cast("map<string,string>").as("after"),
        when(ids, lit("METADATA")).otherwise(lit(RawDropEventType)).as("event_type"),
        col("data.event_count").as("expected_count"),
        col("data.data_collections").as("data_collections"),
        col("partition").as("marker_partition"),
        col("offset").as("marker_offset"),
        col("marker_index"),
        col("topic").as("source_topic"),
        col("partition").as("source_partition"),
        col("offset").as("source_offset"),
        dropReason(ids).as("drop_reason"),
        when(ids, lit(null).cast("string"))
          .when(col("data").isNull, col("json"))
          .otherwise(to_json(col("data"))).as("raw_value"))
  }
}
