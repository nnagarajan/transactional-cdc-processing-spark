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

/** Parses the GoldenGate JSON topics into the unified row shape the sequencer consumes. */
object CdcJsonParsing {

  def parseDataChangeEvents(rawStream: Dataset[Row], eventType: String): Dataset[Row] =
    rawStream
      .selectExpr("CAST(value AS STRING) as json")
      .selectExpr(
        "from_json(json, 'struct<table:string,op_type:string,op_ts:string," +
          "current_ts:string,pos:string,csn:string,xid:string," +
          "before:map<string,string>,after:map<string,string>>') as data")
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
        lit(eventType).as("event_type"),
        lit(null).cast("int").as("expected_count"),
        lit(null).cast("array<struct<data_collection:string,event_count:int>>").as("data_collections"),
        lit(null).cast("int").as("marker_partition"),
        lit(null).cast("long").as("marker_offset"),
        lit(null).cast("int").as("marker_index"))

  /**
   * Parses the transaction metadata topic. A message holds either one marker (a JSON object) or,
   * when GoldenGate's GROUPTRANSOPS delivered several source transactions together, a JSON array
   * of markers in commit order. Each marker keeps its Kafka partition and offset and its position
   * in the message: (offset, index) defines emission order.
   */
  def parseTransactionMetadata(rawStream: Dataset[Row]): Dataset[Row] =
    rawStream
      .selectExpr("CAST(value AS STRING) as json", "partition", "offset")
      .selectExpr(
        "posexplode(from_json(json, 'array<struct<xid:string,csn:string,tx_ts:string," +
          "event_count:int,data_collections:array<struct<data_collection:string,event_count:int>>>>')) " +
          "as (marker_index, data)",
        "partition",
        "offset")
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
        lit("METADATA").as("event_type"),
        col("data.event_count").as("expected_count"),
        col("data.data_collections").as("data_collections"),
        col("partition").as("marker_partition"),
        col("offset").as("marker_offset"),
        col("marker_index"))
}
