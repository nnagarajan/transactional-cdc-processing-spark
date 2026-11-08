/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import com.typesafe.config.Config
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import com.technext.demos.txbuffering.model.order.OrderStream
import com.technext.demos.txbuffering.processor.TransactionOrderingProcessor

/**
 * Spark Structured Streaming application for transactionally consistent CDC processing.
 *
 * Buffers CDC events from orders, order_details, and order_line_items topics,
 * uses transaction metadata to determine when all events in a transaction have arrived,
 * and emits denormalized records only when transactions are complete, strictly in the
 * order their markers appear on the single-partition transaction metadata topic (commit order).
 */
object TransactionalCdcProcessingApp extends App with Logging {

  val config: Config = AppConfig.load("transactional-cdc-processing")

  val kafkaBootstrapServers = config.getString("job.kafka.bootstrap-servers")
  val checkpointLocation = config.getString("job.output.checkpoint-location")
  val deltaTableName = config.getString("job.output.delta-table-name")
  val blockedAlertThresholdMs = config.getDuration("job.ordering.blocked-alert-threshold").toMillis
  val checkIntervalMs = config.getDuration("job.ordering.check-interval").toMillis
  val triggerIntervalMs = config.getDuration("job.ordering.trigger-interval").toMillis

  logger.info("Starting Transactional CDC Processing Application")
  logger.info(s"Kafka Bootstrap Servers: $kafkaBootstrapServers")
  logger.info(s"Checkpoint Location: $checkpointLocation")
  logger.info(s"Delta Table: $deltaTableName")
  logger.info(s"Blocked-transaction alert threshold: ${blockedAlertThresholdMs}ms, check interval: ${checkIntervalMs}ms, trigger interval: ${triggerIntervalMs}ms")

  val spark = SparkSessionCreator.getSpark(config)
  spark.sparkContext.setLogLevel(config.getString("spark.log-level"))

  // Read all CDC streams with proper JSON parsing
  val ordersRaw = readKafkaStream(spark, config, config.getString("job.kafka.topics.orders"))
  val orderDetailsRaw = readKafkaStream(spark, config, config.getString("job.kafka.topics.order-details"))
  val orderLineItemsRaw = readKafkaStream(spark, config, config.getString("job.kafka.topics.order-line-items"))
  val txMetadataRaw = readKafkaStream(spark, config, config.getString("job.kafka.topics.transaction-metadata"))

  // Parse JSON and add event type markers
  val orderEvents = parseDataChangeEvents(ordersRaw, "ORDERS")
  val orderDetailEvents = parseDataChangeEvents(orderDetailsRaw, "ORDER_DETAILS")
  val orderLineItemEvents = parseDataChangeEvents(orderLineItemsRaw, "ORDER_LINE_ITEMS")
  val txMetadataEvents = parseTransactionMetadata(txMetadataRaw)

  // Union all streams
  val allEvents = orderEvents
    .union(orderDetailEvents)
    .union(orderLineItemEvents)
    .union(txMetadataEvents)
    .filter(col("xid").isNotNull.and(col("csn").isNotNull))

  // Assemble transactions and emit them in commit order (single global sequencer)
  val joinedOrders: Dataset[OrderStream] =
    TransactionOrderingProcessor.orderTransactions(allEvents, blockedAlertThresholdMs, checkIntervalMs)

  // Write to Delta Lake table.
  // An explicit trigger is required: in processing-time mode, Spark 4.0's transformWithState
  // always requests another (no-data) batch, so the default trigger would run empty batches
  // back to back. With a fixed interval, idle batches run once per interval and drive the
  // blocked-transaction timer.
  val query = joinedOrders.writeStream
    .format("delta")
    .option("checkpointLocation", checkpointLocation)
    .outputMode(OutputMode.Append())
    .trigger(Trigger.ProcessingTime(triggerIntervalMs))
    .toTable(deltaTableName)

  logger.info(s"Streaming query started. Writing to Delta Lake table: $deltaTableName")
  logger.info("Waiting for data...")
  query.awaitTermination()

  private def readKafkaStream(spark: SparkSession, config: Config, topic: String): Dataset[Row] =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("job.kafka.bootstrap-servers"))
      .option("subscribe", topic)
      .option("startingOffsets", config.getString("kafka.starting-offsets"))
      .option("failOnDataLoss", config.getString("kafka.fail-on-data-loss"))
      .load()

  private def parseDataChangeEvents(rawStream: Dataset[Row], eventType: String): Dataset[Row] =
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
        lit(null).cast("long").as("marker_offset"))

  /** Markers keep their Kafka partition and offset: the offset defines emission order. */
  private def parseTransactionMetadata(rawStream: Dataset[Row]): Dataset[Row] =
    rawStream
      .selectExpr("CAST(value AS STRING) as json", "partition", "offset")
      .selectExpr(
        "from_json(json, 'struct<xid:string,csn:string,tx_ts:string," +
          "event_count:int,data_collections:array<struct<data_collection:string,event_count:int>>>') as data",
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
        col("offset").as("marker_offset"))
}
