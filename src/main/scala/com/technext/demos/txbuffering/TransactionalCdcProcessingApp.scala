/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.typesafe.config.Config
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import com.technext.demos.txbuffering.model.generic.{DataChangeEvent, DataCollectionCount, TransactionMetadata}
import com.technext.demos.txbuffering.model.order.OrderStream
import com.technext.demos.txbuffering.processor.{OrderJoiner, TransactionState}

/**
 * Spark Structured Streaming application for transactionally consistent CDC processing.
 *
 * Buffers CDC events from orders, order_details, and order_line_items topics,
 * uses transaction metadata to determine when all events in a transaction have arrived,
 * and emits denormalized records only when transactions are complete.
 */
object TransactionalCdcProcessingApp extends App with Logging {

  val config: Config = AppConfig.load("transactional-cdc-processing")

  val kafkaBootstrapServers = config.getString("job.kafka.bootstrap-servers")
  val checkpointLocation = config.getString("job.output.checkpoint-location")
  val deltaTableName = config.getString("job.output.delta-table-name")

  logger.info("Starting Transactional CDC Processing Application")
  logger.info(s"Kafka Bootstrap Servers: $kafkaBootstrapServers")
  logger.info(s"Checkpoint Location: $checkpointLocation")
  logger.info(s"Delta Table: $deltaTableName")

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

  // Group by transaction key (xid:csn) and process with state
  implicit val stringEncoder: org.apache.spark.sql.Encoder[String] = Encoders.STRING
  implicit val stateEncoder: org.apache.spark.sql.Encoder[TransactionState] = Encoders.bean(classOf[TransactionState])
  implicit val outputEncoder: org.apache.spark.sql.Encoder[OrderStream] = Encoders.bean(classOf[OrderStream])

  val joinedOrders: Dataset[OrderStream] = allEvents
    .groupByKey((row: Row) => row.getString(row.fieldIndex("xid")) + ":" + row.getString(row.fieldIndex("csn")))
    .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(processTransaction)

  // Write to Delta Lake table
  val query = joinedOrders.writeStream
    .format("delta")
    .option("checkpointLocation", checkpointLocation)
    .outputMode(OutputMode.Append())
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
        lit(null).cast("array<struct<data_collection:string,event_count:int>>").as("data_collections"))

  private def parseTransactionMetadata(rawStream: Dataset[Row]): Dataset[Row] =
    rawStream
      .selectExpr("CAST(value AS STRING) as json")
      .selectExpr(
        "from_json(json, 'struct<xid:string,csn:string,tx_ts:string," +
          "event_count:int,data_collections:array<struct<data_collection:string,event_count:int>>>') as data")
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
        col("data.data_collections").as("data_collections"))

  /**
   * Stateful processor that buffers CDC events per transaction and emits when complete.
   */
  private def processTransaction(
      txKey: String,
      events: Iterator[Row],
      state: GroupState[TransactionState]): Iterator[OrderStream] = {

    // Get or create transaction state
    val txState = if (state.exists) state.get else new TransactionState()

    // Parse transaction key
    val parts = txKey.split(":")
    if (parts.length == 2) {
      txState.xid = parts(0)
      txState.csn = parts(1)
    }

    // Process all incoming events in this micro-batch
    while (events.hasNext) {
      val row = events.next()
      val eventType = row.getAs[String]("event_type")

      if ("METADATA" == eventType) {
        // Transaction metadata event - set expected counts
        val metadata = new TransactionMetadata()
        metadata.xid = row.getAs[String]("xid")
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

        txState.setMetadata(metadata)
      } else {
        // CDC event - add to buffer
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

        txState.addEvent(cdcEvent)
      }
    }

    // Check if transaction is complete
    if (txState.isComplete) {
      logger.info(s"Transaction $txKey is complete. Progress: ${txState.progress}")

      // Join all events and create denormalized records
      val results = OrderJoiner.joinTransaction(txState)

      // Remove state after emitting
      state.remove()

      results.iterator
    } else {
      // Transaction not complete yet - update state and return empty iterator
      if (txState.getMetadata != null) {
        logger.info(s"Transaction $txKey buffering. Progress: ${txState.progress}")
      }

      state.update(txState)
      Iterator.empty
    }
  }
}
