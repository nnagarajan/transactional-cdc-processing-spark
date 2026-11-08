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
  val orderEvents = CdcJsonParsing.parseDataChangeEvents(ordersRaw, "ORDERS")
  val orderDetailEvents = CdcJsonParsing.parseDataChangeEvents(orderDetailsRaw, "ORDER_DETAILS")
  val orderLineItemEvents = CdcJsonParsing.parseDataChangeEvents(orderLineItemsRaw, "ORDER_LINE_ITEMS")
  val txMetadataEvents = CdcJsonParsing.parseTransactionMetadata(txMetadataRaw)

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
  // An explicit trigger is required: in processing-time mode, Spark 4.x's transformWithState
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
}
