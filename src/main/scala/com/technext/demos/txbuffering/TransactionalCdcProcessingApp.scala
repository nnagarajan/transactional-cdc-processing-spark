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

import com.technext.demos.txbuffering.processor.{SequencerOutput, TransactionOrderingProcessor}

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

  // Per-topic starting positions (new checkpoint only) and per-batch limits; validated before any query starts
  val sourceOptions: Map[String, KafkaSourceOptions.SourceOptions] =
    Seq("orders", "order-details", "order-line-items", "transaction-metadata")
      .map(k => k -> KafkaSourceOptions.forTopic(config, k)).toMap
  sourceOptions.foreach { case (k, o) =>
    logger.info(s"Kafka source '$k' (${config.getString(s"job.kafka.topics.$k")}): startingOffsets=${o.startingOffsets}, " +
      s"maxOffsetsPerTrigger=${o.maxOffsetsPerTrigger.map(_.toString).getOrElse("none")}")
  }

  val droppedEvents = DroppedEventsSettings(config)
  logger.info(s"Dropped events: enabled=${droppedEvents.enabled}, table=${droppedEvents.tableName}, " +
    s"replay-detail=${if (droppedEvents.summarizeReplays) "summary" else "full"}")

  val spark = SparkSessionCreator.getSpark(config)
  spark.sparkContext.setLogLevel(config.getString("spark.log-level"))
  CdcOutputSink.checkDroppedEventsTable(spark, droppedEvents)

  // Read all CDC streams with proper JSON parsing
  val ordersRaw = readKafkaStream(spark, config, "orders")
  val orderDetailsRaw = readKafkaStream(spark, config, "order-details")
  val orderLineItemsRaw = readKafkaStream(spark, config, "order-line-items")
  val txMetadataRaw = readKafkaStream(spark, config, "transaction-metadata")

  // Parse JSON and add event type markers
  val orderEvents = CdcJsonParsing.parseDataChangeEvents(ordersRaw, "ORDERS")
  val orderDetailEvents = CdcJsonParsing.parseDataChangeEvents(orderDetailsRaw, "ORDER_DETAILS")
  val orderLineItemEvents = CdcJsonParsing.parseDataChangeEvents(orderLineItemsRaw, "ORDER_LINE_ITEMS")
  val txMetadataEvents = CdcJsonParsing.parseTransactionMetadata(txMetadataRaw)

  // Union all streams. Messages without xid/csn are not filtered here: the parser marks them
  // RAW_DROP and the sequencer records them as dropped.
  val allEvents = orderEvents
    .union(orderDetailEvents)
    .union(orderLineItemEvents)
    .union(txMetadataEvents)

  // Assemble transactions and emit them in commit order (single global sequencer). The output
  // holds the emitted orders and a record of every dropped input.
  val output: Dataset[SequencerOutput] = TransactionOrderingProcessor.orderTransactions(
    allEvents, blockedAlertThresholdMs, checkIntervalMs, droppedEvents.summarizeReplays)

  // Write orders, then drop records, per micro-batch; both writes are idempotent per batch id.
  // The txnAppId prefix is the query id from the checkpoint, read once the query has started.
  @volatile private var txnAppIdPrefix: String = _
  val sink = new CdcOutputSink(deltaTableName, droppedEvents, () => {
    if (txnAppIdPrefix == null) txnAppIdPrefix = CdcOutputSink.queryIdFromCheckpoint(spark, checkpointLocation)
    txnAppIdPrefix
  })
  val writeBatch: (Dataset[SequencerOutput], Long) => Unit = (batch, batchId) => sink.writeBatch(batch, batchId)

  // An explicit trigger is required: in processing-time mode, Spark 4.x's transformWithState
  // always requests another (no-data) batch, so the default trigger would run empty batches
  // back to back. With a fixed interval, idle batches run once per interval and drive the
  // blocked-transaction timer.
  val query = output.writeStream
    .option("checkpointLocation", checkpointLocation)
    .outputMode(OutputMode.Append())
    .trigger(Trigger.ProcessingTime(triggerIntervalMs))
    .foreachBatch(writeBatch)
    .start()

  logger.info(s"Streaming query started. Writing to Delta Lake table: $deltaTableName" +
    (if (droppedEvents.enabled) s", dropped events to ${droppedEvents.tableName}" else " (dropped events: log only)"))
  logger.info("Waiting for data...")
  query.awaitTermination()

  private def readKafkaStream(spark: SparkSession, config: Config, topicKey: String): Dataset[Row] =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("job.kafka.bootstrap-servers"))
      .option("subscribe", config.getString(s"job.kafka.topics.$topicKey"))
      .options(sourceOptions(topicKey).asMap)
      .option("failOnDataLoss", config.getString("kafka.fail-on-data-loss"))
      .load()
}
