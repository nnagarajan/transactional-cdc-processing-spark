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
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.streaming.OutputMode

/**
 * Spark Streaming application to maintain SCD Type 1 (current state) table
 * from SCD Type 2 (historical) order stream table.
 *
 * Reads from order_stream Delta Lake table (nested orders/orderDetails/lineItems arrays)
 * and merges each micro-batch into orders_current via [[ScdType1Merger]].
 */
object ScdType1MergeApp extends App with Logging {

  val config: Config = AppConfig.load("scd-type1-merge")

  val sourceTableName = config.getString("job.source-table-name")
  val targetTableName = config.getString("job.target-table-name")
  val checkpointLocation = config.getString("job.checkpoint-location")

  logger.info("Starting SCD Type 1 Merge Application")
  logger.info(s"Source Table (SCD Type 2): $sourceTableName")
  logger.info(s"Target Table (SCD Type 1): $targetTableName")
  logger.info(s"Checkpoint Location: $checkpointLocation")

  val spark = SparkSessionCreator.getSpark(config)
  spark.sparkContext.setLogLevel(config.getString("spark.log-level"))

  // Read from SCD Type 2 table as a stream
  val sourceStream = spark.readStream
    .format("delta")
    .table(sourceTableName)

  // Process micro-batches with foreachBatch
  val query = sourceStream.writeStream
    .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
      logger.info(s"Processing batch: $batchId with ${batchDF.count()} records")
      ScdType1Merger.mergeScdType1(spark, batchDF, targetTableName)
    }
    .outputMode(OutputMode.Update())
    .option("checkpointLocation", checkpointLocation)
    .start()

  logger.info("Streaming query started. Processing SCD Type 2 -> Type 1 updates...")
  query.awaitTermination()
}
