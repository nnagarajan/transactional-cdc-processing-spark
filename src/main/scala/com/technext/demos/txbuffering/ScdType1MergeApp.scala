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

import com.technext.demos.txbuffering.model.spec.{JobModel, ModelResolver, Schemas}

/**
 * Stage 2 of a CDC job: Spark Streaming application that maintains the job's current table (SCD
 * Type 1, one row per root key) from its stream table (SCD Type 2, one row per transaction and
 * root key).
 *
 * Reads the stream table as a Delta stream and merges each micro-batch into the current table via
 * [[CurrentStateMerger]], generated from the job's model. Run with -Dconfig.file=jobs/<job>.conf
 * (the bundled orders job without it), the same config stage 1 runs with.
 */
object ScdType1MergeApp extends App with Logging {

  val config: Config = AppConfig.loadJob()
  val model: JobModel = ModelResolver.load(config)

  logger.info(s"Starting SCD Type 1 Merge Application, job ${model.name}")
  logger.info(s"Source Table (SCD Type 2): ${model.streamTable}")
  logger.info(s"Target Table (SCD Type 1): ${model.currentTable}")
  logger.info(s"Checkpoint Location: ${model.currentCheckpoint}")

  val spark = SparkSessionCreator.getSpark(config)
  spark.sparkContext.setLogLevel(config.getString("spark.log-level"))
  CdcOutputSink.requireTableSchema(spark, model.streamTable, Schemas.streamSchema(model), model.name)
  CdcOutputSink.requireTableSchema(spark, model.currentTable, Schemas.currentSchema(model), model.name)

  val merger = new CurrentStateMerger(model)

  // Read from the stream table as a stream
  val sourceStream = spark.readStream
    .format("delta")
    .table(model.streamTable)

  // Process micro-batches with foreachBatch
  val query = sourceStream.writeStream
    .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
      logger.info(s"Processing batch: $batchId with ${batchDF.count()} records")
      merger.merge(spark, batchDF)
    }
    .outputMode(OutputMode.Update())
    .option("checkpointLocation", model.currentCheckpoint)
    .start()

  logger.info(s"Streaming query started. Processing ${model.streamTable} -> ${model.currentTable} updates...")
  query.awaitTermination()
}
