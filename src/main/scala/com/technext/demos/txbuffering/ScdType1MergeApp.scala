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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, flatten, row_number}
import org.apache.spark.sql.streaming.OutputMode

import io.delta.tables.DeltaTable

/**
 * Spark Streaming application to maintain SCD Type 1 (current state) table
 * from SCD Type 2 (historical) order stream table.
 *
 * Reads from order_stream Delta Lake table (nested orders/orderDetails/lineItems arrays),
 * transforms to flat orders_current schema, and performs version-aware merges.
 *
 * Dedup logic:
 *  - orders array: dedup by orderId + version (keep highest)
 *  - orderDetails array: dedup by orderId + version (keep highest) → single struct
 *  - lineItems array: dedup by lineItemId + version (keep highest)
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
      mergeScdType1(spark, batchDF, targetTableName)
    }
    .outputMode(OutputMode.Update())
    .option("checkpointLocation", checkpointLocation)
    .start()

  logger.info("Streaming query started. Processing SCD Type 2 -> Type 1 updates...")
  query.awaitTermination()

  /**
   * Transforms the nested order_stream batch to flat orders_current schema,
   * then merges into the target SCD Type 1 table with version-aware logic.
   */
  private def mergeScdType1(spark: SparkSession, batchDF: Dataset[Row], targetTableName: String): Unit = {
    if (batchDF.isEmpty) return

    val transformed = transformSourceBatch(batchDF)

    if (!spark.catalog.tableExists(targetTableName)) {
      logger.info(s"Target table $targetTableName does not exist. Creating initial table...")
      transformed.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(targetTableName)
      return
    }

    val targetTable = DeltaTable.forName(spark, targetTableName)

    // Version-aware merge with two whenMatched clauses (evaluated in order):
    // 1. Source order version wins → take all order fields from source + merge children
    // 2. Catch-all → keep target order fields, only update tx context + merge children
    // Children (orderDetails, lineItems) are versioned independently from the parent.
    val orderSourceWins = "source.version IS NOT NULL AND source.version > COALESCE(target.version, 0)"

    // Child merge expressions (used in both clauses since children version independently)
    val mergedDetails = mergeStructExpr("orderDetails")
    val mergedLineItems = mergeArrayExpr("lineItems", "lineItemId")

    targetTable.alias("target")
      .merge(
        transformed.alias("source"),
        "target.orderId = source.orderId"
      )
      // Source order version is newer — take all order fields from source
      .whenMatched(orderSourceWins)
      .updateExpr(Map(
        "xid" -> "source.xid",
        "csn" -> "source.csn",
        "dwhProcessedTs" -> "source.dwhProcessedTs",
        "orderRef" -> "source.orderRef",
        "version" -> "source.version",
        "orderDate" -> "source.orderDate",
        "orderTs" -> "source.orderTs",
        "orderStatus" -> "source.orderStatus",
        "orderType" -> "source.orderType",
        "totalAmount" -> "source.totalAmount",
        "currency" -> "source.currency",
        "customerId" -> "source.customerId",
        "shippingAddressId" -> "source.shippingAddressId",
        "createdTs" -> "source.createdTs",
        "orderBefore" -> "source.orderBefore",
        "orderDetails" -> mergedDetails,
        "lineItems" -> mergedLineItems
      ))
      // Source order version is NOT newer — keep target order fields, merge children only
      .whenMatched()
      .updateExpr(Map(
        "xid" -> "source.xid",
        "csn" -> "source.csn",
        "dwhProcessedTs" -> "source.dwhProcessedTs",
        "orderDetails" -> mergedDetails,
        "lineItems" -> mergedLineItems
      ))
      .whenNotMatched("source.version IS NOT NULL")
      .insertAll()
      .execute()

    logger.info("Version-aware merge completed successfully")
  }

  /**
   * Transforms the nested order_stream structure to the flat orders_current schema.
   *
   * When multiple order_stream records exist for the same orderId in a single batch
   * (e.g., order creation + child-only update), this method merges them:
   * - Order-level fields: from the row with the highest order version
   * - orderDetails: non-null struct with the highest version across all rows
   * - lineItems: merged from all rows, deduped by lineItemId (highest version wins)
   */
  private def transformSourceBatch(batchDF: Dataset[Row]): Dataset[Row] = {
    // Dedup within arrays: keep element with max version per key
    val latestOrder = dedupExpr("orders", "orderId")
    val latestDetail = dedupExpr("orderDetails", "orderId")
    val dedupLineItemsExpr = dedupExpr("lineItems", "lineItemId")

    // Use try_element_at (1-based) to safely handle empty arrays — returns null instead of throwing
    val safeOrder = s"try_element_at($latestOrder, 1)"
    val safeDetail = s"try_element_at($latestDetail, 1)"

    val flattened = batchDF.selectExpr(
      "xid", "csn", "dwhProcessedTs", "orderId",

      // Flatten latest order to top-level fields (null-safe for empty orders array)
      s"$safeOrder.orderRef as orderRef",
      s"$safeOrder.version as version",
      s"$safeOrder.orderDate as orderDate",
      s"$safeOrder.orderTs as orderTs",
      s"$safeOrder.orderStatus as orderStatus",
      s"$safeOrder.orderType as orderType",
      s"$safeOrder.totalAmount as totalAmount",
      s"$safeOrder.currency as currency",
      s"$safeOrder.customerId as customerId",
      s"$safeOrder.shippingAddressId as shippingAddressId",
      s"$safeOrder.createdTs as createdTs",
      s"$safeOrder.before as orderBefore",

      // Latest order detail as single struct (null-safe for empty orderDetails array)
      s"$safeDetail as orderDetails",

      // Deduped line items as array
      s"$dedupLineItemsExpr as lineItems"
    )

    // Step 1: Pick best row for order-level fields (highest version)
    val w = Window.partitionBy("orderId").orderBy(col("version").desc_nulls_last, col("dwhProcessedTs").desc_nulls_last)
    val bestOrderRow = flattened
      .withColumn("_rn", row_number().over(w))
      .filter("_rn = 1")
      .drop("_rn", "orderDetails", "lineItems")

    // Step 2: Pick best orderDetails across all rows (non-null, highest version)
    val bestDetails = flattened
      .filter("orderDetails IS NOT NULL")
      .withColumn("_rn", row_number().over(
        Window.partitionBy("orderId").orderBy(col("orderDetails.version").desc_nulls_last)))
      .filter("_rn = 1")
      .select("orderId", "orderDetails")

    // Step 3: Merge lineItems from all rows — flatten, then dedup by lineItemId
    val mergedLineItems = flattened
      .filter("lineItems IS NOT NULL AND size(lineItems) > 0")
      .groupBy("orderId")
      .agg(flatten(collect_list(col("lineItems"))).as("_allLineItems"))
      .selectExpr("orderId", s"${dedupExpr("_allLineItems", "lineItemId")} as lineItems")

    // Step 4: Combine order fields + best detail + merged line items
    bestOrderRow
      .join(bestDetails, Seq("orderId"), "left")
      .join(mergedLineItems, Seq("orderId"), "left")
  }

  /**
   * Generates a filter expression that deduplicates an array by a key field,
   * keeping only the element with the highest version for each unique key.
   */
  private def dedupExpr(arrayField: String, keyField: String): String =
    s"filter($arrayField, e -> NOT exists($arrayField, other -> other.$keyField = e.$keyField AND other.version > e.version))"

  /**
   * Generates a SQL expression that merges two struct fields by version,
   * keeping the struct with the higher version.
   */
  private def mergeStructExpr(structField: String): String =
    s"""CASE
       |  WHEN source.$structField IS NULL THEN target.$structField
       |  WHEN target.$structField IS NULL THEN source.$structField
       |  WHEN source.$structField.version > COALESCE(target.$structField.version, 0) THEN source.$structField
       |  ELSE target.$structField
       |END""".stripMargin

  /**
   * Generates a SQL expression that merges two arrays of structs by a key field,
   * keeping the element with the higher version for each unique key.
   *
   * Logic:
   *  - Source elements are included when no target element has the same key with version >= source version
   *  - Target elements are kept when no source element has the same key with version > target version
   *  - New elements are added, updated elements are replaced, unchanged elements are preserved
   */
  private def mergeArrayExpr(arrayField: String, keyField: String): String =
    s"""CASE
       |  WHEN source.$arrayField IS NULL OR size(source.$arrayField) = 0 THEN target.$arrayField
       |  WHEN target.$arrayField IS NULL OR size(target.$arrayField) = 0 THEN source.$arrayField
       |  ELSE concat(
       |    filter(source.$arrayField, se -> NOT exists(target.$arrayField, te -> te.$keyField = se.$keyField AND te.version >= se.version)),
       |    filter(target.$arrayField, te -> NOT exists(source.$arrayField, se -> se.$keyField = te.$keyField AND se.version > te.version))
       |  )
       |END""".stripMargin
}
