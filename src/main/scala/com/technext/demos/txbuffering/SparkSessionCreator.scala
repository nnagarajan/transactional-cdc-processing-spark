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
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionCreator extends Logging {

  /**
   * Spark's vectorized Parquet reader for nested columns fails with
   * `NullPointerException: Cannot store to byte/boolean array because "this.nulls" is null`
   * when Delta's v2 (kernel) connector reads `order_stream`, whose `orders`, `orderDetails` and
   * `lineItems` are arrays of structs. UC catalog-managed tables can only be read through that
   * connector (`spark.databricks.delta.v2.enableMode = NONE` is refused with
   * DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED), so nested vectorization is turned
   * off instead. Top-level columns stay vectorized. A value given with `--conf` is kept.
   */
  val NestedVectorizedReader = "spark.sql.parquet.enableNestedColumnVectorizedReader"

  def getSpark(config: Config): SparkSession = {
    val spark = create(config)
    if (!new SparkConf().contains(NestedVectorizedReader)) {
      spark.conf.set(NestedVectorizedReader, "false")
    }
    spark
  }

  private def create(config: Config): SparkSession = {
    val hasMaster = new SparkConf().getOption("spark.master").isDefined

    if (hasMaster) {
      logger.info("Using spark-submit configuration")
      SparkSession.builder().getOrCreate()
    } else {
      logger.info("Using dev configuration for spark")
      val catalogName = config.getString("spark.unity-catalog.catalog-name")

      SparkSession.builder()
        .master(config.getString("spark.dev.master"))
        .appName(config.getString("spark.dev.app-name"))
        .config("spark.sql.extensions", config.getString("spark.extensions.sql-extensions"))
        .config("spark.sql.catalog.spark_catalog", config.getString("spark.extensions.catalog"))
        .config(s"spark.sql.catalog.$catalogName", config.getString("spark.unity-catalog.catalog-class"))
        .config(s"spark.sql.catalog.$catalogName.uri", config.getString("spark.unity-catalog.uri"))
        .config(s"spark.sql.catalog.$catalogName.auth.type", config.getString("spark.unity-catalog.auth-type"))
        .config(s"spark.sql.catalog.$catalogName.auth.token", config.getString("spark.unity-catalog.auth-token"))
        .config(s"spark.sql.catalog.$catalogName.credentialVending", config.getString("spark.unity-catalog.credential-vending"))
        .config("spark.sql.defaultCatalog", catalogName)
        .config("spark.ui.port", config.getString("spark.ui-port"))
        .config("spark.sql.warehouse.dir", config.getString("spark.warehouse-dir"))
        .config("spark.sql.streaming.stateStore.providerClass", config.getString("spark.state-store.provider-class"))
        .config("spark.executor.memory", config.getString("spark.resources.executor-memory"))
        .config("spark.driver.memory", config.getString("spark.resources.driver-memory"))
        .config("spark.driver.cores", config.getString("spark.resources.driver-cores"))
        .getOrCreate()
    }
  }
}
