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

  def getSpark(config: Config): SparkSession = {
    val hasMaster = new SparkConf().getOption("spark.master").isDefined

    if (hasMaster) {
      logger.info("Using spark-submit configuration")
      SparkSession.builder().getOrCreate()
    } else {
      logger.info("Using dev configuration for spark")
      val builder = SparkSession.builder()
        .master(config.getString("spark.dev.master"))
        .appName(config.getString("spark.dev.app-name"))
        .config("spark.sql.extensions", config.getString("spark.extensions.sql-extensions"))
        .config("spark.sql.catalog.spark_catalog", config.getString("spark.extensions.catalog"))
        .config("spark.ui.port", config.getString("spark.ui-port"))
        .config("spark.sql.warehouse.dir", config.getString("spark.warehouse-dir"))
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", config.getString("spark.hive.connection-url"))
        .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", config.getString("spark.hive.connection-driver"))
        .config("spark.sql.streaming.stateStore.providerClass", config.getString("spark.state-store.provider-class"))
        .config("spark.executor.memory", config.getString("spark.resources.executor-memory"))
        .config("spark.driver.memory", config.getString("spark.resources.driver-memory"))
        .config("spark.driver.cores", config.getString("spark.resources.driver-cores"))

      if (config.getBoolean("spark.dev.enable-hive-support")) builder.enableHiveSupport().getOrCreate()
      else builder.getOrCreate()
    }
  }
}
