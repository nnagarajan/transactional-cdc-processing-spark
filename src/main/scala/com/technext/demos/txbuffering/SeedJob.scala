/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import java.io.File
import java.nio.file.Files

import org.apache.logging.log4j.scala.Logging

/**
 * Deploys database scripts (*.deltalake.sql) from the configured scripts directory.
 *
 * Scans the directory for files matching the *.deltalake.sql pattern,
 * sorts them alphabetically, and executes each SQL statement via Spark SQL.
 */
object SeedJob extends App with Logging {

  val config = AppConfig.load("seed-job")
  val scriptsDir = config.getString("job.scripts-dir")

  logger.info("Starting Seed Job")
  logger.info(s"Scripts directory: $scriptsDir")

  val spark = SparkSessionCreator.getSpark(config)
  spark.sparkContext.setLogLevel(config.getString("spark.log-level"))

  val dir = new File(scriptsDir)
  if (!dir.isDirectory) {
    logger.error(s"Scripts directory does not exist: $scriptsDir")
    System.exit(1)
  }

  val scripts = dir.listFiles()
    .filter(f => f.isFile && f.getName.endsWith(".deltalake.sql"))
    .sortBy(_.getName)

  if (scripts.isEmpty) {
    logger.warn(s"No *.deltalake.sql scripts found in $scriptsDir")
  } else {
    logger.info(s"Found ${scripts.length} script(s): ${scripts.map(_.getName).mkString(", ")}")

    scripts.foreach { scriptFile =>
      logger.info(s"Executing script: ${scriptFile.getName}")
      val content = new String(Files.readAllBytes(scriptFile.toPath))

      val statements = content
        .split(";")
        .map(_.trim)
        .filter(s => s.nonEmpty && !s.matches("^--.*"))

      statements.foreach { sql =>
        logger.info(s"Running SQL: ${sql.take(100)}...")
        spark.sql(sql)
      }

      logger.info(s"Completed script: ${scriptFile.getName}")
    }
  }

  logger.info("Seed Job completed")
  spark.stop()
}
