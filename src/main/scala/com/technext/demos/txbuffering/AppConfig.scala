/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Centralized configuration loader using Typesafe Config (HOCON).
 *
 * A CDC job's config ([[loadJob]]), highest priority first:
 *   1. System properties (-Djob.kafka.bootstrap-servers=...)
 *   2. The job file given with -Dconfig.file=jobs/<job>.conf, or else the bundled orders job
 *      (jobs/orders.conf in the jar)
 *   3. reference.conf, which includes cdc-defaults.conf (shared defaults)
 */
object AppConfig {

  /** Classpath resource of the job used when no -Dconfig.file is given. */
  val BundledJob = "jobs/orders.conf"

  /** The config of the CDC job both pipeline stages run. */
  def loadJob(): Config = sys.props.get("config.file") match {
    case Some(path) => loadJobFile(path)
    case None =>
      ConfigFactory.invalidateCaches()
      val bundled = ConfigFactory.parseResources(BundledJob)
      if (bundled.isEmpty) throw new IllegalStateException(s"No -Dconfig.file given and $BundledJob is not in the jar")
      layered(bundled)
  }

  /** The config of the CDC job in `path`, layered as [[loadJob]] does. */
  def loadJobFile(path: String): Config = {
    ConfigFactory.invalidateCaches()
    val f = new File(path)
    if (!f.isFile) throw new IllegalArgumentException(s"Job config file $path does not exist")
    layered(ConfigFactory.parseFile(f))
  }

  private def layered(job: Config): Config =
    ConfigFactory.systemProperties()
      .withFallback(job)
      .withFallback(ConfigFactory.defaultReference())
      .resolve()

  /** A non-CDC job's config: system properties, then `<jobName>.conf`, then reference.conf. */
  def load(jobName: String): Config =
    ConfigFactory
      .systemProperties()
      .withFallback(ConfigFactory.load(jobName))
      .resolve()
}
