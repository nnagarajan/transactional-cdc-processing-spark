/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Centralized configuration loader using Typesafe Config (HOCON).
 *
 * Config resolution order (highest to lowest priority):
 *   1. System properties (-Djob.kafka.bootstrap-servers=...)
 *   2. Job-specific config file (e.g., transactional-cdc-processing.conf)
 *   3. reference.conf (shared defaults)
 */
object AppConfig {

  def load(jobName: String): Config =
    ConfigFactory
      .systemProperties()
      .withFallback(ConfigFactory.load(jobName))
      .resolve()
}
