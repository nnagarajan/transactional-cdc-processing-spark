/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.model.spec

import org.apache.spark.sql.DataFrame

/**
 * Hand-written stage 1 logic for what a rename and cast can't express (derived values, parsing a
 * composite source value, normalizing codes). Named by `job.output.transform`; the class needs a
 * no-argument constructor.
 *
 * It gets each micro-batch's shaped records, with the stream table's schema (derived columns hold
 * typed nulls), and must return records with exactly that schema.
 */
trait StreamTransform extends Serializable {
  def apply(records: DataFrame, model: JobModel): DataFrame
}
