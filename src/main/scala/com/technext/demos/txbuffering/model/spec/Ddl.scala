/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.model.spec

import org.apache.spark.sql.types.{DataType, StructType}

/**
 * CREATE TABLE scripts for a job's stream and current tables, written from its model alone, for
 * SeedJob (`*.deltalake.sql`, `{{CATALOG_SCHEMA}}` placeholder, no LOCATION: the catalog chooses).
 */
object Ddl {

  val Placeholder = "{{CATALOG_SCHEMA}}"

  def streamDdl(m: JobModel): String =
    script(m, m.streamTable, Schemas.streamSchema(m),
      s"Stage 1 table of job ${m.name}: one row per transaction and root key, one array per entity, with before images.")

  def currentDdl(m: JobModel): String =
    script(m, m.currentTable, Schemas.currentSchema(m),
      s"Stage 2 table of job ${m.name}: the current state per root key.")

  /** The table name without catalog or schema, as the script names it under the placeholder. */
  def unqualified(table: String): String = table.split('.').last

  private def script(m: JobModel, table: String, schema: StructType, description: String): String = {
    val name = s"$Placeholder.${unqualified(table)}"
    val columns = schema.fields.map(f => s"    ${quote(f.name)} ${sqlType(f.dataType)}").mkString(",\n")
    // No semicolons in comments: SeedJob splits scripts into statements on every ';'
    s"""-- Written by `scripts/cdc-model.sh ddl` from the ${m.name} job config. Do not edit by hand: change the
       |-- job config and write it again.
       |-- ${description.replace(";", ",")}
       |
       |DROP TABLE IF EXISTS $name;
       |
       |CREATE TABLE IF NOT EXISTS $name (
       |$columns
       |)
       |USING DELTA
       |TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');
       |""".stripMargin
  }

  /** Spark SQL type text, with nested field names quoted. */
  def sqlType(t: DataType): String = t.sql

  private def quote(name: String): String =
    if (name.matches("[A-Za-z_][A-Za-z0-9_]*")) name else s"`${name.replace("`", "``")}`"
}
