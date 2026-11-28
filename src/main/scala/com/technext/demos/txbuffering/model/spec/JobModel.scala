/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.model.spec

import org.apache.spark.sql.types.DataType

/** How a child relates to the root: at most one row per root key, or any number. */
sealed trait Cardinality
object Cardinality {
  case object One extends Cardinality
  case object Many extends Cardinality
}

/**
 * One output column of an entity. `source` is the source column it is cast from, or None for a
 * derived column that the job's transform fills in.
 */
final case class ModelColumn(source: Option[String], name: String, dataType: DataType)

/**
 * A resolved entity. Key settings name source columns.
 *
 * @param rootKey for each root key column, in root key order, the column of this entity holding
 *                its value (for the root, the key columns themselves)
 * @param ownKey  the columns identifying this entity's elements: the key for the root and for a
 *                one-to-many child, empty for a one-to-one child (identified by the root key)
 */
final case class EntityModel(
    name: String,
    table: String,
    topic: String,
    cardinality: Option[Cardinality],
    rootKey: Seq[(String, String)],
    ownKey: Seq[String],
    version: Option[String],
    before: Boolean,
    columns: Seq[ModelColumn]) {

  def isRoot: Boolean = cardinality.isEmpty

  /** The output column cast from `source`. */
  def columnFor(source: String): ModelColumn =
    columns.find(_.source.contains(source)).getOrElse(
      throw new IllegalArgumentException(s"$name has no column with source $source"))

  def sourceColumns: Seq[String] = columns.flatMap(_.source)
}

/** A job's resolved model, built by [[ModelResolver]]; everything schema-dependent reads only this. */
final case class JobModel(
    name: String,
    entities: Seq[EntityModel],
    streamTable: String,
    streamCheckpoint: String,
    currentTable: String,
    currentCheckpoint: String,
    rootBefore: String,
    transform: Option[String],
    timestampFormat: String) {

  def root: EntityModel = entities.find(_.isRoot).get

  def children: Seq[EntityModel] = entities.filterNot(_.isRoot)

  /** Source table names (unqualified, as markers name them) of every entity. */
  def trackedTables: Seq[String] = entities.map(_.table)

  def entityForTable(table: String): Option[EntityModel] = entities.find(_.table == table)

  /** The root key's output columns, in key order: their names and types key every output row. */
  def rootKeyColumns: Seq[ModelColumn] = root.ownKey.map(root.columnFor)

  def hasDerivedColumns: Boolean = entities.exists(_.columns.exists(_.source.isEmpty))
}
