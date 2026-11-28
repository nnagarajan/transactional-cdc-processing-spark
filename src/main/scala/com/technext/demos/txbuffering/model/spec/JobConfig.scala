/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.model.spec

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.typesafe.config.{Config, ConfigValue, ConfigValueType}

/** A column as written in the job config. `source = None` declares a derived column. */
final case class ColumnConfig(source: Option[String], name: Option[String], typeText: Option[String])

/**
 * A child's root key as written: a single child column (only valid for a one-column root key),
 * or a map from each root key column to the child column holding it.
 */
sealed trait RootKeyConfig
object RootKeyConfig {
  final case class Single(column: String) extends RootKeyConfig
  final case class Mapping(columns: Map[String, String]) extends RootKeyConfig
}

/** An entity as written in the job config; validation happens in [[ModelResolver]]. */
final case class EntityConfig(
    name: Option[String],
    role: Option[String],
    table: Option[String],
    topic: Option[String],
    key: Seq[String],
    rootKey: Option[RootKeyConfig],
    cardinality: Option[String],
    version: Option[String],
    before: Boolean,
    columns: Seq[ColumnConfig]) {

  /** How errors refer to this entity. */
  def label(index: Int): String = name.getOrElse(s"entity #${index + 1}")
}

/** Output settings as written: tables, checkpoints, the root's before column, the transform. */
final case class OutputConfig(
    streamTable: Option[String],
    streamCheckpoint: Option[String],
    currentTable: Option[String],
    currentCheckpoint: Option[String],
    rootBefore: Option[String],
    transform: Option[String],
    checkpointRoot: Option[String])

/**
 * A job's model as written in its config (`job.name`, `job.model.entities`, `job.output`), plus
 * the shared settings it depends on. Parsing never fails: anything missing or of the wrong type
 * is recorded in `parseErrors`, so that [[ModelResolver]] reports every problem together.
 */
final case class JobConfig(
    name: Option[String],
    entities: Seq[EntityConfig],
    output: OutputConfig,
    timestampFormat: String,
    startingOffsetKeys: Seq[String],
    parseErrors: Seq[String])

object JobConfig {

  val DefaultTimestampFormat = "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]"

  def parse(config: Config): JobConfig = {
    val errors = Seq.newBuilder[String]

    val entities: Seq[EntityConfig] =
      if (!config.hasPath("job.model.entities")) {
        errors += "job.model.entities is missing"
        Nil
      } else Try(config.getConfigList("job.model.entities").asScala.toSeq) match {
        case scala.util.Success(list) => list.zipWithIndex.map { case (c, i) => parseEntity(c, i, errors) }
        case scala.util.Failure(_) =>
          errors += "job.model.entities must be a list of entity objects"
          Nil
      }

    val output = OutputConfig(
      streamTable = str(config, "job.output.stream.table"),
      streamCheckpoint = str(config, "job.output.stream.checkpoint"),
      currentTable = str(config, "job.output.current.table"),
      currentCheckpoint = str(config, "job.output.current.checkpoint"),
      rootBefore = str(config, "job.output.current.root-before"),
      transform = str(config, "job.output.transform"),
      checkpointRoot = str(config, "job.output.checkpoint-root"))

    val offsetKeys =
      if (config.hasPath("job.kafka.starting-offsets"))
        Try(config.getObject("job.kafka.starting-offsets").keySet().asScala.toSeq.sorted).getOrElse {
          errors += "job.kafka.starting-offsets must be an object keyed by entity name"
          Nil
        }
      else Nil

    JobConfig(
      name = str(config, "job.name"),
      entities = entities,
      output = output,
      timestampFormat = str(config, "job.source.timestamp-format").getOrElse(DefaultTimestampFormat),
      startingOffsetKeys = offsetKeys,
      parseErrors = errors.result())
  }

  private def parseEntity(c: Config, index: Int, errors: collection.mutable.Builder[String, Seq[String]]): EntityConfig = {
    val name = str(c, "name")
    def label = name.getOrElse(s"entity #${index + 1}")

    val key: Seq[String] =
      if (!c.hasPath("key")) Nil
      else columnList(c.getValue("key")).getOrElse {
        errors += s"$label: key must be a column name or a list of column names"
        Nil
      }

    val rootKey: Option[RootKeyConfig] =
      if (!c.hasPath("root-key")) None
      else {
        val v = c.getValue("root-key")
        v.valueType() match {
          case ConfigValueType.STRING => Some(RootKeyConfig.Single(v.unwrapped().toString))
          case ConfigValueType.OBJECT =>
            val m = c.getObject("root-key").asScala.toMap
            if (m.values.forall(_.valueType() == ConfigValueType.STRING))
              Some(RootKeyConfig.Mapping(m.map { case (k, cv) => k -> cv.unwrapped().toString }))
            else {
              errors += s"$label: root-key must map each root key column to a child column name"
              None
            }
          case _ =>
            errors += s"$label: root-key must be a column name or a map of root key column to child column"
            None
        }
      }

    val before =
      if (!c.hasPath("before")) true
      else Try(c.getBoolean("before")).getOrElse {
        errors += s"$label: before must be true or false"
        true
      }

    val columns: Seq[ColumnConfig] =
      if (!c.hasPath("columns")) Nil
      else Try(c.getConfigList("columns").asScala.toSeq).map(_.map { col =>
        ColumnConfig(str(col, "source"), str(col, "name"), str(col, "type"))
      }).getOrElse {
        errors += s"$label: columns must be a list of {source, name, type} objects"
        Nil
      }

    EntityConfig(name, str(c, "role"), str(c, "table"), str(c, "topic"), key, rootKey,
      str(c, "cardinality"), str(c, "version"), before, columns)
  }

  /** A string setting, or None when absent or blank. */
  private def str(c: Config, path: String): Option[String] =
    if (!c.hasPath(path)) None
    else Try(c.getString(path)).toOption.map(_.trim).filter(_.nonEmpty)

  private def columnList(v: ConfigValue): Option[Seq[String]] = v.valueType() match {
    case ConfigValueType.STRING => Some(Seq(v.unwrapped().toString))
    case ConfigValueType.LIST =>
      val items = v.unwrapped().asInstanceOf[java.util.List[AnyRef]].asScala.toSeq
      if (items.forall(_.isInstanceOf[String])) Some(items.map(_.toString)) else None
    case _ => None
  }
}
