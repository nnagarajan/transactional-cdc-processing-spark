/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.model.spec

import scala.collection.mutable
import scala.util.Try

import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.DataType

/** A job config that can't run; `errors` lists every problem found. */
final class InvalidJobConfigException(val errors: Seq[String])
    extends IllegalArgumentException(errors.mkString("Invalid job configuration:\n  - ", "\n  - ", ""))

/**
 * Builds a [[JobModel]] from a [[JobConfig]]: parses types, resolves keys, applies the checkpoint
 * defaults, and runs every startup validation of spec `cdc-model-config`, collecting all problems
 * before failing.
 */
object ModelResolver {

  /** Key of the metadata topic's starting offsets, next to the entity names. */
  val MetadataSourceKey = "transaction-metadata"

  /** Top-level columns every stream and current row has. */
  val ContextColumns: Seq[String] = Seq("xid", "csn", "dwhProcessedTs")

  /** Field holding the CSN of the change an entity without a version column holds (stage 2). */
  val ChangeCsn = "changeCsn"

  def load(config: Config): JobModel = resolve(JobConfig.parse(config))

  /** The model, or [[InvalidJobConfigException]] listing every problem. */
  def resolve(c: JobConfig, classLoader: ClassLoader = getClass.getClassLoader): JobModel =
    resolveEither(c, classLoader).fold(e => throw new InvalidJobConfigException(e), identity)

  def resolveEither(c: JobConfig, classLoader: ClassLoader = getClass.getClassLoader): Either[Seq[String], JobModel] = {
    val errors = mutable.ArrayBuffer[String](c.parseErrors: _*)

    if (c.name.isEmpty) errors += "job.name is missing"
    if (c.output.streamTable.isEmpty) errors += "job.output.stream.table is missing"
    if (c.output.currentTable.isEmpty) errors += "job.output.current.table is missing"

    // Entities and their columns
    val labels = c.entities.zipWithIndex.map { case (e, i) => e.label(i) }
    c.entities.zip(labels).foreach { case (e, label) =>
      if (e.name.isEmpty) errors += s"$label: entity has no name"
      if (e.table.isEmpty) errors += s"$label: entity has no table"
      if (e.topic.isEmpty) errors += s"$label: entity has no topic"
      if (e.columns.isEmpty) errors += s"$label: entity has no columns"
      e.role.filterNot(_ == "root").foreach(r => errors += s"$label: unknown role '$r' (only root may be given)")
      e.columns.zipWithIndex.foreach { case (col, j) =>
        val colLabel = s"$label.${col.name.orElse(col.source).getOrElse(s"column #${j + 1}")}"
        if (col.name.isEmpty) errors += s"$colLabel: column has no name"
        col.typeText match {
          case None => errors += s"$colLabel: column has no type"
          case Some(t) if parseType(t).isEmpty => errors += s"$colLabel: '$t' is not a valid Spark SQL type"
          case _ =>
        }
      }
      duplicates(e.columns.flatMap(_.name)).foreach(n => errors += s"$label: two columns are named $n")
      duplicates(e.columns.flatMap(_.source)).foreach(s => errors += s"$label: two columns have source $s")
    }
    duplicates(c.entities.flatMap(_.name)).foreach(n => errors += s"two entities are named $n")
    duplicates(c.entities.flatMap(_.table)).foreach(t => errors += s"two entities read table $t")

    // Roles and keys
    val roots = c.entities.zip(labels).filter(_._1.role.contains("root"))
    if (roots.size != 1)
      errors += s"exactly one entity must have role = root, found ${roots.size}" +
        (if (roots.nonEmpty) s" (${roots.map(_._2).mkString(", ")})" else "")
    val rootKey: Seq[String] = roots.headOption.map(_._1.key).getOrElse(Nil)

    c.entities.zip(labels).foreach { case (e, label) =>
      val declared = e.columns.flatMap(_.source).toSet
      def requireDeclared(setting: String, col: String): Unit =
        if (!declared.contains(col)) errors += s"$label: $setting names $col, which is not the source of any of its columns"
      duplicates(e.key).foreach(k => errors += s"$label: key lists $k twice")
      e.key.foreach(requireDeclared("key", _))
      e.version.foreach(requireDeclared("version", _))

      if (e.role.contains("root")) {
        if (e.key.isEmpty) errors += s"$label: the root has no key"
      } else {
        e.cardinality match {
          case None => errors += s"$label: child has no cardinality (one or many)"
          case Some("many") if e.key.isEmpty => errors += s"$label: a child with cardinality many needs a key"
          case Some("one") | Some("many") =>
          case Some(other) => errors += s"$label: unknown cardinality '$other' (one or many)"
        }
        e.rootKey match {
          case None => errors += s"$label: child has no root-key"
          case Some(RootKeyConfig.Single(col)) =>
            if (rootKey.size > 1)
              errors += s"$label: root-key names one column, but the root key has ${rootKey.size} columns " +
                s"(${rootKey.mkString(", ")}); map each of them"
            requireDeclared("root-key", col)
          case Some(RootKeyConfig.Mapping(m)) =>
            if (rootKey.nonEmpty) {
              rootKey.filterNot(m.contains).foreach(k => errors += s"$label: root-key does not map root key column $k")
              m.keys.filterNot(rootKey.contains).toSeq.sorted.foreach(k =>
                errors += s"$label: root-key maps $k, which is not a root key column")
            }
            duplicates(m.values.toSeq).foreach(v => errors += s"$label: root-key maps two root key columns to $v")
            m.values.toSeq.sorted.foreach(requireDeclared("root-key", _))
        }
      }
    }

    // Output names: top-level stream and current columns must not clash
    roots.headOption.foreach { case (root, _) =>
      val rootColumns = root.columns.flatMap(_.name)
      val rootBefore = c.output.rootBefore.getOrElse(root.name.getOrElse("root") + "Before")
      val childNames = c.entities.filterNot(_.role.contains("root")).flatMap(_.name)
      val keyNames = root.key.flatMap(k => root.columns.find(_.source.contains(k)).flatMap(_.name))
      duplicates(ContextColumns ++ keyNames ++ c.entities.flatMap(_.name)).foreach(n =>
        errors += s"stream table: two top-level columns are named $n")
      duplicates(ContextColumns ++ rootColumns ++ Seq(rootBefore) ++ childNames).foreach(n =>
        errors += s"current table: two top-level columns are named $n")
    }

    // Transform
    val hasDerived = c.entities.exists(_.columns.exists(_.source.isEmpty))
    c.output.transform match {
      case None if hasDerived =>
        val derived = c.entities.zip(labels).flatMap { case (e, l) =>
          e.columns.filter(_.source.isEmpty).flatMap(_.name).map(n => s"$l.$n") }
        errors += s"derived columns (${derived.mkString(", ")}) need job.output.transform"
      case Some(cls) => transformError(cls, classLoader).foreach(errors += _)
      case None =>
    }

    // Starting offsets
    val sourceKeys = c.entities.flatMap(_.name).toSet + MetadataSourceKey
    c.startingOffsetKeys.filterNot(sourceKeys.contains).foreach(k =>
      errors += s"job.kafka.starting-offsets.$k: no entity is named $k")

    // Checkpoints
    if (c.output.checkpointRoot.isEmpty && (c.output.streamCheckpoint.isEmpty || c.output.currentCheckpoint.isEmpty))
      errors += "job.output.checkpoint-root is missing"

    if (errors.nonEmpty) Left(errors.distinct.toSeq) else Right(build(c))
  }

  /** Parses a Spark SQL type (`double`, `decimal(20,4)`, `array<string>`, ...). */
  def parseType(text: String): Option[DataType] = Try(CatalystSqlParser.parseDataType(text)).toOption

  /** `<checkpoint root>/<job>/<stage>`. */
  def defaultCheckpoint(root: String, job: String, stage: String): String =
    s"${root.stripSuffix("/")}/$job/$stage"

  private def build(c: JobConfig): JobModel = {
    val rootConfig = c.entities.find(_.role.contains("root")).get
    val job = c.name.get
    val entities = c.entities.map { e =>
      val isRoot = e.role.contains("root")
      val rootKey =
        if (isRoot) e.key.map(k => k -> k)
        else e.rootKey.get match {
          case RootKeyConfig.Single(col) => Seq(rootConfig.key.head -> col)
          case RootKeyConfig.Mapping(m) => rootConfig.key.map(k => k -> m(k))
        }
      val cardinality = if (isRoot) None else Some(if (e.cardinality.contains("one")) Cardinality.One else Cardinality.Many)
      EntityModel(
        name = e.name.get,
        table = e.table.get,
        topic = e.topic.get,
        cardinality = cardinality,
        rootKey = rootKey,
        ownKey = if (cardinality.contains(Cardinality.One)) Nil else e.key,
        version = e.version,
        before = e.before,
        columns = e.columns.map(col => ModelColumn(col.source, col.name.get, parseType(col.typeText.get).get)))
    }
    JobModel(
      name = job,
      entities = entities,
      streamTable = c.output.streamTable.get,
      streamCheckpoint = c.output.streamCheckpoint.getOrElse(defaultCheckpoint(c.output.checkpointRoot.get, job, "stream")),
      currentTable = c.output.currentTable.get,
      currentCheckpoint = c.output.currentCheckpoint.getOrElse(defaultCheckpoint(c.output.checkpointRoot.get, job, "current")),
      rootBefore = c.output.rootBefore.getOrElse(rootConfig.name.get + "Before"),
      transform = c.output.transform,
      timestampFormat = c.timestampFormat)
  }

  private def transformError(cls: String, classLoader: ClassLoader): Option[String] =
    Try(Class.forName(cls, false, classLoader)).toOption match {
      case None => Some(s"job.output.transform: class $cls cannot be loaded")
      case Some(k) if !classOf[StreamTransform].isAssignableFrom(k) =>
        Some(s"job.output.transform: $cls does not implement ${classOf[StreamTransform].getName}")
      case Some(k) if Try(k.getConstructor()).isFailure =>
        Some(s"job.output.transform: $cls has no no-argument constructor")
      case _ => None
    }

  private def duplicates(values: Seq[String]): Seq[String] =
    values.groupBy(identity).collect { case (v, vs) if vs.size > 1 => v }.toSeq.sorted
}
