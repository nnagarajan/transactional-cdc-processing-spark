/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, expr, flatten, row_number}

import io.delta.tables.DeltaTable

import com.technext.demos.txbuffering.model.spec.{Cardinality, EntityModel, JobModel, ModelResolver, Schemas}

/**
 * Merges batches of a job's stream table (one row per transaction and root key, one array per
 * entity) into its current table (one row per root key) with version-aware logic, generated from
 * the job's model:
 *
 *  - root: its latest change, flattened to top-level columns, with its before image as one column
 *  - one-to-one child: one struct, its latest change
 *  - one-to-many child: one array, the latest change per element key (the child's declared key)
 *
 * "Latest" is the highest version when the entity has a version column; otherwise the change from
 * the highest CSN, recorded in a `changeCsn` field. Transaction context (xid, csn, dwhProcessedTs)
 * only ever moves to a higher CSN, and a root key first seen through a child-only change is kept
 * as a placeholder row (null root attributes) until its root record arrives.
 *
 * The current table must exist with the model's schema; the merger never creates it.
 */
final class CurrentStateMerger(model: JobModel) extends Serializable with Logging {

  import CurrentStateMerger._

  private val root = model.root
  private val keys: Seq[String] = model.rootKeyColumns.map(_.name)
  private val keyCols: Seq[Column] = keys.map(col)
  private val ChangeCsn = ModelResolver.ChangeCsn

  def merge(spark: SparkSession, batchDF: Dataset[Row]): Unit = {
    if (batchDF.isEmpty) return
    val transformed = transformSourceBatch(batchDF)
    val newerCsn = "CAST(source.csn AS DECIMAL(38,0)) > COALESCE(CAST(target.csn AS DECIMAL(38,0)), -1)"
    def txContext(column: String): String = s"CASE WHEN $newerCsn THEN source.$column ELSE target.$column END"
    val context = ModelResolver.ContextColumns.map(c => q(c) -> txContext(q(c)))
    val children = model.children.map(e => q(e.name) -> childMerge(e))

    val rootFields =
      root.columns.map(_.name).filterNot(keys.contains) ++
        (if (root.version.isEmpty) Seq(ChangeCsn) else Nil) ++
        (if (root.before) Seq(model.rootBefore) else Nil)

    DeltaTable.forName(spark, model.currentTable).alias("target")
      .merge(transformed.alias("source"), keys.map(k => s"target.${q(k)} = source.${q(k)}").mkString(" AND "))
      // The source's root change is newer: take all root fields from it, and merge the children
      .whenMatched(rootSourceWins)
      .updateExpr((context ++ rootFields.map(f => q(f) -> s"source.${q(f)}") ++ children).toMap)
      // Otherwise keep the target's root fields and merge the children only
      .whenMatched()
      .updateExpr((context ++ children).toMap)
      // Unknown root key: insert, including child-only rows (null root attributes) as placeholders
      .whenNotMatched()
      .insertAll()
      .execute()

    logger.info(s"Merged a batch into ${model.currentTable}")
  }

  /**
   * The stream batch as current-table rows, one per root key. When the batch holds several rows
   * for a key (for example a creation and a later child-only change), they are combined:
   *  - root fields: from the row with the latest root change (ties: highest CSN)
   *  - xid, csn, dwhProcessedTs: from the row with the highest CSN (may be a child-only row)
   *  - one-to-one child: its latest change across all rows
   *  - one-to-many child: elements from all rows, the latest change per element key
   */
  def transformSourceBatch(batchDF: Dataset[Row]): Dataset[Row] = {
    val rootLatest = latestIn(root, q(root.name))
    val rootSelect =
      root.columns.filterNot(c => keys.contains(c.name)).map(c => s"$rootLatest.${q(c.name)} AS ${q(c.name)}") ++
        (if (root.version.isEmpty) Seq(s"CASE WHEN $rootLatest IS NULL THEN NULL ELSE csn END AS $ChangeCsn") else Nil) ++
        (if (root.before) Seq(s"$rootLatest.${Schemas.Before} AS ${q(model.rootBefore)}") else Nil)
    val childSelect = model.children.map { e =>
      val elements = withChangeCsn(e, q(e.name))
      e.cardinality.get match {
        case Cardinality.One => s"${latestIn(e, elements)} AS ${q(e.name)}"
        case Cardinality.Many => s"${dedup(e, elements)} AS ${q(e.name)}"
      }
    }
    val flattened = batchDF.selectExpr(
      (ModelResolver.ContextColumns ++ keys).map(q) ++ rootSelect ++ childSelect: _*)

    // Root fields: the row with the latest root change; ties go to the latest commit
    val rootOrder = root.version match {
      case Some(_) => Seq(col(versionName(root)).desc_nulls_last, csnDecimal(col("csn")).desc_nulls_last)
      case None => Seq(csnDecimal(col(ChangeCsn)).desc_nulls_last)
    }
    val bestRootRow = firstPerKey(flattened, rootOrder)
      .drop(ModelResolver.ContextColumns ++ model.children.map(_.name): _*)

    // Transaction context from the latest commit, which can be a child-only row
    val latestTx = firstPerKey(flattened, Seq(csnDecimal(col("csn")).desc))
      .select(keyCols ++ ModelResolver.ContextColumns.map(col): _*)

    val childFrames = model.children.map { e =>
      e.cardinality.get match {
        case Cardinality.One =>
          val order = e.version match {
            case Some(_) => Seq(col(s"${e.name}.${versionName(e)}").desc_nulls_last, csnDecimal(col("csn")).desc_nulls_last)
            case None => Seq(csnDecimal(col(s"${e.name}.$ChangeCsn")).desc_nulls_last)
          }
          firstPerKey(flattened.filter(col(e.name).isNotNull), order).select(keyCols :+ col(e.name): _*)
        case Cardinality.Many =>
          val all = s"_all_${e.name}"
          flattened
            .filter(col(e.name).isNotNull && expr(s"size(${q(e.name)}) > 0"))
            .groupBy(keyCols: _*)
            .agg(flatten(collect_list(col(e.name))).as(all))
            .select(keyCols :+ expr(dedup(e, q(all))).as(e.name): _*)
      }
    }

    val joined = childFrames.foldLeft(latestTx.join(bestRootRow, keys)) { (acc, f) => acc.join(f, keys, "left") }
    joined.select(Schemas.currentSchema(model).fieldNames.map(f => col(q(f))).toIndexedSeq: _*)
  }

  // ---- generated expressions ----

  /** The ordering value of an element: its version, or the CSN of its change when versionless. */
  private def ordering(e: EntityModel, element: String): String = e.version match {
    case Some(_) => s"$element.${q(versionName(e))}"
    case None => s"CAST($element.$ChangeCsn AS DECIMAL(38,0))"
  }

  private def versionName(e: EntityModel): String = e.columnFor(e.version.get).name

  /** Whether two elements are the same entity row: equal element keys (root key for root and 1:1). */
  private def sameKey(e: EntityModel, a: String, b: String): String = {
    val names = if (e.ownKey.isEmpty) e.rootKey.map(_._2).map(e.columnFor(_).name) else e.ownKey.map(e.columnFor(_).name)
    names.map(n => s"$a.${q(n)} = $b.${q(n)}").mkString("(", " AND ", ")")
  }

  /**
   * A stream array's latest element. With a version: the highest version (the first of equals, as
   * before). Without: the last element, the transaction's final change.
   */
  private def latestIn(e: EntityModel, array: String): String = e.version match {
    case Some(_) => s"try_element_at(${dedup(e, array)}, 1)"
    case None => s"try_element_at($array, -1)"
  }

  /**
   * An array without superseded elements: per element key, those no other element beats. With a
   * version, a strictly higher version beats; without, a higher change CSN, or a later position
   * at the same CSN.
   */
  private def dedup(e: EntityModel, array: String): String = e.version match {
    case Some(_) =>
      s"filter($array, el -> NOT exists($array, other -> ${sameKey(e, "other", "el")} AND ${ordering(e, "other")} > ${ordering(e, "el")}))"
    case None =>
      val indexed = s"transform($array, (x, i) -> named_struct('i', i, 'e', x))"
      s"transform(filter($indexed, p -> NOT exists($indexed, o -> ${sameKey(e, "o.e", "p.e")} AND " +
        s"(${ordering(e, "o.e")} > ${ordering(e, "p.e")} OR (${ordering(e, "o.e")} = ${ordering(e, "p.e")} AND o.i > p.i)))), p -> p.e)"
  }

  /** A child's stream elements, with `changeCsn` (the row's CSN) added when the child has no version. */
  private def withChangeCsn(e: EntityModel, array: String): String =
    if (e.version.isDefined) array
    else {
      val fields = e.columns.map(c => s"'${c.name}', x.${q(c.name)}") ++ Seq(s"'$ChangeCsn', csn") ++
        (if (e.before) Seq(s"'${Schemas.Before}', x.${Schemas.Before}") else Nil)
      s"transform($array, x -> named_struct(${fields.mkString(", ")}))"
    }

  private def rootSourceWins: String = root.version match {
    case Some(_) =>
      val v = q(versionName(root))
      s"source.$v IS NOT NULL AND source.$v > COALESCE(target.$v, 0)"
    case None =>
      s"source.$ChangeCsn IS NOT NULL AND CAST(source.$ChangeCsn AS DECIMAL(38,0)) > COALESCE(CAST(target.$ChangeCsn AS DECIMAL(38,0)), -1)"
  }

  /** Target and source value of a child merged: the struct with the later change, or arrays merged per element key. */
  private def childMerge(e: EntityModel): String = {
    val f = q(e.name)
    e.cardinality.get match {
      case Cardinality.One =>
        s"""CASE
           |  WHEN source.$f IS NULL THEN target.$f
           |  WHEN target.$f IS NULL THEN source.$f
           |  WHEN ${ordering(e, s"source.$f")} > COALESCE(${ordering(e, s"target.$f")}, ${floor(e)}) THEN source.$f
           |  ELSE target.$f
           |END""".stripMargin
      case Cardinality.Many =>
        // A source element is taken unless the target has it at an equal or later change; a target
        // element is kept unless the source has it at a later change
        s"""CASE
           |  WHEN source.$f IS NULL OR size(source.$f) = 0 THEN target.$f
           |  WHEN target.$f IS NULL OR size(target.$f) = 0 THEN source.$f
           |  ELSE concat(
           |    filter(source.$f, se -> NOT exists(target.$f, te -> ${sameKey(e, "te", "se")} AND ${ordering(e, "te")} >= ${ordering(e, "se")})),
           |    filter(target.$f, te -> NOT exists(source.$f, se -> ${sameKey(e, "se", "te")} AND ${ordering(e, "se")} > ${ordering(e, "te")}))
           |  )
           |END""".stripMargin
    }
  }

  /** What a missing target ordering value counts as: version 0, or CSN -1. */
  private def floor(e: EntityModel): String = if (e.version.isDefined) "0" else "-1"

  private def firstPerKey(df: Dataset[Row], order: Seq[Column]): Dataset[Row] =
    df.withColumn("_rn", row_number().over(Window.partitionBy(keyCols: _*).orderBy(order: _*)))
      .filter("_rn = 1").drop("_rn")
}

object CurrentStateMerger {
  /** A column name quoted for SQL expressions. */
  private[txbuffering] def q(name: String): String = s"`${name.replace("`", "``")}`"

  private def csnDecimal(c: Column): Column = c.cast("decimal(38,0)")
}
