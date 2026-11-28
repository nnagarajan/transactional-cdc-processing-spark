/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering.model.spec

import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructField, StructType}

/**
 * The stage 1 (stream) and stage 2 (current) table schemas of a model. The DDL writer, the
 * shaper, the merger and the startup check all use these, so they can't disagree.
 *
 * Stream row: `xid`, `csn`, `dwhProcessedTs`, the root key columns, then one array per entity
 * (in config order) whose elements hold the entity's columns and, when on, a `before` struct.
 *
 * Current row: `xid`, `csn`, `dwhProcessedTs`, the root's columns (with `changeCsn` when the root
 * has no version), the root's before struct, then one struct per one-to-one child and one array
 * per one-to-many child, whose elements also carry `changeCsn` when the child has no version.
 */
object Schemas {

  val Before = "before"

  private def field(name: String, t: DataType): StructField = StructField(name, t, nullable = true)

  private def context: Seq[StructField] = ModelResolver.ContextColumns.map(field(_, StringType))

  private def columnFields(e: EntityModel): Seq[StructField] = e.columns.map(c => field(c.name, c.dataType))

  /** The columns of an entity's before image. */
  def beforeType(e: EntityModel): StructType = StructType(columnFields(e))

  /** A stream array element of an entity. */
  def streamElementType(e: EntityModel): StructType =
    StructType(columnFields(e) ++ (if (e.before) Seq(field(Before, beforeType(e))) else Nil))

  def streamSchema(m: JobModel): StructType =
    StructType(context ++
      m.rootKeyColumns.map(c => field(c.name, c.dataType)) ++
      m.entities.map(e => field(e.name, ArrayType(streamElementType(e), containsNull = true))))

  /** A child's value in the current table: the struct of a 1:1 child, the element of a 1:N child. */
  def currentChildType(e: EntityModel): StructType =
    StructType(columnFields(e) ++
      (if (e.version.isEmpty) Seq(field(ModelResolver.ChangeCsn, StringType)) else Nil) ++
      (if (e.before) Seq(field(Before, beforeType(e))) else Nil))

  def currentSchema(m: JobModel): StructType = {
    val root = m.root
    val children = m.children.map { e =>
      val t = currentChildType(e)
      field(e.name, if (e.cardinality.contains(Cardinality.One)) t else ArrayType(t, containsNull = true))
    }
    StructType(context ++
      columnFields(root) ++
      (if (root.version.isEmpty) Seq(field(ModelResolver.ChangeCsn, StringType)) else Nil) ++
      (if (root.before) Seq(field(m.rootBefore, beforeType(root))) else Nil) ++
      children)
  }

  /**
   * The paths at which two schemas differ in names, types, nesting or field order; nullability and
   * metadata (comments) are ignored. Empty when they match.
   */
  def differences(expected: DataType, actual: DataType, path: String = ""): Seq[String] = (expected, actual) match {
    case (e: StructType, a: StructType) =>
      val missing = e.fieldNames.filterNot(a.fieldNames.contains).map(n => s"${p(path, n)}: missing")
      val extra = a.fieldNames.filterNot(e.fieldNames.contains).map(n => s"${p(path, n)}: not in the model")
      val common = e.fieldNames.filter(a.fieldNames.contains)
      val order =
        if (missing.isEmpty && extra.isEmpty && !e.fieldNames.sameElements(a.fieldNames))
          Seq(s"${if (path.isEmpty) "<top level>" else path}: field order is ${a.fieldNames.mkString(", ")}, expected ${e.fieldNames.mkString(", ")}")
        else Nil
      missing.toSeq ++ extra.toSeq ++ order ++ common.flatMap(n => differences(e(n).dataType, a(n).dataType, p(path, n)))
    case (e: ArrayType, a: ArrayType) => differences(e.elementType, a.elementType, s"$path[]")
    case (e, a) if e.sql == a.sql => Nil
    case (e, a) => Seq(s"$path: type is ${a.sql}, expected ${e.sql}")
  }

  private def p(path: String, name: String): String = if (path.isEmpty) name else s"$path.$name"
}
