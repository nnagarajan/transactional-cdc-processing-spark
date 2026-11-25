package com.technext.demos.txbuffering.e2e

import java.sql.Connection

/** The current source rows of one order in Oracle (column → normalized value). */
final case class SourceOrder(order: Map[String, Any], detail: Option[Map[String, Any]], items: Vector[Map[String, Any]])

object SourceState {

  /** Current Oracle rows of the given orders, read in chunks of 1,000 ids. */
  def load(conn: Connection, orderIds: Iterable[Long]): Map[Long, SourceOrder] = {
    def rows(table: String, chunk: Seq[Long]): Vector[Map[String, Any]] = {
      val ps = conn.prepareStatement(s"SELECT * FROM $table WHERE order_id IN (${chunk.map(_ => "?").mkString(",")})")
      try {
        chunk.zipWithIndex.foreach { case (id, i) => ps.setLong(i + 1, id) }
        OracleSession.readAll(ps.executeQuery())
      } finally ps.close()
    }
    def id(r: Map[String, Any]) = r("ORDER_ID").asInstanceOf[Double].toLong

    orderIds.toSeq.distinct.sorted.grouped(1000).flatMap { chunk =>
      val orders = rows(SourceTables.Orders, chunk).map(r => id(r) -> r).toMap
      val details = rows(SourceTables.Details, chunk).map(r => id(r) -> r).toMap
      val items = rows(SourceTables.Items, chunk).groupBy(id)
      orders.map { case (oid, o) => oid -> SourceOrder(o, details.get(oid), items.getOrElse(oid, Vector.empty)) }
    }.toMap
  }
}

/**
 * The correctness checks (spec requirements "Completeness and content check", "No phantom output",
 * "Commit order check", "Current state check", "No dropped run data"), as pure functions over the
 * ground truth and the rows read from Unity Catalog.
 */
object CorrectnessChecks {

  val Completeness = "completeness and content"
  val NoPhantom = "no phantom output"
  val CommitOrder = "commit order across Delta commits"
  val PerOrderVersions = "per-order versions rise with CSN"
  val CurrentState = "orders_current equals Oracle"
  val NoDropped = "no dropped run data"
  val ProcessedTsOrder = "dwhProcessedTs non-decreasing in CSN order"

  /** Every expected `(orderId, csn)` has exactly one row, with exactly the committed entities and values. */
  def completeness(truth: GroundTruth, stream: Seq[StreamRow]): CheckResult = {
    val byKey = stream.groupBy(r => (r.orderId, r.csn))
    val failures = truth.rows.flatMap { e =>
      byKey.getOrElse((e.orderId, e.csn), Nil) match {
        case Seq()  => Seq(s"missing: ${e.describe}")
        case Seq(r) => content(e, r)
        case many   => Seq(s"duplicate: ${many.size} rows for ${e.describe}")
      }
    }
    CheckResult.of(Completeness, failures, s"${truth.rows.size} expected rows, ${truth.log.size} transactions")
  }

  private def content(e: ExpectedRow, r: StreamRow): Seq[String] = {
    val xid = if (r.xid == null || !r.xid.endsWith("." + e.localTxId)) Seq(s"xid ${r.xid} does not end in .${e.localTxId}: ${e.describe}") else Nil
    xid ++
      entities("orders", e.orders, r.orders, "orderId", e) ++
      entities("orderDetails", e.details, r.details, "orderId", e) ++
      entities("lineItems", e.items, r.items, "lineItemId", e)
  }

  private def entities(field: String, expected: Vector[RowImage], actual: Vector[Entity], keyField: String, e: ExpectedRow): Seq[String] = {
    val byKey = actual.groupBy(a => toLong(a.after.get(keyField).orNull))
    val keys = expected.map(_.key).toSet
    val extra = byKey.keySet.diff(keys).toSeq.sorted.map(k => s"$field has unexpected $keyField $k: ${e.describe}")
    val per = expected.flatMap { img =>
      byKey.getOrElse(img.key, Vector.empty) match {
        case Vector(a) =>
          diff(s"$field $keyField ${img.key}", img.after, a.after, e) ++
            ((img.before, a.before) match {
              case (None, None)       => Nil
              case (Some(b), Some(x)) => diff(s"$field $keyField ${img.key} before", b, x, e)
              case (Some(_), None)    => Seq(s"$field $keyField ${img.key} has no before image: ${e.describe}")
              case (None, Some(_))    => Seq(s"$field $keyField ${img.key} has a before image for an insert: ${e.describe}")
            })
        case Vector() => Seq(s"$field misses $keyField ${img.key}: ${e.describe}")
        case many     => Seq(s"$field has ${many.size} entries for $keyField ${img.key}: ${e.describe}")
      }
    }
    extra ++ per
  }

  /** Columns of the source image that the pipeline struct carries must be equal after normalization. */
  private def diff(what: String, source: Map[String, Any], pipeline: Map[String, Any], e: ExpectedRow): Seq[String] =
    source.toSeq.sortBy(_._1).flatMap { case (col, v) =>
      val f = Values.camel(col)
      pipeline.get(f) match {
        case Some(p) if !Values.same(v, p) => Some(s"$what: $f is ${Values.normalize(p)}, Oracle committed ${Values.normalize(v)}: ${e.describe}")
        case _                              => None
      }
    }

  /** No run row at a CSN outside the ground truth, and no order that only rolled-back transactions created. */
  def noPhantom(truth: GroundTruth, stream: Seq[StreamRow], current: Seq[CurrentRow]): CheckResult = {
    val unknownCsn = stream.filterNot(r => truth.csns(r.csn)).map(r => s"order ${r.orderId} at csn ${r.csn} (xid ${r.xid}) has no committed transaction")
    val rolledBack = truth.rolledBackOrderIds
    val inStream = stream.filter(r => rolledBack(r.orderId)).map(r => s"rolled-back order ${r.orderId} in order_stream at csn ${r.csn}")
    val inCurrent = current.filter(r => rolledBack(r.orderId)).map(r => s"rolled-back order ${r.orderId} in orders_current at csn ${r.csn}")
    CheckResult.of(NoPhantom, unknownCsn ++ inStream ++ inCurrent, s"${truth.rollbacks.size} rolled-back transactions")
  }

  /** Each Delta commit holds only CSNs at or above every CSN of earlier commits. */
  def commitOrder(stream: Seq[StreamRow]): CheckResult = {
    val versions = stream.groupBy(_.commitVersion).toSeq.sortBy(_._1)
    var maxSoFar: Option[(BigDecimal, Long, StreamRow)] = None
    val failures = versions.flatMap { case (v, rows) =>
      val low = rows.minBy(_.csn)
      val f = maxSoFar.filter(_._1 > low.csn).map { case (c, pv, pr) =>
        s"version $v holds csn ${low.csn} (order ${low.orderId}) after version $pv held csn $c (order ${pr.orderId})"
      }
      val high = rows.maxBy(_.csn)
      if (maxSoFar.forall(_._1 < high.csn)) maxSoFar = Some((high.csn, v, high))
      f
    }
    CheckResult.of(CommitOrder, failures, s"${versions.size} Delta commits")
  }

  /** For each order, detail and line item, the version rises strictly with CSN. */
  def perOrderVersions(stream: Seq[StreamRow]): CheckResult = {
    val histories = stream.flatMap { r =>
      r.orders.map(e => (s"order ${r.orderId}", r.csn, e)) ++
        r.details.map(e => (s"detail of order ${r.orderId}", r.csn, e)) ++
        r.items.map(e => (s"line item ${toLong(e.after.get("lineItemId").orNull)} of order ${r.orderId}", r.csn, e))
    }.groupBy(_._1)
    val failures = histories.toSeq.sortBy(_._1).flatMap { case (what, h) =>
      h.sortBy(_._2).sliding(2).collect {
        case Seq((_, c1, e1), (_, c2, e2)) if !(version(e2) > version(e1)) =>
          s"$what: version ${version(e1)} at csn $c1, then ${version(e2)} at csn $c2"
      }
    }
    CheckResult.of(PerOrderVersions, failures, s"${histories.size} entities")
  }

  /** `orders_current` holds Oracle's current values and the order's highest ground-truth CSN. */
  def currentState(truth: GroundTruth, current: Seq[CurrentRow], source: Map[Long, SourceOrder]): CheckResult = {
    val byId = current.groupBy(_.orderId)
    val failures = truth.orderIds.toSeq.sorted.flatMap { id =>
      (byId.getOrElse(id, Nil), source.get(id)) match {
        case (_, None)   => Seq(s"order $id: not in Oracle")
        case (Seq(), _)  => Seq(s"order $id: missing from orders_current")
        case (Seq(c), Some(s)) =>
          val csn = if (c.csn != truth.maxCsnByOrder(id)) Seq(s"order $id: csn ${c.csn}, highest committed ${truth.maxCsnByOrder(id)}") else Nil
          val order = mismatches(s"order $id", s.order, c.order)
          val detail = (s.detail, c.detail) match {
            case (Some(d), Some(x)) => mismatches(s"order $id detail", d, x.after)
            case (None, None)       => Nil
            case (d, x)             => Seq(s"order $id detail: Oracle ${d.isDefined}, orders_current ${x.isDefined}")
          }
          val itemsById = c.items.groupBy(i => toLong(i.after.get("lineItemId").orNull))
          val items = s.items.flatMap { it =>
            val lid = toLong(it("LINE_ITEM_ID"))
            itemsById.getOrElse(lid, Vector.empty) match {
              case Vector(x) => mismatches(s"order $id line item $lid", it, x.after)
              case other     => Seq(s"order $id line item $lid: ${other.size} entries in orders_current")
            }
          }
          val extra = itemsById.keySet.diff(s.items.map(i => toLong(i("LINE_ITEM_ID"))).toSet).toSeq.sorted
            .map(lid => s"order $id: line item $lid in orders_current but not in Oracle")
          csn ++ order ++ detail ++ items ++ extra
        case (many, _) => Seq(s"order $id: ${many.size} rows in orders_current")
      }
    }
    CheckResult.of(CurrentState, failures, s"${truth.orderIds.size} orders")
  }

  private def mismatches(what: String, source: Map[String, Any], pipeline: Map[String, Any]): Seq[String] =
    source.toSeq.sortBy(_._1).flatMap { case (col, v) =>
      pipeline.get(Values.camel(col)) match {
        case Some(p) if !Values.same(v, p) => Some(s"$what: ${Values.camel(col)} is ${Values.normalize(p)}, Oracle has ${Values.normalize(v)}")
        case _                              => None
      }
    }

  def noDropped(dropped: Seq[Map[String, Any]]): CheckResult =
    CheckResult.of(NoDropped, dropped.map(d => s"csn ${d("csn")} xid ${d("xid")}: ${d("reason")} (${d("itemKind")} ${Option(d("tableName")).getOrElse("")}, batch ${d("batchId")})"))

  /** Extra check (D6): emission time follows commit order. */
  def processedTsOrder(stream: Seq[StreamRow]): CheckResult = {
    val perCsn = stream.groupBy(_.csn).toSeq.map { case (c, rs) => (c, rs.map(_.dwhProcessedTs).min, rs.map(_.dwhProcessedTs).max) }.sortBy(_._1)
    val failures = perCsn.sliding(2).collect {
      case Seq((c1, _, max1), (c2, min2, _)) if min2.isBefore(max1) => s"csn $c2 processed at $min2, before csn $c1 at $max1"
    }.toSeq
    CheckResult.of(ProcessedTsOrder, failures)
  }

  private def version(e: Entity): Double = Values.normalize(e.after.get("version").orNull) match {
    case d: Double => d
    case other     => throw new IllegalStateException(s"No version in $other")
  }

  private def toLong(v: Any): Long = Values.normalize(v) match {
    case d: Double => d.toLong
    case other     => throw new IllegalStateException(s"Not a number: $other")
  }
}
