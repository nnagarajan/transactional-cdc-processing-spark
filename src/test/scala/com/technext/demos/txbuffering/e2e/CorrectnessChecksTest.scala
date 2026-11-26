package com.technext.demos.txbuffering.e2e

import java.time.Instant

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CorrectnessChecksTest {
  import TruthFixtures._

  private val multi = record(1, 1, Transactions.InsertMulti, insert(10, Seq(100, 101)) ++ insert(11, Seq(110)))
  private val child = record(1, 2, Transactions.ChildOnly, Vector(img(SourceTables.Items, 100, 10, 2, before = true)))
  private val rolledBack = record(2, 1, Transactions.Rollback, insert(13, Seq(130)), committed = false)
  private val truth = GroundTruth.build(Seq(log(multi, 500), log(child, 510)), orders(multi) ++ orders(child), Seq(multi, child, rolledBack))

  private def entity(i: RowImage): Entity =
    Entity(i.after.map { case (k, v) => Values.camel(k) -> v }, i.before.map(_.map { case (k, v) => Values.camel(k) -> v }))

  /** The rows a correct pipeline writes: one per expected row, version 5 for csn 500 and 6 for 510. */
  private val good: Vector[StreamRow] = truth.rows.map { e =>
    StreamRow(e.orderId, s"1342848513.${e.localTxId}", e.csn, T0.plusSeconds((e.csn - 490).toLong), if (e.csn == 500) 5L else 6L,
      e.orders.map(entity), e.details.map(entity), e.items.map(entity))
  }

  private val source: Map[Long, SourceOrder] = Map(
    10L -> SourceOrder(Map("ORDER_ID" -> 10.0, "VERSION" -> 1.0, "ORDER_STATUS" -> "PENDING"), Some(Map("ORDER_ID" -> 10.0, "VERSION" -> 1.0)),
      Vector(Map("LINE_ITEM_ID" -> 100.0, "ORDER_ID" -> 10.0, "VERSION" -> 2.0), Map("LINE_ITEM_ID" -> 101.0, "ORDER_ID" -> 10.0, "VERSION" -> 1.0))),
    11L -> SourceOrder(Map("ORDER_ID" -> 11.0, "VERSION" -> 1.0, "ORDER_STATUS" -> "PENDING"), Some(Map("ORDER_ID" -> 11.0, "VERSION" -> 1.0)),
      Vector(Map("LINE_ITEM_ID" -> 110.0, "ORDER_ID" -> 11.0, "VERSION" -> 1.0))))

  private def currentOf(id: Long, csn: Long, status: String = "PENDING"): CurrentRow = {
    val s = source(id)
    CurrentRow(id, "x", BigDecimal(csn), T0, 9L,
      Map("orderId" -> id.toDouble, "version" -> 1.0, "orderStatus" -> status),
      s.detail.map(d => Entity(d.map { case (k, v) => Values.camel(k) -> v }, None)),
      s.items.map(i => Entity(i.map { case (k, v) => Values.camel(k) -> v }, None)))
  }

  private val goodCurrent = Vector(currentOf(10, 510), currentOf(11, 500))

  @Test
  def correctOutputPassesEveryCheck(): Unit = {
    assertThat(CorrectnessChecks.completeness(truth, good).passed).isTrue
    assertThat(CorrectnessChecks.noPhantom(truth, good, goodCurrent).passed).isTrue
    assertThat(CorrectnessChecks.commitOrder(good).passed).isTrue
    assertThat(CorrectnessChecks.perOrderVersions(good).passed).isTrue
    assertThat(CorrectnessChecks.currentState(truth, goodCurrent, source).details.mkString("\n")).isEmpty()
    assertThat(CorrectnessChecks.noDropped(Nil).passed).isTrue
    assertThat(CorrectnessChecks.processedTsOrder(good).passed).isTrue
  }

  @Test
  def missingRowIsReported(): Unit = {
    val r = CorrectnessChecks.completeness(truth, good.filterNot(_.orderId == 11L))

    assertThat(r.passed).isFalse
    assertThat(r.details.mkString).contains("missing: session 1 tx 1 (INSERT_MULTI) order 11 csn 500")
  }

  @Test
  def duplicateRowIsReported(): Unit =
    assertThat(CorrectnessChecks.completeness(truth, good :+ good.head).details.mkString).contains("duplicate: 2 rows for session 1 tx 1 (INSERT_MULTI) order 10 csn 500")

  @Test
  def wrongValueMissingEntityAndWrongXidAreReported(): Unit = {
    val wrong = good.head.copy(
      xid = "1342848513.9.9.9",
      items = good.head.items.take(1).map(e => e.copy(after = e.after.updated("version", 7.0))))
    val details = CorrectnessChecks.completeness(truth, wrong +: good.tail).details.mkString("\n")

    assertThat(details).contains("xid 1342848513.9.9.9 does not end in .1.2.1")
    assertThat(details).contains("lineItems lineItemId 100: version is 7.0, Oracle committed 1.0")
    assertThat(details).contains("lineItems misses lineItemId 101")
  }

  @Test
  def outOfOrderCommitIsReported(): Unit = {
    val swapped = good.map(r => r.copy(commitVersion = if (r.csn == 500) 7L else 6L))
    val r = CorrectnessChecks.commitOrder(swapped)

    assertThat(r.passed).isFalse
    assertThat(r.details.head).isEqualTo("version 7 holds csn 500 (order 10) after version 6 held csn 510 (order 10)")
  }

  @Test
  def versionRegressionIsReported(): Unit = {
    val regressed = good.map { r =>
      if (r.csn == 510) r.copy(items = r.items.map(e => e.copy(after = e.after.updated("version", 1.0)))) else r
    }
    val r = CorrectnessChecks.perOrderVersions(regressed)

    assertThat(r.passed).isFalse
    assertThat(r.details.head).isEqualTo("line item 100 of order 10: version 1.0 at csn 500, then 1.0 at csn 510")
  }

  @Test
  def staleOrdersCurrentIsReported(): Unit = {
    val stale = Vector(currentOf(10, 500, status = "CONFIRMED"), currentOf(11, 500).copy(items = Vector.empty))
    val details = CorrectnessChecks.currentState(truth, stale, source).details.mkString("\n")

    assertThat(details).contains("order 10: csn 500, highest committed 510")
    assertThat(details).contains("order 10: orderStatus is CONFIRMED, Oracle has PENDING")
    assertThat(details).contains("order 11 line item 110: 0 entries in orders_current")
    assertThat(CorrectnessChecks.currentState(truth, goodCurrent.take(1), source).details.mkString).contains("order 11: missing from orders_current")
  }

  @Test
  def phantomRowsAreReported(): Unit = {
    val phantom = StreamRow(13, "x", BigDecimal(999), T0, 7L, Vector.empty, Vector.empty, Vector.empty)
    val details = CorrectnessChecks.noPhantom(truth, good :+ phantom, goodCurrent :+ currentOf(10, 999).copy(orderId = 13)).details

    assertThat(details.mkString("\n")).contains(
      "order 13 at csn 999 (xid x) has no committed transaction",
      "rolled-back order 13 in order_stream at csn 999",
      "rolled-back order 13 in orders_current at csn 999")
  }

  @Test
  def droppedRunDataIsReported(): Unit = {
    val r = CorrectnessChecks.noDropped(Seq(Map("csn" -> "500", "xid" -> "x", "reason" -> "DUPLICATE_EVENT", "itemKind" -> "DATA_EVENT",
      "tableName" -> "ORDERS", "batchId" -> 7L)))

    assertThat(r.details).isEqualTo(Seq("csn 500 xid x: DUPLICATE_EVENT (DATA_EVENT ORDERS, batch 7)"))
  }

  @Test
  def processedTimestampsAgainstCommitOrderAreReported(): Unit = {
    val backwards = good.map(r => r.copy(dwhProcessedTs = if (r.csn == 510) Instant.parse("2026-11-25T09:00:00Z") else r.dwhProcessedTs))

    assertThat(CorrectnessChecks.processedTsOrder(backwards).details.head).startsWith("csn 510 processed at 2026-11-25T09:00:00Z, before csn 500")
  }
}
