package com.technext.demos.txbuffering.e2e

import java.time.Instant

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

object TruthFixtures {
  val T0: Instant = Instant.parse("2026-11-25T10:00:00Z")

  def img(table: String, key: Long, orderId: Long, version: Double, before: Boolean = false): RowImage = {
    val idCol = if (table == SourceTables.Items) "LINE_ITEM_ID" else "ORDER_ID"
    val after = Map[String, Any](idCol -> key.toDouble, "ORDER_ID" -> orderId.toDouble, "VERSION" -> version)
    RowImage(table, key, orderId, after, if (before) Some(after.updated("VERSION", version - 1)) else None)
  }

  def insert(orderId: Long, items: Seq[Long]): Vector[RowImage] =
    Vector(img(SourceTables.Orders, orderId, orderId, 1), img(SourceTables.Details, orderId, orderId, 1)) ++
      items.map(i => img(SourceTables.Items, i, orderId, 1))

  def record(session: Int, seq: Int, kind: String, images: Vector[RowImage], committed: Boolean = true): TxRecord = {
    val counts = (images.count(_.table == SourceTables.Orders), images.count(_.table == SourceTables.Details),
      images.count(_.table == SourceTables.Items))
    TxRecord(session, seq, kind, committed, images, images.map(_.orderId).toSet, counts, T0.plusMillis(seq * 10L + 5))
  }

  def log(r: TxRecord, csn: Long): LogEntry =
    LogEntry(r.sessionNo, r.txSeq, r.kind, s"1.2.${r.txSeq}", T0.plusMillis(r.txSeq * 10L), r.counts, BigDecimal(csn))

  def orders(r: TxRecord): Seq[(Int, Int, Long)] = r.orderIds.toSeq.map(id => (r.sessionNo, r.txSeq, id))
}

class GroundTruthTest {
  import TruthFixtures._

  private val multi = record(1, 1, Transactions.InsertMulti, insert(10, Seq(100, 101)) ++ insert(11, Seq(110)) ++ insert(12, Seq(120)))
  private val child = record(1, 2, Transactions.ChildOnly, Vector(img(SourceTables.Items, 100, 10, 2, before = true)))
  private val rolledBack = record(2, 1, Transactions.Rollback, insert(13, Seq(130)), committed = false)

  private val truth = GroundTruth.build(
    Seq(log(multi, 500), log(child, 510)),
    orders(multi) ++ orders(child),
    Seq(multi, child, rolledBack))

  @Test
  def multiOrderTransactionExpectsOneRowPerOrder(): Unit = {
    val atCsn = truth.rows.filter(_.csn == BigDecimal(500))

    assertThat(atCsn.map(_.orderId)).isEqualTo(Vector(10L, 11L, 12L))
    val first = truth.byKey((10L, BigDecimal(500)))
    assertThat(first.orders.size).isEqualTo(1)
    assertThat(first.details.size).isEqualTo(1)
    assertThat(first.items.map(_.key)).isEqualTo(Vector(100L, 101L))
    assertThat(first.localTxId).isEqualTo("1.2.1")
    assertThat(first.clientAckTs.get).isEqualTo(T0.plusMillis(15))
  }

  @Test
  def childOnlyTransactionExpectsOnlyTheChangedLineItem(): Unit = {
    val row = truth.byKey((10L, BigDecimal(510)))

    assertThat(row.orders.isEmpty).isTrue
    assertThat(row.details.isEmpty).isTrue
    assertThat(row.items.map(_.key)).isEqualTo(Vector(100L))
    assertThat(row.items.head.before.isDefined).isTrue
    assertThat(truth.maxCsnByOrder(10L)).isEqualTo(BigDecimal(510))
    assertThat(truth.maxCsnByOrder(11L)).isEqualTo(BigDecimal(500))
  }

  @Test
  def rollbackIsNotExpectedButItsOrdersAreRemembered(): Unit = {
    assertThat(truth.rows.size).isEqualTo(4)
    assertThat(truth.orderIds.contains(13L)).isFalse
    assertThat(truth.rolledBackOrderIds).isEqualTo(Set(13L))
    assertThat(truth.csns).isEqualTo(Set(BigDecimal(500), BigDecimal(510)))
    assertThat(truth.events).isEqualTo(11L)
    assertThat(truth.harnessErrors.isEmpty).isTrue
  }

  @Test
  def clientAndLogDisagreeingIsAHarnessError(): Unit = {
    val t = GroundTruth.build(Seq(log(multi, 500)), orders(multi).take(2), Seq(multi, child))

    assertThat(t.harnessErrors.size).isEqualTo(2)
    assertThat(t.harnessErrors.head).contains("client touched")
    assertThat(t.harnessErrors(1)).contains("session 1 tx 2 committed by the client but has no log row")
  }
}
