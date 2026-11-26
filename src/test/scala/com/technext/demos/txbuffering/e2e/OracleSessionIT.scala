package com.technext.demos.txbuffering.e2e

import scala.collection.mutable
import scala.util.Random

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

/**
 * The transaction builders against Oracle only (no pipeline needed): every kind commits with
 * matching ground-truth rows, and a rollback leaves none. The orders it commits do reach Kafka;
 * they are tagged with this run's id like any other suite data.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OracleSessionIT {

  private val config = E2eConfig.load()
  private val ctx = RunContext.create(seed = 7, sessions = 1)
  private var s: OracleSession = _

  @BeforeAll
  def open(): Unit = s = new OracleSession(config.oracle, ctx, sessionNo = 1, captureImages = true)

  @AfterAll
  def close(): Unit = if (s != null) s.close()

  private def logRow(r: TxRecord): Option[Map[String, Any]] = s.withStatement(
    "SELECT l.*, ORA_ROWSCN AS csn FROM e2e_tx_log l WHERE run_id = ? AND session_no = ? AND tx_seq = ?") { ps =>
    ps.setString(1, ctx.runId); ps.setInt(2, r.sessionNo); ps.setInt(3, r.txSeq)
    s.readAll(ps.executeQuery()).headOption
  }

  private def loggedOrders(r: TxRecord): Set[Long] = s.withStatement(
    "SELECT order_id FROM e2e_tx_orders WHERE run_id = ? AND session_no = ? AND tx_seq = ?") { ps =>
    ps.setString(1, ctx.runId); ps.setInt(2, r.sessionNo); ps.setInt(3, r.txSeq)
    s.readAll(ps.executeQuery()).map(_("ORDER_ID").asInstanceOf[Double].toLong).toSet
  }

  private def assertLogged(r: TxRecord): Unit = {
    val row = logRow(r).getOrElse(throw new AssertionError(s"No log row for ${r.kind} ${r.txSeq}"))
    assertThat(row("KIND")).isEqualTo(r.kind)
    assertThat(row("ORDERS_CNT").asInstanceOf[Double].toInt).isEqualTo(r.counts._1)
    assertThat(row("DETAILS_CNT").asInstanceOf[Double].toInt).isEqualTo(r.counts._2)
    assertThat(row("LINE_ITEMS_CNT").asInstanceOf[Double].toInt).isEqualTo(r.counts._3)
    assertThat(row("CSN").asInstanceOf[Double]).isGreaterThan(0.0)
    assertThat(row("LOCAL_TX_ID").asInstanceOf[String]).matches("\\d+\\.\\d+\\.\\d+")
    assertThat(loggedOrders(r)).isEqualTo(r.orderIds)
  }

  @Test
  def everyKindCommitsWithItsGroundTruthAndRollbackLeavesNone(): Unit = {
    val rnd = new Random(11)
    val hot = Transactions.setupHot(s, rnd, 2)
    assertLogged(hot)
    assertThat(hot.counts).isEqualTo((2, 2, 6))

    val own = mutable.ArrayBuffer[Long]()
    val hotIds = hot.orderIds.toIndexedSeq.sorted
    Transactions.CorrectnessKinds.foreach { kind =>
      val r = Transactions.correctness(kind, s, rnd, own, hotIds)
      if (kind == Transactions.Rollback) {
        assertThat(r.committed).isFalse
        assertThat(logRow(r).isEmpty).isTrue
      } else {
        assertThat(r.committed).isTrue
        assertLogged(r)
      }
    }

    // CSN of one transaction is shared by both log tables
    val csns = s.withStatement(
      "SELECT DISTINCT ORA_ROWSCN FROM e2e_tx_orders WHERE run_id = ? AND tx_seq = ?") { ps =>
      ps.setString(1, ctx.runId); ps.setInt(2, hot.txSeq)
      s.readAll(ps.executeQuery())
    }
    assertThat(csns.size).isEqualTo(1)
    assertThat(csns.head.values.head).isEqualTo(logRow(hot).get("CSN"))
  }

  @Test
  def imagesCarryVersionsAndBeforeImages(): Unit = {
    val rnd = new Random(12)
    val own = mutable.ArrayBuffer[Long]()
    val ins = Transactions.correctness(Transactions.InsertOne, s, rnd, own, IndexedSeq.empty)
    val orderId = ins.orderIds.head
    val inserted = ins.images.filter(_.table == SourceTables.Orders).head
    assertThat(inserted.before.isEmpty).isTrue
    assertThat(inserted.after("VERSION")).isEqualTo(1.0)
    assertThat(inserted.after("CUSTOMER_ID")).isEqualTo(ctx.tag)
    assertThat(ins.images.count(_.table == SourceTables.Items)).isEqualTo(ins.counts._3)

    val upd = Transactions.correctness(Transactions.UpdateOwn, s, rnd, own, IndexedSeq.empty)
    val order = upd.images.filter(_.table == SourceTables.Orders).head
    assertThat(order.key).isEqualTo(orderId)
    assertThat(order.before.get("VERSION")).isEqualTo(1.0)
    assertThat(order.after("VERSION")).isEqualTo(2.0)
    assertThat(order.after("ORDER_STATUS")).isNotEqualTo(order.before.get("ORDER_STATUS"))
    assertThat(upd.images.count(_.table == SourceTables.Items)).isEqualTo(upd.counts._3)
  }

  @Test
  def everyFlavorHasItsShape(): Unit = {
    val rnd = new Random(13)
    val own = mutable.ArrayBuffer[Long]()
    Seq("F1", "F2", "F3", "F4", "F5", "F6", "F7").foreach { f =>
      val r = Transactions.flavor(f, s, rnd, own)
      assertLogged(r)
      assertThat(r.kind).isEqualTo(f)
      assertThat(r.events).isEqualTo(Transactions.eventsOf(f))
    }
  }
}
