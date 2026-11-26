package com.technext.demos.txbuffering.e2e

import scala.collection.mutable

import org.junit.jupiter.api.Test

/**
 * Latency run (spec "Per-order commit latency report"): one session commits 200 F2 orders, one per
 * transaction, with nothing else running; reports each order's latency from the Oracle commit to
 * dwhProcessedTs and to the Delta commits that made it visible in order_stream and orders_current.
 * Run with: ./mvnw -Pe2e verify -Dit.test=CommitLatencyIT
 */
class CommitLatencyIT {

  @Test
  def perOrderCommitLatency(): Unit = {
    val h = new Harness("latency", 1)
    try run(h, h.config.latency) finally h.close()
  }

  private def run(h: Harness, l: LatencySettings): Unit = {
    val r = h.report
    r.setting("orders", l.orders)
    r.setting("interval", l.interval)
    r.setting("flavor", "F2 (1 order, 1 detail, 3 line items), one order per transaction")

    // The canary has gone through both apps, so the pipeline is idle and caught up
    h.preflight(needsCurrent = true)

    h.runSessions(1, captureImages = false) { (s, rnd) =>
      val pacer = new Pacer(System.nanoTime(), l.interval.toNanos)
      val own = mutable.ArrayBuffer[Long]()
      (0 until l.orders).foreach { i =>
        pacer.awaitSlot(i)
        h.record(Transactions.flavor("F2", s, rnd, own))
      }
    }

    val truth = h.groundTruth().forSessions(_ == 1)
    h.awaitStream(truth)
    h.awaitCurrent(truth)

    val keys = h.reader.streamKeys(truth.orderIds)
    val current = h.reader.currentKeys(truth.orderIds)
    val rows = h.latencyRows(truth, keys, Some(current))

    r.check(CheckResult.of(s"all ${l.orders} orders reported", if (rows.size != l.orders) Seq(s"${rows.size} rows") else Nil))
    r.check(CheckResult.of("every order visible in both tables",
      rows.filter(x => x.streamVisibleTs.isEmpty || x.currentVisibleTs.isEmpty).map(x => s"order ${x.orderId} csn ${x.csn}")))
    r.check(CheckResult.of("every latency positive",
      rows.filter(x => x.processedMs <= 0 || x.streamVisibleMs.exists(_ <= 0) || x.currentVisibleMs.exists(_ <= 0))
        .map(x => s"order ${x.orderId}: ${x.processedMs} / ${x.streamVisibleMs} / ${x.currentVisibleMs} ms")))
    r.check(CheckResult.of("client ack within 1 s of the Oracle commit timestamp",
      rows.filter(_.ackMs.forall(a => a < 0 || a >= 1000)).map(x => s"order ${x.orderId}: ack ${x.ackMs} ms")))

    r.latency(rows)
    r.section("Latency per order", r.latencyTable(rows) +
      "\n\nVisibility resolution: exact. A visible time is the in-commit timestamp of the Delta commit that wrote the row " +
      "(`_metadata.row_commit_version` joined with `DESCRIBE HISTORY`), not a polling time. Per-order values are in `latency.csv`.")
    r.finish()
  }
}
