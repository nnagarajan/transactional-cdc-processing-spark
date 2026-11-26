package com.technext.demos.txbuffering.e2e

import java.time.{Duration, Instant}

import scala.collection.mutable

import org.junit.jupiter.api.Test

/**
 * Suite B (spec "Sustained load measurement", "Bulk load measurement"): a paced mixed load of
 * F1–F5 and F7, then 100-order F6 bulk chunks; each phase is checked for completeness and reported
 * with its committed rate, catch-up time and latency percentiles.
 * Run with: ./mvnw -Pe2e verify -Dit.test=PerformanceIT
 */
class PerformanceIT {

  private val BulkFirstSession = 101

  @Test
  def sustainedAndBulkLoad(): Unit = {
    val p = E2eConfig.load().performance
    val h = new Harness("performance", p.sessions)
    try run(h, p) finally h.close()
  }

  private def run(h: Harness, p: PerformanceSettings): Unit = {
    val r = h.report
    val txps = Workload.txPerSecond(p.eventsPerMinute, p.mix)
    r.setting("target events/min", p.eventsPerMinute)
    r.setting("mix", p.mix.toSeq.sorted.map { case (k, w) => s"$k=$w" }.mkString(" "))
    r.setting("mix events/tx", f"${Workload.averageEvents(p.mix)}%.2f")
    r.setting("target tx/s", f"$txps%.1f")
    r.setting("sustained duration", p.duration)
    r.setting("bulk", s"${p.bulk.orders} orders as ${p.bulk.orders / p.bulk.ordersPerTx} F6 transactions on ${p.bulk.sessions} sessions")
    p.maxP95ProcessedLatency.foreach(r.setting("max p95 processed latency", _))
    p.maxCatchUp.foreach(r.setting("max catch-up", _))

    h.preflight(needsCurrent = false)

    // Sustained phase: each session paced at txps / sessions
    val choice = WeightedChoice(p.mix.toSeq)
    val interval = (1e9 * p.sessions / txps).toLong
    val start = System.nanoTime() + 1000000000L
    val end = start + p.duration.toNanos
    val lateSlots = new java.util.concurrent.atomic.AtomicLong()
    h.runSessions(p.sessions, captureImages = false) { (s, rnd) =>
      val pacer = new Pacer(start + interval * s.sessionNo / p.sessions, interval)
      val own = mutable.ArrayBuffer[Long]()
      var i = 0L
      while (pacer.slot(i) < end) {
        if (pacer.awaitSlot(i) > interval) lateSlots.incrementAndGet()
        h.record(Transactions.flavor(choice.pick(rnd), s, rnd, own))
        i += 1
      }
    }
    phase(h, "Sustained phase", 1 to p.sessions, p, Some(lateSlots.get))

    // Bulk phase: unpaced 100-order transactions
    val chunks = p.bulk.orders / p.bulk.ordersPerTx
    val bulkShape = Shape(p.bulk.ordersPerTx, 3)
    h.runSessions(p.bulk.sessions, captureImages = false, firstSessionNo = BulkFirstSession) { (s, rnd) =>
      val mine = (0 until chunks).filter(_ % p.bulk.sessions == s.sessionNo - BulkFirstSession)
      mine.foreach { _ =>
        val tx = s.begin("F6", rnd)
        tx.insertOrders(bulkShape.orders, bulkShape.lineItems)
        h.record(tx.commit())
      }
    }
    phase(h, "Bulk phase", BulkFirstSession until BulkFirstSession + p.bulk.sessions, p, None)
    r.finish()
  }

  private def phase(h: Harness, name: String, sessions: Range, p: PerformanceSettings, lateSlots: Option[Long]): Unit = {
    val r = h.report
    val truth = h.groundTruth().forSessions(sessions.contains)
    println(s"[performance] $name: ${truth.log.size} transactions committed, waiting for the pipeline")
    h.awaitStream(truth)
    val keys = h.reader.streamKeys(truth.orderIds).filter(k => truth.byKey.contains((k.orderId, k.csn)))
    r.check(countsCheck(s"$name: every transaction arrived with its counts", truth, keys))

    val rows = h.latencyRows(truth, keys, None)
    val first = truth.log.map(_.commitTs).min
    val last = truth.log.map(_.commitTs).max
    val window = math.max(1L, Duration.between(first, last).toMillis) / 1000.0
    val lastVisible = rows.flatMap(_.streamVisibleTs).maxOption.getOrElse(Instant.EPOCH)
    val catchUp = Duration.between(last, lastVisible)
    val p95 = Percentiles.nearestRank(rows.map(_.processedMs), 95)

    val target = if (name.startsWith("Sustained")) f"${p.eventsPerMinute}%,d events/min (${Workload.txPerSecond(p.eventsPerMinute, p.mix)}%.1f tx/s)" else "unpaced"
    // Built from lines, not stripMargin: stripMargin would also strip the leading `|` of every table row
    val metrics = Seq(
      "| Metric | Value |",
      "|---|---|",
      s"| transactions committed | ${truth.log.size} |",
      s"| events committed | ${truth.events} |",
      f"| commit window | $window%.1f s |",
      s"| target | $target |",
      f"| achieved | ${truth.events / window * 60}%,.0f events/min (${truth.log.size / window}%.1f tx/s) |",
      s"| catch-up after last commit | ${catchUp.toMillis} ms |") ++
      lateSlots.map(l => s"| pacer slots missed by more than one interval | $l |")
    r.section(name, metrics.mkString("\n") + "\n\n" + r.latencyTable(rows))
    r.latency(rows, s"latency-${name.split(' ').head.toLowerCase}.csv")

    p.maxP95ProcessedLatency.foreach { max =>
      r.check(CheckResult.of(s"$name: p95 processed latency ≤ $max",
        if (p95 > max.toMillis) Seq(s"p95 is $p95 ms") else Nil))
    }
    p.maxCatchUp.foreach { max =>
      r.check(CheckResult.of(s"$name: catch-up ≤ $max",
        if (catchUp.compareTo(max) > 0) Seq(s"catch-up took ${catchUp.toMillis} ms") else Nil))
    }
  }

  /** One row per order at each CSN, and per-CSN entity counts equal to the logged counts. */
  private def countsCheck(name: String, truth: GroundTruth, keys: Seq[StreamKey]): CheckResult = {
    val byCsn = keys.groupBy(_.csn)
    val failures = truth.log.flatMap { l =>
      val rows = byCsn.getOrElse(l.csn, Nil).filter(k => truth.byKey.get((k.orderId, k.csn)).exists(e => e.sessionNo == l.sessionNo && e.txSeq == l.txSeq))
      val got = (rows.map(_.nOrders).sum, rows.map(_.nDetails).sum, rows.map(_.nItems).sum)
      val expectedRows = truth.rows.count(e => e.sessionNo == l.sessionNo && e.txSeq == l.txSeq)
      if (rows.size != expectedRows || got != l.counts)
        Some(s"session ${l.sessionNo} tx ${l.txSeq} (${l.kind}) csn ${l.csn}: ${rows.size} rows with counts $got, expected $expectedRows rows with ${l.counts}")
      else None
    }
    CheckResult.of(name, failures, s"${truth.log.size} transactions")
  }
}
