package com.technext.demos.txbuffering.e2e

import scala.collection.mutable

import org.junit.jupiter.api.Test

/**
 * Suite A (spec end-to-end-verification): 10–20 concurrent sessions commit a mixed workload; once
 * the pipeline has caught up, Oracle is compared with order_stream, orders_current and
 * dropped_events. Run with: ./mvnw -Pe2e verify -Dit.test=CorrectnessOrderingIT
 */
class CorrectnessOrderingIT {

  @Test
  def committedWorkIsWrittenOnceCompleteAndInCommitOrder(): Unit = {
    val probe = E2eConfig.load().correctness
    val h = new Harness("correctness", probe.sessions)
    try run(h, probe) finally h.close()
  }

  private def run(h: Harness, c: CorrectnessSettings): Unit = {
    val r = h.report
    r.setting("tx per session", c.txPerSession)
    r.setting("hot orders", c.hotOrders)
    r.setting("weights", c.weights.toSeq.sorted.map { case (k, w) => s"$k=$w" }.mkString(" "))

    h.preflight(needsCurrent = true)

    val hot = h.record(Transactions.setupHot(h.control, h.ctx.sessionRandom(0), c.hotOrders)).orderIds.toIndexedSeq.sorted
    val choice = WeightedChoice(c.weights.toSeq)
    val started = System.nanoTime()
    h.runSessions(h.ctx.sessions, captureImages = true) { (s, rnd) =>
      val own = mutable.ArrayBuffer[Long]()
      (1 to c.txPerSession).foreach { i =>
        val kind = if (i <= Transactions.CorrectnessKinds.size) Transactions.CorrectnessKinds(i - 1) else choice.pick(rnd)
        h.record(Transactions.correctness(kind, s, rnd, own, hot))
      }
    }
    val workloadSecs = (System.nanoTime() - started) / 1e9
    println(f"[correctness] workload done in $workloadSecs%.1f s")

    val records = h.records
    val kindsPerSession = records.filter(_.sessionNo > 0).groupBy(_.sessionNo).toSeq.sortBy(_._1)
    r.check(CheckResult.of("every kind in every session", kindsPerSession.flatMap { case (s, rs) =>
      Transactions.CorrectnessKinds.filterNot(rs.map(_.kind).toSet).map(k => s"session $s never ran $k")
    }))
    r.section("Workload", {
      val header = "| Session | " + Transactions.CorrectnessKinds.mkString(" | ") + " |\n|---|" + "---|" * Transactions.CorrectnessKinds.size + "\n"
      header + kindsPerSession.map { case (s, rs) =>
        s"| $s | " + Transactions.CorrectnessKinds.map(k => rs.count(_.kind == k)).mkString(" | ") + " |"
      }.mkString("\n") + f"\n\n${records.count(_.committed)} committed and ${records.count(!_.committed)} rolled-back transactions in $workloadSecs%.1f s."
    })

    val truth = h.groundTruth()
    h.report.transactions(records, truth)
    h.awaitStream(truth)
    h.awaitCurrent(truth)

    val ids = truth.orderIds ++ truth.rolledBackOrderIds
    val stream = h.reader.streamRows(ids)
    val current = h.reader.currentRows(ids)
    val source = SourceState.load(h.control.conn, truth.orderIds)
    val dropped = h.reader.droppedFor(truth.csns)

    r.check(CorrectnessChecks.completeness(truth, stream))
    r.check(CorrectnessChecks.noPhantom(truth, stream, current))
    r.check(CorrectnessChecks.commitOrder(stream))
    r.check(CorrectnessChecks.perOrderVersions(stream))
    r.check(CorrectnessChecks.currentState(truth, current, source))
    r.check(CorrectnessChecks.noDropped(dropped))
    r.check(CorrectnessChecks.processedTsOrder(stream))

    val latency = h.latencyRows(truth, h.reader.streamKeys(truth.orderIds), None)
    if (latency.nonEmpty) r.section("Latency (informational)", r.latencyTable(latency))
    r.setting("orders", truth.orderIds.size)
    r.setting("events", truth.events)
    r.finish()
  }
}
