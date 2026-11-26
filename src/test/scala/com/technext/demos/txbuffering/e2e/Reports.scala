package com.technext.demos.txbuffering.e2e

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration, Instant}

import scala.collection.mutable

/** Outcome of one check. `details` names the affected transactions (design.md D9). */
final case class CheckResult(name: String, passed: Boolean, details: Seq[String] = Nil, note: String = "")

object CheckResult {
  def of(name: String, failures: Seq[String], note: String = ""): CheckResult = CheckResult(name, failures.isEmpty, failures, note)
}

/** Nearest-rank percentiles: the smallest value with at least p% of the values at or below it. */
object Percentiles {

  def nearestRank(values: Seq[Long], p: Double): Long = {
    require(values.nonEmpty, "no values")
    val sorted = values.sorted
    val rank = math.max(1, math.ceil(p / 100.0 * sorted.size).toInt)
    sorted(rank - 1)
  }

  final case class Summary(n: Int, p50: Long, p95: Long, p99: Long, max: Long)

  def summary(values: Seq[Long]): Summary =
    Summary(values.size, nearestRank(values, 50), nearestRank(values, 95), nearestRank(values, 99), values.max)
}

/** One order's latencies, all from its Oracle commit timestamp (design.md D9). */
final case class LatencyRow(
    orderId: Long,
    xid: String,
    csn: BigDecimal,
    kind: String,
    commitTs: Instant,
    clientAckTs: Option[Instant],
    dwhProcessedTs: Instant,
    streamVisibleTs: Option[Instant],
    currentVisibleTs: Option[Instant]) {

  def processedMs: Long = Duration.between(commitTs, dwhProcessedTs).toMillis
  def streamVisibleMs: Option[Long] = streamVisibleTs.map(Duration.between(commitTs, _).toMillis)
  def currentVisibleMs: Option[Long] = currentVisibleTs.map(Duration.between(commitTs, _).toMillis)
  def ackMs: Option[Long] = clientAckTs.map(Duration.between(commitTs, _).toMillis)
}

/**
 * The results of one suite run, under `<report-dir>/<suite>-<runId>/`: `summary.md`,
 * `transactions.csv`, `latency.csv` and `failures.csv`.
 */
final class Report(baseDir: String, suite: String, ctx: RunContext) {

  val dir: Path = Paths.get(baseDir, s"$suite-${ctx.runId}")
  Files.createDirectories(dir)

  private val config = mutable.ArrayBuffer[(String, String)]()
  private val sections = mutable.ArrayBuffer[(String, String)]()
  private val checks = mutable.ArrayBuffer[CheckResult]()

  def setting(key: String, value: Any): Unit = config += key -> String.valueOf(value)
  def section(title: String, markdown: String): Unit = sections += title -> markdown
  def check(result: CheckResult): CheckResult = { checks += result; result }
  def results: Seq[CheckResult] = checks.toSeq
  def passed: Boolean = checks.forall(_.passed)

  def transactions(records: Seq[TxRecord], truth: GroundTruth): Path = {
    val logged = truth.log.map(l => (l.sessionNo, l.txSeq) -> l).toMap
    val lines = records.sortBy(r => (r.sessionNo, r.txSeq)).map { r =>
      val l = logged.get((r.sessionNo, r.txSeq))
      Seq(r.sessionNo, r.txSeq, r.kind, r.committed, r.counts._1, r.counts._2, r.counts._3, r.events,
        r.orderIds.toSeq.sorted.mkString(" "), l.map(_.csn).getOrElse(""), l.map(_.localTxId).getOrElse(""),
        l.map(_.commitTs).getOrElse(""), r.clientAckTs).mkString(",")
    }
    csv("transactions.csv", "session_no,tx_seq,kind,committed,orders,details,line_items,events,order_ids,csn,local_tx_id,commit_ts_utc,client_ack_ts", lines)
  }

  def latency(rows: Seq[LatencyRow], file: String = "latency.csv"): Path = {
    val lines = rows.sortBy(r => (r.csn, r.orderId)).map { r =>
      Seq(r.orderId, r.xid, r.csn, r.kind, r.commitTs, r.clientAckTs.getOrElse(""), r.dwhProcessedTs,
        r.streamVisibleTs.getOrElse(""), r.currentVisibleTs.getOrElse(""), r.processedMs,
        r.streamVisibleMs.getOrElse(""), r.currentVisibleMs.getOrElse(""), r.ackMs.getOrElse("")).mkString(",")
    }
    csv(file, "order_id,xid,csn,kind,commit_ts_utc,client_ack_ts,dwh_processed_ts,order_stream_visible_ts," +
      "orders_current_visible_ts,processed_latency_ms,order_stream_visible_latency_ms,orders_current_visible_latency_ms,client_ack_ms", lines)
  }

  /** Percentile table (milliseconds) of the latency columns that have values. */
  def latencyTable(rows: Seq[LatencyRow]): String = {
    val cols = Seq(
      "commit → `dwhProcessedTs`" -> rows.map(_.processedMs),
      "commit → visible in `order_stream`" -> rows.flatMap(_.streamVisibleMs),
      "commit → visible in `orders_current`" -> rows.flatMap(_.currentVisibleMs),
      "commit → client ack (sanity)" -> rows.flatMap(_.ackMs)).filter(_._2.nonEmpty)
    val header = "| Latency (ms) | n | p50 | p95 | p99 | max |\n|---|---|---|---|---|---|\n"
    header + cols.map { case (name, v) =>
      val s = Percentiles.summary(v)
      s"| $name | ${s.n} | ${s.p50} | ${s.p95} | ${s.p99} | ${s.max} |"
    }.mkString("\n")
  }

  /** Writes `summary.md` and `failures.csv`. */
  def write(): Path = {
    val sb = new StringBuilder
    sb ++= s"# $suite ${ctx.runId}\n\n"
    sb ++= s"**Verdict: ${if (passed) "PASS" else "FAIL"}**\n\n"
    sb ++= "## Configuration\n\n| Setting | Value |\n|---|---|\n"
    (Seq("run id" -> ctx.runId, "seed" -> ctx.seed.toString, "sessions" -> ctx.sessions.toString) ++ config)
      .foreach { case (k, v) => sb ++= s"| $k | ${v.replace("|", "\\|")} |\n" }
    sb ++= "\n## Checks\n\n| Check | Result | Note |\n|---|---|---|\n"
    checks.foreach(c => sb ++= s"| ${c.name} | ${if (c.passed) "pass" else s"**FAIL** (${c.details.size})"} | ${c.note} |\n")
    checks.filterNot(_.passed).foreach { c =>
      sb ++= s"\n### Failed: ${c.name}\n\n"
      c.details.take(CatchUp.MaxListed).foreach(d => sb ++= s"- $d\n")
      if (c.details.size > CatchUp.MaxListed) sb ++= s"- ... and ${c.details.size - CatchUp.MaxListed} more (failures.csv)\n"
    }
    sections.foreach { case (t, m) => sb ++= s"\n## $t\n\n$m\n" }
    csv("failures.csv", "check,detail", checks.filterNot(_.passed).flatMap(c => c.details.map(d => s"${quote(c.name)},${quote(d)}")).toSeq)
    val path = dir.resolve("summary.md")
    Files.write(path, sb.toString.getBytes(StandardCharsets.UTF_8))
    path
  }

  /** Writes the report and fails the test with every failed check. */
  def finish(): Path = {
    val path = write()
    println(s"Report: ${path.toAbsolutePath}")
    if (!passed) {
      val failed = checks.filterNot(_.passed).map(c => s"${c.name}: ${c.details.take(5).mkString("; ")}")
      throw new AssertionError(s"$suite ${ctx.runId} failed (${path.toAbsolutePath}):\n" + failed.mkString("\n"))
    }
    path
  }

  private def csv(name: String, header: String, lines: Seq[String]): Path = {
    val path = dir.resolve(name)
    Files.write(path, (header +: lines).mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))
    path
  }

  private def quote(s: String): String = "\"" + s.replace("\"", "\"\"") + "\""
}
