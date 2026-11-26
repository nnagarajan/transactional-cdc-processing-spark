package com.technext.demos.txbuffering.e2e

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Random

/**
 * What every suite run shares: config, run context, the control session (session 0: canary and
 * setup), concurrent workload sessions, the UC reader, catch-up waits and the report.
 */
final class Harness(val suite: String, sessions: Int) extends AutoCloseable {

  val config: E2eConfig = E2eConfig.load()
  val ctx: RunContext = RunContext.create(config.seed, sessions)
  val report = new Report(config.reportDir, suite, ctx)
  private val recorded = new ConcurrentLinkedQueue[TxRecord]()

  println(s"[$suite] run ${ctx.runId}, seed ${ctx.seed}, ${ctx.sessions} sessions")
  val reader: UcReader = UcReader.connect(config)
  val control = new OracleSession(config.oracle, ctx, sessionNo = 0, captureImages = true)

  def record(r: TxRecord): TxRecord = { recorded.add(r); r }
  def records: Vector[TxRecord] = recorded.asScala.toVector

  def preflight(needsCurrent: Boolean): CanaryResult =
    Preflight.run(new Preflight.Live(config, ctx, reader, control, record), config.tables, needsCurrent,
      config.preflightTimeout, config.pollInterval, msg => println(s"[$suite] $msg"))

  /**
   * Runs `body` on sessions 1..n concurrently, each with its own connection and generator, and
   * waits for all of them. A failure in any session fails the run.
   */
  def runSessions(n: Int, captureImages: Boolean, firstSessionNo: Int = 1)(body: (OracleSession, Random) => Unit): Unit = {
    val pool = Executors.newFixedThreadPool(n)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    try {
      val all = (firstSessionNo until firstSessionNo + n).map { no =>
        Future {
          val s = new OracleSession(config.oracle, ctx, no, captureImages)
          try body(s, ctx.sessionRandom(no)) finally s.close()
        }
      }
      Await.result(Future.sequence(all), Duration.Inf)
    } finally {
      pool.shutdown()
      pool.awaitTermination(1, TimeUnit.MINUTES)
    }
  }

  def groundTruth(): GroundTruth = {
    val t = GroundTruth.load(control.conn, ctx.runId, records)
    report.check(CheckResult.of("ground truth consistent with the client", t.harnessErrors, s"${t.log.size} committed transactions"))
    t
  }

  /** Waits until every expected `(orderId, csn)` is in `order_stream`. */
  def awaitStream(truth: GroundTruth): Unit = {
    val expected = truth.rows.map(r => (r.orderId, r.csn) -> r).toMap
    CatchUp.await(s"[$suite] order_stream", config.catchUpTimeout, config.pollInterval) { () =>
      val seen = reader.streamKeys(truth.orderIds).map(k => (k.orderId, k.csn)).toSet
      val missing = expected.keys.filterNot(seen).toSeq.sortBy(_._2).map(k => expected(k).describe)
      Progress(expected.size - missing.size, expected.size, missing)
    }
  }

  /** Waits until every run order is in `orders_current` at its highest committed CSN. */
  def awaitCurrent(truth: GroundTruth): Unit =
    CatchUp.await(s"[$suite] orders_current", config.catchUpTimeout, config.pollInterval) { () =>
      val seen = reader.currentKeys(truth.orderIds).map(k => k._1 -> k._2).toMap
      val missing = truth.maxCsnByOrder.toSeq.sortBy(_._1).collect {
        case (id, csn) if !seen.get(id).contains(csn) => s"order $id at csn $csn (orders_current has ${seen.get(id).map(_.toString).getOrElse("nothing")})"
      }
      Progress(truth.orderIds.size - missing.size, truth.orderIds.size, missing)
    }

  /** Latency rows for every expected row; visible times from row commit versions (design.md D6, D9). */
  def latencyRows(truth: GroundTruth, keys: Seq[StreamKey], current: Option[Seq[(Long, BigDecimal, Long)]]): Vector[LatencyRow] = {
    val streamTimes = reader.commitTimes(config.tables.orderStreamFqn, keys.map(_.commitVersion).minOption.getOrElse(0L))
    val currentTimes = current.filter(_.nonEmpty).map(c => reader.commitTimes(config.tables.ordersCurrentFqn, c.map(_._3).min)).getOrElse(Map.empty[Long, java.time.Instant])
    val currentByKey = current.getOrElse(Nil).map(c => (c._1, c._2) -> c._3).toMap
    val byKey = keys.map(k => (k.orderId, k.csn) -> k).toMap
    truth.rows.flatMap { e =>
      byKey.get((e.orderId, e.csn)).map { k =>
        LatencyRow(e.orderId, k.xid, e.csn, e.kind, e.commitTs, e.clientAckTs, k.dwhProcessedTs,
          streamTimes.get(k.commitVersion), currentByKey.get((e.orderId, e.csn)).flatMap(currentTimes.get))
      }
    }
  }

  override def close(): Unit = {
    try control.close() catch { case _: Exception => () }
    reader.spark.stop()
  }
}
