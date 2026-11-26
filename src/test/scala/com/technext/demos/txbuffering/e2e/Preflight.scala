package com.technext.demos.txbuffering.e2e

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

import scala.util.Random

/** A pre-flight failure, naming the component that isn't ready (design.md D10). */
final class PreflightFailure(val component: String, message: String) extends AssertionError(s"Pre-flight failed [$component]: $message")

/** What the pre-flight needs from the stack; faked in PreflightTest. */
trait PreflightEnv {
  def checkOracle(): Unit
  def checkUcTable(fqn: String): Unit
  def rowTracking(fqn: String): Boolean
  def logTablesExist(): Boolean
  /** Commits the canary; returns its order id and the ORA_ROWSCN of its log row. */
  def commitCanary(): (Long, BigDecimal)
  def streamCsn(orderId: Long): Option[BigDecimal]
  def currentCsn(orderId: Long): Option[BigDecimal]
}

final case class CanaryResult(orderId: Long, csn: BigDecimal)

object Preflight {

  val Oracle = "Oracle"
  val UnityCatalog = "Unity Catalog"
  val RowTracking = "row tracking"
  val SetupScript = "db/e2e/e2e-setup.sql"
  val Pipeline = "TransactionalCdcProcessingApp / GoldenGate"
  val Merge = "ScdType1MergeApp"
  val Csn = "ORA_ROWSCN vs csn"

  def run(env: PreflightEnv, tables: TableNames, needsCurrent: Boolean, timeout: Duration, interval: Duration,
      log: String => Unit = println): CanaryResult = {

    def step[T](component: String)(f: => T): T =
      try f catch {
        case e: PreflightFailure => throw e
        case e: Throwable        => throw new PreflightFailure(component, Option(e.getMessage).getOrElse(e.toString))
      }

    step(Oracle)(env.checkOracle())
    val all = Seq(tables.orderStreamFqn, tables.ordersCurrentFqn, tables.droppedEventsFqn)
    all.foreach(t => step(UnityCatalog)(env.checkUcTable(t)))
    all.foreach { t =>
      if (!step(UnityCatalog)(env.rowTracking(t)))
        throw new PreflightFailure(RowTracking, s"$t has no delta.enableRowTracking; commit versions can't be read (design.md D6)")
    }
    if (!step(Oracle)(env.logTablesExist()))
      throw new PreflightFailure(SetupScript, "E2E_TX_LOG / E2E_TX_ORDERS not found; run db/e2e/e2e-setup.sql as APPUSER")

    val (orderId, csn) = step(Oracle)(env.commitCanary())
    log(s"Pre-flight: canary order $orderId committed at CSN $csn")

    def await(component: String, what: String)(probe: Long => Option[BigDecimal]): BigDecimal = {
      var seen: Option[BigDecimal] = None
      try CatchUp.await(s"Pre-flight canary in $what", timeout, interval, log) { () =>
        seen = step(UnityCatalog)(probe(orderId)).filter(_ == csn).orElse(seen)
        Progress(seen.size, 1, if (seen.isEmpty) Seq(s"canary order $orderId at CSN $csn") else Nil)
      } catch {
        case _: CatchUpTimeout =>
          throw new PreflightFailure(component, s"canary order $orderId (CSN $csn) did not appear in $what within $timeout; is it running?")
      }
      seen.get
    }

    val streamed = await(Pipeline, tables.orderStream)(env.streamCsn)
    if (streamed != csn) throw new PreflightFailure(Csn, s"canary has csn $streamed in ${tables.orderStream}, ORA_ROWSCN $csn")
    if (needsCurrent) await(Merge, tables.ordersCurrent)(env.currentCsn)
    CanaryResult(orderId, csn)
  }

  /** The pre-flight environment of a live run. */
  final class Live(config: E2eConfig, ctx: RunContext, reader: UcReader, session: OracleSession, onCommit: TxRecord => Unit)
      extends PreflightEnv {

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val ucUri = config.raw.getString("spark.unity-catalog.uri")

    override def checkOracle(): Unit = session.withStatement("SELECT 1 FROM dual")(_.executeQuery().close())

    override def checkUcTable(fqn: String): Unit = {
      val req = HttpRequest.newBuilder(URI.create(s"$ucUri/api/2.1/unity-catalog/tables/$fqn"))
        .timeout(Duration.ofSeconds(10)).GET().build()
      val res = http.send(req, HttpResponse.BodyHandlers.ofString())
      if (res.statusCode() != 200) throw new IllegalStateException(s"GET $fqn returned ${res.statusCode()}: ${res.body().take(200)}")
    }

    override def rowTracking(fqn: String): Boolean = reader.rowTrackingEnabled(fqn)

    override def logTablesExist(): Boolean = session.withStatement(
      "SELECT COUNT(*) FROM user_tables WHERE table_name IN ('E2E_TX_LOG', 'E2E_TX_ORDERS')") { ps =>
      val rs = ps.executeQuery(); rs.next(); rs.getInt(1) == 2
    }

    override def commitCanary(): (Long, BigDecimal) = {
      val r = Transactions.canary(session, new Random(ctx.seed))
      onCommit(r)
      val csn = session.withStatement(
        "SELECT TO_CHAR(ORA_ROWSCN) FROM e2e_tx_log WHERE run_id = ? AND session_no = ? AND tx_seq = ?") { ps =>
        ps.setString(1, ctx.runId); ps.setInt(2, r.sessionNo); ps.setInt(3, r.txSeq)
        val rs = ps.executeQuery(); rs.next(); BigDecimal(rs.getString(1))
      }
      (r.orderIds.head, csn)
    }

    override def streamCsn(orderId: Long): Option[BigDecimal] = reader.streamKeys(Seq(orderId)).map(_.csn).maxOption

    override def currentCsn(orderId: Long): Option[BigDecimal] = reader.currentKeys(Seq(orderId)).map(_._2).headOption
  }
}
