package com.technext.demos.txbuffering.e2e

import java.time.Duration

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PreflightTest {

  private val tables = TableNames("mymanaged", "argus1", "order_stream", "orders_current", "dropped_events")
  private val quiet: String => Unit = _ => ()
  private val short = Duration.ofMillis(200)
  private val tick = Duration.ofMillis(10)

  private class Fake extends PreflightEnv {
    var oracleUp = true
    var ucUp = true
    var tracking = true
    var logTables = true
    var streamAfterPolls = 2
    var currentAfterPolls = 2
    var streamedCsn: BigDecimal = BigDecimal(500)
    private var streamPolls, currentPolls = 0

    override def checkOracle(): Unit = if (!oracleUp) throw new java.sql.SQLException("IO Error: Connection refused")
    override def checkUcTable(fqn: String): Unit = if (!ucUp) throw new java.net.ConnectException("Connection refused")
    override def rowTracking(fqn: String): Boolean = tracking
    override def logTablesExist(): Boolean = logTables
    override def commitCanary(): (Long, BigDecimal) = (42L, BigDecimal(500))
    override def streamCsn(orderId: Long): Option[BigDecimal] = {
      streamPolls += 1
      if (streamAfterPolls >= 0 && streamPolls > streamAfterPolls) Some(streamedCsn) else None
    }
    override def currentCsn(orderId: Long): Option[BigDecimal] = {
      currentPolls += 1
      if (currentAfterPolls >= 0 && currentPolls > currentAfterPolls) Some(BigDecimal(500)) else None
    }
  }

  private def run(f: Fake, needsCurrent: Boolean = true) = Preflight.run(f, tables, needsCurrent, short, tick, quiet)

  private def thrown(f: => Any): Throwable =
    try { f; throw new AssertionError("expected an exception") } catch { case e: Throwable if !e.getMessage.startsWith("expected an") => e }

  private def failsWith(f: Fake, component: String, text: String, needsCurrent: Boolean = true): Unit = {
    val e = thrown(run(f, needsCurrent))
    assertThat(e).isInstanceOf(classOf[PreflightFailure])
    assertThat(e.getMessage).contains(s"[$component]", text)
  }

  @Test
  def canaryFoundInBothTables(): Unit = assertThat(run(new Fake)).isEqualTo(CanaryResult(42L, BigDecimal(500)))

  @Test
  def currentNotNeeded(): Unit = {
    val f = new Fake
    f.currentAfterPolls = -1
    assertThat(run(f, needsCurrent = false).orderId).isEqualTo(42L)
  }

  @Test
  def oracleDown(): Unit = { val f = new Fake; f.oracleUp = false; failsWith(f, Preflight.Oracle, "Connection refused") }

  @Test
  def ucDown(): Unit = { val f = new Fake; f.ucUp = false; failsWith(f, Preflight.UnityCatalog, "Connection refused") }

  @Test
  def noRowTracking(): Unit = { val f = new Fake; f.tracking = false; failsWith(f, Preflight.RowTracking, "mymanaged.argus1.order_stream") }

  @Test
  def setupScriptNotRun(): Unit = { val f = new Fake; f.logTables = false; failsWith(f, Preflight.SetupScript, "e2e-setup.sql") }

  @Test
  def pipelineNotRunning(): Unit = { val f = new Fake; f.streamAfterPolls = -1; failsWith(f, Preflight.Pipeline, "did not appear in order_stream") }

  @Test
  def mergeAppNotRunning(): Unit = { val f = new Fake; f.currentAfterPolls = -1; failsWith(f, Preflight.Merge, "did not appear in orders_current") }

  @Test
  def catchUpListsMissingOnTimeout(): Unit = {
    val missing = (1 to 60).map(i => s"tx $i")
    val e = thrown(CatchUp.await("order_stream", short, tick, quiet)(() => Progress(3, 63, missing)))
    assertThat(e).isInstanceOf(classOf[CatchUpTimeout])
    assertThat(e.getMessage).contains("(3 / 63)", "  - tx 50", "... and 10 more", "STATE-STORE-DEBUG.md")
    assertThat(e.getMessage).doesNotContain("tx 51\n")
  }

  @Test
  def catchUpReturnsOnceFound(): Unit = {
    var polls = 0
    val p = CatchUp.await("order_stream", short, tick, quiet) { () =>
      polls += 1
      if (polls < 3) Progress(polls, 3, Seq("x")) else Progress(3, 3, Nil)
    }
    assertThat(p.found).isEqualTo(3)
    assertThat(polls).isEqualTo(3)
  }
}
