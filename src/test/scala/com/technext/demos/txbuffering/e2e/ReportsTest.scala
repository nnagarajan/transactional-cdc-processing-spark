package com.technext.demos.txbuffering.e2e

import java.nio.file.{Files, Path}
import java.time.Instant

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class ReportsTest {

  @TempDir
  var tmp: Path = _

  @Test
  def nearestRankPercentiles(): Unit = {
    val v = (1L to 100L).reverse

    assertThat(Percentiles.nearestRank(v, 50)).isEqualTo(50L)
    assertThat(Percentiles.nearestRank(v, 95)).isEqualTo(95L)
    assertThat(Percentiles.nearestRank(v, 99)).isEqualTo(99L)
    assertThat(Percentiles.summary(v)).isEqualTo(Percentiles.Summary(100, 50, 95, 99, 100))
    assertThat(Percentiles.summary(Seq(7L))).isEqualTo(Percentiles.Summary(1, 7, 7, 7, 7))
    assertThat(Percentiles.summary(Seq(10L, 20L, 30L, 40L)).p50).isEqualTo(20L)
    assertThat(Percentiles.nearestRank((1L to 200L), 99)).isEqualTo(198L)
  }

  @Test
  def summaryListsEachFailedCheck(): Unit = {
    val ctx = RunContext("20261125100000-abcd", 42L, 12)
    val r = new Report(tmp.toString, "correctness", ctx)
    r.setting("tx per session", 40)
    r.check(CheckResult.of("completeness", Nil))
    r.check(CheckResult.of("commit order", Seq("csn 510 in version 7 after csn 520 in version 6")))
    r.check(CheckResult.of("no dropped run data", Seq("csn 500 DUPLICATE_EVENT", "csn 501 UNKNOWN_TABLE")))
    val t0 = Instant.parse("2026-11-25T10:00:00Z")
    val rows = Seq(LatencyRow(1, "9.1.2.3", BigDecimal(500), "F2", t0, Some(t0.plusMillis(3)), t0.plusMillis(4000),
      Some(t0.plusMillis(6000)), None))
    r.latency(rows)
    r.section("Latency", r.latencyTable(rows))

    val summary = new String(Files.readAllBytes(r.write()))

    assertThat(r.passed).isFalse
    assertThat(r.dir.getFileName.toString).isEqualTo("correctness-20261125100000-abcd")
    assertThat(summary).contains("**Verdict: FAIL**", "| seed | 42 |", "| sessions | 12 |", "| tx per session | 40 |")
    assertThat(summary).contains("| completeness | pass |", "| commit order | **FAIL** (1) |", "| no dropped run data | **FAIL** (2) |")
    assertThat(summary).contains("### Failed: commit order", "- csn 510 in version 7 after csn 520 in version 6", "- csn 501 UNKNOWN_TABLE")
    assertThat(summary).doesNotContain("### Failed: completeness")
    assertThat(summary).contains("| commit → `dwhProcessedTs` | 1 | 4000 | 4000 | 4000 | 4000 |", "| commit → visible in `order_stream` | 1 | 6000 |")
    assertThat(summary).doesNotContain("visible in `orders_current`")

    val failures = Files.readAllLines(r.dir.resolve("failures.csv"))
    assertThat(failures.size).isEqualTo(4)
    val latency = Files.readAllLines(r.dir.resolve("latency.csv"))
    assertThat(latency.get(1)).isEqualTo("1,9.1.2.3,500,F2,2026-11-25T10:00:00Z,2026-11-25T10:00:00.003Z,2026-11-25T10:00:04Z,2026-11-25T10:00:06Z,,4000,6000,,3")
  }

  @Test
  def passingReportFinishes(): Unit = {
    val r = new Report(tmp.toString, "latency", RunContext("20261125100000-0001", 1L, 1))
    r.check(CheckResult.of("completeness", Nil))

    assertThat(Files.readString(r.finish())).contains("**Verdict: PASS**")
  }
}
