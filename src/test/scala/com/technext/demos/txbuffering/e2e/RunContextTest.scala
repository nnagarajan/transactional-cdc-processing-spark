package com.technext.demos.txbuffering.e2e

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RunContextTest {

  private val weights = WeightedChoice(Seq("A" -> 25, "B" -> 10, "C" -> 65))

  private def kinds(ctx: RunContext, session: Int, n: Int): Seq[String] = {
    val rnd = ctx.sessionRandom(session)
    Seq.fill(n)(weights.pick(rnd))
  }

  @Test
  def sameSeedGivesSameSessionCountAndKindSequence(): Unit = {
    val a = RunContext.create(seed = 42, sessions = 0)
    val b = RunContext.create(seed = 42, sessions = 0)

    assertThat(a.sessions).isEqualTo(b.sessions)
    assertThat(kinds(a, 3, 50)).isEqualTo(kinds(b, 3, 50))
    assertThat(kinds(a, 3, 50)).isNotEqualTo(kinds(a, 4, 50))
  }

  @Test
  def defaultSessionCountIsBetweenTenAndTwenty(): Unit = {
    val counts = (1L to 2000L).map(RunContext.defaultSessions)

    assertThat(counts.min).isEqualTo(10)
    assertThat(counts.max).isEqualTo(20)
  }

  @Test
  def configuredSessionCountAndClockSeed(): Unit = {
    val c = RunContext.create(seed = 0, sessions = 12)

    assertThat(c.sessions).isEqualTo(12)
    assertThat(c.seed).isNotEqualTo(0L)
    assertThat(c.runId).matches("\\d{14}-[0-9a-f]{4}")
    assertThat(c.tag).isEqualTo("E2E-" + c.runId)
  }

  @Test
  def weightedChoiceFollowsTheWeights(): Unit = {
    val rnd = new scala.util.Random(1)
    val picks = Seq.fill(20000)(weights.pick(rnd)).groupBy(identity).view.mapValues(_.size).toMap

    assertThat(picks("C").toDouble / 20000).isBetween(0.62, 0.68)
    assertThat(picks("B").toDouble / 20000).isBetween(0.08, 0.12)
  }
}
