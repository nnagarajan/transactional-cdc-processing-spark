package com.technext.demos.txbuffering.e2e

import scala.collection.mutable

import org.assertj.core.api.Assertions.{assertThat, within}
import org.junit.jupiter.api.Test

class WorkloadTest {

  private val defaultMix = E2eConfig.load().performance.mix

  @Test
  def defaultMixAveragesAboutSixPointThreeFiveEvents(): Unit = {
    assertThat(Workload.averageEvents(defaultMix)).isCloseTo(6.35, within(1e-9))
    assertThat(Workload.txPerSecond(100000, defaultMix)).isCloseTo(262.47, within(0.01))
  }

  @Test
  def flavorShapes(): Unit = {
    assertThat(Seq("F1", "F2", "F3", "F4", "F5", "F6", "F7").map(Transactions.eventsOf)).isEqualTo(Seq(3, 5, 7, 12, 50, 500, 5))
  }

  @Test
  def pacerDoesNotDriftAfterASlowCommit(): Unit = {
    var clock = 0L
    val sleeps = mutable.ArrayBuffer[Long]()
    val p = new Pacer(0L, 100L, () => clock, ns => { sleeps += ns; clock += ns })

    assertThat(p.awaitSlot(0)).isEqualTo(0L)
    clock += 10        // fast commit
    assertThat(p.awaitSlot(1)).isEqualTo(0L)
    assertThat(clock).isEqualTo(100L)
    clock += 250       // slow commit: 350, past slot 2 (200) and slot 3 (300)
    assertThat(p.awaitSlot(2)).isEqualTo(150L)
    assertThat(p.awaitSlot(3)).isEqualTo(50L)
    clock += 10
    assertThat(p.awaitSlot(4)).isEqualTo(0L)
    assertThat(clock).isEqualTo(400L)   // back on the original schedule
    assertThat(sleeps.toSeq).isEqualTo(Seq(100L - 10L, 400L - 360L))
  }
}
