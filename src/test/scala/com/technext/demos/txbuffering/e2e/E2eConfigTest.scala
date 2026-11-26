package com.technext.demos.txbuffering.e2e

import java.time.Duration

import com.typesafe.config.ConfigFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class E2eConfigTest {

  /**
   * e2e.conf as written, without the JVM system properties that ConfigFactory.load layers on top:
   * Maven passes `-D` options such as `-De2e.seed=42` into the test JVM.
   */
  private def fileOnly: com.typesafe.config.Config = {
    ConfigFactory.invalidateCaches()
    ConfigFactory.parseResources("e2e.conf").withFallback(ConfigFactory.defaultReference()).resolve()
  }

  private def withOverrides(overrides: String): E2eConfig =
    E2eConfig.from(ConfigFactory.parseString(overrides).withFallback(fileOnly).resolve(), Map.empty, Map.empty)

  @Test
  def defaultsLoad(): Unit = {
    val c = E2eConfig.from(fileOnly, Map.empty, Map.empty)

    assertThat(c.seed).isEqualTo(0L)

    assertThat(c.oracle.url).isEqualTo("jdbc:oracle:thin:@//localhost:1521/ORCLPDB")
    assertThat(c.oracle.user).isEqualTo("appuser")
    assertThat(c.oracle.password).isEqualTo("appuser")
    assertThat(c.tables.orderStreamFqn).isEqualTo("mymanaged.argus1.order_stream")
    assertThat(c.tables.ordersCurrentFqn).isEqualTo("mymanaged.argus1.orders_current")
    assertThat(c.correctness.sessions).isEqualTo(0)
    assertThat(c.correctness.txPerSession).isEqualTo(40)
    assertThat(c.correctness.weights("ROLLBACK")).isEqualTo(10)
    assertThat(c.performance.eventsPerMinute).isEqualTo(100000)
    assertThat(c.performance.duration).isEqualTo(Duration.ofSeconds(300))
    assertThat(c.performance.mix.keySet.size).isEqualTo(6)
    assertThat(c.performance.bulk).isEqualTo(BulkSettings(10000, 100, 10))
    assertThat(c.performance.maxP95ProcessedLatency.isEmpty).isTrue
    assertThat(c.latency).isEqualTo(LatencySettings(200, Duration.ofMillis(500)))
  }

  @Test
  def environmentOverridesTheFileAndSystemPropertiesOverrideBoth(): Unit = {
    val base = fileOnly
    val env = Map(E2eConfig.UserEnv -> "envuser", E2eConfig.PasswordEnv -> "envpass")

    val fromEnv = E2eConfig.from(base, env, Map.empty)
    assertThat(fromEnv.oracle.user).isEqualTo("envuser")
    assertThat(fromEnv.oracle.password).isEqualTo("envpass")

    val fromProps = E2eConfig.from(base, env, Map("e2e.oracle.user" -> "propuser", "e2e.oracle.password" -> "proppass"))
    assertThat(fromProps.oracle.user).isEqualTo("propuser")
    assertThat(fromProps.oracle.password).isEqualTo("proppass")
  }

  @Test
  def configOverridesApplyLikeSystemProperties(): Unit = {
    val c = withOverrides("""e2e.correctness.sessions = 12
                            |e2e.performance.max-p95-processed-latency = 20 seconds
                            |e2e.seed = 42""".stripMargin)

    assertThat(c.correctness.sessions).isEqualTo(12)
    assertThat(c.seed).isEqualTo(42L)
    assertThat(c.performance.maxP95ProcessedLatency.get).isEqualTo(Duration.ofSeconds(20))
  }
}
