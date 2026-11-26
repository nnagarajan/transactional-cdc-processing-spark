package com.technext.demos.txbuffering.e2e

import java.time.Duration

import scala.jdk.CollectionConverters._

import com.typesafe.config.{Config, ConfigFactory}

final case class OracleSettings(url: String, user: String, password: String)

final case class TableNames(catalog: String, schema: String, orderStream: String, ordersCurrent: String, droppedEvents: String) {
  private def fqn(t: String) = s"$catalog.$schema.$t"
  def orderStreamFqn: String = fqn(orderStream)
  def ordersCurrentFqn: String = fqn(ordersCurrent)
  def droppedEventsFqn: String = fqn(droppedEvents)
}

final case class CorrectnessSettings(sessions: Int, txPerSession: Int, hotOrders: Int, weights: Map[String, Int])

final case class BulkSettings(orders: Int, ordersPerTx: Int, sessions: Int)

final case class PerformanceSettings(
    sessions: Int,
    eventsPerMinute: Int,
    duration: Duration,
    mix: Map[String, Int],
    bulk: BulkSettings,
    maxP95ProcessedLatency: Option[Duration],
    maxCatchUp: Option[Duration])

final case class LatencySettings(orders: Int, interval: Duration)

/** Settings of the end-to-end suites, from `e2e.conf` (see docs/E2E-TESTING.md). */
final case class E2eConfig(
    raw: Config,
    oracle: OracleSettings,
    tables: TableNames,
    seed: Long,
    pollInterval: Duration,
    preflightTimeout: Duration,
    catchUpTimeout: Duration,
    reportDir: String,
    correctness: CorrectnessSettings,
    performance: PerformanceSettings,
    latency: LatencySettings)

object E2eConfig {

  val UserEnv = "E2E_ORACLE_USER"
  val PasswordEnv = "E2E_ORACLE_PASSWORD"

  /** `e2e.conf` with `-D` system properties on top. */
  def load(): E2eConfig = {
    ConfigFactory.invalidateCaches()
    from(ConfigFactory.load("e2e"), sys.env, sys.props.toMap)
  }

  /**
   * Builds the settings from a resolved config. Precedence for the Oracle login: a `-D` system
   * property, then the environment variable, then the file.
   */
  def from(config: Config, env: Map[String, String], props: Map[String, String]): E2eConfig = {
    val c = config.getConfig("e2e")

    def login(key: String, envName: String): String =
      props.getOrElse(s"e2e.oracle.$key", env.getOrElse(envName, c.getString(s"oracle.$key")))

    def weights(path: String): Map[String, Int] =
      c.getConfig(path).entrySet().asScala.map(e => e.getKey -> c.getConfig(path).getInt(e.getKey)).toMap

    def optDuration(path: String): Option[Duration] = if (c.hasPath(path)) Some(c.getDuration(path)) else None

    E2eConfig(
      raw = config,
      oracle = OracleSettings(c.getString("oracle.url"), login("user", UserEnv), login("password", PasswordEnv)),
      tables = TableNames(
        config.getString("spark.unity-catalog.catalog-name"),
        c.getString("tables.schema"),
        c.getString("tables.order-stream"),
        c.getString("tables.orders-current"),
        c.getString("tables.dropped-events")),
      seed = c.getLong("seed"),
      pollInterval = c.getDuration("poll-interval"),
      preflightTimeout = c.getDuration("preflight-timeout"),
      catchUpTimeout = c.getDuration("catch-up-timeout"),
      reportDir = c.getString("report-dir"),
      correctness = CorrectnessSettings(
        c.getInt("correctness.sessions"),
        c.getInt("correctness.tx-per-session"),
        c.getInt("correctness.hot-orders"),
        weights("correctness.weights")),
      performance = PerformanceSettings(
        c.getInt("performance.sessions"),
        c.getInt("performance.events-per-minute"),
        c.getDuration("performance.duration"),
        weights("performance.mix"),
        BulkSettings(
          c.getInt("performance.bulk.orders"),
          c.getInt("performance.bulk.orders-per-tx"),
          c.getInt("performance.bulk.sessions")),
        optDuration("performance.max-p95-processed-latency"),
        optDuration("performance.max-catch-up")),
      latency = LatencySettings(c.getInt("latency.orders"), c.getDuration("latency.interval")))
  }
}
