package com.technext.demos.txbuffering.e2e

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.util.Random

/**
 * Identity and randomness of one suite run. The run id tags every row the run creates; the seed
 * makes the session count and each session's sequence of kinds and sizes reproducible.
 */
final case class RunContext(runId: String, seed: Long, sessions: Int) {

  /** Tag written into `customer_id` of every order this run creates. */
  def tag: String = s"E2E-$runId"

  /** Each session draws from its own generator, so its choices don't depend on the others. */
  def sessionRandom(sessionNo: Int): Random = new Random(seed * 1000003L + sessionNo)
}

object RunContext {

  val MinSessions = 10
  val MaxSessions = 20

  private val IdFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  /**
   * @param seed           0 picks one from the clock
   * @param sessions       0 picks between [[MinSessions]] and [[MaxSessions]] from the seed
   */
  def create(seed: Long, sessions: Int): RunContext = {
    val s = if (seed != 0) seed else System.nanoTime() & 0xffffffffffL
    val n = if (sessions > 0) sessions else defaultSessions(s)
    val runId = LocalDateTime.now(ZoneOffset.UTC).format(IdFormat) + "-" + f"${Random.nextInt(0x10000)}%04x"
    RunContext(runId, s, n)
  }

  def defaultSessions(seed: Long): Int = MinSessions + new Random(seed).nextInt(MaxSessions - MinSessions + 1)
}

/** Picks names by relative weight, for example transaction kinds or performance flavors. */
final case class WeightedChoice(weights: Seq[(String, Int)]) {
  require(weights.nonEmpty && weights.forall(_._2 >= 0) && weights.map(_._2).sum > 0, s"Invalid weights $weights")

  private val ordered = weights.sortBy(_._1)
  private val total = ordered.map(_._2).sum

  def pick(rnd: Random): String = {
    var r = rnd.nextInt(total)
    ordered.find { case (_, w) => r -= w; r < 0 }.get._1
  }

  def names: Seq[String] = ordered.filter(_._2 > 0).map(_._1)
}
