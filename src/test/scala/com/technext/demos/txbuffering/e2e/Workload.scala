package com.technext.demos.txbuffering.e2e

/** Rates of the performance workload (design.md D8). */
object Workload {

  /** Average events per transaction of a weighted flavor mix. */
  def averageEvents(mix: Map[String, Int]): Double = {
    val total = mix.values.sum.toDouble
    mix.map { case (f, w) => w * Transactions.eventsOf(f).toDouble }.sum / total
  }

  /** Transactions per second that give `eventsPerMinute` with this mix. */
  def txPerSecond(eventsPerMinute: Int, mix: Map[String, Int]): Double = eventsPerMinute / 60.0 / averageEvents(mix)
}

/**
 * Fixed-rate schedule: slot `i` is at `start + i * interval`, computed from the start, so a slow
 * commit makes the session late for the next slot but doesn't shift the ones after it.
 */
final class Pacer(start: Long, intervalNanos: Long, now: () => Long = () => System.nanoTime(), sleep: Long => Unit = Pacer.sleepNanos) {

  def slot(i: Long): Long = start + i * intervalNanos

  /** Waits for slot `i`; returns how late (ns) the caller already was, 0 when on time. */
  def awaitSlot(i: Long): Long = {
    val wait = slot(i) - now()
    if (wait > 0) { sleep(wait); 0L } else -wait
  }
}

object Pacer {
  def sleepNanos(ns: Long): Unit = Thread.sleep(ns / 1000000, (ns % 1000000).toInt)
}
