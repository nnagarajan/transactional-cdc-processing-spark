package com.technext.demos.txbuffering.e2e

import java.time.{Duration, Instant}

/** How far the pipeline has got: `missing` describes what is not yet reflected. */
final case class Progress(found: Int, expected: Int, missing: Seq[String]) {
  def done: Boolean = missing.isEmpty && found >= expected
}

/** Raised when the pipeline doesn't catch up in time; the message lists what is missing. */
final class CatchUpTimeout(message: String) extends AssertionError(message)

/** Polls until the pipeline reflects everything expected, or the timeout passes (design.md D7). */
object CatchUp {

  val MaxListed = 50

  def await(label: String, timeout: Duration, interval: Duration, log: String => Unit = println)(probe: () => Progress): Progress = {
    val deadline = Instant.now().plus(timeout)
    var last = probe()
    while (!last.done) {
      if (Instant.now().isAfter(deadline)) {
        val listed = last.missing.take(MaxListed).map("  - " + _).mkString("\n")
        val more = if (last.missing.size > MaxListed) s"\n  ... and ${last.missing.size - MaxListed} more" else ""
        throw new CatchUpTimeout(
          s"$label: not caught up after $timeout (${last.found} / ${last.expected}). Missing:\n$listed$more\n" +
            "If the pipeline is blocked on an incomplete transaction, look for the blocked-head alert in the " +
            "TransactionalCdcProcessingApp log and see docs/STATE-STORE-DEBUG.md.")
      }
      log(s"$label: ${last.found} / ${last.expected}")
      Thread.sleep(interval.toMillis)
      last = probe()
    }
    log(s"$label: ${last.found} / ${last.expected}, caught up")
    last
  }
}
