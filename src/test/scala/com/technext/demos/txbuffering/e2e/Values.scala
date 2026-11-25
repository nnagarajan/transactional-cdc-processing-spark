package com.technext.demos.txbuffering.e2e

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

/**
 * Makes Oracle values (JDBC) and pipeline values (`order_stream`, written by GoldenGate) comparable
 * (design.md D4). GoldenGate writes NUMBER as DOUBLE, DATE as `2026-02-17 00:00:00` and TIMESTAMP as
 * `2026-02-17 10:00:05.000000000`; JDBC returns BigDecimal and java.sql.Timestamp.
 */
object Values {

  private val GgDateTime = """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,9})?""".r
  private val GgFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSSSSS][.SSSSSS][.SSS]")

  def normalize(v: Any): Any = v match {
    case null                       => null
    case b: java.math.BigDecimal    => b.doubleValue
    case b: scala.math.BigDecimal   => b.toDouble
    case n: java.lang.Number        => n.doubleValue
    case t: java.sql.Timestamp      => t.toLocalDateTime
    case d: java.sql.Date           => d.toLocalDate.atStartOfDay
    case d: LocalDate               => d.atStartOfDay
    case t: LocalDateTime           => t
    case s: String if GgDateTime.matches(s) => parseGg(s)
    case s: String                  => s
    case other                      => other.toString
  }

  private def parseGg(s: String): LocalDateTime = {
    val dot = s.indexOf('.')
    if (dot < 0) LocalDateTime.parse(s.replace(' ', 'T'))
    else {
      val nanos = s.substring(dot + 1).padTo(9, '0')
      LocalDateTime.parse(s.substring(0, dot).replace(' ', 'T')).withNano(nanos.toInt)
    }
  }

  /** Equal after normalization; doubles within a relative 1e-12. */
  def same(a: Any, b: Any): Boolean = (normalize(a), normalize(b)) match {
    case (x: Double, y: Double) => x == y || math.abs(x - y) <= 1e-12 * math.max(math.abs(x), math.abs(y))
    case (x, y)                 => x == y
  }

  /** `ORDER_LINE_ITEMS` column `LINE_ITEM_ID` → `order_stream` field `lineItemId`. */
  def camel(column: String): String = {
    val parts = column.toLowerCase.split('_')
    parts.head + parts.tail.map(_.capitalize).mkString
  }
}
