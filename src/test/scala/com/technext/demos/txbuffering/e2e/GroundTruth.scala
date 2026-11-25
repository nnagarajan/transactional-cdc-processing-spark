package com.technext.demos.txbuffering.e2e

import java.sql.Connection
import java.time.{Instant, LocalDateTime, ZoneOffset}

/** One row of `e2e_tx_log`, with `ORA_ROWSCN` as the transaction's CSN (design.md D3). */
final case class LogEntry(
    sessionNo: Int,
    txSeq: Int,
    kind: String,
    localTxId: String,
    commitTs: Instant,
    counts: (Int, Int, Int),
    csn: BigDecimal)

/** What `order_stream` must hold for one order touched by one committed transaction. */
final case class ExpectedRow(
    orderId: Long,
    csn: BigDecimal,
    localTxId: String,
    sessionNo: Int,
    txSeq: Int,
    kind: String,
    commitTs: Instant,
    clientAckTs: Option[Instant],
    orders: Vector[RowImage],
    details: Vector[RowImage],
    items: Vector[RowImage]) {

  def describe: String = s"session $sessionNo tx $txSeq ($kind) order $orderId csn $csn xid *.$localTxId"
}

/**
 * The run's committed transactions from Oracle, joined with what the client captured (images,
 * rollbacks). Everything the checks compare against comes from here.
 */
final case class GroundTruth(log: Vector[LogEntry], rows: Vector[ExpectedRow], rollbacks: Vector[TxRecord], harnessErrors: Vector[String]) {

  lazy val byKey: Map[(Long, BigDecimal), ExpectedRow] = rows.map(r => (r.orderId, r.csn) -> r).toMap
  lazy val orderIds: Set[Long] = rows.map(_.orderId).toSet
  lazy val csns: Set[BigDecimal] = log.map(_.csn).toSet
  lazy val maxCsnByOrder: Map[Long, BigDecimal] = rows.groupBy(_.orderId).view.mapValues(_.map(_.csn).max).toMap
  lazy val logByCsn: Map[BigDecimal, Vector[LogEntry]] = log.groupBy(_.csn)

  /** Order ids that only rolled-back transactions created. */
  lazy val rolledBackOrderIds: Set[Long] = {
    val inserted = rollbacks.flatMap(_.images.filter(i => i.table == SourceTables.Orders && i.before.isEmpty).map(_.orderId)).toSet
    inserted -- orderIds
  }

  def events: Long = log.map(l => (l.counts._1 + l.counts._2 + l.counts._3).toLong).sum

  /** The part of the run committed by the given sessions (for example one performance phase). */
  def forSessions(sessions: Int => Boolean): GroundTruth =
    GroundTruth(log.filter(l => sessions(l.sessionNo)), rows.filter(r => sessions(r.sessionNo)),
      rollbacks.filter(r => sessions(r.sessionNo)), harnessErrors)
}

object GroundTruth {

  /**
   * @param txOrders `(session_no, tx_seq, order_id)` rows of `e2e_tx_orders`
   * @param records  every transaction the client ran, committed or rolled back
   */
  def build(log: Seq[LogEntry], txOrders: Seq[(Int, Int, Long)], records: Seq[TxRecord]): GroundTruth = {
    val recordByTx = records.filter(_.committed).map(r => (r.sessionNo, r.txSeq) -> r).toMap
    val ordersByTx = txOrders.groupBy(t => (t._1, t._2)).view.mapValues(_.map(_._3).toSet).toMap
    val errors = Vector.newBuilder[String]

    val rows = log.toVector.sortBy(l => (l.csn, l.sessionNo, l.txSeq)).flatMap { l =>
      val rec = recordByTx.get((l.sessionNo, l.txSeq))
      val ids = ordersByTx.getOrElse((l.sessionNo, l.txSeq), Set.empty)
      rec.foreach { r =>
        if (r.orderIds != ids) errors += s"session ${l.sessionNo} tx ${l.txSeq}: client touched ${r.orderIds}, log has $ids"
        if (r.counts != l.counts) errors += s"session ${l.sessionNo} tx ${l.txSeq}: client counts ${r.counts}, log has ${l.counts}"
      }
      ids.toVector.sorted.map { id =>
        val imgs = rec.map(_.images.filter(_.orderId == id)).getOrElse(Vector.empty)
        ExpectedRow(id, l.csn, l.localTxId, l.sessionNo, l.txSeq, l.kind, l.commitTs, rec.map(_.clientAckTs),
          imgs.filter(_.table == SourceTables.Orders),
          imgs.filter(_.table == SourceTables.Details),
          imgs.filter(_.table == SourceTables.Items))
      }
    }

    val logged = log.map(l => (l.sessionNo, l.txSeq)).toSet
    recordByTx.keys.filterNot(logged).toSeq.sorted.foreach { case (s, t) =>
      errors += s"session $s tx $t committed by the client but has no log row"
    }
    GroundTruth(log.toVector, rows, records.filterNot(_.committed).toVector, errors.result())
  }

  /** Reads the run's ground truth from Oracle and joins it with the client's records. */
  def load(conn: Connection, runId: String, records: Seq[TxRecord]): GroundTruth = {
    val log = query(conn,
      """SELECT session_no, tx_seq, kind, local_tx_id, commit_ts_utc, orders_cnt, details_cnt, line_items_cnt,
        |  TO_CHAR(ORA_ROWSCN) AS csn FROM e2e_tx_log WHERE run_id = ?""".stripMargin, runId) { rs =>
      LogEntry(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4),
        rs.getObject(5, classOf[LocalDateTime]).toInstant(ZoneOffset.UTC),
        (rs.getInt(6), rs.getInt(7), rs.getInt(8)), BigDecimal(rs.getString(9)))
    }
    val txOrders = query(conn, "SELECT session_no, tx_seq, order_id FROM e2e_tx_orders WHERE run_id = ?", runId) { rs =>
      (rs.getInt(1), rs.getInt(2), rs.getLong(3))
    }
    build(log, txOrders, records)
  }

  private def query[T](conn: Connection, sql: String, runId: String)(f: java.sql.ResultSet => T): Vector[T] = {
    val ps = conn.prepareStatement(sql)
    ps.setFetchSize(10000)
    try {
      ps.setString(1, runId)
      val rs = ps.executeQuery()
      val b = Vector.newBuilder[T]
      while (rs.next()) b += f(rs)
      b.result()
    } finally ps.close()
  }
}
