package com.technext.demos.txbuffering.e2e

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Types}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit

import scala.collection.mutable
import scala.util.Random

object SourceTables {
  val Orders = "ORDERS"
  val Details = "ORDER_DETAILS"
  val Items = "ORDER_LINE_ITEMS"
}

/**
 * One captured source row: Oracle column names (upper case) to normalized values (Values.normalize).
 * `key` is ORDER_ID for orders and details, LINE_ITEM_ID for line items. `before` is set for updates.
 */
final case class RowImage(table: String, key: Long, orderId: Long, after: Map[String, Any], before: Option[Map[String, Any]])

/** What the client knows about one transaction it ran (design.md D3, D4). */
final case class TxRecord(
    sessionNo: Int,
    txSeq: Int,
    kind: String,
    committed: Boolean,
    images: Vector[RowImage],
    orderIds: Set[Long],
    counts: (Int, Int, Int),
    clientAckTs: Instant) {
  def events: Int = counts._1 + counts._2 + counts._3
}

/**
 * One simulated Oracle session: a JDBC connection with autocommit off (design.md D2). A transaction
 * is opened with [[begin]], filled with the DML helpers of [[Tx]], and ended with [[Tx.commit]] or
 * [[Tx.rollback]]. With `captureImages` the session reads every touched row back inside the
 * transaction (before and after images, D4); without it only ids and counts are kept (performance).
 */
final class OracleSession(settings: OracleSettings, ctx: RunContext, val sessionNo: Int, captureImages: Boolean)
    extends AutoCloseable {

  val conn: Connection = {
    val c = DriverManager.getConnection(settings.url, settings.user, settings.password)
    c.setAutoCommit(false)
    c
  }

  private var txSeq = 0
  private val statuses = Vector("PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED")
  private val types = Vector("STANDARD", "EXPRESS", "SUBSCRIPTION")
  private val methods = Vector("STANDARD", "EXPRESS", "OVERNIGHT", "PICKUP")
  private val carriers = Vector("FEDEX", "UPS", "DHL", "USPS")
  private val deliveries = Vector("PENDING", "IN_TRANSIT", "DELIVERED", "RETURNED")

  def begin(kind: String, rnd: Random): Tx = {
    txSeq += 1
    new Tx(kind, txSeq, rnd)
  }

  override def close(): Unit = {
    try conn.rollback() catch { case _: Exception => () }
    conn.close()
  }

  final class Tx private[OracleSession] (val kind: String, val seq: Int, rnd: Random) {

    private val images = mutable.ArrayBuffer[RowImage]()
    private val orderIds = mutable.LinkedHashSet[Long]()
    private var orders, details, items = 0

    /** Inserts `n` orders, each with a detail and `lineItems` line items. Returns the new order ids. */
    def insertOrders(n: Int, lineItems: Int): Seq[Long] = (1 to n).map(i => insertOrder(i, lineItems))

    private def insertOrder(i: Int, lineItems: Int): Long = {
      val orderId = withStatement(
        """INSERT INTO orders (order_ref, order_date, order_ts, order_status, order_type, total_amount,
          |  currency, customer_id, shipping_address_id) VALUES (?, ?, ?, ?, ?, ?, 'USD', ?, ?)""".stripMargin,
        Array("ORDER_ID")) { ps =>
        val now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)
        ps.setString(1, s"${ctx.tag}-S$sessionNo-$seq-$i")
        ps.setObject(2, now.toLocalDate.atStartOfDay)
        ps.setObject(3, now)
        ps.setString(4, statuses(rnd.nextInt(statuses.size)))
        ps.setString(5, types(rnd.nextInt(types.size)))
        ps.setBigDecimal(6, money(100 + rnd.nextInt(100000)))
        ps.setString(7, ctx.tag)
        ps.setString(8, f"ADDR-${rnd.nextInt(1000)}%03d")
        ps.executeUpdate()
        generatedKey(ps)
      }
      withStatement(
        """INSERT INTO order_details (order_id, shipping_method, tracking_number, shipped_ts,
          |  estimated_delivery_date, carrier, delivery_status) VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin) { ps =>
        ps.setLong(1, orderId)
        ps.setString(2, methods(rnd.nextInt(methods.size)))
        ps.setString(3, s"${ctx.tag}-$orderId")
        if (rnd.nextBoolean()) ps.setObject(4, LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS))
        else ps.setNull(4, Types.TIMESTAMP)
        ps.setObject(5, LocalDate.now(ZoneOffset.UTC).plusDays(1 + rnd.nextInt(10)).atStartOfDay)
        ps.setString(6, carriers(rnd.nextInt(carriers.size)))
        ps.setString(7, deliveries(rnd.nextInt(deliveries.size)))
        ps.executeUpdate()
      }
      insertLineItems(orderId, firstProduct = 1, lineItems)
      orders += 1
      details += 1
      orderIds += orderId
      if (captureImages) {
        images += image(SourceTables.Orders, "order_id", orderId, orderId, None)
        images += image(SourceTables.Details, "order_id", orderId, orderId, None)
        selectRows(SourceTables.Items, "order_id = ?", orderId).foreach { r =>
          images += RowImage(SourceTables.Items, long(r("LINE_ITEM_ID")), orderId, r, None)
        }
      }
      orderId
    }

    private def insertLineItems(orderId: Long, firstProduct: Int, n: Int): Unit = if (n > 0) {
      withStatement(
        """INSERT INTO order_line_items (order_id, product_id, item_qty, item_price, item_amount, item_currency)
          |VALUES (?, ?, ?, ?, ?, 'USD')""".stripMargin) { ps =>
        (0 until n).foreach { k =>
          val qty = new java.math.BigDecimal(1 + rnd.nextInt(50)).setScale(4)
          val price = new java.math.BigDecimal(rnd.nextInt(10000000)).movePointLeft(4).add(java.math.BigDecimal.ONE)
          ps.setLong(1, orderId)
          ps.setString(2, s"P-${firstProduct + k}")
          ps.setBigDecimal(3, qty)
          ps.setBigDecimal(4, price)
          ps.setBigDecimal(5, qty.multiply(price).setScale(4, java.math.RoundingMode.HALF_UP))
          ps.addBatch()
        }
        ps.executeBatch()
      }
      items += n
    }

    /** Adds `n` line items to an existing order (no order or detail change). */
    def addLineItems(orderId: Long, n: Int): Unit = {
      val next = withStatement(
        "SELECT NVL(MAX(TO_NUMBER(SUBSTR(product_id, 3))), 0) + 1 FROM order_line_items WHERE order_id = ? AND product_id LIKE 'P-%'") { ps =>
        ps.setLong(1, orderId)
        val rs = ps.executeQuery()
        rs.next()
        rs.getInt(1)
      }
      val known = if (captureImages) selectRows(SourceTables.Items, "order_id = ?", orderId).map(r => long(r("LINE_ITEM_ID"))).toSet else Set.empty[Long]
      insertLineItems(orderId, next, n)
      orderIds += orderId
      if (captureImages)
        selectRows(SourceTables.Items, "order_id = ?", orderId)
          .filterNot(r => known(long(r("LINE_ITEM_ID"))))
          .foreach(r => images += RowImage(SourceTables.Items, long(r("LINE_ITEM_ID")), orderId, r, None))
    }

    /** Locks the given orders in ascending id order, so concurrent sessions can't deadlock (D4). */
    def lockOrders(ids: Seq[Long]): Unit = if (ids.nonEmpty) {
      val sorted = ids.distinct.sorted
      withStatement(s"SELECT order_id FROM orders WHERE order_id IN (${sorted.map(_ => "?").mkString(",")}) ORDER BY order_id FOR UPDATE") { ps =>
        sorted.zipWithIndex.foreach { case (id, i) => ps.setLong(i + 1, id) }
        val rs = ps.executeQuery()
        while (rs.next()) ()
      }
    }

    /** Updates the order row (status and amount). The version trigger bumps VERSION. */
    def updateOrder(orderId: Long): Unit = {
      val before = if (captureImages) Some(selectRows(SourceTables.Orders, "order_id = ? FOR UPDATE", orderId).head) else None
      withStatement(
        """UPDATE orders SET order_status = CASE order_status WHEN 'PENDING' THEN 'CONFIRMED' ELSE 'PENDING' END,
          |  total_amount = total_amount + 1.25 WHERE order_id = ?""".stripMargin) { ps =>
        ps.setLong(1, orderId)
        require(ps.executeUpdate() == 1, s"Order $orderId not found")
      }
      orders += 1
      orderIds += orderId
      if (captureImages) images += image(SourceTables.Orders, "order_id", orderId, orderId, before)
    }

    /** Updates the detail row (carrier and delivery status). */
    def updateDetail(orderId: Long): Unit = {
      val before = if (captureImages) Some(selectRows(SourceTables.Details, "order_id = ? FOR UPDATE", orderId).head) else None
      withStatement(
        """UPDATE order_details SET carrier = CASE carrier WHEN 'UPS' THEN 'FEDEX' ELSE 'UPS' END,
          |  delivery_status = CASE delivery_status WHEN 'IN_TRANSIT' THEN 'DELIVERED' ELSE 'IN_TRANSIT' END
          |WHERE order_id = ?""".stripMargin) { ps =>
        ps.setLong(1, orderId)
        require(ps.executeUpdate() == 1, s"Detail of order $orderId not found")
      }
      details += 1
      orderIds += orderId
      if (captureImages) images += image(SourceTables.Details, "order_id", orderId, orderId, before)
    }

    /** Updates the price of the order's first `n` line items (by LINE_ITEM_ID). */
    def updateLineItems(orderId: Long, n: Int): Unit = {
      val ids = withStatement(
        "SELECT line_item_id FROM order_line_items WHERE order_id = ? ORDER BY line_item_id FETCH FIRST ? ROWS ONLY") { ps =>
        ps.setLong(1, orderId)
        ps.setInt(2, n)
        val rs = ps.executeQuery()
        val b = Vector.newBuilder[Long]
        while (rs.next()) b += rs.getLong(1)
        b.result()
      }
      require(ids.size == n, s"Order $orderId has ${ids.size} line items, need $n")
      ids.foreach { id =>
        val before = if (captureImages) Some(selectRows(SourceTables.Items, "line_item_id = ? FOR UPDATE", id).head) else None
        withStatement(
          """UPDATE order_line_items SET item_price = item_price + 0.01,
            |  item_amount = item_qty * (item_price + 0.01) WHERE line_item_id = ?""".stripMargin) { ps =>
          ps.setLong(1, id)
          ps.executeUpdate()
        }
        if (captureImages) images += image(SourceTables.Items, "line_item_id", id, orderId, before)
      }
      items += n
      orderIds += orderId
    }

    /**
     * Writes the ground-truth rows (D3) in this transaction, then commits. The log row is the last
     * statement before COMMIT, so its timestamp is the commit time to within the round trip.
     */
    def commit(): TxRecord = {
      require(orderIds.nonEmpty, s"Transaction $kind touched nothing")
      withStatement(
        """INSERT INTO e2e_tx_orders (run_id, session_no, tx_seq, order_id) VALUES (?, ?, ?, ?)""") { ps =>
        orderIds.foreach { id =>
          ps.setString(1, ctx.runId); ps.setInt(2, sessionNo); ps.setInt(3, seq); ps.setLong(4, id)
          ps.addBatch()
        }
        ps.executeBatch()
      }
      withStatement(
        """INSERT INTO e2e_tx_log (run_id, session_no, tx_seq, kind, local_tx_id, commit_ts_utc,
          |  orders_cnt, details_cnt, line_items_cnt)
          |VALUES (?, ?, ?, ?, DBMS_TRANSACTION.LOCAL_TRANSACTION_ID, SYS_EXTRACT_UTC(SYSTIMESTAMP), ?, ?, ?)""".stripMargin) { ps =>
        ps.setString(1, ctx.runId); ps.setInt(2, sessionNo); ps.setInt(3, seq); ps.setString(4, kind)
        ps.setInt(5, orders); ps.setInt(6, details); ps.setInt(7, items)
        ps.executeUpdate()
      }
      conn.commit()
      record(committed = true)
    }

    def rollback(): TxRecord = {
      conn.rollback()
      record(committed = false)
    }

    private def record(committed: Boolean): TxRecord =
      TxRecord(sessionNo, seq, kind, committed, images.toVector, orderIds.toSet, (orders, details, items), Instant.now())

    private def image(table: String, keyColumn: String, key: Long, orderId: Long, before: Option[Map[String, Any]]): RowImage =
      RowImage(table, key, orderId, selectRows(table, s"$keyColumn = ?", key).head, before)
  }

  /** Rows of a source table as column → normalized value. */
  def selectRows(table: String, where: String, arg: Long): Vector[Map[String, Any]] =
    withStatement(s"SELECT * FROM $table WHERE $where") { ps =>
      ps.setLong(1, arg)
      readAll(ps.executeQuery())
    }

  def withStatement[T](sql: String, keys: Array[String] = null)(f: PreparedStatement => T): T = {
    val ps = if (keys == null) conn.prepareStatement(sql) else conn.prepareStatement(sql, keys)
    try f(ps) finally ps.close()
  }

  private def generatedKey(ps: PreparedStatement): Long = {
    val rs = ps.getGeneratedKeys
    rs.next()
    rs.getLong(1)
  }

  private def money(cents: Int): java.math.BigDecimal = new java.math.BigDecimal(cents).movePointLeft(2)

  private def long(v: Any): Long = v.asInstanceOf[Double].toLong

  private[e2e] def readAll(rs: ResultSet): Vector[Map[String, Any]] = OracleSession.readAll(rs)
}

object OracleSession {

  def readAll(rs: ResultSet): Vector[Map[String, Any]] = {
    val md = rs.getMetaData
    val b = Vector.newBuilder[Map[String, Any]]
    while (rs.next()) {
      b += (1 to md.getColumnCount).map { i =>
        val v = md.getColumnType(i) match {
          case Types.DATE | Types.TIMESTAMP => rs.getTimestamp(i)
          case _                            => rs.getObject(i)
        }
        md.getColumnName(i).toUpperCase -> Values.normalize(v)
      }.toMap
    }
    rs.close()
    b.result()
  }
}
