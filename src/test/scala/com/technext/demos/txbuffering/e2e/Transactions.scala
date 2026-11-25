package com.technext.demos.txbuffering.e2e

import scala.collection.mutable
import scala.util.Random

/** Shape of a performance flavor: orders per transaction and line items per order. */
final case class Shape(orders: Int, lineItems: Int) {
  /** Events per transaction: an order and a detail per order, plus the line items. */
  def events: Int = orders * (2 + lineItems)
}

/**
 * The transaction kinds of the correctness workload and the flavors of the performance workload
 * (design.md D8, spec "Correctness workload mix" and "Performance transaction flavors").
 */
object Transactions {

  val InsertOne = "INSERT_ONE"
  val InsertMulti = "INSERT_MULTI"
  val UpdateOwn = "UPDATE_OWN"
  val UpdateHot = "UPDATE_HOT"
  val ChildOnly = "CHILD_ONLY"
  val Rollback = "ROLLBACK"
  val Canary = "CANARY"
  val SetupHot = "SETUP_HOT"

  /** The first transactions of every correctness session, so each kind appears in each session. */
  val CorrectnessKinds: Seq[String] = Seq(InsertOne, InsertMulti, UpdateOwn, UpdateHot, ChildOnly, Rollback)

  val Shapes: Map[String, Shape] = Map(
    "F1" -> Shape(1, 1),
    "F2" -> Shape(1, 3),
    "F3" -> Shape(1, 5),
    "F4" -> Shape(1, 10),
    "F5" -> Shape(10, 3),
    "F6" -> Shape(100, 3))

  /** F7: an update of an existing order, its detail and 3 of its line items. */
  val F7 = "F7"
  val F7Events = 5

  def eventsOf(flavor: String): Int = if (flavor == F7) F7Events else Shapes(flavor).events

  def canary(s: OracleSession, rnd: Random): TxRecord = {
    val tx = s.begin(Canary, rnd)
    tx.insertOrders(1, 1)
    tx.commit()
  }

  /** Creates the orders shared by all correctness sessions, each with 3 line items. */
  def setupHot(s: OracleSession, rnd: Random, n: Int): TxRecord = {
    val tx = s.begin(SetupHot, rnd)
    tx.insertOrders(n, 3)
    tx.commit()
  }

  /**
   * Runs one correctness transaction. `own` holds the committed orders of this session (each with
   * at least 2 line items); committed inserts are added to it. `hot` are the shared orders.
   */
  def correctness(kind: String, s: OracleSession, rnd: Random, own: mutable.ArrayBuffer[Long], hot: IndexedSeq[Long]): TxRecord = {
    def pickOwn() = own(rnd.nextInt(own.size))
    val effective = if (kind == Rollback) CorrectnessKinds.filterNot(_ == Rollback)(rnd.nextInt(5)) else kind
    val needsOwn = Set(UpdateOwn, ChildOnly)(effective)
    val tx = s.begin(kind, rnd)
    val created = mutable.ArrayBuffer[Long]()

    (if (needsOwn && own.isEmpty) InsertOne else effective) match {
      case InsertOne   => created ++= tx.insertOrders(1, 2 + rnd.nextInt(4))
      case InsertMulti => created ++= tx.insertOrders(2 + rnd.nextInt(4), 2 + rnd.nextInt(4))
      case UpdateOwn =>
        val id = pickOwn()
        tx.updateOrder(id)
        tx.updateDetail(id)
        tx.updateLineItems(id, 1 + rnd.nextInt(2))
      case UpdateHot =>
        val ids = rnd.shuffle(hot.toList).take(1 + rnd.nextInt(math.min(2, hot.size))).sorted
        tx.lockOrders(ids)
        ids.foreach { id =>
          tx.updateOrder(id)
          if (rnd.nextBoolean()) tx.updateDetail(id)
          if (rnd.nextBoolean()) tx.updateLineItems(id, 1)
        }
      case ChildOnly =>
        val id = pickOwn()
        rnd.nextInt(3) match {
          case 0 => tx.addLineItems(id, 1 + rnd.nextInt(2))
          case 1 => tx.updateLineItems(id, 1)
          case _ => tx.updateDetail(id)
        }
    }

    if (kind == Rollback) tx.rollback()
    else {
      val r = tx.commit()
      own ++= created
      r
    }
  }

  /** Runs one performance transaction. F7 updates an order from `own`, or runs F2 while `own` is empty. */
  def flavor(name: String, s: OracleSession, rnd: Random, own: mutable.ArrayBuffer[Long]): TxRecord = {
    if (name == F7 && own.nonEmpty) {
      val id = own(rnd.nextInt(own.size))
      val tx = s.begin(F7, rnd)
      tx.updateOrder(id)
      tx.updateDetail(id)
      tx.updateLineItems(id, 3)
      tx.commit()
    } else {
      val actual = if (name == F7) "F2" else name
      val shape = Shapes(actual)
      val tx = s.begin(actual, rnd)
      val ids = tx.insertOrders(shape.orders, shape.lineItems)
      val r = tx.commit()
      if (shape.lineItems >= 3) own ++= ids
      r
    }
  }
}
