package com.technext.demos.txbuffering.processor

import java.{util => ju}

import scala.jdk.CollectionConverters._

import com.technext.demos.txbuffering.model.generic.{DataChangeEvent, DataCollectionCount, TransactionMetadata}

/** Builders for CDC events and markers used across processor tests. */
object Fixtures {

  def marker(xid: String, csn: String, orders: Int = 0, details: Int = 0, lineItems: Int = 0): TransactionMetadata = {
    val m = new TransactionMetadata()
    m.xid = xid
    m.csn = csn
    m.eventCount = orders + details + lineItems
    m.dataCollections = Seq(
      new DataCollectionCount("ORDERS", orders),
      new DataCollectionCount("ORDER_DETAILS", details),
      new DataCollectionCount("ORDER_LINE_ITEMS", lineItems)).asJava
    m
  }

  def event(
      xid: String,
      csn: String,
      table: String,
      pos: String,
      opType: String = "I",
      after: Map[String, String] = null,
      before: Map[String, String] = null): DataChangeEvent = {
    val e = new DataChangeEvent()
    e.xid = xid
    e.csn = csn
    e.table = s"APPUSER.$table"
    e.pos = pos
    e.opType = opType
    e.after = if (after == null) null else new ju.HashMap[String, String](after.asJava)
    e.before = if (before == null) null else new ju.HashMap[String, String](before.asJava)
    e
  }

  def lineItem(xid: String, csn: String, pos: String, lineItemId: Int, orderId: Int = 1, version: Int = 1): DataChangeEvent =
    event(xid, csn, "ORDER_LINE_ITEMS", pos,
      after = Map("LINE_ITEM_ID" -> lineItemId.toString, "ORDER_ID" -> orderId.toString, "VERSION" -> version.toString))

  def order(xid: String, csn: String, pos: String, orderId: Int = 1, version: Int = 1): DataChangeEvent =
    event(xid, csn, "ORDERS", pos, after = Map("ORDER_ID" -> orderId.toString, "VERSION" -> version.toString))

  def state(m: TransactionMetadata, events: DataChangeEvent*): TransactionState = {
    val s = new TransactionState()
    s.xid = m.xid
    s.csn = m.csn
    s.setMetadata(m)
    events.foreach(s.addEvent)
    s
  }
}
