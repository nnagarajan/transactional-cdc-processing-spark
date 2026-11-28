package com.technext.demos.txbuffering.processor

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import com.technext.demos.txbuffering.model.generic.{DataCollectionCount, TransactionMetadata}
import com.technext.demos.txbuffering.processor.Fixtures._

/** Mirrors the completeness cases of [[TransactionStateTest]] for the buffered summary. */
class TransactionSummaryTest {

  private def summary(m: TransactionMetadata, tables: String*): TransactionSummary = {
    val s = TransactionSummary(m.xid, m.csn)
    s.acceptMarker(m, OrdersTables)
    tables.foreach(s.recordEvent)
    s
  }

  private def counts(s: TransactionSummary): Map[String, Int] =
    s.received.asScala.map { case (k, v) => k -> v.intValue }.toMap

  @Test
  def exactCountsAreComplete(): Unit =
    assertThat(summary(marker("x1", "100", orders = 1, lineItems = 1), "ORDERS", "ORDER_LINE_ITEMS").isComplete(OrdersTables)).isTrue

  @Test
  def fewerEventsThanDeclaredIsIncomplete(): Unit =
    assertThat(summary(marker("x1", "100", lineItems = 2), "ORDER_LINE_ITEMS").isComplete(OrdersTables)).isFalse

  @Test
  def moreEventsThanDeclaredIsIncomplete(): Unit =
    assertThat(summary(marker("x1", "100", lineItems = 1), "ORDER_LINE_ITEMS", "ORDER_LINE_ITEMS").isComplete(OrdersTables)).isFalse

  @Test
  def zeroDeclaredEventsIsCompleteImmediately(): Unit =
    assertThat(summary(marker("x1", "100")).isComplete(OrdersTables)).isTrue

  @Test
  def noMarkerIsIncomplete(): Unit = {
    val s = TransactionSummary("x1", "100")
    s.recordEvent("ORDER_LINE_ITEMS")
    assertThat(s.hasMarker).isFalse
    assertThat(s.isComplete(OrdersTables)).isFalse
  }

  @Test
  def recordEventReturnsTheBufferedIndexAndCountsPerTable(): Unit = {
    val s = TransactionSummary("x1", "100")
    assertThat(s.recordEvent("ORDERS")).isEqualTo(0)
    assertThat(s.recordEvent("ORDER_LINE_ITEMS")).isEqualTo(1)
    assertThat(s.recordEvent("ORDER_DETAILS")).isEqualTo(2)
    assertThat(s.recordEvent("ORDER_LINE_ITEMS")).isEqualTo(3)
    assertThat(s.eventCount).isEqualTo(4)
    assertThat(counts(s).asJava).isEqualTo(Map("ORDERS" -> 1, "ORDER_DETAILS" -> 1, "ORDER_LINE_ITEMS" -> 2).asJava)
  }

  @Test
  def markerCoversTablesOfAnotherJob(): Unit = {
    // The marker declares 1 ORDERS and 3 CUSTOMERS events; CUSTOMERS belongs to another job
    val m = marker("x1", "100", orders = 1)
    m.dataCollections = java.util.List.of(new DataCollectionCount("ORDERS", 1), new DataCollectionCount("CUSTOMERS", 3))
    val s = summary(m, "ORDERS")
    assertThat(s.isComplete(OrdersTables)).isTrue
    assertThat(s.expected.containsKey("CUSTOMERS")).isFalse
  }

  @Test
  def modelWithASingleTable(): Unit = {
    val m = new TransactionMetadata()
    m.xid = "x1"
    m.csn = "100"
    m.dataCollections = java.util.List.of(new DataCollectionCount("CUSTOMERS", 2))
    val s = TransactionSummary("x1", "100")
    s.acceptMarker(m, Seq("CUSTOMERS"))
    s.recordEvent("CUSTOMERS")
    assertThat(s.isComplete(Seq("CUSTOMERS"))).isFalse
    s.recordEvent("CUSTOMERS")
    assertThat(s.isComplete(Seq("CUSTOMERS"))).isTrue
    assertThat(s.progress(Seq("CUSTOMERS"))).isEqualTo("CUSTOMERS: 2/2")
  }

  @Test
  def progressTextMatchesTheAssembledTransaction(): Unit = {
    val m = marker("x1", "100", orders = 1, details = 1, lineItems = 3)
    val s = summary(m, "ORDERS", "ORDER_LINE_ITEMS")
    val st = state(m, order("x1", "100", "p1"), lineItem("x1", "100", "p2", 1))
    assertThat(s.progress(OrdersTables)).isEqualTo("ORDERS: 1/1, ORDER_DETAILS: 0/1, ORDER_LINE_ITEMS: 1/3")
    assertThat(s.progress(OrdersTables)).isEqualTo(st.progress)
  }

  @Test
  def beanEncoderRoundTripKeepsTheMarkerAndCounts(): Unit = {
    val s = summary(marker("x1", "100", orders = 1, lineItems = 2), "ORDERS", "ORDER_LINE_ITEMS")
    implicit val bean: Encoder[TransactionSummary] = Encoders.bean(classOf[TransactionSummary])
    val enc = encoderFor[TransactionSummary]
    val back = enc.resolveAndBind().createDeserializer()(enc.createSerializer()(s))
    assertThat(back.xid).isEqualTo("x1")
    assertThat(back.csn).isEqualTo("100")
    assertThat(back.hasMarker).isTrue
    assertThat(TransactionCompletion.counts(back.expected, OrdersTables).asJava)
      .isEqualTo(Seq("ORDERS" -> 1, "ORDER_DETAILS" -> 0, "ORDER_LINE_ITEMS" -> 2).asJava)
    assertThat(counts(back).asJava).isEqualTo(Map("ORDERS" -> 1, "ORDER_LINE_ITEMS" -> 1).asJava)
    assertThat(back.eventCount).isEqualTo(2)
    assertThat(back.isComplete(OrdersTables)).isFalse
  }
}
