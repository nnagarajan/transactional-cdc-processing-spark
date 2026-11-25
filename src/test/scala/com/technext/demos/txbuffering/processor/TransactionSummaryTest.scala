package com.technext.demos.txbuffering.processor

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.Test

import com.technext.demos.txbuffering.model.generic.TransactionMetadata
import com.technext.demos.txbuffering.processor.Fixtures._

/** Mirrors the completeness cases of [[TransactionStateTest]] for the buffered summary. */
class TransactionSummaryTest {

  private def summary(m: TransactionMetadata, tables: String*): TransactionSummary = {
    val s = TransactionSummary(m.xid, m.csn)
    s.setMetadata(m)
    tables.foreach(s.recordEvent)
    s
  }

  @Test
  def exactCountsAreComplete(): Unit =
    assertThat(summary(marker("x1", "100", orders = 1, lineItems = 1), "ORDERS", "ORDER_LINE_ITEMS").isComplete).isTrue

  @Test
  def fewerEventsThanDeclaredIsIncomplete(): Unit =
    assertThat(summary(marker("x1", "100", lineItems = 2), "ORDER_LINE_ITEMS").isComplete).isFalse

  @Test
  def moreEventsThanDeclaredIsIncomplete(): Unit =
    assertThat(summary(marker("x1", "100", lineItems = 1), "ORDER_LINE_ITEMS", "ORDER_LINE_ITEMS").isComplete).isFalse

  @Test
  def zeroDeclaredEventsIsCompleteImmediately(): Unit =
    assertThat(summary(marker("x1", "100")).isComplete).isTrue

  @Test
  def noMarkerIsIncomplete(): Unit = {
    val s = TransactionSummary("x1", "100")
    s.recordEvent("ORDER_LINE_ITEMS")

    assertThat(s.hasMarker).isFalse
    assertThat(s.isComplete).isFalse
  }

  @Test
  def recordEventReturnsTheBufferedIndexAndCountsPerTable(): Unit = {
    val s = TransactionSummary("x1", "100")

    assertThat(s.recordEvent("ORDERS")).isEqualTo(0)
    assertThat(s.recordEvent("ORDER_LINE_ITEMS")).isEqualTo(1)
    assertThat(s.recordEvent("ORDER_DETAILS")).isEqualTo(2)
    assertThat(s.recordEvent("ORDER_LINE_ITEMS")).isEqualTo(3)

    assertThat(s.eventCount).isEqualTo(4)
    assertThat(s.received).isEqualTo(TableCounts(1, 1, 2))
    assertThatThrownBy(() => s.recordEvent("CUSTOMERS")).isInstanceOf(classOf[IllegalArgumentException])
  }

  @Test
  def progressTextMatchesTheAssembledTransaction(): Unit = {
    val m = marker("x1", "100", orders = 1, details = 1, lineItems = 3)
    val s = summary(m, "ORDERS", "ORDER_LINE_ITEMS")
    val st = state(m, order("x1", "100", "p1"), lineItem("x1", "100", "p2", 1))

    assertThat(s.progress).isEqualTo("Orders: 1/1, Details: 0/1, LineItems: 1/3")
    assertThat(s.progress).isEqualTo(st.progress)
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
    assertThat(back.expected).isEqualTo(TableCounts(1, 0, 2))
    assertThat(back.received).isEqualTo(TableCounts(1, 0, 1))
    assertThat(back.eventCount).isEqualTo(2)
    assertThat(back.isComplete).isFalse
  }
}
