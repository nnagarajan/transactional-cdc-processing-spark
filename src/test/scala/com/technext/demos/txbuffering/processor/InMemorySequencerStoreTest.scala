package com.technext.demos.txbuffering.processor

import java.math.BigDecimal

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import com.technext.demos.txbuffering.processor.Fixtures._

class InMemorySequencerStoreTest {

  @Test
  def keyHelpersFormatEventAndPositionKeys(): Unit = {
    assertThat(TransactionSequencer.eventKey("x1:100", 0)).isEqualTo("x1:100#0")
    assertThat(TransactionSequencer.eventKey("x1:100", 54999)).isEqualTo("x1:100#54999")
    assertThat(TransactionSequencer.positionKey("x1:100", "ORDER_LINE_ITEMS", "00000000010000001234"))
      .isEqualTo("x1:100|ORDER_LINE_ITEMS|00000000010000001234")
  }

  @Test
  def csnIsStillParsedFromTransactionKeys(): Unit = {
    assertThat(TransactionSequencer.csnOf(TransactionSequencer.txKey("0.4.27.12345", "987654321")).get)
      .isEqualTo(new BigDecimal("987654321"))
    assertThat(TransactionSequencer.csnOf("x1:not-a-number").isEmpty).isTrue
  }

  @Test
  def summaryChangedWithoutPutIsNotStored(): Unit = {
    val store = new InMemorySequencerStore
    store.putSummary("x1:100", TransactionSummary("x1", "100"))

    store.getSummary("x1:100").get.recordEvent("ORDERS")

    assertThat(store.getSummary("x1:100").get.eventCount).isEqualTo(0)
  }

  @Test
  def eventChangedAfterPutIsNotStored(): Unit = {
    val store = new InMemorySequencerStore
    val e = lineItem("x1", "100", "p1", 1)
    store.putEvent("x1:100", 0, e)

    e.pos = "changed"

    assertThat(store.getEvent("x1:100", 0).get.pos).isEqualTo("p1")
  }

  @Test
  def operationsAreCounted(): Unit = {
    val store = new InMemorySequencerStore
    store.putEvent("x1:100", 0, lineItem("x1", "100", "p1", 1))
    store.putPosition("x1:100", "ORDER_LINE_ITEMS", "p1", 0)
    store.positionExists("x1:100", "ORDER_LINE_ITEMS", "p1")

    assertThat(store.count("putEvent")).isEqualTo(1L)
    assertThat(store.count("positionExists")).isEqualTo(1L)
    assertThat(store.count("getEvent")).isEqualTo(0L)
    assertThat(store.writes).isEqualTo(2L)
  }
}
