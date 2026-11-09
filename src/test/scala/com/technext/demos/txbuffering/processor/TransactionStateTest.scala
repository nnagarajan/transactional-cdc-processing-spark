package com.technext.demos.txbuffering.processor

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import com.technext.demos.txbuffering.processor.Fixtures._

class TransactionStateTest {

  @Test
  def duplicatePosIsIgnoredAndDoesNotCountTowardCompletion(): Unit = {
    val s = state(marker("x1", "100", lineItems = 3))

    assertThat(s.addEvent(lineItem("x1", "100", "p100", 1))).isEqualTo(TransactionState.Added)
    assertThat(s.addEvent(lineItem("x1", "100", "p100", 1))).isEqualTo(TransactionState.DuplicatePos)
    assertThat(s.addEvent(lineItem("x1", "100", "p101", 2))).isEqualTo(TransactionState.Added)

    assertThat(s.orderLineItemEvents.size).isEqualTo(2)
    assertThat(s.isComplete).isFalse

    s.addEvent(lineItem("x1", "100", "p102", 3))
    assertThat(s.isComplete).isTrue
  }

  @Test
  def eventForAnUntrackedTableIsIgnored(): Unit = {
    val s = state(marker("x1", "100", lineItems = 1))

    assertThat(s.addEvent(event("x1", "100", "CUSTOMERS", "p1"))).isEqualTo(TransactionState.UnknownTable)
    assertThat(s.orderEvents.size + s.orderDetailEvents.size + s.orderLineItemEvents.size).isEqualTo(0)
  }

  @Test
  def twoDistinctChangesToTheSameRowAreBothKept(): Unit = {
    val s = state(marker("x1", "100", lineItems = 2),
      lineItem("x1", "100", "p100", 1, version = 1),
      lineItem("x1", "100", "p101", 1, version = 2))

    assertThat(s.orderLineItemEvents.size).isEqualTo(2)
    assertThat(s.isComplete).isTrue
  }

  @Test
  def exactCountsAreComplete(): Unit = {
    val s = state(marker("x1", "100", orders = 1, lineItems = 1),
      order("x1", "100", "p1"), lineItem("x1", "100", "p2", 1))

    assertThat(s.isComplete).isTrue
  }

  @Test
  def fewerEventsThanDeclaredIsIncomplete(): Unit = {
    val s = state(marker("x1", "100", lineItems = 2), lineItem("x1", "100", "p1", 1))

    assertThat(s.isComplete).isFalse
  }

  @Test
  def moreEventsThanDeclaredIsIncomplete(): Unit = {
    val s = state(marker("x1", "100", lineItems = 1),
      lineItem("x1", "100", "p1", 1), lineItem("x1", "100", "p2", 2))

    assertThat(s.isComplete).isFalse
  }

  @Test
  def zeroDeclaredEventsIsCompleteImmediately(): Unit =
    assertThat(state(marker("x1", "100")).isComplete).isTrue

  @Test
  def noMarkerIsIncomplete(): Unit = {
    val s = new TransactionState()
    s.addEvent(lineItem("x1", "100", "p1", 1))

    assertThat(s.isComplete).isFalse
  }

  @Test
  def deleteEventCountsTowardCompletion(): Unit = {
    val s = state(marker("x1", "100", lineItems = 2), lineItem("x1", "100", "p1", 1))
    assertThat(s.isComplete).isFalse

    s.addEvent(event("x1", "100", "ORDER_LINE_ITEMS", "p2", opType = "D",
      before = Map("LINE_ITEM_ID" -> "2", "ORDER_ID" -> "1", "VERSION" -> "1")))

    assertThat(s.isComplete).isTrue
  }
}
