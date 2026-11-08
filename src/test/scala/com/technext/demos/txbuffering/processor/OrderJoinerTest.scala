package com.technext.demos.txbuffering.processor

import scala.jdk.CollectionConverters._

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import com.technext.demos.txbuffering.processor.Fixtures._

class OrderJoinerTest {

  @Test
  def deleteEventsProduceNoEntityRecords(): Unit = {
    val updated = event("x1", "100", "ORDER_LINE_ITEMS", "p1", opType = "U",
      after = Map("LINE_ITEM_ID" -> "10", "ORDER_ID" -> "1", "VERSION" -> "2"),
      before = Map("LINE_ITEM_ID" -> "10", "ORDER_ID" -> "1", "VERSION" -> "1"))
    val deleted = event("x1", "100", "ORDER_LINE_ITEMS", "p2", opType = "D",
      before = Map("LINE_ITEM_ID" -> "11", "ORDER_ID" -> "1", "VERSION" -> "1"))

    val out = OrderJoiner.joinTransaction(state(marker("x1", "100", lineItems = 2), updated, deleted))

    assertThat(out.size).isEqualTo(1)
    val items = out.head.lineItems.asScala
    assertThat(items.size).isEqualTo(1)
    assertThat(items.head.lineItemId).isEqualTo(10.0)
    assertThat(out.head.xid).isEqualTo("x1")
    assertThat(out.head.csn).isEqualTo("100")
  }
}
