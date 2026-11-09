package com.technext.demos.txbuffering

import com.typesafe.config.{Config, ConfigFactory}
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.Test

class KafkaSourceOptionsTest {

  private def config(extra: String): Config =
    ConfigFactory.parseString(
      s"""
         |kafka.starting-offsets = "earliest"
         |job.kafka.topics {
         |  orders = "dev.appuser.orders.json"
         |  transaction-metadata = "dev.transaction_metadata_json"
         |}
         |$extra
         |""".stripMargin)

  @Test
  def noOverrideFallsBackToSharedDefault(): Unit = {
    val o = KafkaSourceOptions.forTopic(config(""), "orders")
    assertThat(o.startingOffsets).isEqualTo("earliest")
    assertThat(o.maxOffsetsPerTrigger.isEmpty).isTrue
  }

  @Test
  def latestOverride(): Unit = {
    val o = KafkaSourceOptions.forTopic(config("""job.kafka.starting-offsets.orders = "LATEST""""), "orders")
    assertThat(o.startingOffsets).isEqualTo("latest")
  }

  @Test
  def partitionMapIsWrappedWithTheTopicName(): Unit = {
    val c = config("""job.kafka.starting-offsets.orders = "{\"0\":12345,\"1\":0}"""")
    assertThat(KafkaSourceOptions.forTopic(c, "orders").startingOffsets)
      .isEqualTo("""{"dev.appuser.orders.json":{"0":12345,"1":0}}""")
    // The override applies only to its own topic.
    assertThat(KafkaSourceOptions.forTopic(c, "transaction-metadata").startingOffsets).isEqualTo("earliest")
  }

  @Test
  def nonNumericOffsetIsRejectedNamingTheTopicKey(): Unit =
    assertThatThrownBy(() =>
      KafkaSourceOptions.forTopic(config("""job.kafka.starting-offsets.orders = "{\"0\":\"abc\"}""""), "orders"))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessageContaining("'orders'")
      .hasMessageContaining("""{"0":"abc"}""")

  @Test
  def negativeOffsetIsRejected(): Unit =
    assertThatThrownBy(() =>
      KafkaSourceOptions.forTopic(config("""job.kafka.starting-offsets.orders = "{\"0\":-2}""""), "orders"))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessageContaining("'orders'")

  @Test
  def nonNumericPartitionAndGarbageAreRejected(): Unit = {
    assertThatThrownBy(() => KafkaSourceOptions.startingOffsets("""{"x":1}""", "t", "k"))
      .isInstanceOf(classOf[IllegalArgumentException])
    assertThatThrownBy(() => KafkaSourceOptions.startingOffsets("beginning", "t", "k"))
      .isInstanceOf(classOf[IllegalArgumentException])
    assertThatThrownBy(() => KafkaSourceOptions.startingOffsets("{}", "t", "k"))
      .isInstanceOf(classOf[IllegalArgumentException])
  }

  @Test
  def maxOffsetsPerTriggerIsApplied(): Unit = {
    val o = KafkaSourceOptions.forTopic(config("job.kafka.max-offsets-per-trigger = 50000"), "orders")
    assertThat(o.asMap("maxOffsetsPerTrigger")).isEqualTo("50000")
  }

  @Test
  def noneOmitsTheLimit(): Unit = {
    val o = KafkaSourceOptions.forTopic(config("""job.kafka.max-offsets-per-trigger = "none""""), "orders")
    assertThat(o.asMap.contains("maxOffsetsPerTrigger")).isFalse
  }

  @Test
  def invalidLimitIsRejected(): Unit =
    assertThatThrownBy(() => KafkaSourceOptions.forTopic(config("job.kafka.max-offsets-per-trigger = 0"), "orders"))
      .isInstanceOf(classOf[IllegalArgumentException])

  @Test
  def runbookHoconFormParses(): Unit = {
    val c = config("job.kafka.starting-offsets {\n  orders = \"\"\"{\"0\":113}\"\"\"\n  transaction-metadata = \"earliest\"\n}")
    assertThat(KafkaSourceOptions.forTopic(c, "orders").startingOffsets)
      .isEqualTo("""{"dev.appuser.orders.json":{"0":113}}""")
    assertThat(KafkaSourceOptions.forTopic(c, "transaction-metadata").startingOffsets).isEqualTo("earliest")
  }
}
