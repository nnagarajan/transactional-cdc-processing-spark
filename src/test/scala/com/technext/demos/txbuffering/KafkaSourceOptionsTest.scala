package com.technext.demos.txbuffering

import com.typesafe.config.{Config, ConfigFactory}
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.Test

import com.technext.demos.txbuffering.model.spec.{InvalidJobConfigException, ModelResolver, TestModels}

class KafkaSourceOptionsTest {

  private val Orders = "dev.appuser.orders.json"
  private val Metadata = "dev.transaction_metadata_json"

  private def config(extra: String): Config =
    ConfigFactory.parseString(
      s"""
         |kafka.starting-offsets = "earliest"
         |$extra
         |""".stripMargin)

  @Test
  def noOverrideFallsBackToSharedDefault(): Unit = {
    val o = KafkaSourceOptions.forTopic(config(""), "orders", Orders)
    assertThat(o.startingOffsets).isEqualTo("earliest")
    assertThat(o.maxOffsetsPerTrigger.isEmpty).isTrue
  }

  @Test
  def latestOverride(): Unit = {
    val o = KafkaSourceOptions.forTopic(config("""job.kafka.starting-offsets.orders = "LATEST""""), "orders", Orders)
    assertThat(o.startingOffsets).isEqualTo("latest")
  }

  @Test
  def partitionMapIsWrappedWithTheTopicName(): Unit = {
    val c = config("""job.kafka.starting-offsets.orders = "{\"0\":12345,\"1\":0}"""")
    assertThat(KafkaSourceOptions.forTopic(c, "orders", Orders).startingOffsets)
      .isEqualTo("""{"dev.appuser.orders.json":{"0":12345,"1":0}}""")
    // The override applies only to its own topic.
    assertThat(KafkaSourceOptions.forTopic(c, "transaction-metadata", Metadata).startingOffsets).isEqualTo("earliest")
  }

  @Test
  def nonNumericOffsetIsRejectedNamingTheTopicKey(): Unit =
    assertThatThrownBy(() =>
      KafkaSourceOptions.forTopic(config("""job.kafka.starting-offsets.orders = "{\"0\":\"abc\"}""""), "orders", Orders))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessageContaining("'orders'")
      .hasMessageContaining("""{"0":"abc"}""")

  @Test
  def negativeOffsetIsRejected(): Unit =
    assertThatThrownBy(() =>
      KafkaSourceOptions.forTopic(config("""job.kafka.starting-offsets.orders = "{\"0\":-2}""""), "orders", Orders))
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
    val o = KafkaSourceOptions.forTopic(config("job.kafka.max-offsets-per-trigger = 50000"), "orders", Orders)
    assertThat(o.asMap("maxOffsetsPerTrigger")).isEqualTo("50000")
  }

  @Test
  def noneOmitsTheLimit(): Unit = {
    val o = KafkaSourceOptions.forTopic(config("""job.kafka.max-offsets-per-trigger = "none""""), "orders", Orders)
    assertThat(o.asMap.contains("maxOffsetsPerTrigger")).isFalse
  }

  @Test
  def invalidLimitIsRejected(): Unit =
    assertThatThrownBy(() => KafkaSourceOptions.forTopic(config("job.kafka.max-offsets-per-trigger = 0"), "orders", Orders))
      .isInstanceOf(classOf[IllegalArgumentException])

  @Test
  def runbookHoconFormParses(): Unit = {
    val c = config("job.kafka.starting-offsets {\n  orders = \"\"\"{\"0\":113}\"\"\"\n  transaction-metadata = \"earliest\"\n}")
    assertThat(KafkaSourceOptions.forTopic(c, "orders", Orders).startingOffsets)
      .isEqualTo("""{"dev.appuser.orders.json":{"0":113}}""")
    assertThat(KafkaSourceOptions.forTopic(c, "transaction-metadata", Metadata).startingOffsets).isEqualTo("earliest")
  }

  @Test
  def ordersJobSourcesAreItsEntitiesPlusTheMetadataTopic(): Unit = {
    val config = TestModels.ordersConfig
    val sources = KafkaSourceOptions.forJob(config, ModelResolver.load(config))
    assertThat(sources.map(s => s"${s._1}=${s._2}").toArray).containsExactly(
      "orders=dev.appuser.orders.json",
      "orderDetails=dev.appuser.order_details.json",
      "lineItems=dev.appuser.order_line_items.json",
      "transaction-metadata=dev.transaction_metadata_json")
  }

  @Test
  def entityKeyedOverrideApplies(): Unit = {
    val config = TestModels.config(TestModels.SingleTable + "job.kafka.starting-offsets.customers = latest\n")
    val sources = KafkaSourceOptions.forJob(config, ModelResolver.load(config)).map(s => s._1 -> s._3.startingOffsets).toMap
    assertThat(sources("customers")).isEqualTo("latest")
    assertThat(sources("transaction-metadata")).isEqualTo("earliest")
  }

  @Test
  def unknownSourceKeyFailsNamingIt(): Unit =
    assertThatThrownBy(() => ModelResolver.load(TestModels.config(
      TestModels.SingleTable + "job.kafka.starting-offsets.order-line-items = latest\n")))
      .isInstanceOf(classOf[InvalidJobConfigException])
      .hasMessageContaining("job.kafka.starting-offsets.order-line-items")
}
