package com.technext.demos.txbuffering

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.{AfterEach, Test}

import com.technext.demos.txbuffering.model.spec.{ModelResolver, Schemas, TestModels}

class AppConfigTest {

  private val props = Seq("config.file", "job.output.stream.table", "job.kafka.bootstrap-servers")

  @AfterEach
  def clearProperties(): Unit = props.foreach(System.clearProperty)

  @Test
  def bundledOrdersJobIsUsedWithoutConfigFile(): Unit = {
    val model = ModelResolver.load(AppConfig.loadJob())
    assertThat(model.name).isEqualTo("orders")
    // The real loader yields the existing table schemas (task 3.1)
    assertThat(Schemas.differences(GoldenOutputTest.goldenSchema("order_stream.schema.json"),
      Schemas.streamSchema(model)).asJava).isEmpty()
    assertThat(Schemas.differences(GoldenOutputTest.goldenSchema("orders_current.schema.json"),
      Schemas.currentSchema(model)).asJava).isEmpty()
  }

  @Test
  def sharedDefaultsAreLayeredUnderTheJob(): Unit = {
    val c = AppConfig.loadJob()
    assertThat(c.getString("job.kafka.metadata-topic")).isEqualTo("dev.transaction_metadata_json")
    assertThat(c.getString("job.kafka.bootstrap-servers")).isEqualTo("localhost:9092")
    assertThat(c.getString("spark.unity-catalog.catalog-name")).isEqualTo("mymanaged")
  }

  @Test
  def systemPropertyWins(): Unit = {
    System.setProperty("job.output.stream.table", "argus1.other_stream")
    System.setProperty("job.kafka.bootstrap-servers", "kafka:9092")
    val c = AppConfig.loadJob()
    assertThat(ModelResolver.load(c).streamTable).isEqualTo("argus1.other_stream")
    assertThat(c.getString("job.kafka.bootstrap-servers")).isEqualTo("kafka:9092")
  }

  @Test
  def configFileSelectsTheJob(): Unit = {
    val f = Files.createTempFile("customers", ".conf")
    Files.writeString(f, TestModels.SingleTable)
    System.setProperty("config.file", f.toString)
    val model = ModelResolver.load(AppConfig.loadJob())
    assertThat(model.name).isEqualTo("customers")
    assertThat(model.streamCheckpoint).isEqualTo("/tmp/spark-checkpoints/customers/stream")
  }

  @Test
  def missingConfigFileFails(): Unit = {
    System.setProperty("config.file", "/no/such/job.conf")
    assertThatThrownBy(() => AppConfig.loadJob()).hasMessageContaining("/no/such/job.conf")
  }
}
