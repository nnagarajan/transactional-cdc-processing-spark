package com.technext.demos.txbuffering.model.spec

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import com.technext.demos.txbuffering.GoldenOutputTest
import com.technext.demos.txbuffering.model.spec.TestModels._

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemasTest {

  private var spark: SparkSession = _

  @BeforeAll
  def startSpark(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("schemas-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("schemas-warehouse").toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE SCHEMA IF NOT EXISTS ddl")
  }

  @AfterAll
  def stopSpark(): Unit = if (spark != null) spark.stop()

  // ---- 2.5 derivation ----

  @Test
  def ordersStreamSchemaEqualsTheExistingTable(): Unit =
    assertThat(Schemas.differences(GoldenOutputTest.goldenSchema("order_stream.schema.json"),
      Schemas.streamSchema(ordersModel)).asJava).isEmpty()

  @Test
  def ordersCurrentSchemaEqualsTheExistingTable(): Unit =
    assertThat(Schemas.differences(GoldenOutputTest.goldenSchema("orders_current.schema.json"),
      Schemas.currentSchema(ordersModel)).asJava).isEmpty()

  @Test
  def ordersModelHasNoChangeCsn(): Unit =
    assertThat(Schemas.currentSchema(ordersModel).treeString).doesNotContain(ModelResolver.ChangeCsn)

  @Test
  def versionlessChildGetsChangeCsn(): Unit = {
    val current = Schemas.currentSchema(resolve(Compound).toOption.get)
    val element = current("shipments").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    assertThat(element.fieldNames.toSeq.asJava)
      .containsExactly("shipRegion", "ordRef", "shipmentSeq", "carrier", "changeCsn", "before")
    assertThat(element("changeCsn").dataType).isEqualTo(StringType)
    // The root has a version, so no changeCsn at top level
    assertThat(current.fieldNames.toSeq.asJava).doesNotContain("changeCsn")
  }

  @Test
  def versionlessRootGetsTopLevelChangeCsn(): Unit = {
    val current = Schemas.currentSchema(resolve(SingleTable).toOption.get)
    assertThat(current.fieldNames.toSeq.asJava)
      .containsExactly("xid", "csn", "dwhProcessedTs", "customerId", "name", "changeCsn", "customersBefore")
  }

  @Test
  def compoundRootKeyBecomesTopLevelStreamColumns(): Unit = {
    val stream = Schemas.streamSchema(resolve(Compound).toOption.get)
    assertThat(stream.fieldNames.toSeq.asJava)
      .containsExactly("xid", "csn", "dwhProcessedTs", "region", "orderNo", "orders", "shipments")
    assertThat(stream("orderNo").dataType.sql).isEqualTo("BIGINT")
  }

  @Test
  def differencesNameEachPath(): Unit = {
    val golden = GoldenOutputTest.goldenSchema("order_stream.schema.json")
    val changed = StructType(golden.fields.filterNot(_.name == "orderId"))
    assertThat(Schemas.differences(golden, changed).asJava).containsExactly("orderId: missing")
  }

  // ---- 2.6 DDL ----

  @Test
  def ordersDdlCreatesTheExistingSchemas(): Unit = {
    val m = ordersModel
    runDdl(spark, Ddl.streamDdl(m), "ddl")
    runDdl(spark, Ddl.currentDdl(m), "ddl")
    assertThat(Schemas.differences(GoldenOutputTest.goldenSchema("order_stream.schema.json"),
      spark.table("ddl.order_stream").schema).asJava).isEmpty()
    assertThat(Schemas.differences(GoldenOutputTest.goldenSchema("orders_current.schema.json"),
      spark.table("ddl.orders_current").schema).asJava).isEmpty()
  }

  @Test
  def ddlHasPlaceholderAndNoLocation(): Unit = {
    val ddl = Ddl.streamDdl(ordersModel)
    assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS {{CATALOG_SCHEMA}}.order_stream (")
    assertThat(ddl.toUpperCase).doesNotContain("LOCATION")
  }
}
