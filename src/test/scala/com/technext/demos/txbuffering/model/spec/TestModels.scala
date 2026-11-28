package com.technext.demos.txbuffering.model.spec

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

/** Job configs used by the model tests. */
object TestModels {

  /** `jobs/orders.conf` as the jobs load it, layered over the shared defaults. */
  def ordersConfig: Config =
    ConfigFactory.parseFile(new File("jobs/orders.conf")).withFallback(ConfigFactory.defaultReference()).resolve()

  def ordersModel: JobModel = ModelResolver.load(ordersConfig)

  /** The orders job with a derived `totalWithTax` order column and the given transform. */
  def ordersWithDerivedColumn(transform: String): JobModel = {
    val text = new String(java.nio.file.Files.readAllBytes(new File("jobs/orders.conf").toPath))
      .replace("{ source = CREATED_TS,          name = createdTs,         type = string }",
        "{ source = CREATED_TS, name = createdTs, type = string }\n        { name = totalWithTax, type = double }")
    ModelResolver.load(config(text + s"\njob.output.transform = $transform\n"))
  }

  /**
   * Runs a DDL script the way SeedJob does, with `{{CATALOG_SCHEMA}}` replaced by `schema`, on a
   * local Delta catalog (whose tables are not catalog-managed).
   */
  def runDdl(spark: SparkSession, script: String, schema: String): Unit =
    script.replace(Ddl.Placeholder, schema)
      .replace("TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')", "")
      .split(";").map(_.linesIterator.filterNot(_.trim.startsWith("--")).mkString("\n").trim)
      .filter(_.nonEmpty).foreach(spark.sql)

  /** A job config from HOCON text, over the shared defaults. */
  def config(hocon: String): Config =
    ConfigFactory.parseString(hocon).withFallback(ConfigFactory.defaultReference()).resolve()

  def jobConfig(hocon: String): JobConfig = JobConfig.parse(config(hocon))

  def resolve(hocon: String): Either[Seq[String], JobModel] = ModelResolver.resolveEither(jobConfig(hocon))

  val SingleTable: String =
    """job.name = customers
      |job.model.entities = [
      |  { name = customers, role = root, table = CUSTOMERS, topic = "dev.appuser.customers.json"
      |    key = CUSTOMER_ID
      |    columns = [
      |      { source = CUSTOMER_ID, name = customerId, type = bigint }
      |      { source = NAME,        name = name,       type = string }
      |    ] }
      |]
      |job.output.stream.table = "argus1.customers_stream"
      |job.output.current.table = "argus1.customers_current"
      |""".stripMargin

  /** Root key (REGION, ORDER_NO); shipments holds it as (SHIP_REGION, ORD_REF) and has no version. */
  val Compound: String =
    """job.name = regional
      |job.model.entities = [
      |  { name = orders, role = root, table = ORDERS, topic = "dev.appuser.orders.json"
      |    key = [REGION, ORDER_NO], version = VERSION
      |    columns = [
      |      { source = REGION,   name = region,  type = string }
      |      { source = ORDER_NO, name = orderNo, type = bigint }
      |      { source = VERSION,  name = version, type = bigint }
      |      { source = STATUS,   name = status,  type = string }
      |    ] }
      |  { name = shipments, table = SHIPMENTS, topic = "dev.appuser.shipments.json"
      |    cardinality = many
      |    root-key { ORDER_NO = ORD_REF, REGION = SHIP_REGION }
      |    key = [SHIP_REGION, ORD_REF, SHIPMENT_SEQ]
      |    columns = [
      |      { source = SHIP_REGION,  name = shipRegion,  type = string }
      |      { source = ORD_REF,      name = ordRef,      type = string }
      |      { source = SHIPMENT_SEQ, name = shipmentSeq, type = int }
      |      { source = CARRIER,      name = carrier,     type = string }
      |    ] }
      |]
      |job.output.stream.table = "argus1.regional_stream"
      |job.output.current.table = "argus1.regional_current"
      |""".stripMargin
}
