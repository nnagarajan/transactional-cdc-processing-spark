package com.technext.demos.txbuffering.model.spec

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.{DecimalType, DoubleType, StringType, TimestampType}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import com.technext.demos.txbuffering.model.spec.TestModels._

class ModelResolverTest {

  private def errorsOf(hocon: String): Seq[String] = resolve(hocon) match {
    case Left(e) => e
    case Right(_) => throw new AssertionError("expected the config to be rejected")
  }

  private def assertRejected(hocon: String, expected: String*): Unit = {
    val errors = errorsOf(hocon).mkString("\n")
    expected.foreach(e => assertThat(errors).contains(e))
  }

  /** The single-table job with `edit` applied to its text. */
  private def single(edit: String => String): String = edit(SingleTable)

  // ---- 2.1 parsing ----

  @Test
  def parsesTheOrdersJobConfig(): Unit = {
    val c = JobConfig.parse(ordersConfig)
    assertThat(c.parseErrors.asJava).isEmpty()
    assertThat(c.name.get).isEqualTo("orders")
    assertThat(c.entities.flatMap(_.name).asJava).containsExactly("orders", "orderDetails", "lineItems")
    val items = c.entities(2)
    assertThat(items.key.asJava).containsExactly("LINE_ITEM_ID")
    assertThat(items.rootKey.get).isEqualTo(RootKeyConfig.Single("ORDER_ID"))
    assertThat(items.columns.head).isEqualTo(ColumnConfig(Some("LINE_ITEM_ID"), Some("lineItemId"), Some("double")))
    assertThat(c.output.rootBefore.get).isEqualTo("orderBefore")
  }

  @Test
  def parsesADerivedColumn(): Unit = {
    val c = jobConfig(single(_.replace(
      "{ source = NAME,        name = name,       type = string }",
      "{ source = NAME, name = name, type = string }\n      { name = nameUpper, type = string }")))
    assertThat(c.entities.head.columns.last).isEqualTo(ColumnConfig(None, Some("nameUpper"), Some("string")))
  }

  @Test
  def parsesCompoundKeys(): Unit = {
    val c = jobConfig(Compound)
    assertThat(c.parseErrors.asJava).isEmpty()
    assertThat(c.entities.head.key.asJava).containsExactly("REGION", "ORDER_NO")
    assertThat(c.entities(1).rootKey.get)
      .isEqualTo(RootKeyConfig.Mapping(Map("REGION" -> "SHIP_REGION", "ORDER_NO" -> "ORD_REF")))
    assertThat(c.entities(1).key.asJava).containsExactly("SHIP_REGION", "ORD_REF", "SHIPMENT_SEQ")
  }

  // ---- 2.2 types and checkpoints ----

  @Test
  def parsesSparkSqlTypes(): Unit = {
    assertThat(ModelResolver.parseType("double").get).isEqualTo(DoubleType)
    assertThat(ModelResolver.parseType("string").get).isEqualTo(StringType)
    assertThat(ModelResolver.parseType("decimal(20,4)").get).isEqualTo(DecimalType(20, 4))
    assertThat(ModelResolver.parseType("timestamp").get).isEqualTo(TimestampType)
    assertThat(ModelResolver.parseType("dubble").isEmpty).isTrue
  }

  @Test
  def invalidTypeNamesTheColumn(): Unit =
    assertRejected(single(_.replace("type = bigint", "type = dubble")),
      "customers.customerId: 'dubble' is not a valid Spark SQL type")

  @Test
  def checkpointsDefaultPerJobAndStage(): Unit = {
    val orders = ordersModel
    val customers = resolve(SingleTable).toOption.get
    assertThat(orders.streamCheckpoint).isEqualTo("/tmp/spark-checkpoints/orders/stream")
    assertThat(orders.currentCheckpoint).isEqualTo("/tmp/spark-checkpoints/orders/current")
    assertThat(customers.streamCheckpoint).isEqualTo("/tmp/spark-checkpoints/customers/stream")
  }

  @Test
  def checkpointOverrideWins(): Unit = {
    val m = resolve(SingleTable + "job.output.stream.checkpoint = /data/ckpt\n").toOption.get
    assertThat(m.streamCheckpoint).isEqualTo("/data/ckpt")
    assertThat(m.currentCheckpoint).isEqualTo("/tmp/spark-checkpoints/customers/current")
  }

  // ---- 2.3 resolution ----

  @Test
  def buildsTheOrdersModel(): Unit = {
    val m = ordersModel
    assertThat(m.trackedTables.asJava).containsExactly("ORDERS", "ORDER_DETAILS", "ORDER_LINE_ITEMS")
    assertThat(m.root.name).isEqualTo("orders")
    assertThat(m.root.ownKey.asJava).containsExactly("ORDER_ID")
    assertThat(m.rootKeyColumns.map(_.name).asJava).containsExactly("orderId")
    val details = m.entities(1)
    assertThat(details.cardinality.get).isEqualTo(Cardinality.One)
    assertThat(details.ownKey.asJava).isEmpty()
    assertThat(details.rootKey.asJava).containsExactly("ORDER_ID" -> "ORDER_ID")
    val items = m.entities(2)
    assertThat(items.cardinality.get).isEqualTo(Cardinality.Many)
    assertThat(items.ownKey.asJava).containsExactly("LINE_ITEM_ID")
    assertThat(items.version.get).isEqualTo("VERSION")
    assertThat(items.before).isTrue
    assertThat(m.rootBefore).isEqualTo("orderBefore")
    assertThat(m.streamTable).isEqualTo("mymanaged.argus1.order_stream")
  }

  @Test
  def buildsASingleTableModel(): Unit = {
    val m = resolve(SingleTable).toOption.get
    assertThat(m.entities.size).isEqualTo(1)
    assertThat(m.children.asJava).isEmpty()
    assertThat(m.root.version.isEmpty).isTrue
    assertThat(m.rootBefore).isEqualTo("customersBefore")
  }

  @Test
  def buildsACompoundKeyModel(): Unit = {
    val m = resolve(Compound).toOption.get
    assertThat(m.rootKeyColumns.map(_.name).asJava).containsExactly("region", "orderNo")
    val shipments = m.entities(1)
    // Mapped in root key order, whatever the order in the config
    assertThat(shipments.rootKey.asJava).containsExactly("REGION" -> "SHIP_REGION", "ORDER_NO" -> "ORD_REF")
    assertThat(shipments.ownKey.asJava).containsExactly("SHIP_REGION", "ORD_REF", "SHIPMENT_SEQ")
  }

  @Test
  def beforeImageCanBeTurnedOff(): Unit = {
    val m = resolve(single(_.replace("key = CUSTOMER_ID", "key = CUSTOMER_ID, before = false"))).toOption.get
    assertThat(m.root.before).isFalse
  }

  // ---- 2.4 validations ----

  @Test
  def noRoot(): Unit = assertRejected(single(_.replace("role = root, ", "")), "exactly one entity must have role = root, found 0")

  @Test
  def twoRoots(): Unit =
    assertRejected(Compound.replace("{ name = shipments,", "{ name = shipments, role = root,"),
      "exactly one entity must have role = root, found 2 (orders, shipments)")

  @Test
  def missingEntityFields(): Unit = {
    assertRejected(single(_.replace("table = CUSTOMERS, ", "")), "customers: entity has no table")
    assertRejected(single(_.replace("""topic = "dev.appuser.customers.json"""", "")), "customers: entity has no topic")
    assertRejected(single(_.replace("name = customers, ", "")), "entity #1: entity has no name")
    assertRejected(
      """job.name = x
        |job.model.entities = [ { name = a, role = root, table = A, topic = t, key = ID, columns = [] } ]
        |job.output.stream.table = s
        |job.output.current.table = c""".stripMargin, "a: entity has no columns")
  }

  @Test
  def missingColumnNameOrType(): Unit = {
    assertRejected(single(_.replace("name = name,       type = string", "type = string")), "customers.NAME: column has no name")
    assertRejected(single(_.replace("name = name,       type = string", "name = name")), "customers.name: column has no type")
  }

  @Test
  def duplicateEntityNamesAndTables(): Unit = {
    val dup = Compound.replace("{ name = shipments, table = SHIPMENTS", "{ name = orders, table = ORDERS")
    assertRejected(dup, "two entities are named orders", "two entities read table ORDERS")
  }

  @Test
  def duplicateColumnNamesAndSources(): Unit = {
    assertRejected(single(_.replace("name = name,", "name = customerId,")), "customers: two columns are named customerId")
    assertRejected(single(_.replace("source = NAME,", "source = CUSTOMER_ID,")), "customers: two columns have source CUSTOMER_ID")
  }

  @Test
  def keyNamesAnUndeclaredColumn(): Unit =
    assertRejected(Compound.replace("SHIPMENT_SEQ]", "SHIPMENT_SEQQ]"),
      "shipments: key names SHIPMENT_SEQQ, which is not the source of any of its columns")

  @Test
  def versionNamesAnUndeclaredColumn(): Unit =
    assertRejected(Compound.replace("version = VERSION", "version = VERSIONN"),
      "orders: version names VERSIONN")

  @Test
  def rootKeyNamesAnUndeclaredColumn(): Unit =
    assertRejected(Compound.replace("REGION = SHIP_REGION }", "REGION = SHIP_REGIONN }"),
      "shipments: root-key names SHIP_REGIONN")

  @Test
  def keyListsAColumnTwice(): Unit =
    assertRejected(Compound.replace("key = [REGION, ORDER_NO]", "key = [REGION, REGION]"), "orders: key lists REGION twice")

  @Test
  def rootWithoutKey(): Unit = assertRejected(single(_.replace("key = CUSTOMER_ID", "")), "customers: the root has no key")

  @Test
  def childWithoutRootKey(): Unit =
    assertRejected(Compound.replace("root-key { ORDER_NO = ORD_REF, REGION = SHIP_REGION }", ""), "shipments: child has no root-key")

  @Test
  def childWithoutCardinality(): Unit =
    assertRejected(Compound.replace("cardinality = many", ""), "shipments: child has no cardinality (one or many)")

  @Test
  def manyChildWithoutKey(): Unit =
    assertRejected(Compound.replace("key = [SHIP_REGION, ORD_REF, SHIPMENT_SEQ]", ""),
      "shipments: a child with cardinality many needs a key")

  @Test
  def rootKeyMapMissesAKeyColumn(): Unit =
    assertRejected(Compound.replace("root-key { ORDER_NO = ORD_REF, REGION = SHIP_REGION }", "root-key { ORDER_NO = ORD_REF }"),
      "shipments: root-key does not map root key column REGION")

  @Test
  def singleRootKeyAgainstCompoundRootKey(): Unit =
    assertRejected(Compound.replace("root-key { ORDER_NO = ORD_REF, REGION = SHIP_REGION }", "root-key = ORD_REF"),
      "shipments: root-key names one column, but the root key has 2 columns")

  @Test
  def derivedColumnWithoutTransform(): Unit =
    assertRejected(single(_.replace("{ source = NAME,", "{ name = nameUpper, type = string }\n      { source = NAME,")),
      "derived columns (customers.nameUpper) need job.output.transform")

  @Test
  def transformClassThatCannotBeLoaded(): Unit =
    assertRejected(SingleTable + "job.output.transform = com.example.NoSuchTransform\n",
      "job.output.transform: class com.example.NoSuchTransform cannot be loaded")

  @Test
  def transformClassOfTheWrongType(): Unit =
    assertRejected(SingleTable + "job.output.transform = java.lang.String\n",
      "job.output.transform: java.lang.String does not implement")

  @Test
  def unknownStartingOffsetsKey(): Unit =
    assertRejected(SingleTable + "job.kafka.starting-offsets.order-details = earliest\n",
      "job.kafka.starting-offsets.order-details: no entity is named order-details")

  @Test
  def metadataStartingOffsetsKeyIsAccepted(): Unit =
    assertThat(resolve(SingleTable + "job.kafka.starting-offsets.transaction-metadata = earliest\n").isRight).isTrue

  @Test
  def missingTables(): Unit =
    assertRejected(single(_.replace("job.output.stream.table = \"argus1.customers_stream\"", "")),
      "job.output.stream.table is missing")

  @Test
  def topLevelNameClash(): Unit =
    assertRejected(single(_.replace("name = name,", "name = csn,")), "current table: two top-level columns are named csn")

  @Test
  def everyProblemIsReported(): Unit = {
    val errors = errorsOf(Compound
      .replace("cardinality = many", "")
      .replace("version = VERSION", "version = VERSIONN")
      .replace("type = int", "type = innt"))
    assertThat(errors.size).isGreaterThanOrEqualTo(3)
  }

  @Test
  def resolveThrowsWithEveryError(): Unit = {
    val e = org.junit.jupiter.api.Assertions.assertThrows(classOf[InvalidJobConfigException],
      () => ModelResolver.resolve(jobConfig(Compound.replace("cardinality = many", "").replace("type = int", "type = innt"))))
    assertThat(e.getMessage).contains("shipments: child has no cardinality").contains("'innt' is not a valid Spark SQL type")
  }
}
