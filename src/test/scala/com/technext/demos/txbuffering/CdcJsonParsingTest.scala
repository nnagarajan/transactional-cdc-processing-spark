package com.technext.demos.txbuffering

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.jdk.CollectionConverters._

import com.technext.demos.txbuffering.processor.{CdcRows, MarkerInput}

/**
 * Parsing of the transaction metadata topic, using messages in the exact shapes produced by
 * gg-formatter's TransactionBoundaryEventJsonFormatter: a JSON object for one source transaction,
 * a JSON array of markers when GROUPTRANSOPS delivered several source transactions together.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CdcJsonParsingTest {

  private var spark: SparkSession = _

  @BeforeAll
  def startSpark(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("cdc-json-parsing-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("parsing-warehouse").toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  @AfterAll
  def stopSpark(): Unit = if (spark != null) spark.stop()

  // Real marker from dev.transaction_metadata_json (offset 0), as published today.
  private val singleMarker =
    """{"xid":"1342848513.8.14.5716","csn":"344374437","tx_ts":"2026-11-07 21:24:36.000000","event_count":550,""" +
      """"data_collections":[{"data_collection":"ORDER_DETAILS","event_count":100},""" +
      """{"data_collection":"ORDERS","event_count":100},{"data_collection":"ORDER_LINE_ITEMS","event_count":350}]}"""

  // Three source transactions grouped into one Replicat transaction.
  private val groupedMarkers =
    """[{"xid":"X1","csn":"500","tx_ts":"2026-11-07 21:24:36.000000","event_count":3,""" +
      """"data_collections":[{"data_collection":"ORDERS","event_count":1},{"data_collection":"ORDER_LINE_ITEMS","event_count":2}]},""" +
      """{"xid":"X2","csn":"600","tx_ts":"2026-11-07 21:24:37.000000","event_count":2,""" +
      """"data_collections":[{"data_collection":"ORDERS","event_count":1},{"data_collection":"ORDER_DETAILS","event_count":1}]},""" +
      """{"xid":"X3","csn":"700","tx_ts":"2026-11-07 21:24:38.000000","event_count":3,""" +
      """"data_collections":[{"data_collection":"ORDER_LINE_ITEMS","event_count":3}]}]"""

  private def parse(messages: (String, Long)*): Seq[MarkerInput] = {
    val session = spark
    import session.implicits._
    val raw = messages.map { case (json, offset) => (json.getBytes("UTF-8"), 0, offset) }
      .toDF("value", "partition", "offset")
    CdcJsonParsing.parseTransactionMetadata(raw).collect().toSeq
      .map(CdcRows.toSequencerInput(_).asInstanceOf[MarkerInput])
  }

  private def counts(m: MarkerInput): Map[String, Int] =
    m.metadata.dataCollections.asScala.map(dc => dc.dataCollection -> dc.eventCount).toMap

  @Test
  def singleObjectMessageIsOneMarker(): Unit = {
    val markers = parse(singleMarker -> 0L)

    assertThat(markers.size).isEqualTo(1)
    val m = markers.head
    assertThat(m.metadata.xid).isEqualTo("1342848513.8.14.5716")
    assertThat(m.metadata.csn).isEqualTo("344374437")
    assertThat(m.offset).isEqualTo(0L)
    assertThat(m.index).isEqualTo(0)
    assertThat(counts(m).asJava).isEqualTo(
      Map("ORDERS" -> 100, "ORDER_DETAILS" -> 100, "ORDER_LINE_ITEMS" -> 350).asJava)
  }

  @Test
  def arrayMessageIsOneMarkerPerSourceTransactionInOrder(): Unit = {
    val markers = parse(groupedMarkers -> 7L).sortBy(_.index)

    assertThat(markers.map(m => s"${m.metadata.xid}:${m.metadata.csn}").toArray)
      .containsExactly("X1:500", "X2:600", "X3:700")
    assertThat(markers.map(_.index).toArray).containsExactly(0, 1, 2)
    assertThat(markers.map(_.offset).distinct.toArray).containsExactly(7L)
    assertThat(markers.map(_.metadata.eventCount).toArray).containsExactly(3, 2, 3)
    assertThat(counts(markers(0)).asJava).isEqualTo(Map("ORDERS" -> 1, "ORDER_LINE_ITEMS" -> 2).asJava)
    assertThat(counts(markers(1)).asJava).isEqualTo(Map("ORDERS" -> 1, "ORDER_DETAILS" -> 1).asJava)
    assertThat(counts(markers(2)).asJava).isEqualTo(Map("ORDER_LINE_ITEMS" -> 3).asJava)
  }

  @Test
  def singleAndGroupedMessagesMix(): Unit = {
    val markers = parse(singleMarker -> 0L, groupedMarkers -> 1L)
      .sortBy(m => (m.offset, m.index))

    assertThat(markers.map(m => (m.offset, m.index, m.metadata.xid)).map(_.toString).toArray).containsExactly(
      "(0,0,1342848513.8.14.5716)", "(1,0,X1)", "(1,1,X2)", "(1,2,X3)")
  }
}
