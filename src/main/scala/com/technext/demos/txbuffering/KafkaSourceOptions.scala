/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config

/**
 * Builds the Spark Kafka source options for one topic.
 *
 * Starting position (applies only when the query starts on a new checkpoint):
 *   - `job.kafka.starting-offsets.<topic-key>` if set, otherwise `kafka.starting-offsets`
 *   - value: `earliest`, `latest`, or a JSON map of partition to offset, such as `{"0":12345}`.
 *     A partition map is wrapped as `{"<topic>":{...}}`, so the topic name is never typed by hand.
 *
 * Batch size: `job.kafka.max-offsets-per-trigger`, a positive number, or `none` for no limit.
 */
object KafkaSourceOptions {

  final case class SourceOptions(startingOffsets: String, maxOffsetsPerTrigger: Option[Long]) {
    def asMap: Map[String, String] =
      Map("startingOffsets" -> startingOffsets) ++
        maxOffsetsPerTrigger.map(n => "maxOffsetsPerTrigger" -> n.toString)
  }

  private val SharedStartingOffsetsPath = "kafka.starting-offsets"
  private val TopicStartingOffsetsPath = "job.kafka.starting-offsets"
  private val MaxOffsetsPath = "job.kafka.max-offsets-per-trigger"

  private val mapper = new ObjectMapper()

  /** Options for the topic configured at `job.kafka.topics.<topicKey>`. */
  def forTopic(config: Config, topicKey: String): SourceOptions = {
    val topic = config.getString(s"job.kafka.topics.$topicKey")
    val overridePath = s"$TopicStartingOffsetsPath.$topicKey"
    val (source, value) =
      if (config.hasPath(overridePath)) (overridePath, config.getString(overridePath))
      else (SharedStartingOffsetsPath, config.getString(SharedStartingOffsetsPath))
    SourceOptions(startingOffsets(value, topic, s"$source (topic key '$topicKey')"), maxOffsetsPerTrigger(config))
  }

  /** Validates a starting position and returns it in Spark's `startingOffsets` form. */
  def startingOffsets(value: String, topic: String, source: String): String = {
    val v = value.trim
    if (v.equalsIgnoreCase("earliest") || v.equalsIgnoreCase("latest")) return v.toLowerCase

    def invalid(reason: String) = new IllegalArgumentException(
      s"Invalid starting position for $source: '$value'. $reason. " +
        """Expected "earliest", "latest", or a JSON map of partition to offset such as {"0":12345}.""")

    val node = Try(mapper.readTree(v)).getOrElse(throw invalid("Not valid JSON"))
    if (node == null || !node.isObject || node.isEmpty) throw invalid("Not a non-empty JSON object")

    val partitions = mapper.createObjectNode()
    node.properties().asScala.foreach { e =>
      val partition = e.getKey
      if (!partition.forall(_.isDigit) || Try(partition.toInt).isFailure)
        throw invalid(s"Partition '$partition' is not a non-negative integer")
      val offset = e.getValue
      if (!offset.isIntegralNumber || !offset.canConvertToLong || offset.asLong() < 0)
        throw invalid(s"Offset for partition $partition is not a non-negative integer")
      partitions.put(partition, offset.asLong())
    }
    val wrapped = mapper.createObjectNode()
    wrapped.set(topic, partitions)
    mapper.writeValueAsString(wrapped)
  }

  /** The per-batch offset limit for each source, or None when set to `none`. */
  def maxOffsetsPerTrigger(config: Config): Option[Long] = {
    if (!config.hasPath(MaxOffsetsPath)) return None
    val raw = config.getString(MaxOffsetsPath).trim
    if (raw.equalsIgnoreCase("none")) None
    else Try(raw.toLong).toOption.filter(_ > 0).map(Some(_)).getOrElse(
      throw new IllegalArgumentException(s"Invalid $MaxOffsetsPath: '$raw'. Expected a positive number or \"none\"."))
  }
}
