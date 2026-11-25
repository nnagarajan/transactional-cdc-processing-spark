package com.technext.demos.txbuffering.processor

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable

import com.technext.demos.txbuffering.model.generic.DataChangeEvent
import com.technext.demos.txbuffering.processor.TransactionSequencer.{eventKey, positionKey}

/**
 * In-memory [[SequencerStore]] for sequencer unit tests.
 *
 * Like Spark state, it stores copies: a value changed after `get` and never `put` back is not
 * changed in the store, so a missing write fails a test instead of passing by accident. It also
 * counts every operation by name (see [[count]]), so tests can assert how much state work a
 * step did.
 */
class InMemorySequencerStore extends SequencerStore {
  val summaries: mutable.Map[String, TransactionSummary] = mutable.Map()
  val events: mutable.Map[String, DataChangeEvent] = mutable.Map()
  val positions: mutable.Map[String, Long] = mutable.Map()
  val queue: mutable.Map[Long, String] = mutable.Map()
  private var cursor: Cursor = new Cursor()

  private val ops: mutable.Map[String, Long] = mutable.Map().withDefaultValue(0L)

  /** Buffered transactions by `xid:csn` (their summaries). */
  def buffers: collection.Map[String, TransactionSummary] = summaries

  /** How many times the named operation (a method name of [[SequencerStore]]) was called. */
  def count(op: String): Long = ops(op)

  /** Total writes and removals across all state, including the queue and the cursor. */
  def writes: Long = Seq("putSummary", "removeSummary", "putEvent", "removeEvent", "putPosition",
    "removePosition", "queuePut", "queueRemove", "putCursor").map(ops).sum

  def resetCounts(): Unit = ops.clear()

  /** Puts a summary straight into the store, bypassing counters and copies (test setup only). */
  def seed(txKey: String, summary: TransactionSummary): Unit = summaries(txKey) = summary

  private def tick(op: String): Unit = ops(op) += 1

  override def getSummary(txKey: String): Option[TransactionSummary] = { tick("getSummary"); summaries.get(txKey).map(copy) }
  override def putSummary(txKey: String, summary: TransactionSummary): Unit = { tick("putSummary"); summaries(txKey) = copy(summary) }
  override def removeSummary(txKey: String): Unit = { tick("removeSummary"); summaries.remove(txKey) }
  override def summaryKeys: Iterator[String] = summaries.keys.toList.iterator

  override def getEvent(txKey: String, index: Int): Option[DataChangeEvent] = {
    tick("getEvent"); events.get(eventKey(txKey, index)).map(copy)
  }
  override def putEvent(txKey: String, index: Int, event: DataChangeEvent): Unit = {
    tick("putEvent"); events(eventKey(txKey, index)) = copy(event)
  }
  override def removeEvent(txKey: String, index: Int): Unit = { tick("removeEvent"); events.remove(eventKey(txKey, index)) }

  override def positionExists(txKey: String, table: String, pos: String): Boolean = {
    tick("positionExists"); positions.contains(positionKey(txKey, table, pos))
  }
  override def putPosition(txKey: String, table: String, pos: String, index: Int): Unit = {
    tick("putPosition"); positions(positionKey(txKey, table, pos)) = index.toLong
  }
  override def removePosition(txKey: String, table: String, pos: String): Unit = {
    tick("removePosition"); positions.remove(positionKey(txKey, table, pos))
  }

  override def queueGet(seq: Long): Option[String] = { tick("queueGet"); queue.get(seq) }
  override def queuePut(seq: Long, txKey: String): Unit = { tick("queuePut"); queue(seq) = txKey }
  override def queueRemove(seq: Long): Unit = { tick("queueRemove"); queue.remove(seq) }

  override def getCursor: Cursor = { tick("getCursor"); copy(cursor) }
  override def putCursor(c: Cursor): Unit = { tick("putCursor"); cursor = copy(c) }

  private def copy[T <: java.io.Serializable](value: T): T = {
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    out.writeObject(value)
    out.close()
    new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray)).readObject().asInstanceOf[T]
  }
}
