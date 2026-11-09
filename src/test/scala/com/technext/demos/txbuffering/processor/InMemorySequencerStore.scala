package com.technext.demos.txbuffering.processor

import scala.collection.mutable

/** In-memory [[SequencerStore]] for sequencer unit tests. */
class InMemorySequencerStore extends SequencerStore {
  val buffers: mutable.Map[String, TransactionState] = mutable.Map()
  val queue: mutable.Map[Long, String] = mutable.Map()
  private var cursor: Cursor = new Cursor()

  override def getBuffer(txKey: String): Option[TransactionState] = buffers.get(txKey)
  override def putBuffer(txKey: String, state: TransactionState): Unit = buffers(txKey) = state
  override def removeBuffer(txKey: String): Unit = buffers.remove(txKey)
  override def bufferKeys: Iterator[String] = buffers.keys.toList.iterator

  override def queueGet(seq: Long): Option[String] = queue.get(seq)
  override def queuePut(seq: Long, txKey: String): Unit = queue(seq) = txKey
  override def queueRemove(seq: Long): Unit = queue.remove(seq)

  override def getCursor: Cursor = cursor
  override def putCursor(c: Cursor): Unit = cursor = c
}
