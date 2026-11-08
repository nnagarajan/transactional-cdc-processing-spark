# Debugging the Sequencer State Store

How to read the state of `TransactionalCdcProcessingApp`'s ordering sequencer straight from its
checkpoint: what has been emitted, what is queued, and which transaction is holding up output.

Spark's `statestore` data source reads the state from the checkpoint directory. You can read it
while the app runs, and you see the last committed micro-batch.

## What the Sequencer Stores

`TransactionOrderingProcessor` keeps three state variables, all under a single grouping key `all`
([TransactionOrderingProcessor.scala:35-37](src/main/scala/com/technext/demos/txbuffering/processor/TransactionOrderingProcessor.scala#L35-L37)):

| `stateVarName` | Type | Key → value | Holds |
|---|---|---|---|
| `cursor` | `ValueState[Cursor]` | `all` → `Cursor` | Emission position, last emitted CSN, blocked-since time, timer |
| `queue` | `MapState[Long, String]` | sequence number → `xid:csn` | Transactions whose marker arrived, in marker (commit) order, not yet emitted |
| `buffers` | `MapState[String, TransactionState]` | `xid:csn` → `TransactionState` | Buffered events and expected counts per transaction, not yet emitted |

A transaction is removed from `queue` and `buffers` when it's emitted.

## Before You Start

1. **`read_statestore(...)` isn't available.** It's a Databricks SQL function. Open-source Spark
   4.1.1 doesn't have it. Use the `statestore` data source as shown below.
2. **Run `spark.streams` once per session** before the first state read:

   ```scala
   %spark
   spark.streams   // creates the StateStoreCoordinator on the driver
   ```

   Without it, Spark 4.1 fails with `RpcEndpointNotFoundException: Cannot find endpoint:
   spark://StateStoreCoordinator@...`. The cause: the RocksDB reader reports a snapshot upload to
   the driver's state store coordinator, and that coordinator only exists after the session has
   touched its streaming query manager. A session that ran a streaming query already has it.
3. **Use a `file://` path in Zeppelin.** The Zeppelin Spark interpreter loads
   `/opt/hadoop/hadoop-3.3.4/etc/hadoop/core-site.xml`, which sets
   `fs.defaultFS = hdfs://localhost:9411`. A bare `/tmp/...` path is then looked up in HDFS and
   fails with `Connection refused` (`Call From ... to localhost:9411`). Write
   `file:///tmp/spark-checkpoints-ordered`, or add `spark.hadoop.fs.defaultFS file:///` to the
   note's `%spark.conf` paragraph.
4. **Find the checkpoint path** in `job.output.checkpoint-location` in
   `src/main/resources/transactional-cdc-processing.conf`. The default is
   `/tmp/spark-checkpoints-ordered`. The directory only exists after the app has run.
5. **Match the Spark version.** Read with Spark 4.1.x. The state is stored in RocksDB with Spark
   4.1's format.

## Reading the State

### Scala

```scala
%spark
spark.streams
val ckpt = "file:///tmp/spark-checkpoints-ordered"

// Emission position: last emitted CSN, sequence numbers, blocked-since
spark.read.format("statestore").option("stateVarName", "cursor").load(ckpt).show(false)

// Queue: sequence number -> xid:csn, in emission order
spark.read.format("statestore").option("stateVarName", "queue").load(ckpt)
  .orderBy("user_map_key").show(false)

// Buffered transactions
spark.read.format("statestore").option("stateVarName", "buffers").load(ckpt).show(false)

// Which operators and state stores the checkpoint holds
spark.read.format("state-metadata").load(ckpt).show(false)
```

### SQL

Register a temporary view per state variable. The views last for the session.

```sql
%sql
CREATE OR REPLACE TEMPORARY VIEW seq_cursor USING statestore
OPTIONS (path 'file:///tmp/spark-checkpoints-ordered', stateVarName 'cursor');

CREATE OR REPLACE TEMPORARY VIEW seq_queue USING statestore
OPTIONS (path 'file:///tmp/spark-checkpoints-ordered', stateVarName 'queue');

CREATE OR REPLACE TEMPORARY VIEW seq_buffers USING statestore
OPTIONS (path 'file:///tmp/spark-checkpoints-ordered', stateVarName 'buffers');
```

Run `spark.streams` in a `%spark` paragraph first (see [Before You Start](#before-you-start)).

## Queries

### Cursor: where emission stands

```sql
SELECT value.lastEmittedCsn,
       value.emittedAtLastCsn,
       value.nextSeqToEmit,
       value.nextSeqToAssign,
       value.nextSeqToAssign - value.nextSeqToEmit AS queue_depth,
       value.blockedSinceMs,
       CASE WHEN value.blockedSinceMs > 0
            THEN timestamp_millis(value.blockedSinceMs) END AS blocked_since,
       CASE WHEN value.timerMs > 0
            THEN timestamp_millis(value.timerMs) END AS next_timer
FROM seq_cursor;
```

| Field | Meaning |
|---|---|
| `lastEmittedCsn` | CSN of the most recently emitted transaction. Events and markers below it are dropped as replays |
| `emittedAtLastCsn` | xids already emitted at `lastEmittedCsn`. Several transactions can share a CSN |
| `nextSeqToEmit` | Sequence number of the queue head, the next transaction to emit |
| `nextSeqToAssign` | Sequence number the next marker will get |
| `blockedSinceMs` | Epoch ms when the head started blocking output; `-1` means not blocked |
| `timerMs` | Epoch ms of the registered timer; `-1` means none (queue empty) |

`show()` on the raw `value` struct prints the fields in alphabetical order:
`{blockedSinceMs, emittedAtLastCsn, lastEmittedCsn, nextSeqToAssign, nextSeqToEmit, timerMs}`.

Example, after the 100-order sample transaction from `db/oracle-ddl.sql` was emitted:

```
{-1, [1342848513.8.14.5716], 344374437, 1, 1, -1}
```

So nothing is blocked. Transaction `1342848513.8.14.5716` was emitted at CSN `344374437`.
`nextSeqToEmit = nextSeqToAssign = 1`, so the queue is empty.

### Queue: transactions waiting to be emitted

```sql
SELECT user_map_key.value   AS seq,
       user_map_value.value AS txn     -- xid:csn
FROM seq_queue
ORDER BY seq;
```

The row with the lowest `seq` is the head. It equals `cursor.nextSeqToEmit`.

### Buffers: progress per transaction

```sql
SELECT user_map_key.value                               AS txn,
       user_map_value.metadata IS NOT NULL              AS has_marker,
       size(user_map_value.orderEvents)                 AS orders,
       user_map_value.expectedOrderCount                AS orders_expected,
       size(user_map_value.orderDetailEvents)           AS details,
       user_map_value.expectedOrderDetailCount          AS details_expected,
       size(user_map_value.orderLineItemEvents)         AS line_items,
       user_map_value.expectedOrderLineItemCount        AS line_items_expected,
       user_map_value.complete                          AS complete
FROM seq_buffers
ORDER BY txn;
```

- `has_marker = false`: data events arrived, but the transaction's marker hasn't. These rows can't
  block output, but they stay in state until the marker arrives.
- `complete` is the value of `TransactionState.isComplete` when the state was last written.

### Which transaction is blocking output

The head of the queue, joined to its buffer:

```sql
WITH head AS (
  SELECT user_map_value.value AS txn
  FROM seq_queue
  ORDER BY user_map_key.value
  LIMIT 1
)
SELECT h.txn,
       size(b.user_map_value.orderEvents)          AS orders,
       b.user_map_value.expectedOrderCount         AS orders_expected,
       size(b.user_map_value.orderDetailEvents)    AS details,
       b.user_map_value.expectedOrderDetailCount   AS details_expected,
       size(b.user_map_value.orderLineItemEvents)  AS line_items,
       b.user_map_value.expectedOrderLineItemCount AS line_items_expected
FROM head h
LEFT JOIN seq_buffers b ON b.user_map_key.value = h.txn;
```

The table whose received count is below its expected count is the one missing events. The app
logs the same information as an error once the head has been blocked longer than
`job.ordering.blocked-alert-threshold`:

```
ERROR TransactionSequencer: Transaction <xid:csn> has blocked emission for <n>s; <k> transaction(s)
queued behind it. Progress: Orders: a/b, Details: c/d, LineItems: e/f
```

### Events of one transaction

```sql
SELECT e.pos, e.table, e.opType, e.after['ORDER_ID'] AS order_id
FROM seq_buffers
LATERAL VIEW explode(user_map_value.orderLineItemEvents) t AS e
WHERE user_map_key.value = '<xid:csn>'
ORDER BY e.pos;
```

Use `orderEvents` or `orderDetailEvents` for the other tables. Events are deduplicated on `pos`,
so each `pos` appears once.

## Options

| Option | Default | Use |
|---|---|---|
| `stateVarName` | (required for `transformWithState`) | `cursor`, `queue` or `buffers` |
| `batchId` | Latest committed batch | Read an earlier batch, within the checkpoint's retained history |
| `operatorId` | `0` | The sequencer is the query's only stateful operator |

Read an earlier batch, for example to see the state before a transaction was emitted:

```scala
spark.read.format("statestore")
  .option("stateVarName", "queue")
  .option("batchId", 5)
  .load("file:///tmp/spark-checkpoints-ordered")
  .show(false)
```

List the batches that can be read:

```bash
ls /tmp/spark-checkpoints-ordered/commits
```

## Column Layout

| `stateVarName` | Columns |
|---|---|
| `cursor` | `key` (`struct<value: string>`), `value` (`Cursor` struct), `partition_id` |
| `queue` | `key`, `user_map_key` (`struct<value: bigint>`), `user_map_value` (`struct<value: string>`), `partition_id` |
| `buffers` | `key`, `user_map_key` (`struct<value: string>`), `user_map_value` (`TransactionState` struct), `partition_id` |

Map state comes back one row per map entry. `key.value` is always `all`, the single grouping key.

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `RpcEndpointNotFoundException: Cannot find endpoint: spark://StateStoreCoordinator@...` | No `StateStoreCoordinator` on the driver yet | Run `spark.streams` first |
| `ConnectException: Call From ... to localhost:9411 ... Connection refused` | Bare path resolved against `fs.defaultFS = hdfs://localhost:9411` | Use `file:///...` |
| `read_statestore` not found | Databricks-only function | Use the `statestore` data source |
| Path does not exist | The app hasn't run, or its checkpoint was deleted | Start `TransactionalCdcProcessingApp`, or check `job.output.checkpoint-location` |
| Empty `queue` and `buffers` | Every queued transaction has been emitted | Expected when the sequencer has caught up. Check `cursor.lastEmittedCsn` |

## What Was Verified

Tested with Spark 4.1.1 on a copy of `/tmp/spark-checkpoints-ordered`:

- `spark.streams` workaround
- the `seq_queue` view and query
- reading `cursor` and `buffers` through `format("statestore")`
- the `buffers` schema

The field-level SQL queries above follow that schema but weren't run against a checkpoint with a
blocked transaction.
