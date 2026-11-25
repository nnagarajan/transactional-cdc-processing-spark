# Debugging the Sequencer State Store

How to read the state of `TransactionalCdcProcessingApp`'s ordering sequencer straight from its
checkpoint: what has been emitted, what is queued, and which transaction is holding up output.

Spark's `statestore` data source reads the state from the checkpoint directory. You can read it
while the app runs, and you see the last committed micro-batch.

## What the Sequencer Stores

`TransactionOrderingProcessor` keeps five state variables, all under a single grouping key `all`
([TransactionOrderingProcessor.scala](../src/main/scala/com/technext/demos/txbuffering/processor/TransactionOrderingProcessor.scala)):

| `stateVarName` | Type | Key → value | Holds |
|---|---|---|---|
| `cursor` | `ValueState[Cursor]` | `all` → `Cursor` | Emission position, last emitted CSN, blocked-since time, timer |
| `queue` | `MapState[Long, String]` | sequence number → `xid:csn` | Transactions whose marker arrived, in marker (commit) order, not yet emitted |
| `txs` | `MapState[String, TransactionSummary]` | `xid:csn` → `TransactionSummary` | Per buffered transaction: the marker, expected and received counts per table, and `eventCount` |
| `events` | `MapState[String, DataChangeEvent]` | `xid:csn#i` → event | Each buffered event once; `i` runs from 0 to `eventCount - 1` in buffered order |
| `positions` | `MapState[String, Long]` | `xid:csn\|TABLE\|pos` → `i` | The `pos` values buffered per table, for duplicate detection |

A transaction's `txs`, `events` and `positions` entries, and its `queue` entry, are removed when
it's emitted. A buffered transaction without a marker loses its entries when the sequencer removes
it (see below).

The processor also declares `buffers`, the whole-transaction state of releases before per-event
buffering. It's always empty; the processor declares it only to refuse a checkpoint from those
releases (see `gg-configs/README.md`, "Migrating to per-event buffering state").

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

// Buffered transactions: summaries, then their events
spark.read.format("statestore").option("stateVarName", "txs").load(ckpt).show(false)
spark.read.format("statestore").option("stateVarName", "events").load(ckpt).show(false)

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

CREATE OR REPLACE TEMPORARY VIEW seq_txs USING statestore
OPTIONS (path 'file:///tmp/spark-checkpoints-ordered', stateVarName 'txs');

CREATE OR REPLACE TEMPORARY VIEW seq_events USING statestore
OPTIONS (path 'file:///tmp/spark-checkpoints-ordered', stateVarName 'events');
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

### Buffered transactions: progress per transaction

```sql
SELECT user_map_key.value                       AS txn,
       user_map_value.metadata IS NOT NULL      AS has_marker,
       user_map_value.receivedOrders            AS orders,
       user_map_value.expectedOrders            AS orders_expected,
       user_map_value.receivedOrderDetails      AS details,
       user_map_value.expectedOrderDetails      AS details_expected,
       user_map_value.receivedOrderLineItems    AS line_items,
       user_map_value.expectedOrderLineItems    AS line_items_expected,
       user_map_value.eventCount                AS events,
       user_map_value.complete                  AS complete
FROM seq_txs
ORDER BY txn;
```

- `has_marker = false`: data events arrived, but the transaction's marker hasn't. These rows can't
  block output. They stay in state until the marker arrives, or until a transaction with a higher
  CSN is emitted, when the sequencer removes them (see below).
- `complete` is the value of `TransactionSummary.isComplete` when the summary was last written.

### Buffered transactions the sequencer will remove

Buffers whose CSN is below `cursor.lastEmittedCsn` can never be emitted: a queued transaction
never has a CSN below it, and any marker for them would be dropped as a replay. The sequencer
removes them each time the cursor advances, at most 100,000 per step, and logs
`Removed <n> buffered transaction(s) without a marker below CSN <csn>`. After a start from
`earliest` with pre-marker history, this count shows how much is left to remove:

```sql
SELECT count(*)                                                             AS removable,
       min(CAST(substring_index(user_map_key.value, ':', -1) AS DECIMAL(38,0))) AS lowest_csn
FROM seq_txs
WHERE CAST(substring_index(user_map_key.value, ':', -1) AS DECIMAL(38,0))
      < (SELECT CAST(value.lastEmittedCsn AS DECIMAL(38,0)) FROM seq_cursor);
```

Use the scalar subquery as written. Joining `seq_txs` to `seq_cursor` instead fails with
`CANNOT_LOAD_STATE_STORE`, because the join reopens the same state stores concurrently.

It stays above 0 between steps while more than 100,000 remain, and when no transaction has been
emitted since the buffers were written.

### Which transaction is blocking output

The head of the queue, joined to its summary:

```sql
WITH head AS (
  SELECT user_map_value.value AS txn
  FROM seq_queue
  ORDER BY user_map_key.value
  LIMIT 1
)
SELECT h.txn,
       t.user_map_value.receivedOrders          AS orders,
       t.user_map_value.expectedOrders          AS orders_expected,
       t.user_map_value.receivedOrderDetails    AS details,
       t.user_map_value.expectedOrderDetails    AS details_expected,
       t.user_map_value.receivedOrderLineItems  AS line_items,
       t.user_map_value.expectedOrderLineItems  AS line_items_expected
FROM head h
LEFT JOIN seq_txs t ON t.user_map_key.value = h.txn;
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
SELECT CAST(substring_index(user_map_key.value, '#', -1) AS INT) AS i,
       user_map_value.pos,
       user_map_value.table,
       user_map_value.opType,
       user_map_value.after['ORDER_ID']                         AS order_id
FROM seq_events
WHERE user_map_key.value LIKE '<xid:csn>#%'
ORDER BY i;
```

`i` is the buffered order, which is the order the transaction emits each table's events in.
Events are deduplicated on `pos` per table, so each `(table, pos)` appears once.

## Options

| Option | Default | Use |
|---|---|---|
| `stateVarName` | (required for `transformWithState`) | `cursor`, `queue`, `txs`, `events` or `positions` |
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
| `txs` | `key`, `user_map_key` (`struct<value: string>`), `user_map_value` (`TransactionSummary` struct), `partition_id` |
| `events` | `key`, `user_map_key` (`struct<value: string>`), `user_map_value` (`DataChangeEvent` struct), `partition_id` |
| `positions` | `key`, `user_map_key` (`struct<value: string>`), `user_map_value` (`struct<value: bigint>`), `partition_id` |

Map state comes back one row per map entry. `key.value` is always `all`, the single grouping key.

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `RpcEndpointNotFoundException: Cannot find endpoint: spark://StateStoreCoordinator@...` | No `StateStoreCoordinator` on the driver yet | Run `spark.streams` first |
| `ConnectException: Call From ... to localhost:9411 ... Connection refused` | Bare path resolved against `fs.defaultFS = hdfs://localhost:9411` | Use `file:///...` |
| `read_statestore` not found | Databricks-only function | Use the `statestore` data source |
| Path does not exist | The app hasn't run, or its checkpoint was deleted | Start `TransactionalCdcProcessingApp`, or check `job.output.checkpoint-location` |
| Empty `queue` and `txs` | Every queued transaction has been emitted | Expected when the sequencer has caught up. Check `cursor.lastEmittedCsn` |
| App fails with "This checkpoint was written by a version that buffered whole transactions" | The checkpoint predates per-event buffering | Start on a new checkpoint; see `gg-configs/README.md`, "Migrating to per-event buffering state" |

## What Was Verified

Tested with Spark 4.1.1:

- On a copy of `/tmp/spark-checkpoints-ordered` (before per-event buffering): the `spark.streams`
  workaround, and reading `cursor` and `queue` through `format("statestore")`.
- Every SQL statement in this document, run unchanged against a checkpoint from a local query
  that left in-flight state. That state was a blocked head with 2 of 3 line items, a complete
  transaction queued behind it, a transaction without a marker above the cursor, and one below
  the cursor that had been removed. Each query returned the expected rows.
