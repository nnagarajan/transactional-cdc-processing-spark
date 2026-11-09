# Transactional CDC Processing with Spark Structured Streaming

This implementation assembles CDC events from several Kafka topics into complete source
transactions. It emits them to Delta Lake strictly in source commit order, using a single
`transformWithState` sequencer backed by RocksDB.

Versions: Spark 4.1.1, Delta Lake 4.2.0 (`delta-spark_4.1_2.13`), Unity Catalog Spark connector
0.5.0 (`unitycatalog-spark_4.1_2.13`), Scala 2.13.17, JDK 17. This matches Databricks Runtime 18.
Diagrams are in [PIPELINE_DIAGRAMS.md](PIPELINE_DIAGRAMS.md).

## Architecture

The application processes CDC events from 4 Kafka topics:

1. **dev.appuser.orders.json** - Order records
2. **dev.appuser.order_details.json** - Order shipping/fulfillment details
3. **dev.appuser.order_line_items.json** - Order line items
4. **dev.transaction_metadata_json** - Transaction metadata (markers) with per-table event counts.
   **Must have exactly one partition.**

```
4 Kafka topics -> parse JSON -> union -> groupByKey("all") -> transformWithState
    (TransactionOrderingProcessor) -> argus1.order_stream (UC managed Delta table)
```

### Key Components

#### Model Classes

- **DataChangeEvent**: Generic CDC event in Oracle GoldenGate format (table, op_type, before/after, xid, csn, pos)
- **TransactionMetadata**: Transaction marker with per-table event counts (`DataCollectionCount`)
- **Order, OrderDetail, OrderLineItem**: Domain models for the business entities
- **OrderStream**: Denormalized output record with nested orders/orderDetails/lineItems arrays

Model classes use `@BeanProperty` with `@JsonProperty`, so they work with both `Encoders.bean()`
and Jackson.

#### Processing Logic

1. **TransactionalCdcProcessingApp**: Entry point
   - Reads the 4 topics, parses JSON, unions them, drops rows without `xid`/`csn`
   - Markers keep their Kafka `partition` and `offset`; data events carry nulls in those columns
   - Writes with `Trigger.ProcessingTime(job.ordering.trigger-interval)`

2. **CdcRows**: Converts union rows into `DataChangeEvent`, `TransactionMetadata` and sequencer inputs

3. **TransactionOrderingProcessor**: Spark `StatefulProcessor` adapter
   - `orderTransactions()` sets the RocksDB state store provider, groups every row under the key
     `all`, and calls `transformWithState` in processing-time mode
   - Owns the three state variables and one processing-time timer, registered while the queue is non-empty
   - Uses the classic (non-Connect) `KeyValueGroupedDataset`, where the Scala `transformWithState` API is public

4. **TransactionSequencer**: The sequencing logic, independent of Spark
   - Accept and drop rules for data events and markers
   - The emission walk and blocked-transaction tracking
   - Unit-tested through the `SequencerStore` trait with an in-memory store

5. **TransactionState**: Buffered events for one transaction (`xid:csn`)
   - Events separated by table, deduplicated on `pos`
   - Expected counts from the marker
   - `isComplete`: marker received **and** received count equals expected count for every table.
     More events than declared logs an error and stays incomplete

6. **OrderJoiner**: Joins a complete transaction's events into `OrderStream` records
   - Groups events by ORDER_ID
   - Builds nested arrays with before images

7. **ScdType1Merger** / **ScdType1MergeApp**: Downstream merge of `order_stream` into
   `orders_current` (see [SCD_TYPE1_README.md](SCD_TYPE1_README.md))

## Building

```bash
./mvnw clean package          # build with tests (33 tests)
./mvnw -Dquick clean package  # skip tests
./mvnw test -Dtest=TransactionSequencerTest
```

## Running

Prerequisites: a running Unity Catalog server with catalog `mymanaged` and schema `argus1`
(created with storage roots), and the tables created by `SeedJob`. See
[UNITY_CATALOG.md](UNITY_CATALOG.md).

### Local Mode (Spark 4.1.x)

```bash
spark-submit \
  --class com.technext.demos.txbuffering.TransactionalCdcProcessingApp \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_4.1_2.13:4.2.0,io.unitycatalog:unitycatalog-spark_4.1_2.13:0.5.0,com.typesafe:config:1.4.3,org.apache.logging.log4j:log4j-api-scala_2.13:13.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.mymanaged=io.unitycatalog.spark.UCSingleCatalog \
  --conf spark.sql.catalog.mymanaged.uri=http://localhost:7071 \
  --conf spark.sql.catalog.mymanaged.auth.type=static \
  --conf spark.sql.catalog.mymanaged.auth.token=dummy \
  --conf spark.sql.defaultCatalog=mymanaged \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar
```

The jar doesn't bundle its own dependencies: `com.typesafe:config` and `log4j-api-scala` have to
be in `--packages` as well, or the app fails at startup with `NoClassDefFoundError`.

Run from the IDE or with plain `java`, and `SparkSessionCreator` applies these settings from
`reference.conf` automatically.

### Cluster Mode

```bash
spark-submit \
  --class com.technext.demos.txbuffering.TransactionalCdcProcessingApp \
  --master yarn \
  --deploy-mode cluster \
  --executor-cores 2 \
  --executor-memory 4G \
  --driver-memory 2G \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_4.1_2.13:4.2.0,io.unitycatalog:unitycatalog-spark_4.1_2.13:0.5.0,com.typesafe:config:1.4.3,org.apache.logging.log4j:log4j-api-scala_2.13:13.1.0 \
  --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider \
  --conf spark.sql.catalog.mymanaged=io.unitycatalog.spark.UCSingleCatalog \
  --conf spark.sql.catalog.mymanaged.uri=<uc-server-url> \
  --conf spark.sql.defaultCatalog=mymanaged \
  --conf "spark.driver.extraJavaOptions=-Djob.kafka.bootstrap-servers=kafka-broker1:9092,kafka-broker2:9092 -Djob.output.checkpoint-location=hdfs://namenode:8020/checkpoints/cdc" \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar
```

The app takes no command-line arguments. Settings come from `transactional-cdc-processing.conf`
and can be overridden with `-D` system properties on the driver, as above.
The sequencer runs as a single task, so extra executors don't speed up transaction assembly.

### Configuration

| Key | Default | Purpose |
|---|---|---|
| `job.output.delta-table-name` | `argus1.order_stream` | Output table, resolved against the default catalog `mymanaged` |
| `job.output.checkpoint-location` | `/tmp/spark-checkpoints-ordered` | Kafka offsets and sequencer state |
| `job.ordering.blocked-alert-threshold` | `10 minutes` | Log an error once the head transaction has been blocked this long |
| `job.ordering.check-interval` | `1 minute` | Processing-time timer, so the blocked check runs with no input |
| `job.ordering.trigger-interval` | `10 seconds` | Micro-batch trigger |
| `kafka.starting-offsets` | `earliest` | Starting position for every Kafka source, on a new checkpoint only |
| `job.kafka.starting-offsets.<topic-key>` | unset | Per-topic override: `earliest`, `latest`, or a partition map such as `{"0":12345}` (see the go-live runbook in `gg-configs/README.md`) |
| `job.kafka.max-offsets-per-trigger` | `50000` | Most offsets each Kafka source reads per micro-batch; `none` removes the limit |
| `job.output.dropped-events.enabled` | `true` | Write a row per discarded input to the dropped-events table; `false` only logs drops, and the table need not exist |
| `job.output.dropped-events.table-name` | `argus1.dropped_events` | Dropped-events table |
| `job.output.dropped-events.replay-detail` | `full` | `full`: one row per replayed event or marker. `summary`: one row per replayed transaction, with `droppedCount` |

## How It Works

### 1. Event Ingestion

All 4 Kafka topics are read as streaming DataFrames and parsed into a unified schema:

```
xid, csn, table, op_type, pos, before, after, event_type, marker_partition, marker_offset, ...
```

### 2. One Global Key

Every row is grouped under the single key `all`, so one `TransactionOrderingProcessor` instance
sees every in-flight transaction. That's what makes ordering across transactions possible.

### 3. Accepting Input

For each micro-batch the sequencer first handles data events, then markers sorted by Kafka offset.

**Markers:**
- From a partition other than 0: the query fails, because the metadata topic must have exactly
  one partition.
- Already emitted, or already queued: ignored.
- Otherwise: the marker's counts are attached to the transaction's buffer, and `xid:csn` is
  appended to the queue with the next sequence number. Queue order is Kafka offset order, which
  is commit order.

**Data events:**
- Transaction already emitted: dropped, with no buffer created.
- Same `pos` already buffered: ignored, and not counted.
- Otherwise: buffered by table.

A transaction counts as already emitted if its CSN is numerically below `cursor.lastEmittedCsn`,
or equal to it with an xid in `cursor.emittedAtLastCsn`. A marker with a CSN below the last
emitted CSN is dropped with a WARN, which means markers arrived out of commit order.

### 4. Emitting in Order

After each batch, and on each timer:

```
while queue head exists and buffers(head).isComplete:
    emit OrderJoiner.joinTransaction(buffers(head))
    remove head from buffers and queue
    cursor.nextSeqToEmit += 1; record head's csn/xid in the cursor
```

Example: the marker for T1 precedes the marker for T2, and T2's events all arrive first. T2 stays
buffered. When T1's last event arrives, T1 and T2 are emitted in the same batch, T1 first.

An incomplete head blocks every transaction behind it and is **never skipped**. The sequencer
records when blocking started. Once the head has been blocked longer than
`job.ordering.blocked-alert-threshold`, it logs:

```
ERROR TransactionSequencer: Transaction <xid:csn> has blocked emission for <n>s; <k> transaction(s)
queued behind it. Progress: Orders: a/b, Details: c/d, LineItems: e/f
```

### 5. Joining and Output

A complete transaction is grouped by ORDER_ID, its order details and line items are attached, and
the records are written to `argus1.order_stream`:

```json
{
  "orderId": 248.0,
  "xid": "1342848513.2.24.5354",
  "csn": "334516829",
  "dwhProcessedTs": "2026-02-17T18:10:23.456Z",
  "orders": [
    { "orderId": 248.0, "orderRef": "ORD-20260217-001", "orderStatus": "CONFIRMED", "totalAmount": 25250.0, "version": 2.0, "before": { ... } }
  ],
  "orderDetails": [ ... ],
  "lineItems": [ ... ]
}
```

Within a transaction, events aren't sorted by `pos`. Ordering is guaranteed between transactions,
not between the events of one transaction.

### 6. Dropped Events

Every input the pipeline discards becomes a row in `argus1.dropped_events`
(`db/create_dropped_events_table.deltalake.sql`, partitioned by `droppedDate`). The sequencer
returns its drops next to the emitted transactions, and the stateful operator outputs both in one
envelope (`SequencerOutput`). Messages that can't be used are marked `RAW_DROP` by
`CdcJsonParsing` and passed through the sequencer, which records them without touching state.

| `reason` | `itemKind` | Cause |
|---|---|---|
| `REPLAY_BELOW_CURSOR` | `DATA_EVENT`, `MARKER` | CSN below the last emitted CSN |
| `REPLAY_ALREADY_EMITTED` | `DATA_EVENT`, `MARKER` | CSN equal to the last emitted CSN, xid already emitted |
| `DUPLICATE_MARKER` | `MARKER` | The transaction already has a marker |
| `DUPLICATE_EVENT` | `DATA_EVENT` | Same `pos` already buffered |
| `UNKNOWN_TABLE` | `DATA_EVENT` | Table other than ORDERS, ORDER_DETAILS, ORDER_LINE_ITEMS |
| `NO_MARKER_BELOW_CURSOR` | `PURGED_BUFFER` | Buffer without a marker removed once the cursor passed its CSN; one row per transaction with `bufferedOrders`, `bufferedDetails`, `bufferedLineItems` |
| `MISSING_IDS` | `RAW_MESSAGE` | Valid JSON without `xid` or `csn` (`rawValue` holds the message, or the one marker) |
| `UNPARSEABLE` | `RAW_MESSAGE` | Not valid JSON (`rawValue` holds the message) |

Every row has `lastEmittedCsn` (the cursor at the time), `batchId`, `droppedTs`, and the Kafka
`kafkaTopic`/`kafkaPartition`/`kafkaOffset` (plus `markerIndex` for markers).

The sink is a `foreachBatch` that appends orders to `order_stream`, then drops to
`dropped_events`. Both writes use Delta's `txnAppId` (`<query id>-<table>`, with the query id read
from `<checkpoint>/metadata`) and `txnVersion = batchId`, so a retried batch writes nothing twice.
If the dropped-events write fails, the batch fails and is retried: output stops until the table is
healthy again, or until `job.output.dropped-events.enabled = false`.

Example queries:

```sql
-- Everything dropped for one transaction
SELECT droppedTs, itemKind, reason, droppedCount, lastEmittedCsn, kafkaTopic, kafkaOffset, tableName, pos
FROM mymanaged.argus1.dropped_events
WHERE xid = '1342848513.8.14.5716' AND csn = '344374437'
ORDER BY droppedTs, kafkaTopic, kafkaOffset;

-- Drops per reason per day
SELECT droppedDate, reason, itemKind, sum(droppedCount) AS items, count(*) AS rows
FROM mymanaged.argus1.dropped_events
GROUP BY droppedDate, reason, itemKind
ORDER BY droppedDate DESC, items DESC;

-- Messages that could not be parsed, with where to find them in Kafka
SELECT droppedTs, reason, kafkaTopic, kafkaPartition, kafkaOffset, markerIndex, rawValue
FROM mymanaged.argus1.dropped_events
WHERE itemKind = 'RAW_MESSAGE'
ORDER BY droppedTs DESC;
```

## State Management

### State Variables

All state lives under the key `all`:

| Variable | Type | Content |
|---|---|---|
| `buffers` | `MapState[String, TransactionState]` | `xid:csn` → buffered events and expected counts |
| `queue` | `MapState[Long, String]` | Sequence number → `xid:csn`, for transactions whose marker has arrived |
| `cursor` | `ValueState[Cursor]` | `nextSeqToEmit`, `nextSeqToAssign`, `lastEmittedCsn`, `emittedAtLastCsn`, `blockedSinceMs`, `timerMs` |

The state store is RocksDB (`RocksDBStateStoreProvider`), set on both the dev and the
`spark-submit` paths. To read the state from a checkpoint, see
[STATE-STORE-DEBUG.md](STATE-STORE-DEBUG.md).

### Checkpoint Management

Checkpoints include:
- RocksDB state snapshots
- Kafka offsets
- Processing metadata

The state layout differs from the earlier per-transaction (`flatMapGroupsWithState`) version, so
upgrading needs a new checkpoint location. Deleting the checkpoint makes the app re-read Kafka
from `earliest`. Transactions already in `order_stream` would then be emitted again, so reseed the
tables (`SeedJob`) at the same time.

## Source Contract (GoldenGate)

Ordering and completeness depend on the source configuration in `gg-configs/`
(see [gg-configs/README.md](gg-configs/README.md)):

| Setting | Where | Why |
|---|---|---|
| `IGNOREDELETES` | Extract `ext04.prm` | Deletes are filtered before the trail, so the data topics and the marker counts always agree |
| No `IGNOREDELETES` | Replicat `rep3.prm` | Filtering at only one replicat would make the counts disagree with the data |
| `blockingSend=true` | `rep3.props` | Markers are published synchronously, in commit order |
| `acks=all`, `enable.idempotence=true` | `kp.properties` | No lost or reordered markers on retry |
| 1 partition | `dev.transaction_metadata_json` | Kafka keeps order within a partition only |
| `TransactionBoundaryEventJsonFormatter` from `gg-formatter/` | `rep3.props` | One marker per source transaction even with `GROUPTRANSOPS 1000`. A grouped delivery is one message holding a JSON array of markers, queued by `(offset, index)` |

The marker contract is tested on both sides: the formatter's output in
[gg-formatter](gg-formatter/README.md), and its parsing and ordering in `CdcJsonParsingTest` and
`TransactionSequencerTest`.

## Key Features

1. **Commit-order output:** transactions reach `order_stream` in source commit order, and every
   Delta commit contains only transactions later than those in earlier commits.
2. **Exact completeness:** per-table counts must match exactly, after deduplication on `pos`.
3. **Replay safety:** duplicate markers and events of emitted transactions are dropped, including
   after restart.
4. **Visible blocking:** a stuck head transaction is logged with its progress and queue depth.
5. **Persistent state:** RocksDB state and Kafka offsets survive restarts via the checkpoint.

## Limitations and Considerations

1. **Head-of-line blocking:** a transaction that never completes (lost event or marker) halts all
   output until someone intervenes. It's alerted on, never skipped.
2. **No scale-out:** transaction assembly runs as a single task on one global key.
3. **Orphan buffers:** data events whose marker never arrives stay buffered until a transaction
   with a higher CSN is emitted. Then they're removed, since any marker for them would be dropped
   as a replay (at most 100,000 per step, logged at INFO). They can't block output. There is no
   time-based TTL, which would also remove a slow in-flight transaction.
4. **Single-partition metadata topic:** markers must arrive in commit (CSN-nondecreasing) order.
5. **Checkpoint compatibility:** the state layout is incompatible with pre-ordering checkpoints.
6. **Unity Catalog dependency:** writes to managed tables commit through the UC server, so the
   server must be running.
7. **No watermark advancement**, unlike the Flink implementation.

## Testing

| Test | Count | Covers |
|---|---|---|
| `TransactionStateTest` | 9 | Dedup on `pos`, exact completeness, overfill |
| `TransactionSequencerTest` | 34 | Ordering, blocking, replay drops, same-CSN transactions, partition check, alerting, orphan-buffer removal, drop records per reason, summary mode |
| `KafkaSourceOptionsTest` | 9 | Per-topic starting positions, validation, per-batch limit |
| `CdcJsonParsingTest` | 8 | Single-object and array marker messages, Kafka coordinates, unparseable and id-less messages |
| `CdcOutputSinkTest` | 5 | Two-table sink, idempotent retries, disabled mode, per-checkpoint `txnAppId` |
| `SequencerOutputEncodingTest` | 2 | Bean schemas match the dropped-events table |
| `OrderJoinerTest` | 1 | Denormalization |
| `TransactionOrderingProcessorIntegrationTest` | 3 | End to end through `transformWithState` with `MemoryStream` |
| `ScdType1MergerTest` | 4 | Placeholders, CSN guard, tie-breaking, on Delta |

The integration tests use `Trigger.ProcessingTime` and wait on query progress, not
`processAllAvailable()`. In processing-time mode `transformWithState` keeps requesting batches, so
`processAllAvailable()` never returns.

## Monitoring

Key things to watch:

- `TransactionSequencer` ERROR lines (blocked head) and WARN lines (out-of-order markers)
- Queue depth (`nextSeqToAssign - nextSeqToEmit` in the cursor)
- State store size and number of buffered transactions
- Kafka consumer lag
- Checkpoint and batch duration

## Troubleshooting

### Inspect the Sequencer State

```scala
spark.streams   // required once per session on Spark 4.1
spark.read.format("statestore").option("stateVarName", "cursor")
  .load("file:///tmp/spark-checkpoints-ordered").show(false)
```

[STATE-STORE-DEBUG.md](STATE-STORE-DEBUG.md) has queries for the queue, the buffers and the
blocking transaction.

### Output Stopped

1. Look for the `TransactionSequencer` blocked-emission ERROR, which names the transaction and
   the table that's short.
2. Check that transaction's events on the data topics and its marker on the metadata topic.
3. A lost event or marker has to be republished. The sequencer doesn't skip it.

### Debugging

Enable event logs:
```bash
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=<log-directory>
```

## Comparison with Flink Implementation

| Feature | Flink (transactional-cdc-processing) | Spark (this implementation) |
|---------|--------------------------------------|------------------------------|
| State Backend | Keyed state | RocksDB state store |
| Watermarks | Custom LSN-based | Not used |
| Ordering | Watermark advancement | Marker order on a single-partition topic, one global sequencer |
| Parallelism | Currently 1 | 1 (single global key) |
| API | DataStream v2 custom operators | Structured Streaming `transformWithState` |
| Terminal Logic | Watermark advancement | Exact event-count matching from markers |
| Language | Java records | Scala with bean-style model classes |
