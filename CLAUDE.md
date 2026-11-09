# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spark Structured Streaming application for transactionally consistent CDC event processing. Reads CDC events from multiple Kafka topics (Oracle GoldenGate format), buffers them per-transaction in a single `transformWithState` sequencer with RocksDB, and emits denormalized records to Delta Lake only when all events for a transaction have arrived, strictly in source commit order.

Written in Scala 2.13. This is the Spark port of the Flink-based `transactional-cdc-processing` project in the parent repository. The key difference: Flink uses custom operators with LSN-based watermarks, while Spark uses event-count matching from transaction metadata.

## Building and Testing

```bash
./mvnw clean package          # Build with tests
./mvnw -Dquick clean package  # Skip tests
./mvnw test                   # Unit tests only
./mvnw test -Dtest=ClassName  # Single test
```

JDK 17 target. Scala 2.13.17 compiled via `scala-maven-plugin`. Spark SQL and Jackson are `provided` scope (supplied at runtime by spark-submit). Key versions: Spark 4.1.1, Delta Lake 4.2.0 (`delta-spark_4.1_2.13`), Unity Catalog Spark connector 0.5.0 (`unitycatalog-spark_4.1_2.13`), matching Databricks Runtime 18.

Surefire/Failsafe require `--add-opens` JVM args for Spark/Java 17 compatibility (configured in pom.xml).

## Architecture

### Two Applications

**TransactionalCdcProcessingApp** (Scala object) — Main streaming pipeline:
```
4 Kafka topics → parse JSON → union → group by one global key → transformWithState (TransactionOrderingProcessor) → foreachBatch → order_stream + dropped_events
```

**ScdType1MergeApp** (Scala object) — Downstream merge pipeline:
```
order_stream (SCD Type 2) → foreachBatch → ScdType1Merger (Delta MERGE) → orders_current (SCD Type 1)
```

### Ordered Transaction Assembly (core pattern)

All rows share one grouping key, so a single `TransactionOrderingProcessor` (`transformWithState`, processing-time mode) sees every in-flight transaction. It is a thin Spark adapter over `TransactionSequencer`, which holds the logic and is unit-tested without Spark through the `SequencerStore` trait.

1. Data events are buffered per transaction (`xid:csn`) in `TransactionState`, separated by table and deduplicated on `pos`
2. Markers (transaction metadata) must come from partition 0 of the metadata topic, or the query fails. They are queued in Kafka offset order, which is commit order. Duplicate markers are ignored.
3. After each batch (and on a timer), transactions are emitted from the head of the queue while `TransactionState.isComplete` is true, meaning exact per-table counts. `OrderJoiner.joinTransaction()` produces the `OrderStream` records.
4. An incomplete head blocks all later transactions and is never skipped. An error is logged once it has been blocked longer than `job.ordering.blocked-alert-threshold`.
5. Events and markers for already-emitted transactions are dropped. The `Cursor` tracks the last emitted CSN (compared numerically) and the xids emitted at it.
6. Every drop (replays, duplicates, unknown tables, purged buffers without a marker, unusable Kafka messages) is returned in `SequencerResult.dropped` and written to `argus1.dropped_events` with its reason (`job.output.dropped-events.*`; `enabled = false` only logs drops)

State: `MapState` buffers, `MapState` queue (sequence number → `xid:csn`), `ValueState[Cursor]`. The sink uses a processing-time trigger (`job.ordering.trigger-interval`), because Spark 4.x `transformWithState` in processing-time mode otherwise runs empty batches back to back. Tests therefore wait on query progress, not `processAllAvailable()`.

### Key Classes

| Class | Role |
|-------|------|
| `TransactionalCdcProcessingApp` | Entry point (Scala object); Kafka source, JSON parsing, stream union, `foreachBatch` sink |
| `CdcOutputSink` | Writes each micro-batch: orders to `order_stream`, then drop records to `dropped_events`; both idempotent via Delta `txnAppId` (query id from the checkpoint) and `txnVersion = batchId` |
| `SequencerOutput` / `DroppedEvent` | Envelope output of the stateful operator (an `OrderStream` or a `DroppedEvent`); `DroppedEvent` mirrors the `dropped_events` table |
| `TransactionOrderingProcessor` | `StatefulProcessor` adapter; `orderTransactions()` wires `transformWithState` (classic Spark only) |
| `TransactionSequencer` | Pure sequencing logic: accept/drop rules, emission walk, blocked tracking |
| `CdcRows` | Converts union rows into `DataChangeEvent` / `TransactionMetadata` / sequencer inputs |
| `CdcJsonParsing` | Parses the GoldenGate JSON topics; a metadata message may hold one marker object or an array of markers (kept with Kafka offset and index). Every row carries its Kafka topic/partition/offset; unusable messages become `RAW_DROP` rows |
| `ScdType1MergeApp` | Scala object; streams the SCD Type 2 Delta table into `ScdType1Merger` |
| `ScdType1Merger` | Version-aware Delta MERGE; CSN-guarded transaction context; child-only rows become placeholders |
| `TransactionState` | Serializable state class buffering events per transaction; dedups on `pos`; exact expected vs. received counts |
| `OrderJoiner` | Scala object with `joinTransaction()` that denormalizes buffered events into `OrderStream` records |
| `DataChangeEvent` | Generic CDC event model (table, op_type, before/after maps, xid, csn) |
| `TransactionMetadata` | Transaction metadata with per-table event counts; `DataCollectionCount` is a companion top-level class |
| `OrderStream` | Denormalized output model with nested orders/order details/line items arrays, each with before images |

Model classes use `@BeanProperty` with `@JsonProperty` annotations for compatibility with both Spark's `Encoders.bean()` and Jackson `ObjectMapper.convertValue()`.

### Data Model

Source: 3 Oracle tables related by ORDER_ID — `orders` (1), `order_details` (1:1), `order_line_items` (1:N).

Output (`order_stream`): Top-level `orderId`, `xid`, `csn`, `dwhProcessedTs` with `orders`, `orderDetails`, and `lineItems` as nested arrays. Each entity struct carries a `before` image (null for inserts, populated for updates). The downstream `orders_current` (SCD Type 1) flattens order attributes to top level, uses a single `orderDetails` struct (1:1), and keeps `lineItems` as an array (1:N).

### Kafka Topics

- `dev.appuser.orders.json`
- `dev.appuser.order_details.json`
- `dev.appuser.order_line_items.json`
- `dev.transaction_metadata_json`

### Spark Configuration

Required Spark configs set programmatically in the app:
- Delta Lake extensions (`io.delta.sql.DeltaSparkSessionExtension`, `DeltaCatalog`)
- RocksDB state store provider (for durable stateful processing)
- 4 shuffle partitions

## Running

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

The jar doesn't bundle Typesafe Config or `log4j-api-scala`, so both must be in `--packages`, or the app fails with `NoClassDefFoundError`. The app takes no positional arguments: Kafka servers, checkpoint location and table come from `transactional-cdc-processing.conf`, overridable with `-D` options (for example `--driver-java-options "-Djob.kafka.bootstrap-servers=host:9092 -Djob.output.checkpoint-location=/path"`).

## Known Limitations

- A transaction that never completes (lost event or marker) halts all output until someone intervenes; it is alerted on, never skipped
- Transaction assembly runs as a single task (one global key), so it does not scale out
- Data events whose marker never arrives (for example history from before the metadata topic existed) stay buffered until a transaction with a higher CSN is emitted; then the sequencer removes them, at most 100,000 per step. They never block output. There is no time-based TTL
- The metadata topic must have exactly one partition, and markers must arrive in commit (CSN-nondecreasing) order; a marker below the last emitted CSN is dropped with a WARN
- Markers come from the `gg-formatter/` formatter (one marker per source transaction; a `GROUPTRANSOPS`-grouped delivery is one message holding a JSON array of markers, queued by `(offset, index)`). That module builds only where GoldenGate's jars exist (`/opt/oracle/ggbd/ggjava/resources/lib`) and is not part of the main Maven build
- The state layout is incompatible with pre-ordering checkpoints; upgrading requires a new checkpoint location
- The first start with markers (data topics with history, empty metadata topic) needs one shared cut point for `kc` and `rep3` and per-topic starting offsets (`job.kafka.starting-offsets.<topic-key>`); see the go-live runbook in [gg-configs/README.md](gg-configs/README.md#go-live-first-start-with-markers). Each Kafka source reads at most `job.kafka.max-offsets-per-trigger` (default 50,000) offsets per batch
- No watermark advancement (unlike the Flink implementation)
