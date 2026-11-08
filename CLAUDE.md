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
4 Kafka topics → parse JSON → union → group by one global key → transformWithState (TransactionOrderingProcessor) → Delta Lake
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

State: `MapState` buffers, `MapState` queue (sequence number → `xid:csn`), `ValueState[Cursor]`. The sink uses a processing-time trigger (`job.ordering.trigger-interval`), because Spark 4.x `transformWithState` in processing-time mode otherwise runs empty batches back to back. Tests therefore wait on query progress, not `processAllAvailable()`.

### Key Classes

| Class | Role |
|-------|------|
| `TransactionalCdcProcessingApp` | Entry point (Scala object); Kafka source, JSON parsing (markers keep Kafka partition/offset), stream union, Delta sink |
| `TransactionOrderingProcessor` | `StatefulProcessor` adapter; `orderTransactions()` wires `transformWithState` (classic Spark only) |
| `TransactionSequencer` | Pure sequencing logic: accept/drop rules, emission walk, blocked tracking |
| `CdcRows` | Converts union rows into `DataChangeEvent` / `TransactionMetadata` / sequencer inputs |
| `CdcJsonParsing` | Parses the GoldenGate JSON topics; a metadata message may hold one marker object or an array of markers (kept with Kafka offset and index) |
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
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_4.1_2.13:4.2.0 \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar \
  <kafka-bootstrap-servers> <checkpoint-location> <delta-table-path>
```

## Known Limitations

- A transaction that never completes (lost event or marker) halts all output until someone intervenes; it is alerted on, never skipped
- Transaction assembly runs as a single task (one global key), so it does not scale out
- Data events whose marker never arrives stay buffered indefinitely (no TTL); they cannot block output, but they consume state
- The metadata topic must have exactly one partition, and markers must arrive in commit (CSN-nondecreasing) order; a marker below the last emitted CSN is dropped with a WARN
- Markers come from the `gg-formatter/` formatter (one marker per source transaction; a `GROUPTRANSOPS`-grouped delivery is one message holding a JSON array of markers, queued by `(offset, index)`). That module builds only where GoldenGate's jars exist (`/opt/oracle/ggbd/ggjava/resources/lib`) and is not part of the main Maven build
- The state layout is incompatible with pre-ordering checkpoints; upgrading requires a new checkpoint location
- No watermark advancement (unlike the Flink implementation)
