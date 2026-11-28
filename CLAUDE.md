# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spark Structured Streaming application for transactionally consistent CDC event processing. Reads CDC events from multiple Kafka topics (Oracle GoldenGate format), buffers them per-transaction in a single `transformWithState` sequencer with RocksDB, and emits denormalized records to Delta Lake only when all events for a transaction have arrived, strictly in source commit order.

The data model is configuration, not code: a **job** is one root table plus direct children, described by `jobs/<job>.conf` (tables, topics, explicit source-to-output column mapping with types, keys, output tables). Nothing is read from Oracle. One jar runs any job; `jobs/orders.conf` (orders, order_details, order_line_items) is bundled and used when no `-Dconfig.file` is given. Logic beyond rename and cast goes in a hand-written `StreamTransform`. See [docs/ADDING-A-JOB.md](docs/ADDING-A-JOB.md).

Written in Scala 2.13. This is the Spark port of the Flink-based `transactional-cdc-processing` project in the parent repository. The key difference: Flink uses custom operators with LSN-based watermarks, while Spark uses event-count matching from transaction metadata.

## Building and Testing

```bash
./mvnw clean package          # Build with tests
./mvnw -Dquick clean package  # Skip tests
./mvnw test                   # Unit tests only
./mvnw test -Dtest=ClassName  # Single test
scripts/cdc-model.sh check jobs/orders.conf   # Validate a job config
scripts/cdc-model.sh ddl jobs/orders.conf     # Write db/<job>_stream / _current .deltalake.sql
```

`GoldenOutputTest` compares the orders job's `order_stream` and `orders_current` output with files recorded from the pre-job-model pipeline (`src/test/resources/golden/`); re-record only on an intended change (`-Dgolden.record=true`).

JDK 17 target. Scala 2.13.17 compiled via `scala-maven-plugin`. Spark SQL and Jackson are `provided` scope (supplied at runtime by spark-submit). Key versions: Spark 4.1.1, Delta Lake 4.2.0 (`delta-spark_4.1_2.13`), Unity Catalog Spark connector 0.5.0 (`unitycatalog-spark_4.1_2.13`), matching Databricks Runtime 18.

Surefire/Failsafe require `--add-opens` JVM args for Spark/Java 17 compatibility (configured in pom.xml).

## Architecture

### Two Applications

Both read the same job config (`AppConfig.loadJob()` → `ModelResolver.load()` → `JobModel`) and check their tables' schemas against the model before starting (they never create or alter tables).

**TransactionalCdcProcessingApp** (Scala object) — Stage 1:
```
one Kafka topic per entity + metadata topic → parse JSON → union → group by one global key → transformWithState (TransactionOrderingProcessor) → foreachBatch (StreamShaper [+ StreamTransform]) → <stream table> + dropped_events
```

**ScdType1MergeApp** (Scala object) — Stage 2:
```
<stream table> (SCD Type 2) → foreachBatch → CurrentStateMerger (Delta MERGE generated from the model) → <current table> (SCD Type 1)
```

### Job Model

`model.spec`: `JobConfig.parse` reads the config without failing; `ModelResolver` validates everything (errors collected, all reported) and builds `JobModel` (entities with table, topic, cardinality, root-key mapping, own key, version, before image, `ModelColumn(source, name, dataType)`). `Schemas` derives the stream and current `StructType`s, `Ddl` the CREATE TABLE scripts; everything schema-dependent uses these, so DDL, shaping, merge and startup checks can't disagree. Keys may be compound; a child maps each root key column to its own column, and key values are converted to the root key's types before grouping. Entities without a version column are ordered by CSN (`changeCsn` field in the current table).

### Ordered Transaction Assembly (core pattern)

All rows share one grouping key, so a single `TransactionOrderingProcessor` (`transformWithState`, processing-time mode) sees every in-flight transaction. It is a thin Spark adapter over `TransactionSequencer`, which holds the logic and is unit-tested without Spark through the `SequencerStore` trait.

1. Data events of the job's tracked tables (`JobModel.trackedTables`, names without schema) are buffered per transaction (`xid:csn`): a small `TransactionSummary` (marker, expected and received count per tracked table, event count) plus one state entry per event, deduplicated on `(table, pos)` by key lookup. Buffering is linear in transaction size; each event is written once and read once. Events of other tables are dropped as `UNKNOWN_TABLE`; a marker's counts for other tables are ignored, so jobs can share the metadata topic
2. Markers (transaction metadata) must come from partition 0 of the metadata topic, or the query fails. They are queued in Kafka offset order, which is commit order. Duplicate markers are ignored.
3. After each batch (and on a timer), transactions are emitted from the head of the queue while the summary is complete, meaning exact per-table counts. Each is emitted as one model-independent `TRANSACTION` row (`EmittedTransaction`: xid, csn, processedTs, events in tracked-table then buffered order); `StreamShaper` turns them into stream records in `foreachBatch`, outside the single-task operator.
4. An incomplete head blocks all later transactions and is never skipped. An error is logged once it has been blocked longer than `job.ordering.blocked-alert-threshold`.
5. Events and markers for already-emitted transactions are dropped. The `Cursor` tracks the last emitted CSN (compared numerically) and the xids emitted at it.
6. Every drop (replays, duplicates, unknown tables, purged buffers without a marker, unusable Kafka messages) is returned in `SequencerResult.dropped` and written to `dropped_events` with its reason and the job name (`job.output.dropped-events.*`; `enabled = false` only logs drops)

State: `MapState` `transactions` (`xid:csn` → `TransactionSummary`), `events` (`xid:csn#i` → event, in buffered order), `positions` (`xid:csn|TABLE|pos` → `i`), `queue` (sequence number → `xid:csn`), `ValueState[Cursor]`. A summary read back from state holds unmodifiable maps: copy before mutating. The sink uses a processing-time trigger (`job.ordering.trigger-interval`), because Spark 4.x `transformWithState` in processing-time mode otherwise runs empty batches back to back. Tests therefore wait on query progress, not `processAllAvailable()`.

### Key Classes

| Class | Role |
|-------|------|
| `TransactionalCdcProcessingApp` | Entry point (Scala object); one Kafka source per entity plus the metadata topic, JSON parsing, stream union, `foreachBatch` sink |
| `AppConfig` | `loadJob()`: system properties > `-Dconfig.file` (or bundled `jobs/orders.conf`) > `reference.conf` + `cdc-defaults.conf` |
| `JobConfig` / `ModelResolver` / `JobModel` | Job config as written; validation and resolution; the resolved model (`model.spec`) |
| `Schemas` / `Ddl` | Stream and current `StructType`s and schema differences; CREATE TABLE scripts (`model.spec`) |
| `CdcModelTool` | `ddl` and `check` commands for job configs (`scripts/cdc-model.sh`); no Spark session, no Oracle |
| `StreamShaper` | Emitted transactions → stream records (explode, typed per-entity structs with before images, compound-key extraction, group by key); counts cast failures, warns once about undeclared columns |
| `StreamTransform` | Trait for hand-written stage 1 logic, named by `job.output.transform`; output must match the stream schema |
| `CdcOutputSink` | Writes each micro-batch: shaped (and transformed) records to the stream table, then drop records (with `jobName`) to `dropped_events`; both idempotent via Delta `txnAppId` (query id from the checkpoint) and `txnVersion = batchId`. `requireTableSchema` is the startup check |
| `SequencerOutput` / `EmittedTransaction` / `DroppedEvent` | Envelope output of the stateful operator (a `TRANSACTION` or a `DroppedEvent`); `DroppedEvent` mirrors the `dropped_events` table |
| `TransactionOrderingProcessor` | `StatefulProcessor` adapter; `orderTransactions()` wires `transformWithState` (classic Spark only) |
| `TransactionSequencer` | Pure sequencing logic: accept/drop rules, emission walk, blocked tracking |
| `CdcRows` | Converts union rows into `DataChangeEvent` / `TransactionMetadata` / sequencer inputs |
| `CdcJsonParsing` | Parses the GoldenGate JSON topics; a metadata message may hold one marker object or an array of markers (kept with Kafka offset and index). Every row carries its Kafka topic/partition/offset; unusable messages become `RAW_DROP` rows |
| `ScdType1MergeApp` | Scala object; streams the stream table into `CurrentStateMerger` |
| `CurrentStateMerger` | Version-aware Delta MERGE generated from the model (root flattened, 1:1 struct, 1:N array by element key); CSN-guarded transaction context; child-only rows become placeholders |
| `TransactionSummary` | Per buffered transaction state: marker, expected/received count per tracked table, event count; decides completeness without reading events |
| `TransactionState` | A transaction's events grouped by tracked table, assembled once at emission; shares completion rules (`TransactionCompletion`) with the summary |
| `DataChangeEvent` | Generic CDC event model (table, op_type, before/after maps, xid, csn) |
| `TransactionMetadata` | Transaction metadata with per-table event counts; `DataCollectionCount` is a companion top-level class |

Bean classes use `@BeanProperty` for Spark's `Encoders.bean()`; read-only getters are part of the encoded schema too.

### Data Model

Per job: stream table rows are `xid`, `csn`, `dwhProcessedTs`, the root key columns, then one array per entity (config order) whose elements hold the declared columns and a `before` struct. Current table rows are the context columns, the root's columns flattened (+ `changeCsn` if versionless), the root before struct (`<root>Before`), one struct per 1:1 child and one array per 1:N child.

Orders job source: 3 Oracle tables related by ORDER_ID — `orders` (1), `order_details` (1:1), `order_line_items` (1:N).

Orders output (`order_stream`): Top-level `orderId`, `xid`, `csn`, `dwhProcessedTs` with `orders`, `orderDetails`, and `lineItems` as nested arrays. Each entity struct carries a `before` image (null for inserts, populated for updates). The downstream `orders_current` (SCD Type 1) flattens order attributes to top level, uses a single `orderDetails` struct (1:1), and keeps `lineItems` as an array (1:N).

### Kafka Topics

Per entity, as the job config names them, plus the shared metadata topic (`job.kafka.metadata-topic`). For the orders job:

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
  --master 'local[2]' \
  --driver-java-options "-Dconfig.file=jobs/orders.conf" \
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

`ScdType1MergeApp` runs the same way with its own `--class` and the same job config. The jar doesn't bundle Typesafe Config or `log4j-api-scala`, so both must be in `--packages`, or the app fails with `NoClassDefFoundError`. The apps take no positional arguments: everything comes from the job config over `cdc-defaults.conf`, overridable with `-D` options (for example `-Djob.kafka.bootstrap-servers=host:9092 -Djob.output.stream.checkpoint=/path`). Checkpoints default to `/tmp/spark-checkpoints/<job>/stream` and `.../current`. Tables must exist first: `scripts/cdc-model.sh ddl` writes their scripts, `SeedJob` runs them.

## Known Limitations

- A transaction that never completes (lost event or marker) halts all output until someone intervenes; it is alerted on, never skipped
- Transaction assembly runs as a single task (one global key), so it does not scale out
- Data events whose marker never arrives (for example history from before the metadata topic existed) stay buffered until a transaction with a higher CSN is emitted; then the sequencer removes them, at most 100,000 per step. They never block output. There is no time-based TTL
- The metadata topic must have exactly one partition, and markers must arrive in commit (CSN-nondecreasing) order; a marker below the last emitted CSN is dropped with a WARN
- Markers come from the `gg-formatter/` formatter (one marker per source transaction; a `GROUPTRANSOPS`-grouped delivery is one message holding a JSON array of markers, queued by `(offset, index)`). That module builds only where GoldenGate's jars exist (`/opt/oracle/ggbd/ggjava/resources/lib`) and is not part of the main Maven build
- A model is a root with direct children; every child must carry every root key column (compound keys allowed). Grandchildren and children joined to other root columns aren't supported
- Column lists are explicit and maintained by hand; there is no drift check against Oracle (undeclared source columns are left out with a warning, missing ones become null)
- Every job reads the whole shared metadata topic, so each marker costs every job a queue entry; there is no atomicity across jobs
- The state layout is incompatible with checkpoints of earlier layouts, and the app doesn't detect one: after a state change, start stage 1 on a new checkpoint (or delete the old one, e.g. `scripts/reset-pipeline.sh`)
- A large transaction is still emitted whole, in commit order: it holds back the transactions behind it until all its events have arrived, and is assembled in memory once at emission
- The first start with markers (data topics with history, empty metadata topic) needs one shared cut point for `kc` and `rep3` and per-source starting offsets (`job.kafka.starting-offsets.<entity name>`, or `transaction-metadata`); see the go-live runbook in [gg-configs/README.md](gg-configs/README.md#go-live-first-start-with-markers). Each Kafka source reads at most `job.kafka.max-offsets-per-trigger` (default 50,000) offsets per batch
- No watermark advancement (unlike the Flink implementation)
