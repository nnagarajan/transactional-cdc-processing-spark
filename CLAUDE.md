# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spark Structured Streaming application for transactionally consistent CDC event processing. Reads CDC events from multiple Kafka topics (Oracle GoldenGate format), buffers them per-transaction using `flatMapGroupsWithState` with RocksDB, and emits denormalized records to Delta Lake only when all events for a transaction have arrived.

Written in Scala 2.13. This is the Spark port of the Flink-based `transactional-cdc-processing` project in the parent repository. The key difference: Flink uses custom operators with LSN-based watermarks, while Spark uses event-count matching from transaction metadata.

## Building and Testing

```bash
./mvnw clean package          # Build with tests
./mvnw -Dquick clean package  # Skip tests
./mvnw test                   # Unit tests only
./mvnw test -Dtest=ClassName  # Single test
```

JDK 17 target. Scala 2.13.16 compiled via `scala-maven-plugin`. Spark SQL and Jackson are `provided` scope (supplied at runtime by spark-submit). Key versions: Spark 4.0.0, Delta Lake 4.0.0.

Surefire/Failsafe require `--add-opens` JVM args for Spark/Java 17 compatibility (configured in pom.xml).

## Architecture

### Two Applications

**TransactionalCdcProcessingApp** (Scala object) — Main streaming pipeline:
```
4 Kafka topics → parse JSON → union → group by xid:csn → flatMapGroupsWithState → Delta Lake
```

**ScdType1MergeApp** (Scala object) — Downstream merge pipeline:
```
order_stream (SCD Type 2) → foreachBatch → Delta MERGE → orders_current (SCD Type 1)
```

### Transaction-Aware Buffering (core pattern)

Events are grouped by transaction key (`xid:csn` = transaction ID : commit sequence number). The `processTransaction` method in `TransactionalCdcProcessingApp` is the stateful processing function passed to `flatMapGroupsWithState`:

1. Receives a transaction metadata event containing expected per-table event counts
2. Buffers CDC events in `TransactionState` (separated by table: ORDERS, ORDER_DETAILS, ORDER_LINE_ITEMS)
3. When `TransactionState.isComplete` returns true (all expected counts met), calls `OrderJoiner.joinTransaction()` to produce denormalized `OrderStream` records
4. Removes state after emission; returns empty iterator while incomplete

### Key Classes

| Class | Role |
|-------|------|
| `TransactionalCdcProcessingApp` | Entry point (Scala object); Kafka source, JSON parsing, stream union, stateful processing, Delta sink |
| `ScdType1MergeApp` | Scala object; reads SCD Type 2 Delta table, merges into SCD Type 1 using version-aware Delta MERGE |
| `TransactionState` | Serializable state class buffering events per transaction; tracks expected vs. received counts |
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
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,io.delta:delta-spark_2.13:4.0.0 \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar \
  <kafka-bootstrap-servers> <checkpoint-location> <delta-table-path>
```

## Known Limitations

- No TTL or timeout for incomplete transactions (state grows if metadata or events are lost)
- Late-arriving events after transaction completion are silently lost
- No watermark advancement (unlike the Flink implementation)
