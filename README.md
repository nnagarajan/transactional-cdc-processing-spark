# Transactional CDC Processing (Spark)

Spark Structured Streaming implementation for transactionally consistent CDC event processing. This implementation demonstrates joining multiple CDC streams (orders, order_details, order_line_items) with transaction-level consistency using Oracle GoldenGate CDC format.

## Features

- **Transaction-Aware Buffering**: Buffers CDC events until all events in a transaction arrive
- **Event Count Terminal Logic**: Uses transaction metadata event counts to determine completion
- **RocksDB State Management**: Persistent state storage for durability and scalability
- **Delta Lake Output**: Writes denormalized records to Delta Lake tables with ACID guarantees

## Architecture

The application processes 4 Kafka topics:
1. `dev.appuser.orders.json` - Order records
2. `dev.appuser.order_details.json` - Order shipping/fulfillment details
3. `dev.appuser.order_line_items.json` - Order line items
4. `dev.transaction_metadata_json` - Transaction metadata with event counts

Events are grouped by transaction (xid:csn), buffered in RocksDB state, and emitted as denormalized records when all expected events arrive.

## Building

```bash
mvn clean package
```

Or using Maven wrapper:

```bash
./mvnw clean package
```

## Running

### Local Mode

```bash
spark-submit \
  --class com.technext.demos.txbuffering.TransactionalCdcProcessingApp \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar \
  localhost:9092 \
  /tmp/spark-checkpoints \
  /tmp/delta-tables/order_stream
```

### With SPARK_HOME

```bash
$SPARK_HOME/bin/spark-submit \
  --class com.technext.demos.txbuffering.TransactionalCdcProcessingApp \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar \
  localhost:9092 \
  /tmp/spark-checkpoints \
  /tmp/delta-tables/order_stream
```

## Input Topics

### CDC Event Format (orders, order_details, order_line_items)

```json
{
  "table": "APPUSER.ORDERS",
  "op_type": "U",
  "op_ts": "2025-07-07 23:33:54.000000",
  "current_ts": "2025-07-07 23:36:00.631000",
  "pos": "00000000110009047521",
  "csn": "334516829",
  "xid": "1342848513.2.24.5354",
  "before": { ... },
  "after": { ... }
}
```

### Transaction Metadata Format

```json
{
  "xid": "1342848513.2.24.5354",
  "csn": "334516829",
  "tx_ts": "2025-07-07 23:33:54.000000",
  "event_count": 6,
  "data_collections": [
    {"data_collection": "ORDERS", "event_count": 2},
    {"data_collection": "ORDER_DETAILS", "event_count": 2},
    {"data_collection": "ORDER_LINE_ITEMS", "event_count": 2}
  ]
}
```

## Output

The application writes to a Delta Lake table (`order_stream`) with nested arrays for all entities, each containing before images for change tracking:

```json
{
  "xid": "1342848513.2.24.5354",
  "csn": "334516829",
  "dwhProcessedTs": "2026-02-17T18:10:23.456Z",
  "orderId": 248.0,
  "orders": [
    {
      "orderId": 248.0,
      "orderRef": "ORD-20260217-001",
      "version": 2.0,
      "orderDate": "2026-02-17",
      "orderTs": "2026-02-17 10:15:30",
      "orderStatus": "CONFIRMED",
      "orderType": "STANDARD",
      "totalAmount": 25250.0,
      "currency": "USD",
      "customerId": "CUST-001",
      "shippingAddressId": "ADDR-001",
      "before": { "orderStatus": "PENDING", "version": 1.0, ... }
    }
  ],
  "orderDetails": [
    {
      "orderId": 248.0,
      "shippingMethod": "EXPRESS",
      "trackingNumber": "TRK-00000248",
      "version": 1.0,
      "before": { ... }
    }
  ],
  "lineItems": [
    {
      "lineItemId": 1.0,
      "orderId": 248.0,
      "productId": "PROD-100",
      "itemQty": 600.0,
      "version": 1.0,
      "before": { ... }
    }
  ]
}
```

**Schema Features:**
- **Top-Level Identifiers**: `orderId`, `xid`, `csn`, `dwhProcessedTs` at the top level
- **Nested Orders Array**: Order attributes stored as `ARRAY<STRUCT<...>>` with `before` inside each element
- **Before Images**: Each entity (orders, order details, line items) includes a `before` field showing previous values
- **Version Tracking**: `version` fields track record versions for all entities independently
- **Change Detection**: Before images enable CDC change type detection (INSERT vs UPDATE)

## How It Works

1. **Ingestion**: All 4 topics are read as Kafka streams
2. **Parsing**: JSON events are parsed into structured format
3. **Grouping**: Events are grouped by transaction key (xid:csn)
4. **Buffering**: CDC events are buffered in RocksDB state
5. **Completion Detection**: Transaction metadata provides expected event counts
6. **Joining**: When complete, events are joined by ORDER_ID
7. **Emission**: Denormalized records are written to Delta Lake table

## State Management

The application uses RocksDB for state storage, configured via:

```
spark.sql.streaming.stateStore.providerClass=
  org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
```

State is persisted to the checkpoint location and survives application restarts.

## Delta Lake Table Management

The application writes to Delta Lake tables, providing ACID transactions, time travel, and schema evolution capabilities.

See the [db/](db/) directory for:
- Table creation scripts
- Query examples
- Maintenance operations (VACUUM, OPTIMIZE)
- Schema evolution examples

To query the output table:

```bash
spark-sql \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -f db/query_table.sql
```

## Documentation

- **[PIPELINE_DIAGRAMS.md](PIPELINE_DIAGRAMS.md)** - Mermaid diagrams illustrating both pipelines and data flows
- **[IMPLEMENTATION.md](IMPLEMENTATION.md)** - Detailed architecture, configuration options, and troubleshooting guide
- **[SCD_TYPE1_README.md](SCD_TYPE1_README.md)** - SCD Type 1 merge application documentation
- **[oracle-schema.md](oracle-schema.md)** - Oracle source table schema definitions

## SCD Type 1 Maintenance

This repository includes a second application to maintain SCD Type 1 (current state) from the SCD Type 2 (historical) data:

**[ScdType1MergeApp](SCD_TYPE1_README.md)** - Maintains `orders_current` table with entity-level versioning

Key features:
- Reads from `order_stream` (SCD Type 2) as a stream
- Updates order, order details, and line items independently based on their versions
- Handles cases where order version might not increase but order detail/line item versions do
- Ignores duplicate records with same version

See [SCD_TYPE1_README.md](SCD_TYPE1_README.md) for detailed documentation.

## Comparison with Flink Implementation

This Spark implementation achieves similar transactional consistency guarantees as the Flink-based [transactional-cdc-processing](../transactional-cdc-processing) but uses:
- Spark Structured Streaming instead of Flink DataStream API
- Event count-based completion instead of watermark advancement
- RocksDB state store instead of Flink keyed state
- `flatMapGroupsWithState` instead of custom operators
