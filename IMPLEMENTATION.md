# Transactional CDC Processing with Spark Structured Streaming

This implementation demonstrates transaction-aware joining of multiple CDC streams using Spark Structured Streaming with RocksDB state management.

## Architecture

The application processes CDC events from 4 Kafka topics:

1. **dev.appuser.orders.json** - Order records
2. **dev.appuser.order_details.json** - Order shipping/fulfillment details
3. **dev.appuser.order_line_items.json** - Order line items
4. **dev.transaction_metadata_json** - Transaction metadata with event counts

### Key Components

#### Model Classes

- **DataChangeEvent**: Generic CDC event from Oracle GoldenGate format
- **TransactionMetadata**: Transaction metadata with expected event counts
- **Order, OrderDetail, OrderLineItem**: Domain models for the business entities
- **OrderStream**: Denormalized output record with nested orders/orderDetails/lineItems arrays

#### Processing Logic

1. **TransactionState**: Maintains buffered events per transaction (xid:csn)
   - Tracks expected counts from metadata
   - Buffers CDC events by table type
   - Checks for transaction completion

2. **OrderJoiner**: Joins buffered events into denormalized records
   - Groups events by ORDER_ID
   - Creates nested structures with order details and line items

3. **TransactionBufferingProcessor**: Stateful Spark processor
   - Uses `flatMapGroupsWithState` for transaction-scoped buffering
   - Keyed by `xid:csn` (transaction ID : commit sequence number)
   - Emits only when all expected events arrive
   - Leverages RocksDB for persistent state storage

## Building

```bash
cd transactional-cdc-processing-spark
mvn clean package
```

## Running

### Local Mode

```bash
spark-submit \
  --class com.technext.demos.txbuffering.TransactionalCdcProcessingApp \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar \
  localhost:9092 \
  /tmp/spark-checkpoints
```

### Cluster Mode

```bash
spark-submit \
  --class com.technext.demos.txbuffering.TransactionalCdcProcessingApp \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-cores 2 \
  --executor-memory 4G \
  --driver-memory 2G \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar \
  kafka-broker1:9092,kafka-broker2:9092 \
  hdfs://namenode:8020/checkpoints/transactional-cdc
```

## How It Works

### 1. Event Ingestion

All 4 Kafka topics are read as streaming DataFrames and parsed into a unified schema:

```
xid, csn, table, op_type, before, after, event_type, ...
```

### 2. Grouping by Transaction

Events are grouped by transaction key: `xid:csn`

Example: `1342848513.2.24.5354:334516829`

### 3. Stateful Buffering

For each transaction:

- **Metadata event arrives**: Sets expected counts
  ```
  ORDERS: 2 events expected
  ORDER_DETAILS: 2 events expected
  ORDER_LINE_ITEMS: 2 events expected
  ```

- **CDC events arrive**: Buffered in RocksDB state
  ```
  Order event 1/2 buffered
  Order event 2/2 buffered
  OrderDetail event 1/2 buffered
  ...
  ```

- **Completion check**: When all expected events arrive
  ```
  if (order_count == 2 && detail_count == 2 && line_item_count == 2) {
    emit joined records
    clear state
  }
  ```

### 4. Joining and Output

When complete:
1. Group events by ORDER_ID
2. Attach all related order details and line items
3. Emit to output: `order_stream`

Output format:
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

## State Management

### RocksDB Configuration

The application uses RocksDB as the state store backend for:
- **Durability**: State persists across restarts via checkpoints
- **Performance**: Efficient storage for large state
- **Scalability**: Handles high cardinality transaction keys

State is keyed by `xid:csn` and includes:
- Transaction metadata
- Buffered CDC events for orders, order_details, order_line_items
- Event counts and completion status

### Checkpoint Management

Checkpoints include:
- RocksDB state snapshots
- Kafka offset tracking
- Processing metadata

Location: Specified via command-line argument (HDFS/S3/local filesystem)

## Key Features

1. **Transaction Consistency**: Emits records only when all events in a transaction arrive
2. **Event-Driven Completion**: Uses transaction metadata event_count for terminal logic
3. **Persistent State**: RocksDB ensures state survives failures
4. **Scalability**: Parallel processing per transaction key
5. **Idempotency**: State removal after emission prevents duplicates

## Limitations and Considerations

1. **State Growth**: Long-running transactions may accumulate state
   - Consider adding state TTL or timeout logic
   - Monitor state store size

2. **Late Arrivals**: Events arriving after transaction completion are lost
   - Ensure proper ordering at source
   - Consider implementing grace periods

3. **Memory**: State must fit in executor memory + RocksDB
   - Tune `spark.executor.memory`
   - Adjust RocksDB cache settings

4. **Parallelism**: Limited by transaction key cardinality
   - More unique transactions = better parallelism
   - Consider hash-based key distribution if needed

## Monitoring

Key metrics to monitor:

- State store size and growth rate
- Processing latency per transaction
- Incomplete transaction count
- Kafka consumer lag
- Checkpoint duration

## Troubleshooting

### State Store Issues

```bash
# Check state store location
ls -lh <checkpoint-location>/state/

# View state store metadata
spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0
```

### Debugging

Enable detailed logging:
```bash
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=<log-directory>
```

Add to code:
```java
System.out.println("Transaction " + txKey + " progress: " + txState.getProgress());
```

## Comparison with Flink Implementation

| Feature | Flink (transactional-cdc-processing) | Spark (this implementation) |
|---------|--------------------------------------|------------------------------|
| State Backend | Keyed state | RocksDB via state store |
| Watermarks | Custom LSN-based | Not used (event-driven) |
| Parallelism | Currently 1 | Configurable by key |
| API | DataStream v2 custom operators | Structured Streaming |
| Terminal Logic | Watermark advancement | Event count matching |
| Language | Java records | Java beans |
