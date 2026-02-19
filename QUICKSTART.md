# Quick Start Guide

## Overview

This Spark application joins 4 CDC topics with transaction-level consistency:
- `dev.appuser.orders.json`
- `dev.appuser.order_details.json`
- `dev.appuser.order_line_items.json`
- `dev.transaction_metadata_json`

## Build

```bash
mvn clean package
```

## Run

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

## How It Works

1. **Reads** all 4 Kafka topics as streaming DataFrames
2. **Groups** events by transaction key (`xid:csn`)
3. **Buffers** events in RocksDB state until transaction completes
4. **Checks** completion using event counts from transaction metadata
5. **Joins** all events by `ORDER_ID` when transaction is complete
6. **Writes** denormalized records to Delta Lake table

## Example Transaction

Given this transaction metadata:
```json
{
  "xid": "1342848513.2.24.5354",
  "csn": "334516829",
  "event_count": 6,
  "data_collections": [
    {"data_collection": "ORDERS", "event_count": 2},
    {"data_collection": "ORDER_DETAILS", "event_count": 2},
    {"data_collection": "ORDER_LINE_ITEMS", "event_count": 2}
  ]
}
```

The application will:
1. Buffer 2 ORDERS events
2. Buffer 2 ORDER_DETAILS events
3. Buffer 2 ORDER_LINE_ITEMS events
4. When all 6 events arrive, join them and emit

## Output Format

```json
{
  "orderId": 248.0,
  "xid": "1342848513.2.24.5354",
  "csn": "334516829",
  "dwhProcessedTs": "2026-02-17T18:10:23.456Z",
  "orders": [
    {
      "orderId": 248.0,
      "orderRef": "ORD-20260217-001",
      "version": 2.0,
      "orderStatus": "CONFIRMED",
      "totalAmount": 25250.0,
      "currency": "USD",
      "before": { "orderStatus": "PENDING", "version": 1.0, ... }
    }
  ],
  "orderDetails": [ ... ],
  "lineItems": [ ... ]
}
```

## Monitoring

The application prints progress to stdout:
```
Transaction 1342848513.2.24.5354:334516829 buffering. Progress: Orders: 1/2, Details: 1/2, LineItems: 0/2
Transaction 1342848513.2.24.5354:334516829 is complete. Progress: Orders: 2/2, Details: 2/2, LineItems: 2/2
```

## Configuration

Key Spark configurations:
- **State Store**: RocksDB (configured automatically)
- **Checkpoint Location**: Specified as 2nd argument
- **Delta Table Path**: Specified as 3rd argument
- **Kafka Bootstrap Servers**: Specified as 1st argument

## Troubleshooting

### Issue: State not persisting
- Ensure checkpoint location is accessible and has write permissions
- Use HDFS or S3 for production deployments

### Issue: Events not joining
- Check transaction metadata has correct table names
- Verify xid and csn match between CDC events and metadata
- Enable debug output in code (see IMPLEMENTATION.md)

### Issue: Memory errors
- Increase executor memory: `--executor-memory 4G`
- Tune RocksDB cache: Add Spark conf for state store

## Querying Output

To query the Delta Lake output table:

```bash
spark-sql \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -f db/query_table.sql
```

See the [db/](db/) directory for more query examples and table management scripts.

## Next Steps

See [IMPLEMENTATION.md](IMPLEMENTATION.md) for:
- Detailed architecture
- State management details
- Monitoring and metrics
- Production deployment guide
