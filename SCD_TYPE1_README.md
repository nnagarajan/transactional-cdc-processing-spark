# SCD Type 1 Merge Application

This application maintains an SCD Type 1 (current state only) table from the SCD Type 2 (historical) `order_stream` table.

## Overview

The SCD Type 1 Merge Application:
- **Reads** from `order_stream` (SCD Type 2) as a Delta Lake stream
- **Transforms** nested `orders` array to flattened order attributes for `orders_current`
- **Deduplicates** arrays: orders by orderId+version, orderDetails by orderId+version, lineItems by lineItemId+version
- **Merges** updates based on entity-level versioning
- **Maintains** `orders_current` (SCD Type 1) with only latest versions
- **Handles** version conflicts at order, order detail, and line item levels independently

## Key Features

### Entity-Level Versioning
Unlike simple SCD Type 1 which replaces entire records, this implementation:
- Updates **order** fields only if incoming `VERSION` > current `VERSION`
- Updates each **order detail** independently based on its `VERSION` and `ORDER_ID`
- Updates each **line item** independently based on its `VERSION` and `LINE_ITEM_ID`

### Duplicate Handling
- Ignores records with same `ORDER_ID` and `VERSION`
- Uses `dwh_processed_ts` as tiebreaker for same-version updates

### Smart Merging
- An order at version 3 with order details at version 2 can coexist
- Incoming order detail at version 3 updates only that detail, not the order
- Preserves before images from the most recent change

## Building

```bash
mvn clean package
```

## Running

### Prerequisites
1. `order_stream` table must exist and be populated
2. Delta Lake packages must be available
3. Checkpoint directory must be accessible

### Local Mode

```bash
spark-submit \
  --class com.technext.demos.txbuffering.ScdType1MergeApp \
  --master local[2] \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar \
  /tmp/delta-tables/order_stream \
  /tmp/delta-tables/orders_current \
  /tmp/spark-checkpoints-scd1
```

### Cluster Mode

```bash
spark-submit \
  --class com.technext.demos.txbuffering.ScdType1MergeApp \
  --master yarn \
  --deploy-mode cluster \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar \
  hdfs:///delta-tables/order_stream \
  hdfs:///delta-tables/orders_current \
  hdfs:///spark-checkpoints-scd1
```

## How It Works

### Data Flow

```
order_stream (SCD Type 2 — nested arrays)
       | (streaming read)
Transform: flatten orders array, extract single orderDetails, dedup lineItems
       |
Version-Aware Merge Logic
       |
orders_current (SCD Type 1 — flattened order + single orderDetails struct + lineItems array)
```

### Merge Logic

For each incoming record from `order_stream`:

1. **Match on ORDER_ID**
   ```sql
   target.ORDER_ID = source.ORDER_ID
   ```

2. **Update Conditions**
   - Order version increased: `source.VERSION > target.VERSION`
   - Order detail version increased: Any detail has higher version than current
   - Line item version increased: Any line item has higher version than current

3. **Update Strategy**
   - **Order fields**: Update only if `source.version > target.version`
   - **Order details struct**: Replace if `source.orderDetails.version > target.orderDetails.version`
   - **Line items array**: Merge each line item by `lineItemId`, keeping higher `version`

### Example Scenario

**Initial State (orders_current)** — flattened order, single orderDetails struct, lineItems array:
```json
{
  "orderId": 248,
  "version": 2,
  "orderRef": "ORD-20260217-001",
  "totalAmount": 25250.0,
  "orderDetails": {"orderId": 248, "version": 1, "shippingMethod": "EXPRESS", "trackingNumber": "TRK-00000248"},
  "lineItems": [
    {"lineItemId": 1, "version": 1, "productId": "PROD-100", "itemQty": 600},
    {"lineItemId": 2, "version": 1, "productId": "PROD-200", "itemQty": 400}
  ]
}
```

**Incoming Update** (transformed from order_stream):
```json
{
  "orderId": 248,
  "version": 2,
  "orderRef": "ORD-20260217-001",
  "totalAmount": 25250.0,
  "orderDetails": {"orderId": 248, "version": 1, "shippingMethod": "EXPRESS", "trackingNumber": "TRK-00000248"},
  "lineItems": [
    {"lineItemId": 1, "version": 2, "productId": "PROD-100", "itemQty": 700}
  ]
}
```

**Result**:
```json
{
  "orderId": 248,
  "version": 2,
  "orderRef": "ORD-20260217-001",
  "totalAmount": 25250.0,
  "orderDetails": {"orderId": 248, "version": 1, "shippingMethod": "EXPRESS", "trackingNumber": "TRK-00000248"},
  "lineItems": [
    {"lineItemId": 1, "version": 2, "productId": "PROD-100", "itemQty": 700},
    {"lineItemId": 2, "version": 1, "productId": "PROD-200", "itemQty": 400}
  ]
}
```

## Querying

### Create the SCD Type 1 Table

```bash
spark-sql \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -f db/create_scd1_table.sql
```

### Query Current State

```bash
spark-sql \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -f db/query_scd1_table.sql
```

## Monitoring

### Check Processing Progress

```sql
-- Count records in both tables
SELECT 'SCD Type 1' as type, COUNT(*) FROM orders_current
UNION ALL
SELECT 'SCD Type 2' as type, COUNT(*) FROM delta.`/tmp/delta-tables/order_stream`;
```

### Verify Updates

```sql
-- Recent updates
SELECT ORDER_ID, VERSION, dwh_processed_ts
FROM orders_current
ORDER BY dwh_processed_ts DESC
LIMIT 10;
```

### Check for Duplicates

```sql
-- Should return empty
SELECT ORDER_ID, COUNT(*)
FROM orders_current
GROUP BY ORDER_ID
HAVING COUNT(*) > 1;
```

## Troubleshooting

### Issue: Duplicate ORDER_IDs

**Cause**: Merge logic not working correctly

**Solution**: Check Delta Lake configuration and merge conditions

### Issue: Old versions persisting

**Cause**: Version comparison logic issue

**Solution**: Verify VERSION fields are properly populated in source data

### Issue: Performance degradation

**Cause**: Large number of small files

**Solution**: Run OPTIMIZE on the target table

```sql
OPTIMIZE orders_current;
```

## Best Practices

1. **Run both applications simultaneously**
   - SCD Type 2: `TransactionalCdcProcessingApp`
   - SCD Type 1: `ScdType1MergeApp`

2. **Use separate checkpoint locations**
   - Prevents checkpoint conflicts

3. **Monitor lag between tables**
   - Compare record counts and timestamps

4. **Regular maintenance**
   ```sql
   OPTIMIZE orders_current;
   VACUUM orders_current RETAIN 168 HOURS;
   ```

## Comparison: SCD Type 1 vs Type 2

| Feature | SCD Type 2 (order_stream) | SCD Type 1 (orders_current) |
|---------|---------------------------|-------------------------------|
| **Purpose** | Historical tracking | Current state |
| **Records per Order** | Multiple (all versions) | One (latest) |
| **Order Structure** | Nested `orders` array | Flattened top-level fields |
| **Order Details** | `ARRAY<STRUCT>` | Single `STRUCT` (1:1) |
| **Line Items** | `ARRAY<STRUCT>` | `ARRAY<STRUCT>` (1:N) |
| **Use Case** | Audit, time travel, analysis | Operational queries, reports |
| **Storage** | Higher (keeps all history) | Lower (current only) |
| **Query Performance** | Slower (more records) | Faster (fewer records) |
| **Version Tracking** | Complete history | Latest version only |

## See Also

- [Main Application README](README.md)
- [Database Scripts](db/)
- [Implementation Guide](IMPLEMENTATION.md)
- [Oracle Source Schema](oracle-schema.md)
