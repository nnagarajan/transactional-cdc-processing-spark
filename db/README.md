# Delta Lake Table Scripts

This directory contains SQL scripts for managing the Delta Lake tables used by the Transactional CDC Processing application.

## Table Overview

### order_stream (SCD Type 2)

Denormalized order records with nested arrays for orders, order details, and line items — created from transactionally consistent CDC event processing.

**Location**: Configurable via application parameter (e.g., `/tmp/delta-tables/order_stream`)

**Schema**:
- `orderId` (DOUBLE) - Primary order identifier
- `xid` (STRING) - Transaction ID from Oracle
- `csn` (STRING) - Commit sequence number
- `dwhProcessedTs` (STRING) - Processing timestamp
- `orders` (ARRAY<STRUCT>) - Order attributes with before images
- `orderDetails` (ARRAY<STRUCT>) - Order shipping/fulfillment details with before images
- `lineItems` (ARRAY<STRUCT>) - Order line items with before images

### orders_current (SCD Type 1)

Current state table with flattened order attributes, single orderDetails struct (1:1), and lineItems array (1:N).

**Schema**:
- `orderId` (DOUBLE) - Primary order identifier (unique key)
- `orderRef`, `version`, `orderStatus`, `totalAmount`, etc. - Flattened order fields
- `orderBefore` (STRUCT) - Before image from last order change
- `orderDetails` (STRUCT) - Current order details (single struct, 1:1)
- `lineItems` (ARRAY<STRUCT>) - Current line items (array, 1:N)

## Scripts

- **create_table.deltalake.sql** - Creates the order_stream Delta Lake table
- **create_scd1_table.deltalake.sql** - Creates the orders_current (SCD Type 1) table
- **query_table.sql** - Sample queries for the SCD Type 2 table
- **query_scd1_table.sql** - Sample queries for the SCD Type 1 table
- **table_maintenance.sql** - Maintenance operations (vacuum, optimize, etc.)
- **schema_evolution.sql** - Examples of schema evolution operations
- **oracle-ddl.sql** - Oracle source table DDL

## Usage

### Using spark-sql CLI

```bash
spark-sql \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -f db/create_table.deltalake.sql
```

### Using PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Delta Lake Management") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sql(open('db/create_table.deltalake.sql').read())
```

### Using spark-submit

The application automatically creates the Delta Lake table on first write. These scripts are provided for manual table management and querying.

## Table Location

The default table location can be customized via the application's third argument:

```bash
spark-submit ... \
  localhost:9092 \
  /tmp/spark-checkpoints \
  /path/to/delta-tables/order_stream
```
