# Delta Lake Table Scripts

This directory contains SQL scripts for managing the Delta Lake tables used by the Transactional CDC Processing application.

## Table Overview

### order_stream (SCD Type 2)

Denormalized order records with nested arrays for orders, order details, and line items — created from transactionally consistent CDC event processing. Its schema follows the orders job config, `jobs/orders.conf`; Unity Catalog chooses its storage location.

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

- **orders_stream.deltalake.sql**, **orders_current.deltalake.sql** - Create the orders job's order_stream and orders_current tables. Written from `jobs/orders.conf` by `scripts/cdc-model.sh ddl jobs/orders.conf`; don't edit them by hand. Every job gets a `<job>_stream` and `<job>_current` script the same way ([docs/ADDING-A-JOB.md](../docs/ADDING-A-JOB.md)), and each stage refuses to start if its table doesn't match its job config
- **create_dropped_events_table.deltalake.sql** - Creates the dropped_events audit table (one row per discarded input, with the job and the reason), shared by every job. It has no `DROP TABLE`, so re-seeding keeps the audit history. SeedJob runs it first: scripts run in file-name order
- **alter_dropped_events_add_job.sql** - Adds `jobName` and `bufferedCounts` to a dropped_events table created before configurable job models. Run it once by hand (it is not a `*.deltalake.sql`, so SeedJob skips it); see "Migrating to configurable job models" in [gg-configs/README.md](../gg-configs/README.md)
- **truncate_tables.sql** - Empties order_stream, orders_current and dropped_events (`DELETE FROM`, definitions kept). Use it through `scripts/reset-pipeline.sh`, which also deletes both checkpoints so the pipeline reprocesses from the start
- **query_table.sql** - Sample queries for the SCD Type 2 table
- **query_scd1_table.sql** - Sample queries for the SCD Type 1 table
- **table_maintenance.sql** - Maintenance operations (vacuum, optimize, etc.)
- **schema_evolution.sql** - Examples of schema evolution operations
- **oracle-ddl.sql** - Oracle source table DDL
- **simulate-grouped-markers.sql** - Oracle script that reproduces the grouped-delivery marker bug (`GROUPTRANSOPS 1000`, several CSNs in one Replicat transaction); steps are in its header
- **e2e/e2e-setup.sql**, **e2e/e2e-cleanup.sql** - Oracle ground-truth log tables for the end-to-end suites, and removal of a run's test data ([docs/E2E-TESTING.md](../docs/E2E-TESTING.md))
- **views.sql** - Temporary views over both tables (not run by SeedJob; the Unity Catalog Spark connector has no view support before Spark 4.2)

## Usage

### Using spark-sql CLI

```bash
spark-sql \
  --packages io.delta:delta-spark_4.1_2.13:4.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -f db/orders_stream.deltalake.sql
```

### Using PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Delta Lake Management") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sql(open('db/orders_stream.deltalake.sql').read())
```

### Using SeedJob

`SeedJob` runs every `*.deltalake.sql` script in `db/`, in file-name order, with `{{CATALOG_SCHEMA}}` replaced by `job.catalog-schema` (default `mymanaged.argus1`). The applications never create or alter the stream and current tables: create them with these scripts first.
