# Proposal

## Why

The pipeline's tables should be governed by Unity Catalog the way they are on Databricks: created by name, with the catalog choosing where the data lives. On Spark 4.0.0 / Delta 4.0.0 / UC connector 0.4.0 that isn't possible:
- `CREATE TABLE` without `LOCATION` fails with `DELTA_UNSUPPORTED_CATALOG_OWNED_TABLE_CREATION`.
- The `delta.feature.catalogManaged` property fails with `DELTA_UNSUPPORTED_FEATURES_IN_CONFIG`.

Moving to the versions in Databricks Runtime 18 (Spark 4.1, Delta 4.2.0) removes both blockers. It also fixes three mismatches that kept the apps from finding the tables at all:
- unqualified table names resolved to a schema that doesn't exist
- views the connector can't create
- table paths hard-coded in the seed scripts

## What Changes

- **BREAKING** Runtime moves from Spark 4.0.0 / Delta 4.0.0 to:
  - Spark 4.1.1
  - Delta Lake 4.2.0 (`delta-spark_4.1_2.13`)
  - Unity Catalog Spark connector 0.5.0 (`unitycatalog-spark_4.1_2.13`)

  Scala 2.13.17 and Jackson 2.20.0 move with it, to match Spark 4.1.1. The apps must run on a Spark 4.1.x runtime.
- **BREAKING** `order_stream` and `orders_current` become Unity Catalog **managed** tables in `mymanaged.argus1`, whose catalog and schema have storage roots. The create scripts drop `LOCATION` and set `TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')`. The seed job no longer takes a table location.
- The default catalog becomes `mymanaged`. The apps reference schema-qualified names, `argus1.order_stream` and `argus1.orders_current`.
- The views move out of the seed scripts into `db/views.sql` as session-scoped temporary views. The connector doesn't support catalog views before Spark 4.2.
- Tests adapt to Spark 4.1. `MemoryStream` moved to `org.apache.spark.sql.execution.streaming.runtime` and now takes an implicit `SparkSession`.
- Documentation:
  - `UNITY_CATALOG.md`: install, storage roots, managed vs external tables, and tested version results
  - `STATE-STORE-DEBUG.md`: reading sequencer state with the `statestore` data source
  - Rewritten `PIPELINE_DIAGRAMS.md` and `IMPLEMENTATION.md`
  - Updated package coordinates across the docs

## Capabilities

### New Capabilities
- `catalog-tables`: where the pipeline's Delta tables live and how they're created, named and recreated in Unity Catalog, including the view and availability constraints of catalog-managed tables.

### Modified Capabilities
- None. No main specs exist yet; `ordered-transaction-emission` is still in progress.

## Impact

- **Code:**
  - `pom.xml`: versions, Spark-suffixed artifact names, `spark.binary.version` and `unitycatalog.version` properties
  - `SeedJob`: drop `job.table-location`
  - `TransactionOrderingProcessorIntegrationTest`: `MemoryStream` import and implicit
- **Config:**
  - `reference.conf`: `catalog-name = "mymanaged"`
  - `seed-job.conf`: `catalog-schema = "mymanaged.argus1"`, no table location
  - `transactional-cdc-processing.conf`, `scd-type1-merge.conf`: schema-qualified table names
- **SQL:**
  - `db/create_table.deltalake.sql`, `db/create_scd1_table.deltalake.sql`: managed tables, no views
  - new `db/views.sql`
- **Unity Catalog server:** catalog `mymanaged` (storage root `file:///opt/delta/warehouse/managed`) and schema `argus1` (storage root `.../managed/argus1`). Every write to a managed table commits through the server, so it must be running while the apps write.
- **Operations:**
  - Rerunning the seed job deletes table data, because dropping a managed table removes its files. App checkpoints must be cleared after a reseed.
  - Zeppelin and `spark-submit` must use Spark 4.1.x and the new package coordinates.
- **Superseded setup:** the old external tables in `mycatalog.argus1` (`/opt/delta/warehouse/{order_stream,orders_current}`) are no longer used.
