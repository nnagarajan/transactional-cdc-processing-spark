# Design

## Context

See proposal.md for motivation. The state before this change:
- **Runtime:** Spark 4.0.0, `delta-spark_2.13:4.0.0`, `unitycatalog-spark_2.13:0.4.0`.
- **Tables:** external, at `/opt/delta/warehouse/<table>`, registered in `mycatalog.argus1` (no storage root).
- **Apps:** referenced unqualified table names.
- **Unity Catalog server:** built from source (0.8.0-SNAPSHOT) on port 7071, with managed tables enabled by default.

All results below were measured on this machine against that server.

## Goals / Non-Goals

**Goals:**
- Catalog-managed tables created without `LOCATION`, matching Databricks Runtime 18's Spark and Delta versions.
- The existing pipeline (`transformWithState` sequencer, Delta streaming sink, `foreachBatch` MERGE) unchanged in behaviour.

**Non-Goals:**
- Catalog views, which need Spark 4.2+ with this connector.
- Credential vending or cloud storage. The tables are local files.
- Migrating data from the old external tables. They are reseeded.

## Decisions

### D1. Versions: Spark 4.1.1 + Delta 4.2.0 + UC connector 0.5.0
Tested combinations:

| Spark / Delta / connector | Managed `CREATE` / `INSERT` / `MERGE` |
|---|---|
| 4.0.0 / 4.0.0 / 0.4.0 | Fails: `DELTA_UNSUPPORTED_CATALOG_OWNED_TABLE_CREATION` |
| 4.1.1 / 4.2.0 / 0.5.0 | Works |
| 4.1.1 / 4.4.1 / 0.7.0 | Works |

- **Why 4.2.0:** it's the Delta version in Databricks Runtime 18.
- **Why connector 0.5.0:** it's the release built against Delta 4.2.0, per its `build.sbt`.
- **Alternative:** Delta 4.4.1 + UC 0.7.0 also works, but moves further from the Databricks Runtime 18 versions.

The artifacts are now Spark-suffixed (`_4.1_2.13`). The connector stopped publishing `unitycatalog-spark_2.13` after 0.4.x. `pom.xml` gains `spark.binary.version = 4.1` and `unitycatalog.version = 0.5.0`. Scala (2.13.17) and Jackson (2.20.0) match the jars Spark 4.1.1 ships.

### D2. Managed tables via `delta.feature.catalogManaged`
The connector rejects a `CREATE TABLE` without `LOCATION` unless the table sets `delta.feature.catalogManaged` (or the older `catalogOwned-preview`). With it set:
- `SeedJob` created both tables as `MANAGED`, under `.../managed/argus1/__unitystorage/schemas/<schema-id>/tables/<table-id>`.
- A streaming `toTable` append (240 rows over 7 commits) and a `foreachBatch` `DeltaTable.merge` (3 commits) both ran without error, against throwaway managed tables.

**Alternative:** keep external tables (`LOCATION` plus a catalog without a root). Rejected because the goal is catalog-chosen locations, matching Databricks. It remains the fallback; `UNITY_CATALOG.md` describes how to revert.

### D3. Storage roots on both the catalog and the schema
The `mymanaged` catalog's root is `file:///opt/delta/warehouse/managed`; schema `argus1`'s is `.../managed/argus1`. The schema root takes precedence. A root can't be added to an existing catalog or schema, so they were created new rather than altering `mycatalog`. They're under `/opt/delta/warehouse` because that directory is user-owned (no sudo).

### D4. Schema-qualified names resolved against a default catalog
- **How:** `spark.unity-catalog.catalog-name = mymanaged` becomes `spark.sql.defaultCatalog`, and the apps use `argus1.<table>`.
- **Why:** this keeps the catalog in one setting.
- **Alternative:** fully qualified `mymanaged.argus1.<table>` in each app config. It works, but repeats the catalog name in three places.

### D5. Views as temporary views in `db/views.sql`
The connector's `UCSingleCatalog` has no view support before Spark 4.2 ("Catalog ... does not support views"). The seed job stopped at the first view, leaving the second table uncreated. Moving the views to `db/views.sql`, which the seed job doesn't run, as `CREATE OR REPLACE TEMPORARY VIEW` over fully qualified names keeps them usable per session.

### D6. Spark 4.1 test API changes
- `MemoryStream` moved to `org.apache.spark.sql.execution.streaming.runtime`.
- `MemoryStream` now takes an implicit `SparkSession` instead of `SQLContext`.
- No application code changed. The explicit processing-time trigger kept from Spark 4.0 still works on 4.1; whether 4.1 still needs it wasn't re-examined.

### D7. Reading the sequencer state for debugging
`read_statestore` is Databricks-only. The OSS `statestore` data source reads `transformWithState` state (`stateVarName` = `buffers` | `queue` | `cursor`), with two Spark 4.1 quirks:
- `spark.streams` must be touched first. Otherwise the RocksDB reader fails with `RpcEndpointNotFoundException` for `StateStoreCoordinator`.
- Zeppelin's Hadoop config sets `fs.defaultFS = hdfs://localhost:9411`, so paths need `file://`.

Documented in `STATE-STORE-DEBUG.md`, whose SQL statements were all run against a real checkpoint.

## Risks / Trade-offs

- **[Catalog server is a write dependency]:** every managed-table commit goes through it. → Documented. The app fails the batch and resumes from its checkpoint after restart.
- **[Reseed deletes data]:** `DROP TABLE` on a managed table removes its files. → Documented in `UNITY_CATALOG.md`, along with clearing the checkpoints.
- **[Catalog-managed commits are new in OSS Delta]:** INSERT, MERGE, streaming append and `foreachBatch` MERGE were tested, but not a long-running pipeline. → Task 5.1 (live pipeline run).
- **[Spark 4.1 runtime everywhere]:** jars built for `_4.1_2.13` don't load on Spark 4.0. → The docs' `--packages` lines are updated. Zeppelin must run Spark 4.1.x with the new packages.
- **[Data not readable without the catalog]:** managed paths are opaque IDs. → Accepted. Use `DESCRIBE DETAIL` to find a path.

## Migration Plan

1. Build Unity Catalog from source on JDK 17. Start the server with `bin/start-uc-server -p 7071`.
2. Create `/opt/delta/warehouse/managed`, then the catalog `mymanaged` and schema `argus1` with storage roots.
3. Build the project (`./mvnw clean package`) and run `SeedJob`.
4. Delete the old checkpoints, then start the apps on Spark 4.1.x.

Rollback: restore the previous `pom.xml` and configs, re-add `LOCATION` to the create scripts, point the configs at a catalog without a storage root, and reseed.
