# Tasks

## 1. Runtime upgrade

- [x] 1.1 In `pom.xml`, set Spark 4.1.1, Delta 4.2.0 (`delta-spark_${spark.binary.version}_${spark.scala.version}`), UC connector 0.5.0 (`unitycatalog-spark_${spark.binary.version}_${spark.scala.version}`), Scala 2.13.17 and Jackson 2.20.0, with new properties `spark.binary.version` and `unitycatalog.version`; verify `./mvnw -Dquick clean package` succeeds
- [x] 1.2 Update `TransactionOrderingProcessorIntegrationTest` for Spark 4.1: import `org.apache.spark.sql.execution.streaming.runtime.MemoryStream` and supply an implicit `SparkSession`; verify `./mvnw clean package` passes all tests
- [x] 1.3 Update the `--packages` coordinates in `README.md`, `QUICKSTART.md`, `IMPLEMENTATION.md`, `SCD_TYPE1_README.md`, `CLAUDE.md` and `db/README.md`; verify `grep` finds no `delta-spark_2.13:4.0.0`, `spark-sql-kafka-0-10_2.13:4.0.0` or `unitycatalog-spark_2.13:0.4.0` outside `medium/` and `openspec/`

## 2. Unity Catalog setup

- [x] 2.1 Create the storage root `/opt/delta/warehouse/managed`, the catalog `mymanaged` (`--storage_root file:///opt/delta/warehouse/managed`) and the schema `mymanaged.argus1` (`--storage_root file:///opt/delta/warehouse/managed/argus1`); verify with `bin/uc ... catalog get` and `schema get` that both show a `STORAGE_ROOT`
- [x] 2.2 Verify managed tables on the new runtime: `CREATE TABLE` without `LOCATION` (with `delta.feature.catalogManaged`), then `INSERT` and `MERGE`, run from `spark-sql` 4.1.1 with Delta 4.2.0 / UC 0.5.0; check `DESCRIBE DETAIL` shows a `__unitystorage` location, then drop the test tables
- [x] 2.3 Verify a streaming `toTable` append and a `foreachBatch` `DeltaTable.merge` into managed tables from `spark-shell` 4.1.1; check both queries end with no exception and the row and commit counts are non-zero, then drop the test tables

## 3. Managed tables in the seed scripts and configs

- [x] 3.1 In `db/create_table.deltalake.sql` and `db/create_scd1_table.deltalake.sql`, replace `LOCATION '{{TABLE_LOCATION}}/<table>'` with `TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')`; verify `grep` finds no `LOCATION` or `{{TABLE_LOCATION}}` in `db/*.deltalake.sql`
- [x] 3.2 Remove `job.table-location` from `seed-job.conf` and from `SeedJob`, and set `job.catalog-schema = "mymanaged.argus1"`; verify `grep` finds no `table-location` or `TABLE_LOCATION` in `src/` or `db/`
- [x] 3.3 Set `spark.unity-catalog.catalog-name = "mymanaged"` in `reference.conf`, and use schema-qualified names (`argus1.order_stream`, `argus1.orders_current`) in `transactional-cdc-processing.conf` and `scd-type1-merge.conf`; verify by reading the configs
- [x] 3.4 Move the three views from the create scripts into `db/views.sql` as `CREATE OR REPLACE TEMPORARY VIEW` over `mymanaged.argus1.*`, and list the file in `db/README.md`; verify `grep` finds no `CREATE OR REPLACE VIEW` in `db/*.deltalake.sql`, and that `SeedJob` completes without the "does not support views" error
- [x] 3.5 Run `SeedJob`; verify it exits 0, and the UC API lists `order_stream` and `orders_current` in `mymanaged.argus1` as `MANAGED` with `__unitystorage` locations
- [ ] 3.6 Run `db/views.sql` in a `spark-sql` 4.1.1 session against the seeded tables (UC server running); verify each of `order_stream_current`, `order_stream_changes` and `orders_current_view` can be queried

## 4. Documentation

- [x] 4.1 Write `UNITY_CATALOG.md` (build on JDK 17, start with `-p 7071`, storage roots, seed, Spark and `spark-submit` settings, managed vs external, tested version table, troubleshooting); verify the documented `catalog create`, `schema create` (with `--storage_root`) and `table list` commands were run against the local server
- [x] 4.2 Write `STATE-STORE-DEBUG.md`: the `statestore` data source, the `spark.streams` workaround, `file://` paths in Zeppelin, and queries for `cursor`/`queue`/`buffers` and the blocking head; verify every SQL statement in the file runs against a copy of `/tmp/spark-checkpoints-ordered`
- [x] 4.3 Rewrite `PIPELINE_DIAGRAMS.md` and `IMPLEMENTATION.md` for the sequencer, managed tables and the new versions; verify `grep` finds no `mycatalog` or Spark 4.0 coordinates in either file, and `flatMapGroupsWithState` appears only as the earlier version in the checkpoint note
- [ ] 4.4 Render-check the Mermaid diagrams in `PIPELINE_DIAGRAMS.md` (Markdown preview or `mmdc`); verify all six diagrams render without a parse error
- [ ] 4.5 Update the "Running" section of `CLAUDE.md`, which still shows positional arguments the app ignores; verify the command matches `IMPLEMENTATION.md`

## 5. Integration

- [ ] 5.1 Run `TransactionalCdcProcessingApp` and `ScdType1MergeApp` against Kafka with the managed tables for at least several transactions; verify `order_stream` and `orders_current` receive the expected records and the apps log no commit errors
- [ ] 5.2 Stop the UC server while `TransactionalCdcProcessingApp` runs; verify the batch fails, and that after restarting the server and the app it resumes from its checkpoint with no gap or duplicate in `order_stream`
- [ ] 5.3 Update the Zeppelin interpreter to Spark 4.1 packages and register `mymanaged` (space-separated `%spark.conf` lines), then restart it; verify `spark.catalog.listTables("mymanaged.argus1")` lists both tables
- [ ] 5.4 Decide on the superseded `mycatalog` and `catalog` catalogs and the files in `/opt/delta/warehouse/{order_stream,orders_current}`; verify the chosen cleanup with `bin/uc ... catalog list`
