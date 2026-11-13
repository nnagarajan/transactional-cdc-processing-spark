# Tasks

## 1. Compatibility baseline

- [ ] 1.1 Before any refactor, add a golden test fixture. Run the integration test's input set through the current pipeline, then record the resulting `order_stream` rows and the `orders_current` rows produced by `ScdType1Merger` as JSON under `src/test/resources/golden/`. Also record `StructType` JSON for both tables, taken from the existing DDL. Verify: a new `GoldenOutputTest` passes against the current code.

## 2. Model spec core (no Spark session)

- [ ] 2.1 Add the `model.spec` case classes (`SourceSchema`, `JobConfig`, `JobModel`, `EntityModel`, `ModelColumn`, `Cardinality`) and a parser for `models/<job>.schema.conf`. Verify: a unit test parses a hand-written orders schema file into the expected `SourceSchema`.
- [ ] 2.2 Implement conventions: camelCase names, the topic template, the default table names and checkpoint paths. Verify: unit tests cover `ORDER_LINE_ITEMS` → `orderLineItems`, `LINE_ITEM_ID` → `lineItemId`, `APPUSER.ORDER_LINE_ITEMS` → `dev.appuser.order_line_items.json`, and the `<root>_stream` / `<root>_current` / `<root>/<job>/<stage>` defaults.
- [ ] 2.3 Implement the Oracle type mapping with family and column overrides, using the precedence column > family > default. Verify: unit tests cover every row of the mapping table in spec `cdc-model-generator`, the orders legacy overrides (`NUMBER=double`, `DATE`/`TIMESTAMP=string`), a column override winning over a family override, and unsupported `XMLTYPE` failing.
- [ ] 2.4 Implement `ModelResolver` (`JobConfig` + `SourceSchema` + defaults → `JobModel`). It covers exclusions, before-image and version defaults, entity and column renames, child root-key resolution from FKs or override, and cardinality. Verify: unit tests build the orders model and a single-table model.
- [ ] 2.5 Implement the startup validations from spec `cdc-model-config`. Errors are collected, not fail-fast. Cover: missing schema file, unknown table, child without root key, composite root key, duplicate entity names, unknown override column, invalid type, and an unknown starting-offsets key. Verify: one unit test per validation, each asserting the named item appears in the error.
- [ ] 2.6 Implement stream and current `StructType` derivation, including `changeCsn` for versionless entities. Verify: the schemas derived from the orders job config equal the golden `StructType`s from 1.1, ignoring nullability, and a versionless child gets `changeCsn`.
- [ ] 2.7 Implement the DDL writer (stream + current, `{{CATALOG_SCHEMA}}`, no LOCATION). Verify: a unit test checks that the orders DDL text, parsed with `spark.sql` against a local catalog, yields the golden schemas.

## 3. Configuration files and loading

- [ ] 3.1 Add `cdc-defaults.conf`, which `reference.conf` includes. It holds the topic template, naming, type mapping, timestamp format, catalog schema, checkpoint root, metadata topic and dropped-events defaults. Add `jobs/orders.conf` with the legacy overrides from design D2, and bundle copies of it and its schema file as classpath resources. Verify: the `ModelResolver` test loads `jobs/orders.conf` through the real loader.
- [ ] 3.2 Rework `AppConfig.load` to layer system properties > `-Dconfig.file` job conf > bundled orders job > defaults, and to resolve `job.model.schema-file`, including the `classpath:` prefix. Verify: unit tests cover a `-D` override winning, the default orders job being used with no `config.file`, and a file-path schema.
- [ ] 3.3 Rework `KafkaSourceOptions` to key on entity names plus `transaction-metadata`. Verify: `KafkaSourceOptionsTest` is updated for the new keys, and an unknown key fails, naming it.

## 4. Generic sequencer

- [ ] 4.1 Make `TransactionState` generic: per-table `events` and `expected` maps, a tracked-table set, marker counts for untracked tables ignored, and `UnknownTable` for untracked events. Verify: `TransactionStateTest` is rewritten for the orders tables, plus new cases for "Marker covers tables of another job" and "Model with a single table" from the `transaction-buffering` delta.
- [ ] 4.2 Pass tracked tables into `TransactionSequencer` and `TransactionOrderingProcessor`. Emit `SequencerOutput` of kind `TRANSACTION` (`xid`, `csn`, `processedTs`, `events`) instead of joined orders, and remove the `OrderJoiner` call. Verify: `TransactionSequencerTest` and `SequencerOutputEncodingTest` pass with assertions on emitted transactions, including the existing ordering, blocking, replay and purge cases.
- [ ] 4.3 Make purged-buffer drop records generic. `DroppedEvent` gains `jobName` and `bufferedCounts`, the legacy count fields are left null, and `ofPurgedBuffer` takes a per-table count map. Verify: the sequencer test for "Orphan buffer purged" asserts counts ORDERS 1, ORDER_DETAILS 1, ORDER_LINE_ITEMS 3, and a drop test asserts `jobName` is set.
- [ ] 4.4 Update `db/create_dropped_events_table.deltalake.sql` with the new columns and add `db/alter_dropped_events_add_job.sql`. Extend `checkDroppedEventsTable` to require the new columns and name the migration script. Verify: `CdcOutputSinkTest` covers both the missing-column error and the happy path.

## 5. Stage 1 shaping and sink

- [ ] 5.1 Implement `StreamShaper(model)` (design D4): explode, per-entity typed structs with before images, key extraction, group-by with order-preserving arrays, empty arrays for absent entities, and no elements for deletes. Verify: unit tests with a local SparkSession cover the `stream-record-shaping` scenarios "Transaction touching two orders", "Entity absent for a key", "Update with a before image", "Insert" and "Delete in the transaction".
- [ ] 5.2 Add cast-failure counting per table and column per batch, unmapped-column warnings once per run and table, and the configured timestamp format. Verify: tests cover "Malformed number" (null value, count logged), "New column not yet in the model" (one warning), "Column missing from the event", and "GoldenGate timestamp string" with a `timestamp` override.
- [ ] 5.3 Rework `CdcOutputSink` to shape `TRANSACTION` rows with `StreamShaper` and write them to the model's stream table. Keep the same `txnAppId` / `txnVersion` idempotency, and write drops (with `jobName`) after. Verify: `CdcOutputSinkTest` passes, including the retry-after-dropped-write-failure case.
- [ ] 5.4 Add the startup table-schema check (design D7) for the stream table, and remove the implicit table creation. Verify: tests cover "Table created from stale DDL" (named column) and "Missing table".
- [ ] 5.5 Rewire `TransactionalCdcProcessingApp` to build its sources from `JobModel` entities plus the metadata topic, and its tracked tables and output table from the model. Delete `OrderJoiner`, `model/order/*` and `OrderJoinerTest`. Verify: `TransactionOrderingProcessorIntegrationTest` passes against `jobs/orders.conf`, and `GoldenOutputTest` stage 1 output equals the 1.1 golden rows except `dwhProcessedTs`.

## 6. Stage 2 generic merge

- [ ] 6.1 Generalize `ScdType1Merger` into `CurrentStateMerger(model)` (design D5). It generates the flattening, the 1:1 struct and 1:N array merges, the root update maps, `orderingExpr` with `changeCsn` for versionless entities, and the CSN-guarded transaction context. Remove the create-on-missing branch. Verify: `ScdType1MergerTest` passes unchanged in substance against the orders model, and new tests cover "Child table without a VERSION column" and "Orders model unaffected".
- [ ] 6.2 Rewire `ScdType1MergeApp` to read source and target tables and its checkpoint from the job conf, and add the startup schema check for the current table. Verify: `GoldenOutputTest` stage 2 output equals the 1.1 golden `orders_current` rows except `dwhProcessedTs`.

## 7. Model generator

- [ ] 7.1 Add `ojdbc11` as a `provided` dependency and a `model-gen` Maven profile running `exec:java` with `classpathScope=compile`. Verify: `./mvnw -Dquick clean package` succeeds and `jar tf` on the job jar shows no `oracle/jdbc` classes.
- [ ] 7.2 Implement the dictionary reader over `ALL_TAB_COLUMNS`, `ALL_CONSTRAINTS` and `ALL_CONS_COLUMNS` behind a small trait, so it can be tested with an in-memory fake. Derive the root key and cardinality. Verify: unit tests with the fake cover "One-to-one child", "One-to-many child" and "Configured table missing in Oracle".
- [ ] 7.3 Implement `ModelGenerator gen`, which writes `models/<job>.schema.conf` deterministically plus `db/<job>_stream.deltalake.sql` and `db/<job>_current.deltalake.sql`, reading the password from `ORACLE_PASSWORD`. Verify: running against the local Docker Oracle with `db/oracle-ddl.sql` produces a schema file that matches the hand-written fixture from 2.1, and running it twice produces no diff.
- [ ] 7.4 Implement `ModelGenerator check`, which reports added, removed and retyped columns and changed keys, writes nothing, and exits non-zero on drift. Verify: fake-backed tests cover "Column added in Oracle" and "No drift".
- [ ] 7.5 Generate the orders schema and DDL, and replace `db/create_table.deltalake.sql` and `db/create_scd1_table.deltalake.sql` with the generated files, keeping seed order. Verify: `SeedJob` against a local catalog creates tables whose schemas equal the golden schemas.

## 8. Documentation

- [ ] 8.1 Update CLAUDE.md:
  - Architecture: the job model, the generic sequencer, and shaping in the sink
  - Key Classes
  - Running: `-Dconfig.file=jobs/<job>.conf`, and the generator command
  - Known Limitations: star models only, single-column root key, shared metadata topic cost, new checkpoint required

  Verify: the documented run and generator commands match the pom profile and config keys.
- [ ] 8.2 Update `gg-configs/README.md`: entity-keyed starting offsets in the go-live runbook, and a migration section following the design's Migration Plan. Also update `db/README.md` for generated DDL and the `dropped_events` migration. Verify: every config key named in the docs exists in `cdc-defaults.conf` or `jobs/orders.conf`.
- [ ] 8.3 Add a `docs/` or README section, "Adding a new job", with a worked single-table example (config, gen, seed, run). Verify: following it with a scratch `jobs/example.conf` against the local stack produces a stream table.

## 9. Integration check

- [ ] 9.1 Run `./mvnw clean package` with the full test suite. Then run the orders job end to end on the local stack (Kafka, GoldenGate replicats, UC), with stage 1 and stage 2 on new checkpoints. Confirm that `order_stream` and `orders_current` receive rows with unchanged schemas and that `dropped_events` rows carry `jobName = orders`.
