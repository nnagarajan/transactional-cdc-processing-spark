# Tasks

## 1. Compatibility baseline

- [x] 1.1 Before any refactor, add a golden test fixture. Run the integration test's input set through the current pipeline, then record the resulting `order_stream` rows and the `orders_current` rows produced by `ScdType1Merger` as JSON under `src/test/resources/golden/`. Also record `StructType` JSON for both tables, taken from the existing DDL. Verify: a new `GoldenOutputTest` passes against the current code.

## 2. Model spec core (no Spark session)

- [x] 2.1 Add the `model.spec` case classes (`JobConfig`, `JobModel`, `EntityModel`, `ModelColumn` with an optional source, `Cardinality`) and a parser for the `job.model.entities` and `job.output` config sections. `key` accepts a string or a list; `root-key` accepts a string or a map. Verify: a unit test parses a hand-written orders job config into the expected `JobConfig`, including a derived column without `source`, and parses a compound-key model (root key `[REGION, ORDER_NO]`, child `root-key` map with different column names, a `many` child with a three-column key).
- [x] 2.2 Implement type parsing with `DataType.fromDDL` (type required on every column) and the checkpoint default `<checkpoint root>/<job>/<stage>`. Verify: unit tests cover `double`, `string`, `decimal(20,4)`, `timestamp`, an invalid type failing with the column named, and the checkpoint default for two jobs.
- [x] 2.3 Implement `ModelResolver` (`JobConfig` + defaults → `JobModel`): roles, cardinality, version columns, root key columns, each child's root-key mapping in root key order (expanding the single-name shorthand), own key columns, and before-image defaults. Verify: unit tests build the orders model, a single-table model, and the compound-key model from 2.1.
- [x] 2.4 Implement the startup validations from spec `cdc-model-config`, collecting errors instead of failing on the first: root count, missing entity fields, missing column name or type, invalid type, duplicate entity names or tables, duplicate column names or sources, key/root-key/version naming an undeclared column, a key listing a column twice, child without root key or cardinality, `many` child without key, a root-key map not covering exactly the root's key columns, a single-name root key against a compound root key, derived column without a transform, unloadable transform class, unknown starting-offsets key. Verify: one unit test per validation, each asserting that the named item appears in the error.
- [x] 2.5 Implement stream and current `StructType` derivation, including one top-level stream column per root key column, derived columns, and `changeCsn` for versionless entities. Verify: the schemas derived from the orders job config equal the golden `StructType`s from 1.1, ignoring nullability; a versionless child gets `changeCsn`; the compound-key model's stream schema has top-level `region` and `orderNo`.
- [x] 2.6 Implement the DDL writer (stream + current, `{{CATALOG_SCHEMA}}`, no LOCATION). Verify: a unit test checks that the orders DDL text, parsed with `spark.sql` against a local catalog, yields the golden schemas.

## 3. Configuration files and loading

- [x] 3.1 Add `cdc-defaults.conf`, included by `reference.conf`, with the timestamp format, catalog schema, checkpoint root, metadata topic and dropped-events defaults. Add `jobs/orders.conf`, listing every column of the three entities explicitly with the types that reproduce today's tables (design D2), and bundle a copy as a classpath resource. Verify: the `ModelResolver` test loads `jobs/orders.conf` through the real loader and its schemas equal the golden schemas.
- [x] 3.2 Rework `AppConfig.load` to layer system properties > `-Dconfig.file` job conf > bundled orders job > defaults. Verify: unit tests cover a `-D` override winning and the default orders job being used with no `config.file`.
- [x] 3.3 Rework `KafkaSourceOptions` to key on entity names plus `transaction-metadata`. Verify: `KafkaSourceOptionsTest` is updated for the new keys, and an unknown key fails, naming it.

## 4. Generic sequencer

- [x] 4.1 Make the per-table parts of the sequencer generic (design D3):
  - `TransactionSummary`: a `received` map per tracked table instead of the three counters
  - `TransactionCompletion`: compare over the tracked-table set, ignoring marker counts for untracked tables
  - `TransactionState`: group events by table in a map at emission
  - an event for an untracked table returns `UnknownTable`

  Verify: `TransactionSummary` and `TransactionState` tests are rewritten for the orders tables, plus new cases for "Marker covers tables of another job" and "Model with a single table" from the `transaction-buffering` delta. The 55,000-event linear-buffering test still passes.
- [x] 4.2 Pass tracked tables into `TransactionSequencer` and `TransactionOrderingProcessor`. Emit `SequencerOutput` of kind `TRANSACTION` (`xid`, `csn`, `processedTs`, `events`) instead of joined orders, one row per emitted transaction, and remove the `OrderJoiner` call. Verify: `TransactionSequencerTest` and `SequencerOutputEncodingTest` pass with assertions on emitted transactions, including the existing ordering, blocking, replay and purge cases.
- [x] 4.3 Make purged-buffer drop records generic. `DroppedEvent` gains `jobName` and `bufferedCounts`, the legacy count fields are left null, and `ofPurgedBuffer` takes a per-table count map. Verify: the sequencer test for "Orphan buffer purged" asserts counts ORDERS 1, ORDER_DETAILS 1, ORDER_LINE_ITEMS 3, and a drop test asserts `jobName` is set.
- [x] 4.4 Update `db/create_dropped_events_table.deltalake.sql` with the new columns and add `db/alter_dropped_events_add_job.sql`. Extend `checkDroppedEventsTable` to require the new columns and name the migration script. Verify: `CdcOutputSinkTest` covers both the missing-column error and the happy path.

## 5. Stage 1 shaping and sink

- [x] 5.1 Implement `StreamShaper(model)` (design D4): explode, per-entity typed structs with before images, typed nulls for derived columns, per-part key extraction through each child's root-key mapping with conversion to the root key types, group-by on all key parts with order-preserving arrays, empty arrays for absent entities, and no elements for deletes. Verify: unit tests with a local SparkSession cover the `stream-record-shaping` scenarios "Transaction touching two orders", "Entity absent for a key", "Update with a before image", "Insert" and "Delete in the transaction", plus "Compound root key" from the same spec and "Root key under a different name in the child" and "Key values converted to the root key's type" from `cdc-model-config`.
- [x] 5.2 Add cast-failure counting per table and column per batch, undeclared-column warnings once per run and table, and the configured timestamp format. Verify: tests cover "Malformed number" (null value, count logged), "New column not yet in the config" (one warning), "Column missing from the event", and "GoldenGate timestamp string" with a `timestamp` column.
- [x] 5.3 Add the `StreamTransform` trait and hook (design D6): load the class named by `job.output.transform` at startup, apply it after shaping, and compare its output schema with the stream schema before writing. Verify: tests cover "Derived column filled in", "Transform changes the schema" (batch fails naming the column, nothing written) and "No transform".
- [x] 5.4 Rework `CdcOutputSink` to shape `TRANSACTION` rows with `StreamShaper`, apply the transform, and write them to the model's stream table. Keep the same `txnAppId` / `txnVersion` idempotency, and write drops (with `jobName`) after. Verify: `CdcOutputSinkTest` passes, including the retry-after-dropped-write-failure case.
- [x] 5.5 Add the startup table-schema check (design D7) for the stream table, and remove the implicit table creation. Verify: tests cover "Table created from stale DDL" (named column) and "Missing table".
- [x] 5.6 Rewire `TransactionalCdcProcessingApp` to build its sources from `JobModel` entities plus the metadata topic, and its tracked tables and output table from the model. Delete `OrderJoiner`, `model/order/*` and `OrderJoinerTest`. Verify: `TransactionOrderingProcessorIntegrationTest` passes against `jobs/orders.conf`, and `GoldenOutputTest` stage 1 output equals the 1.1 golden rows except `dwhProcessedTs`.

## 6. Stage 2 generic merge

- [x] 6.1 Generalize `ScdType1Merger` into `CurrentStateMerger(model)` (design D5). It generates the flattening, the merge condition over all root key columns, the 1:1 struct and 1:N array merges (by each child's declared key columns), the root update maps, `orderingExpr` with `changeCsn` for versionless entities, and the CSN-guarded transaction context from each key's highest-CSN row. Remove the create-on-missing branch. Verify: `ScdType1MergerTest` passes unchanged in substance against the orders model, and new tests cover "Child table without a VERSION column", "Orders model unaffected", "Compound root key" (two rows for `(EU, 7)` and `(US, 7)`), and element dedup by a three-column child key.
- [x] 6.2 Rewire `ScdType1MergeApp` to read source and target tables and its checkpoint from the job conf, and add the startup schema check for the current table. Verify: `GoldenOutputTest` stage 2 output equals the 1.1 golden `orders_current` rows except `dwhProcessedTs`.

## 7. DDL command

- [x] 7.1 Add the `CdcModelTool ddl` entry point (design D7), runnable with `spark-submit --class` and `scripts/cdc-model.sh`. It writes `db/<job>_stream.deltalake.sql` and `db/<job>_current.deltalake.sql` from a job config, with no Spark session and no Oracle access. Verify: running it twice for `jobs/orders.conf` produces no diff.
- [x] 7.2 Replace `db/create_table.deltalake.sql` and `db/create_scd1_table.deltalake.sql` with the files written for the orders job, keeping seed order. Verify: `SeedJob` against a local catalog creates tables whose schemas equal the golden schemas.

## 8. Documentation

- [x] 8.1 Update CLAUDE.md:
  - Architecture: the job model, the generic sequencer, shaping and the transform hook in the sink
  - Key Classes
  - Running: `-Dconfig.file=jobs/<job>.conf`, and the DDL command
  - Known Limitations: star models only, every child must carry every root key column, explicit column lists with no drift check against Oracle, shared metadata topic cost, new checkpoint required

  Verify: the documented run and DDL commands match the entry point and config keys.
- [x] 8.2 Update `gg-configs/README.md`: entity-keyed starting offsets in the go-live runbook, and a migration section following the design's Migration Plan. Also update `db/README.md` for the written DDL and the `dropped_events` migration. Verify: every config key named in the docs exists in `cdc-defaults.conf` or `jobs/orders.conf`.
- [ ] 8.3 Add a `docs/` or README section, "Adding a new job", with a worked single-table example: the job config with its column mapping, an optional transform class, the DDL command, seeding, and running. Include a child that maps a compound root key to differently named columns, and a first-run check that children land in rows with their root (no child-only rows for keys whose root exists). Verify: following it with a scratch `jobs/example.conf` against the local stack produces a stream table.

## 9. Integration check

- [ ] 9.1 Run `./mvnw clean package` with the full test suite. Then run the orders job end to end on the local stack (Kafka, GoldenGate replicats, UC), with stage 1 and stage 2 on new checkpoints. Confirm that `order_stream` and `orders_current` receive rows with unchanged schemas and that `dropped_events` rows carry `jobName = orders`. Then run `./mvnw -Pe2e verify -Dit.test=CorrectnessOrderingIT` and confirm it passes unchanged.
