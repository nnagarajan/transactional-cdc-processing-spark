# Tasks

## 1. Table and model

- [x] 1.1 Add `db/create_dropped_events_table.deltalake.sql` with the D1 columns, partitioned by `droppedDate`, with `delta.feature.catalogManaged` and 30-day log retention, and list it in `db/README.md`. Verify `SeedJob` creates `mymanaged.argus1.dropped_events` as `MANAGED` (UC server running) and `DESCRIBE TABLE` shows every column
- [x] 1.2 Add the `DroppedEvent` bean (fields matching the table, `@BeanProperty`, as with the other models) and the `SequencerOutput` envelope (`kind`, `order`, `dropped`). Verify `./mvnw -Dquick clean package` compiles and `Encoders.bean` builds a schema for both

## 2. Sequencer drops

- [x] 2.1 Change `TransactionState.addEvent` to return `Added`, `DuplicatePos` or `UnknownTable`, and update its callers and `TransactionStateTest`. Verify the existing tests pass, with one new assertion per result
- [x] 2.2 Add `dropped` to `SequencerResult` and fill it in `acceptMarker` (`REPLAY_BELOW_CURSOR`, `REPLAY_ALREADY_EMITTED`, `DUPLICATE_MARKER`), `acceptData` (the replay reasons, `DUPLICATE_EVENT`, `UNKNOWN_TABLE`) and `purgeBelow` (`NO_MARKER_BELOW_CURSOR` with buffered counts per table, which means loading each purged value). Keep the existing log lines. Verify with a `TransactionSequencerTest` case per spec scenario, each asserting the reason, identity and `lastEmittedCsn`
- [x] 2.3 Add `summary` replay mode: fold the step's `REPLAY_*` drops by `(xid, csn, reason)` into one row with `droppedCount`. Verify with a test that 350 replayed events of one transaction produce one row with count 350, and that non-replay reasons stay one row each

## 3. Parsing and raw drops

- [x] 3.1 In `CdcJsonParsing`, add `source_topic`, `source_partition`, `source_offset` and the raw `json` to every row, and turn data rows with a null `xid`/`csn` into `RAW_DROP` rows (`UNPARSEABLE` or `MISSING_IDS`, per D4). Verify with new `CdcJsonParsingTest` cases: truncated JSON, a missing `csn`, and a valid event keeping its coordinates
- [x] 3.2 Make an unparseable metadata message produce one `RAW_DROP` row instead of vanishing in `posexplode`, and markers without ids inside an array produce `MISSING_IDS` rows. Verify with `CdcJsonParsingTest` cases for a truncated metadata message (offset kept) and an array holding one bad marker next to two good ones
- [x] 3.3 Add `RawDropInput` to `CdcRows` and the sequencer: it becomes a drop record and never touches state. Remove the app's xid/csn `.filter`. Verify with a sequencer test that a raw drop leaves buffers, queue and cursor unchanged

## 4. Envelope output and sink

- [x] 4.1 Change `TransactionOrderingProcessor` to output `SequencerOutput` rows (orders, then drop records), with `ordersOf` and `droppedOf` helpers, and update `TransactionOrderingProcessorIntegrationTest` to read `ordersOf`. Verify its 3 tests pass unchanged otherwise
- [x] 4.2 Replace `toTable` with the D5 `foreachBatch`: persist the batch; append orders to `order_stream`, then drops (stamped with `batchId`, `droppedTs` and `droppedDate`) to `dropped_events`; use `txnAppId = <queryId>-<table>` from `<checkpoint>/metadata` and `txnVersion = batchId`. Add `job.output.dropped-events.{enabled, table-name, replay-detail}` to the config. Verify `./mvnw clean package` passes
- [x] 4.3 Add an integration test that writes to two local Delta tables through the D5 sink and runs the same batch twice. Verify the second run adds no rows to either table, and that with `enabled = false` only the orders table is written
- [x] 4.4 Document the table, its reasons, the config keys and example queries (all drops for one `xid:csn`; counts per reason per day) in `IMPLEMENTATION.md`, and add the table to `CLAUDE.md` and `PIPELINE_DIAGRAMS.md`. Verify each example query runs against the table created in 1.1

## 5. Integration

- [ ] 5.1 Resume test: on a copy of `/tmp/spark-checkpoints-ordered`, run the old jar, stop it, run the new jar on the same checkpoint, then the old jar again. Verify `order_stream` has no gap or duplicate across each switch
- [ ] 5.2 Live run against local Kafka with the UC server up: restart `kc` from an earlier trail position to cause a replay, and publish one malformed message to the metadata topic. Verify `dropped_events` holds `REPLAY_*` rows for the replayed events and one `UNPARSEABLE` row with the right offset, and that `order_stream` matches a run with `enabled = false`
