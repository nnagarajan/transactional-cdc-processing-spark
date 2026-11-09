# Tasks

## 1. Source delivery contract (GoldenGate / Kafka config)

- [x] 1.1 Remove `IGNOREDELETES` from `gg-configs/replicat/rep3.prm`; verify with `grep -c IGNOREDELETES gg-configs/replicat/rep3.prm` returning 0
- [x] 1.2 Set `gg.handler.kafkahandler.blockingSend=true` in `gg-configs/replicat/rep3.props`, and `acks=all` and `enable.idempotence=true` in `gg-configs/replicat/kp.properties`; verify by grepping all three settings
- [x] 1.3 Document in `gg-configs/README.md` the single-partition requirement for `dev.transaction_metadata_json`, the `kafka-topics` create command (`--partitions 1`), and the rep3 reposition steps from design.md Migration Plan; verify the README section lists each Migration Plan step
- [x] 1.4 Add `IGNOREDELETES` to `gg-configs/extract/ext04.prm` before its `TABLE` statements (design.md D14); verify with `grep -n IGNOREDELETES gg-configs/extract/ext04.prm` showing it above the first `table` line

## 2. Completeness in `TransactionState`

- [x] 2.1 Create `src/test/scala/com/technext/demos/txbuffering/processor/` and confirm `./mvnw test` runs and passes with a placeholder JUnit 5 test written in Scala (test compilation works)
- [x] 2.2 Make `TransactionState.addEvent` ignore an event whose `pos` matches an already-buffered event of the same table, and report whether it was accepted; verify with `TransactionStateTest` cases: duplicate `pos` ignored, and two distinct `pos` for the same row both kept
- [x] 2.3 Change `isComplete` to require exact per-table equality and log an error when a count exceeds expected; verify with `TransactionStateTest` cases: exact counts → complete; fewer → incomplete; zero declared → complete; no marker → incomplete; a delete event counts toward completion
- [x] 2.4 Confirm `OrderJoiner.joinTransaction` emits no entity record for delete events, and add a test covering a transaction with one update and one delete on line items; verify the test passes

## 3. Sequencer core (pure Scala, no Spark runtime)

- [x] 3.1 Define the `SequencerStore` trait and the `Cursor` state class (fields per design.md D3), plus an in-memory store for tests, under `processor/`; verify it compiles with `./mvnw -Dquick clean package`
- [x] 3.2 Implement `TransactionSequencer` marker handling per design.md D4: reject `partition != 0` with an exception, drop duplicates (already marked, `csn < C`, or `csn == C` with xid already emitted) with WARN for the `csn < C` case, and enqueue accepted markers in `marker_offset` order within a batch; verify with `TransactionSequencerTest` cases for each rule, including two markers at the same CSN with different xids
- [x] 3.3 Implement data-event handling per D4 (drop replays for emitted transactions, buffer otherwise, data-before-marker kept); verify with `TransactionSequencerTest`: a replayed event after emission creates no buffer; data before the marker is buffered and not queued
- [x] 3.4 Implement the emission walk per D6, including the `C` / `emittedAtLastCsn` update; verify with tests: T2 complete before T1 is held until T1 completes, then both are emitted in order; three complete transactions emit in one call in queue order; an incomplete head blocks everything behind it
- [x] 3.5 Implement blocked tracking per D7 (`blockedSinceMs` set and cleared, and an alert decision once the threshold is exceeded, with head key, progress and queue depth); verify with tests using an injected clock: no alert before the threshold, an alert after it with no new input, and a reset after the head emits

## 4. Spark integration

- [x] 4.1 Extract the existing `Row` → `TransactionMetadata` and `Row` → `DataChangeEvent` blocks from `processTransaction` into helpers, unchanged; verify `./mvnw -Dquick clean package` compiles
- [x] 4.2 Add `marker_partition` and `marker_offset` to `parseTransactionMetadata` output, and as null columns in `parseDataChangeEvents`, so the union schemas match; verify the app compiles and the union succeeds in the 4.4 integration test
- [x] 4.3 Implement the `StatefulProcessor[String, Row, OrderStream]` over `MapState` / `ValueState` handles (design.md D3), backing `SequencerStore`, with timer management per D7 in `handleInputRows` and `handleExpiredTimer`
- [x] 4.4 Replace `groupByKey(xid:csn)` + `flatMapGroupsWithState` with constant-key `transformWithState` (`TimeMode.ProcessingTime()`, `OutputMode.Append()`), and set the RocksDB provider with `spark.conf.set` before the query starts; verify with a `MemoryStream`-based integration test on a local session: out-of-order completion is emitted in marker order across two micro-batches, and a marker from partition 1 fails the query
- [x] 4.5 Add an integration test that stops and restarts the query on the same checkpoint while T1 is blocked and T2 is complete, then verifies T2 is not emitted before T1 and replayed events for emitted transactions are dropped
- [x] 4.6 Add `job.ordering.blocked-alert-threshold`, `job.ordering.check-interval` and `job.ordering.trigger-interval` (design.md D12) to `transactional-cdc-processing.conf`, read them in the app, and change the default checkpoint location to a new path; verify the app logs the loaded values at startup

## 5. SCD Type 1 merge

- [x] 5.1 Remove the `source.version IS NOT NULL` condition from `whenNotMatched` in `ScdType1MergeApp`; verify with a local-Delta test: a child-only batch for an unknown order inserts a placeholder row, and a later parent batch fills its attributes while keeping a higher-version child
- [x] 5.2 Guard `xid`, `csn` and `dwhProcessedTs` in both `whenMatched` clauses with a numeric CSN comparison (design.md D10); verify with tests: an older CSN doesn't overwrite, and `csn=100` overwrites `csn=99`
- [x] 5.3 Change the intra-batch window ordering to `version DESC NULLS LAST, CAST(csn AS DECIMAL(38,0)) DESC`; verify with a test where two rows share an order version and the higher-CSN row wins
- [x] 5.4 Update `SCD_TYPE1_README.md` to describe placeholder rows (null `version`) and the CSN guard; verify the doc mentions both

## 6. Documentation and end-to-end check

- [x] 6.1 Update `README.md` (How It Works, State Management) and `CLAUDE.md` (Architecture, Known Limitations: halts on a stuck transaction, single-task assembly, unbounded buffers for marker-less data, new checkpoint required) to match the new design; verify no remaining references to `xid:csn` grouping or `flatMapGroupsWithState` with `grep -n "flatMapGroupsWithState" README.md CLAUDE.md`
- [x] 6.2 Run `./mvnw clean package` and confirm all unit and integration tests pass
- [ ] 6.3 Run locally against Kafka with a single-partition metadata topic: produce two transactions where the later one's data arrives first, and confirm with a Delta history query that the earlier transaction's rows are committed first (or in the same commit) in `order_stream`
