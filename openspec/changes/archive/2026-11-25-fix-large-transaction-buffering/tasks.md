# Tasks

## 1. Summary and store abstraction

- [x] 1.1 Extract the completeness rules from `TransactionState` into a shared helper over expected and received per-table counts (`isComplete`, `isOverfilled`, `progress`, the over-filled error log), and make `TransactionState` delegate to it. Verify: `TransactionStateTest` and `OrderJoinerTest` pass unchanged.
- [x] 1.2 Add the `TransactionSummary` bean (xid, csn, metadata, received count per tracked table, `eventCount`) using the shared helper. Verify: a new `TransactionSummaryTest` covers complete, missing events, no marker, an over-filled table and the progress text, mirroring the matching `TransactionStateTest` cases. An `Encoders.bean(classOf[TransactionSummary])` round trip in `SequencerOutputEncodingTest` keeps the marker and its counts.
- [x] 1.3 Reshape `SequencerStore` per design D4: summary, event and position operations replace the buffer operations, and the key-format helpers go in the `TransactionSequencer` companion. Verify: `./mvnw -Dquick clean package` compiles once 1.4 and 2.x are done. Key helpers get unit tests for `xid:csn#i` and `xid:csn|TABLE|pos`, and `csnOf` still parses `txs` keys.
- [x] 1.4 Rework `InMemorySequencerStore`: separate maps, per-operation counters, and a serialization copy on put and get. Verify: a store test shows that mutating a summary after `getSummary` without `putSummary` leaves the stored value unchanged.

## 2. Sequencer on the new layout

- [x] 2.1 Rewrite `acceptData` and `acceptMarker` per design D3. The precedence is replay, then unknown table, then duplicate `pos`. A null `pos` is never a duplicate. Verify: every buffering, duplicate, replay and drop-record case in `TransactionSequencerTest` passes with unchanged assertions, including `duplicateEventIsRecorded` and `eventForAnUntrackedTableIsRecorded`.
- [x] 2.2 Rewrite `emitReady`: check completeness from the summary, read `events` 0 … n-1 once into a `TransactionState`, and remove the transaction through one helper that deletes its events, positions, summary and queue entry. Verify: the ordering, blocking and same-CSN tests pass. A new test emits a transaction with interleaved ORDERS and ORDER_LINE_ITEMS events and asserts per-table buffered order. Another asserts the store holds no entries for the transaction after emission.
- [x] 2.3 Rewrite `purgeBelow` over `summaryKeys`: build the drop record from summary counts and remove entries with the same helper. Verify: the purge tests (`historyWithoutMarkersIsRemovedOnceTheCursorPassesIt`, `largeHistoryIsRemovedOverSeveralSteps`, `purgedBufferIsOneSummaryRowWithCounts` and others) pass unchanged, and the store is empty for purged transactions.
- [x] 2.4 Take `trackBlocked` progress from the head's summary. Verify: `alertAfterThresholdWithNoNewInput` asserts the same progress text as before.
- [x] 2.5 Add the large-transaction tests from design D7 (55,000 events in 11 batches of 5,000, plus a 5-event control) that assert on the in-memory store's counters. Also add a test that a duplicate `pos` in a 50,000-event transaction is dropped with zero event reads. Verify: `./mvnw test -Dtest=TransactionSequencerTest` passes, and the 55,000-event test finishes in seconds.

## 3. Spark adapter

- [x] 3.1 Replace `buffers` in `TransactionOrderingProcessor` with the `txs`, `events` and `positions` `MapState`s (`Encoders.bean` for `TransactionSummary` and `DataChangeEvent`, `Encoders.scalaLong` for positions), and implement `StateHandleStore` over them. Verify: `TransactionOrderingProcessorIntegrationTest` passes on a fresh checkpoint.
- [x] 3.2 Add an integration test that runs one transaction across several micro-batches, restarts the query on the same checkpoint mid-transaction, and completes it. Verify: the order is emitted once, with all events, after the restart.
- [x] 3.3 Find out what Spark 4.1 does when this jar starts on a checkpoint written by the previous layout (generate one with the old jar in the test, or check in a small fixture). If Spark doesn't refuse with a clear error, add the guard from design D6. Verify: a test starts on the old-layout checkpoint and asserts the query fails before emitting anything, with a message naming the migration section.

## 4. Documentation

- [x] 4.1 Update `PERFORMANCE-ESTIMATES.md`: rewrite "Large transactions slow down sharply" with the new linear cost (about 3 small writes per event, and each event read once at emission). Restate the 550 and 55,000 / 75,000–150,000 event rows, and keep the note that a large transaction still delays the transactions behind it for as long as it takes to arrive. Verify: the numbers match the operation counts asserted in 2.5.
- [x] 4.2 Add "Migrating to per-event buffering state" to `gg-configs/README.md` with the Migration Plan steps from the design (cut-point start, `earliest` fallback, rollback). Verify: every config key it names exists in `reference.conf` or `transactional-cdc-processing.conf`.
- [x] 4.3 Update `STATE-STORE-DEBUG.md` (state variable table, `seq_*` views, and the in-flight, orphan and head-progress queries) for `txs`, `events` and `positions`, and the State paragraph and Known Limitations in `CLAUDE.md`. Verify: the SQL queries run against a checkpoint produced by the integration test.

## 5. Integration check

- [x] 5.1 Run `./mvnw clean package` with the full test suite. Then run stage 1 on the local stack on a new checkpoint, with a bulk insert of at least 10,000 orders in one transaction. Verify: the transaction's rows appear in `order_stream` within one or two triggers after its last event is read, `orders_current` follows, and no `has blocked emission` error is logged once the events have arrived. *(Full build and tests passed, 101 tests. The end-to-end run on the local stack was not performed here; the user will run it after archiving.)*
