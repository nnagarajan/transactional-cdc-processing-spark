# Proposal

## Why

The stage 1 sequencer's cost grows with the square of a transaction's size. For every data event, `TransactionSequencer.acceptData` reads the transaction's whole `TransactionState`, adds the event, and writes the whole state back. `TransactionState.containsPos` also scans every buffered event of that table to find duplicates.

A 55,000-op bulk transaction therefore costs about 1.5 billion event serializations. Because output is in strict commit order, every transaction behind it waits (PERFORMANCE-ESTIMATES.md, "Large transactions slow down sharply"). Chunking bulk loads at the source helps, but it cannot cover bulk changes the team doesn't control. The pipeline itself must handle large transactions in linear time.

## What Changes

- Buffered state is split into three maps under the global key, replacing the per-transaction `TransactionState` blob:
  - a small per-transaction summary (marker, per-table received counts, event count)
  - each event, written once
  - a `pos` index for de-duplication by key lookup instead of a scan
- Accepting an event costs a constant number of small state reads and writes, whatever the size of its transaction.
- Completeness is decided from the summary alone, without reading any events.
- Emission and purge read each of a transaction's events once, then delete the transaction's entries.
- `TransactionSequencer` keeps using the `SequencerStore` abstraction, reshaped for the new maps, so it stays testable without Spark. The in-memory test store counts reads and writes.
- **BREAKING:** the state layout changes, so stage 1 must start from a new checkpoint location. Stage 2 (`ScdType1MergeApp`) and its checkpoint are unaffected. The migration is documented in `gg-configs/README.md`.
- `PERFORMANCE-ESTIMATES.md` is updated with the new cost.

All observable behavior stays the same:
- global commit order
- exact per-table counts
- de-duplication by `pos`
- replay, duplicate-marker and purge rules
- blocked alerts
- `dropped_events` records
- `order_stream` output

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `transaction-buffering`: adds a requirement that buffering cost is linear in transaction size. Accepting an event costs the same regardless of how many events its transaction has already buffered, and each event is written to state once. The existing requirements are unchanged.

## Impact

- **Code:**
  - `processor/SequencerStore.scala`: new store operations and a summary type.
  - `processor/TransactionSequencer.scala`: `acceptData`, `acceptMarker`, `emitReady`, `purgeBelow` and `trackBlocked` use the summary and the event map.
  - `processor/TransactionOrderingProcessor.scala`: three `MapState`s replace `buffers`.
  - `processor/TransactionState.scala`: stays as the assembled transaction handed to `OrderJoiner`; its completeness logic is shared with the summary.
- **Tests:**
  - `InMemorySequencerStore` gains operation counters and copies values on put and get.
  - A new large-transaction test.
  - Existing `TransactionSequencerTest`, `TransactionStateTest`, `OrderJoinerTest`, `SequencerOutputEncodingTest` and `TransactionOrderingProcessorIntegrationTest` must pass.
- **Operations:** stage 1 must be redeployed on a new checkpoint location (see `design.md`, Migration Plan).
- **Docs:** `PERFORMANCE-ESTIMATES.md`, `gg-configs/README.md` and the CLAUDE.md state description.
- **Unchanged:** dependencies, the Kafka topics, the GoldenGate configuration, and the Delta tables.
