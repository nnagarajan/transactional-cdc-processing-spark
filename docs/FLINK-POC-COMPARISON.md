# Comparison with the Flink Transactional CDC Proof of Concept

This compares this pipeline with Gunnar Morling's Flink proof of concept, [streaming-examples/transactional-cdc-processing](https://github.com/gunnarmorling/streaming-examples/tree/main/transactional-cdc-processing). This project started as a Spark port of it. The comparison is based on commit `ee4e06c` (2026-01-28) of that repository: the job wiring (`DataStreamV2Job`), the operators in `join/`, and its design note `transactional-cdc-processing-poc.md`.

## Summary

Both pipelines keep **global commit order**. Each runs one sequencer at parallelism 1 that releases a transaction only after every earlier transaction is complete. They get there differently:

- **The Flink PoC** turns transaction metadata into custom watermarks based on the Postgres commit LSN. Each table's watermark advances only while the next transaction's events for that table are all there, and the join releases data at the *minimum* watermark of its inputs.
- **This pipeline** queues markers in Kafka offset order from the single-partition metadata topic, which is commit order. It emits from the head of the queue while the head is complete.

Where they differ, this pipeline is stricter on consistency: a transaction's output is written in one Delta commit, duplicates are removed, stuck transactions are alerted on, and state is cleaned up and checkpointed. The PoC is a proof of concept and lists most of these gaps itself. Both share the same two weaknesses: no parallelism, and a buffering cost that grows with the square of the transaction size.

## How each one orders transactions

| | Flink PoC | This pipeline |
|---|---|---|
| Source | Postgres through Debezium: two data topics and the `dbserver1.transaction` topic | Oracle through GoldenGate: three data topics (`kc`) and `dev.transaction_metadata_json` (`rep3` with the custom marker formatter) |
| Order signal | Commit LSN, which always increases in Postgres | Kafka offset of the marker, then its index in a grouped message; CSN is used only to drop replays |
| Completeness | `WatermarkInjector` counts events per commit LSN for each table and moves the watermark forward only while `count == countFor(table)`. It stops at the first incomplete transaction, so the watermark moves in commit order | `TransactionState.isComplete`: exact per-table counts against the marker |
| Combining tables | `TxAwareTwoInputNonBroadcastJoinProcessOperator` takes the minimum watermark of both inputs before flushing | One buffer per transaction holds every table, so no cross-input step is needed |
| Emission | The join flushes each watermark in LSN order; `TxAwareAggregationFunction` emits an order when a watermark equal to its last commit LSN arrives | `emitReady` walks the queue from the head while transactions are complete |
| Parallelism | 1 (PoC limitation: per-partition transaction metadata from Debezium would be needed) | 1 (one global grouping key, see [Known Limitations](../CLAUDE.md)) |
| **Global commit order** | **Yes**, at parallelism 1 | **Yes**; the query fails if markers come from more than one partition |

## Where the designs differ

### 1. Several output records from one transaction

The PoC emits each output record (one purchase order with its lines) separately to Kafka with `DeliveryGuarantee.AT_LEAST_ONCE`. A transaction that changes two purchase orders becomes two messages, so a reader can see one without the other. The PoC's design note lists this under "Current Limitations".

This pipeline emits all `OrderStream` rows of a transaction from one sequencer step, so they land in the same micro-batch and the same Delta commit (`CdcOutputSink`, idempotent per batch id). Readers see all of a transaction or none of it.

### 2. Duplicate events and markers

The PoC does not remove duplicates. `WatermarkInjector` adds one to the count for every event with a given commit LSN. A duplicate that arrives before its transaction completes (for example from an at-least-once redelivery by Debezium or Kafka) pushes the count past the expected value. `count == countFor(table)` is then never true, and the watermark stops for good. A repeated END event is appended to the transaction list again; it is harmless only if it is already below the watermark.

This pipeline removes duplicate data events by `pos` (`TransactionState.addEvent`), ignores a second marker for a buffered transaction, and drops anything below the CSN cursor (or at it, for an xid already emitted) as a replay. Every drop is written to `dropped_events` with its reason.

### 3. Stuck transactions

In the PoC, a transaction that never completes stops the watermark without any signal; there is no timer or alert.

This pipeline also never skips an incomplete transaction, but it keeps a processing-time timer while transactions are queued and logs an error once the head has blocked emission for longer than `job.ordering.blocked-alert-threshold`.

### 4. Intermediate versions

When the PoC's join watermark jumps past several transactions at once, it first applies all their records, then passes the watermarks downstream. `TxAwareAggregationFunction` emits an order only when the watermark equals the order's last commit LSN, so if two of those transactions changed the same order, only the latest state is emitted. That is a valid consistent state, but intermediate versions are lost.

This pipeline emits every transaction separately, so `order_stream` keeps the full history (SCD Type 2), and `ScdType1MergeApp` reduces it to the latest version.

### 5. State and recovery

The PoC's design note lists these as limitations: join state is never removed and grows without limit, `CommitLsnFixer` removes old entries using a heuristic (transaction ids more than 1,000 behind), and the watermark progress is kept in fields that are not checkpointed.

This pipeline removes a transaction's buffer when it is emitted, purges buffers without a marker once the cursor passes their CSN (at most 100,000 per step), and keeps buffers, queue and cursor in RocksDB-backed `transformWithState` state that is checkpointed.

### 6. Source quirks

Debezium's Postgres connector puts a non-commit LSN on data events (DBZ-1555), so the PoC needs `CommitLsnFixer` to hold every event back until the transaction's END event arrives with the real commit LSN.

GoldenGate writes the source transaction's `xid` and `csn` on every data message, so this pipeline needs no such step. It needs instead the custom marker formatter (`gg-formatter/`), because GoldenGate has no transaction metadata topic of its own, and it must handle grouped markers (`GROUPTRANSOPS` puts several markers in one Kafka message, ordered by offset and index).

### 7. Large transactions

Both rewrite a growing buffer for every event. The PoC's `CommitLsnFixer` reads, extends and rewrites a JSON list per transaction for each event; this pipeline's `TransactionSequencer.acceptData` reads and rewrites the whole `TransactionState`, and `containsPos` scans the buffered events. Either way a transaction of n events costs about n²/2 event serializations (see [PERFORMANCE-ESTIMATES.md](PERFORMANCE-ESTIMATES.md)). The fix is the same in both: store each event as its own state entry, written once.

## Marker contract

With the formatter's recommended setup (`markerKeys=false`, `markerPos=false`, no `MAXTRANSOPS` on `rep3`), each source transaction has exactly one marker with per-table counts. That matches Debezium's transaction topic, where the END event carries `event_count` and `data_collections`. The two designs therefore start from the same contract, and the differences above come from the processing, not the input.

| | Debezium transaction topic (PoC) | GoldenGate marker (this pipeline) |
|---|---|---|
| Events per transaction | BEGIN and END; the PoC uses END only | One marker (several per Kafka message with `GROUPTRANSOPS`) |
| Identity | `id` (transaction id) and the END event's commit LSN | `xid` and `csn` |
| Counts | `event_count`, `data_collections[].event_count` | `event_count`, `data_collections[].event_count` |
| Order | Commit order on one partition | Commit order on one partition (required) |

## Takeaway

Both are correct global sequencers, and neither scales beyond one task. The PoC proves that watermarks can carry transaction boundaries through Flink operators. This pipeline uses a simpler queue and adds what a production consumer needs: transactions written all at once, duplicates removed, alerts, state cleanup and checkpointed state. The PoC's path to parallelism depends on per-partition transaction metadata from Debezium. This pipeline's path is to group by `xid:csn` and rely on stage 2's version-based merge instead of strict global order, with a published "consistent up to CSN" watermark for readers that need a snapshot. Both should fix the large-transaction cost first.
