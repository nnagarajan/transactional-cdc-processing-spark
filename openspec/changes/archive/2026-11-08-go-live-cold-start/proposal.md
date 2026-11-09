# Proposal

## Why

In production the three data topics already exist and hold history, but the transaction metadata topic will only be switched on at go-live. On the first start, the pipeline reads every topic from `earliest` (one `kafka.starting-offsets` value shared by all four sources). That has three problems:

- **History stays forever.** Every historical data event is buffered under its `xid:csn` and never gets a marker. It is never emitted, and also never removed: buffers are deleted only when their transaction is emitted. State grows by the whole retained history and never shrinks.
- **The first batch can be huge.** No `maxOffsetsPerTrigger` is set, so the first micro-batch reads the entire retained history through the single sequencer task, which can take hours.
- **Picking another start point is unsafe.** With `latest`, or any start point not aligned with where `rep3` begins publishing markers, a marker can arrive for a transaction whose data was skipped. It never completes and blocks all output forever.

The go-live needs a way to start the data topics at exactly the point where markers begin, and a safety net for when it isn't done exactly right.

## What Changes

- **Per-topic starting offsets.** Each of the four Kafka sources can be given its own starting position: `earliest`, `latest`, or explicit per-partition offsets. A topic without an override keeps the shared `kafka.starting-offsets` default. An invalid value fails at startup.
- **Bounded batches.** Each Kafka source reads at most a configured number of offsets per micro-batch (`maxOffsetsPerTrigger`), with a default, so a start from `earliest` or a backlog after downtime is spread over many batches.
- **Orphan-buffer purge.** Once a transaction has been emitted at CSN C, any buffered transaction with no marker and a CSN below C can never be emitted, because markers arrive in commit order. The sequencer removes such buffers and logs how many it removed. Queued transactions and buffers at or above C are never touched.
- **Go-live runbook** in `gg-configs/README.md`: stop `kc` at a transaction boundary, record its trail checkpoint and the data topics' end offsets, start `rep3` from that same trail position, and start the pipeline with those offsets on a new checkpoint. A fallback section covers starting from `earliest` and relying on the purge.
- Update the docs that describe buffered events without markers as kept indefinitely (`CLAUDE.md` Known Limitations, `IMPLEMENTATION.md`, `STATE-STORE-DEBUG.md`).

No state layout change: existing checkpoints keep working.

## Capabilities

### New Capabilities
- `stream-startup`: where each Kafka source starts reading on a new checkpoint, and how much each source reads per micro-batch.
- `buffer-retention`: when buffered data for a transaction that can never be emitted is removed from state.

### Modified Capabilities
- None. There are no main specs yet. The related requirements in the in-progress `ordered-transaction-emission` change (`transaction-buffering`, `transaction-ordering`) are unchanged: the purge only removes buffers that those rules already make unemittable.

## Impact

- **Code:**
  - `TransactionalCdcProcessingApp.readKafkaStream`: per-topic starting offsets and `maxOffsetsPerTrigger`, built by a new testable helper.
  - `TransactionSequencer`: purge after emission advances the cursor.
  - `SequencerStore`: a way to list buffer keys, implemented by `TransactionOrderingProcessor`'s state store and by the test `InMemorySequencerStore`.
- **Config:** new optional `job.kafka.starting-offsets.<topic-key>` overrides and `job.kafka.max-offsets-per-trigger`. The existing `kafka.starting-offsets` stays as the default.
- **Tests:** new `TransactionSequencerTest` cases for the purge rules, and unit tests for the Kafka options helper.
- **Docs:** go-live runbook in `gg-configs/README.md`, plus `CLAUDE.md`, `IMPLEMENTATION.md` and `STATE-STORE-DEBUG.md`.
- **Operations:** the go-live procedure briefly stops `kc` (seconds) to take a clean cut point.
