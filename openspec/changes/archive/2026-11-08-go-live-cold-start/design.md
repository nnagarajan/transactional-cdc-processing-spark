# Design

## Context

See proposal.md for motivation. Current state:

- `TransactionalCdcProcessingApp.readKafkaStream` applies one `kafka.starting-offsets` value (default `earliest`, in `reference.conf`) to all four sources, and sets no `maxOffsetsPerTrigger`. The app is a Scala `App` object, so nothing in it is unit-testable.
- `TransactionSequencer.acceptData` creates a buffer for any event whose transaction hasn't been emitted. `emitReady` removes a buffer only when its transaction is emitted. No other path removes buffers.
- Buffer keys are `xid:csn` (`TransactionSequencer.txKey`). GoldenGate xids contain no `:`, so the CSN is the text after the last `:`.
- A queued transaction's CSN is always at or above the last emitted CSN: markers are accepted in commit order, and a marker below the cursor is dropped as a replay.
- `kc` and `rep3` both read trail `e4` and checkpoint at source-transaction boundaries (whole transactions, `GROUPTRANSOPS` never splits one).
- Spark applies `startingOffsets` only when a query starts on a new checkpoint. After that, offsets come from the checkpoint.

## Goals / Non-Goals

**Goals:**
- A go-live that starts with no orphaned history and no blocked head.
- A safe outcome if the go-live starts from `earliest` instead: bounded batches and state that shrinks back to in-flight transactions.
- No change to the state layout, so existing checkpoints keep working.

**Non-Goals:**
- Automating the cut point (reading the `kc` checkpoint or Kafka end offsets from the app).
- Time-based expiry of buffers, or removing buffers at or above the cursor.
- Recovering from a go-live that already produced a blocked head. That still needs a new checkpoint.

## Decisions

### D1. Per-topic starting position, as partition offsets
Config: optional `job.kafka.starting-offsets.<topic-key>` (`orders`, `order-details`, `order-line-items`, `transaction-metadata`, the same keys as `job.kafka.topics`). Each value is `earliest`, `latest`, or a JSON map of partition to offset, such as `{"0":12345}`. A missing key falls back to `kafka.starting-offsets`.

A new object, `KafkaSourceOptions` in the main package, validates the value and builds the Spark options. For a partition map it wraps the map in Spark's form, `{"<topic>":{"0":12345}}`, using the configured topic name, so the operator can't mistype the topic. Invalid values throw at startup, naming the topic key and value.

**Alternatives:**
- **Raw Spark JSON per topic:** works, but repeats the topic name and allows a mismatch with the subscribed topic.
- **`startingOffsetsByTimestamp`:** rejected. Kafka timestamps are `kc` publish times, while the cut point has to be a trail position. A timestamp can land in the middle of a GoldenGate delivery.

### D2. The cut point is `kc`'s checkpoint
The runbook stops `kc`, which checkpoints at a transaction boundary. It records `kc`'s trail position (`INFO REPLICAT kc, SHOWCH`) and the data topics' end offsets, positions `rep3` at that same trail position, and starts both. Every marker then refers to a transaction whose data is after the recorded offsets, and no data after them lacks a marker.

**Alternative:** start everything from `earliest` and rely on the purge (D3). It's kept as the documented fallback, not the primary path: the first run still reads and buffers all retained history before the first marker's transaction is emitted.

### D3. Purge by CSN from the key, after the cursor advances
After `emitReady`, if `lastEmittedCsn` increased in this step, the sequencer scans the buffer keys. It parses each key's CSN and removes keys whose CSN is numerically below `lastEmittedCsn`. It doesn't need to load any buffer value or check for a marker: a queued transaction can't have a CSN below the cursor (see Context). The sequencer logs the count removed and the CSN bound.

- `SequencerStore` gains `bufferKeys: Iterator[String]`, implemented with `MapState.keys()` in `TransactionOrderingProcessor` and over the map in `InMemorySequencerStore`.
- Keys to remove are collected first and removed after the scan, so no removal happens while the state iterator is open. At most 100,000 keys are removed per step, which bounds memory and batch time. The rest go in later steps where the cursor advances. Removing large history can therefore take several batches.
- **Cost:** in steady state only in-flight transactions are buffered, so the scan is small. After a go-live from `earliest` the first scans cover the whole history, which happens once.
- **No new state:** the trigger is the cursor advancing, which is already tracked. Nothing new is stored, so checkpoints stay compatible.

**Alternatives:**
- **State TTL (`TTLConfig`):** rejected. It expires by age, so it would also remove a slow in-flight transaction, including a blocked head. That would break the rule that an incomplete head is never skipped.
- **Purge on every step, or on a timer:** rejected. Without the cursor advancing, nothing new can have become unemittable.
- **Loading values and removing only those with no metadata:** unnecessary, given the CSN invariant, and much slower.

### D4. `maxOffsetsPerTrigger` per source, default 50,000
Config: `job.kafka.max-offsets-per-trigger`, applied to each of the four sources, default 50,000 in `reference.conf`. Setting `none` omits the option, so reads are unbounded.

At 100,000 events per minute and a 10-second trigger, about 17,000 events arrive per batch across all topics. 50,000 per source leaves at least 3 times headroom to catch up. On the metadata topic the limit counts messages, and a grouped message can hold many markers.

**Alternative:** separate limits per topic. Deferred: one value is enough at today's volumes and keeps the config simple.

## Risks / Trade-offs

- **[The data topics advance at different speeds during catch-up]:** line items carry about 3.5 times as many events as orders, so with equal limits the line-items topic lags. Transactions then complete only as line items arrive. → Emission continues every batch, so the blocked alert (10 minutes without any emission) shouldn't fire. Raise the limit if it does.
- **[The limit is too low for steady-state volume]:** the pipeline would never catch up. → The default is about 3 times the expected rate. Check `numInputRows` against the limit in query progress.
- **[The cut point is wrong, for example offsets recorded while `kc` was still running]:** a marker can refer to skipped data and block forever. → The runbook checks that the end offsets are unchanged before starting `kc` again. The purge can't fix this case; recovery is a new checkpoint.
- **[A marker arrives out of commit order]:** it was already dropped below the cursor. Its data is now also purged. → No change in outcome: the contract already requires commit order.
- **[Removing large history spans several batches]:** state shrinks gradually. → The scope is bounded and logged. `STATE-STORE-DEBUG.md` shows how to count the remaining buffers.

## Migration Plan

1. Deploy the new jar. Existing checkpoints keep working. The new limit applies from the next batch, and the starting positions matter only on a new checkpoint.
2. At go-live, follow the runbook in `gg-configs/README.md`.

Rollback: redeploy the previous jar on the same checkpoint. Buffers already removed were unemittable, so nothing is lost.

## Open Questions

- Whether 50,000 is the right production default depends on production volume. It's a config value and can be tuned without changing the design.
