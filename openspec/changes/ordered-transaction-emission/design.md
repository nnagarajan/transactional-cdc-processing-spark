# Design

## Context

See proposal.md (Why) for the motivation.

Current state, as relevant to this design:

- **Grouping and state.** `TransactionalCdcProcessingApp` unions 4 Kafka topics into one `Dataset[Row]`. It groups by `xid:csn` and runs `flatMapGroupsWithState` with `NoTimeout`. The state type is `TransactionState`, a bean-encoded class.
- **Completion.** `TransactionState.isComplete` compares list sizes with `>=` and has no dedup.
- **Marker parsing.** `parseTransactionMetadata` discards the Kafka `partition` and `offset` columns.
- **Two independent replicats.** `kc` publishes data events to topics keyed by primary key; these may be multi-partition. `rep3` publishes markers using `kp.properties` (`acks=1`), with `blockingSend=false` and `IGNOREDELETES`. Both replicats read the same extract trail, which is written in commit order. Extract `ext04` does not filter deletes, so `kc` publishes them while `rep3` leaves them out of marker counts.
- **State store config.** `SparkSessionCreator` sets the RocksDB provider only on the dev path. On the spark-submit path it is not set.
- **SCD Type 1 merge.** `ScdType1MergeApp` merges with version-aware clauses:
  - `whenNotMatched("source.version IS NOT NULL")` drops child-only rows.
  - Transaction context columns are overwritten unconditionally.
  - Intra-batch tie-break uses `dwhProcessedTs`.
- `OrderStream` carries `xid` and `csn` (string) but not `pos`.
- There are no test sources. JUnit 5 and AssertJ are declared, and `scala-maven-plugin` runs `testCompile`.

## Goals / Non-Goals

**Goals:**
- A single stateful operator whose emission order is the metadata-topic order, with every state access O(1) per event or per emitted transaction (no full-state rewrite per batch).
- Core sequencing logic testable without a Spark runtime.
- Fail fast on contract violations that can be detected (multi-partition marker topic). Make the undetectable ones visible (blocked head, out-of-order marker).

**Non-Goals:**
- Parallel assembly, for example sharding the sequencer by key range. Commit-order emission across all orders requires a single sequencer.
- Automatic recovery or skipping of a permanently stuck transaction (no dead-letter path). See Risks.
- Migrating existing checkpoint state.
- Changing the `order_stream` or `orders_current` table schemas.

## Decisions

### D1. `transformWithState` with a single constant key

All rows are grouped by a constant key, and a `StatefulProcessor[String, Row, OrderStream]` runs with `TimeMode.ProcessingTime()` and `OutputMode.Append()`.

- *Alternative:* `flatMapGroupsWithState` with one Kryo-encoded state blob, as in the originally sketched design. Rejected: the entire buffer of every in-flight transaction is re-serialized and rewritten each batch, and Kryo state is brittle across class changes.
- *Alternative:* keep `xid:csn` grouping and sequence in a second operator. Rejected: an operator cannot know about keys it hasn't seen, so it would still need a single global view. This only adds a shuffle.

### D2. Order by marker arrival, not numeric CSN

The emission queue is ordered by the sequence in which markers are accepted from the single-partition metadata topic. Within a batch, accepted markers are first sorted by their Kafka `offset`, because shuffle delivery order is not relied on.

- *Why:* Oracle can commit several transactions at the same SCN. Keying the queue by CSN either merges them or drops the later one. Marker order is the trail's commit order, so it breaks ties correctly.
- *Why:* data events that arrive before their marker are simply not in the queue yet, so the walk cannot pass them. No special "data without marker" rule is needed.
- *Alternative:* `TreeMap[BigDecimal csn, TransactionState]`. Rejected for the tie problem above, and because `MapState` iteration is not ordered by key anyway.

### D3. State layout

| State | Type | Purpose |
|---|---|---|
| `buffers` | `MapState[String xid:csn, TransactionState]` | Per-transaction events + marker. Written only for keys touched in the batch. |
| `queue` | `MapState[Long seq, String xid:csn]` | Emission queue. Lookups are by seq, so no iteration is needed. |
| `cursor` | `ValueState[Cursor]` | `nextSeqToEmit`, `nextSeqToAssign`, `lastEmittedCsn` (decimal string), `emittedAtLastCsn` (xids emitted at `lastEmittedCsn`), `blockedSinceMs`, `timerMs`. |

A `ListState` queue was rejected: removing the head means rewriting the whole list.

### D4. Accept / drop rules

Here `C` is `lastEmittedCsn`, and CSNs are compared numerically (`BigDecimal`).

**Marker for `k = xid:csn`:**
1. If `partition != 0`, throw. This fails the query, as the spec requires.
2. Drop as a duplicate if any of these hold:
   - `buffers(k)` already has a marker.
   - `csn < C`.
   - `csn == C` and `xid ∈ emittedAtLastCsn`.
3. Otherwise, set the marker on `buffers(k)` (creating it if absent), then put `queue(nextSeqToAssign++) = k`.

**Data event for `k`:**
1. Drop if `csn < C`, or if `csn == C` and `xid ∈ emittedAtLastCsn`.
2. Otherwise, add it to `buffers(k)` unless an event with the same `pos` is already buffered there.

**On emit:** when emitting a transaction with CSN `c`:
- If `c > C`, set `C = c` and `emittedAtLastCsn = {xid}`.
- Otherwise, add `xid` to `emittedAtLastCsn`.

This keeps the emitted-transaction record bounded, at one CSN plus its xids.

*Why this is sound:* under the ordering contract, every transaction with CSN < `C` has had its marker accepted and been emitted. So anything arriving for such a CSN is a replay.

*Risk:* if markers are ever not CSN-nondecreasing, a genuinely new marker would be dropped. Every marker dropped by the `csn < C` rule is logged at WARN with its key and offset, so a contract break is visible. Replayed markers are only expected after a replicat restart, so these warnings should be rare.

### D5. Dedup on `pos` inside `TransactionState`

`addEvent` rejects an event whose `pos` matches an already-buffered event of the same table. The check scans the existing lists; transaction sizes are small, and this avoids adding a field the bean encoder can't represent as a `Set`.

`isComplete` changes from `>=` to `==` per table. A count above expected is logged as an error and treated as not complete, so the condition is visible as a blocked head rather than silently emitting a wrong transaction.

### D6. Emission walk

After applying all of a batch's rows:

```
while queue has nextSeqToEmit:
  k = queue(nextSeqToEmit)
  if not buffers(k).isComplete: break
  emit OrderJoiner.joinTransaction(buffers(k))
  remove buffers(k), queue(nextSeqToEmit)
  nextSeqToEmit++
  update C / emittedAtLastCsn
```

The same walk also runs on timer expiry. That is a no-op unless state changed, but it keeps the code path single.

### D7. Blocked alert via processing-time timer

The processor keeps exactly one registered timer, `cursor.timerMs`, at `now + checkInterval` (default 1 min). Each `handleInputRows` and `handleExpiredTimer` call deletes the old timer and registers a new one.

`blockedSinceMs`:
- is set when a walk emits nothing while the queue is non-empty;
- is cleared when anything is emitted or the queue empties.

If `now - blockedSinceMs > blockedAlertThreshold` (default 10 min), an error is logged with the head key, its `progress`, and the queue depth (`nextSeqToAssign - nextSeqToEmit - 1`).

The alert is deliberately also raised when the head has no marker problem but is just slow. The threshold is configurable for that reason.

### D8. Pure core plus a thin Spark adapter

The accept/drop/walk logic lives in a `TransactionSequencer` class. It operates on a small store trait (`getBuffer`, `putBuffer`, `removeBuffer`, `queueGet`, `queuePut`, `queueRemove`, `cursor`).

- The `StatefulProcessor` implements the trait over Spark state handles and converts `Row` into `DataChangeEvent` / `TransactionMetadata`. These are the existing parsing blocks, extracted unchanged.
- Unit tests use an in-memory store.
- One integration test runs the real processor through `MemoryStream` on a local session with RocksDB.

### D9. Stream assembly changes

- **Markers:** `parseTransactionMetadata` keeps `partition` and `offset` as `marker_partition` / `marker_offset`.
- **Data events:** these columns are set to null.
- **Grouping:** `groupByKey(_ => "all")`.
- **State store:** the RocksDB provider is set with `spark.conf.set` in the app before the query starts, so the spark-submit path is covered too. The dev-path builder setting stays.

### D10. SCD Type 1 merge changes

- **Unmatched rows:** `whenNotMatched` drops its `source.version IS NOT NULL` condition, so child-only rows insert as placeholders. The existing `orderSourceWins` clause already treats a null target version as 0, so the parent applies cleanly when it arrives.
- **Transaction context:** `xid`, `csn` and `dwhProcessedTs` in both `whenMatched` clauses use `CASE WHEN CAST(source.csn AS DECIMAL(38,0)) > CAST(target.csn AS DECIMAL(38,0)) THEN source.x ELSE target.x END`.
- **Intra-batch tie-break:** the window ordering becomes `version DESC NULLS LAST, CAST(csn AS DECIMAL(38,0)) DESC`. This replaces `dwhProcessedTs`, which is a processing-time stamp, not commit order.
- `pos` is not in `order_stream`, so the tie-break is CSN only. Two `order_stream` rows for the same order can only share a CSN if they come from the same transaction. That doesn't happen, because `OrderJoiner` produces one row per order per transaction.

### D11. Configuration

New keys in `transactional-cdc-processing.conf`:

```
job.ordering.blocked-alert-threshold = 10 minutes
job.ordering.check-interval = 1 minute
```

The checkpoint location default changes to a new path, so that a fresh run does not try to reuse the incompatible old checkpoint.

### D14. Filter deletes at the Extract (decided after implementation)

`IGNOREDELETES` is set in `ext04.prm`, before its `TABLE` statements, and `rep3` does not set it. Deletes never enter the trail, so `kc` and `rep3` see the same operations and marker counts always match the data topics.

- *Alternative:* count deletes on both sides (`IGNOREDELETES` removed everywhere). This was the original plan. Rejected by the project owner: the pipeline has no use for deletes, since `OrderJoiner` emits nothing for them and `ScdType1Merger` has no delete handling, so carrying them only adds events to wait for.
- *Consequence:* Oracle deletes on the tracked tables, and on the heartbeat table, are invisible downstream. Supporting deletes in `orders_current` later would mean removing this setting and adding delete handling to the merge.
- `TransactionState` still counts any delete event that does arrive, for example one already in the trail before the Extract change, so such a transaction completes when its marker said it would.

### D12. Explicit processing-time trigger (found during implementation)

In processing-time mode, Spark 4.0's `TransformWithStateExec.shouldRunAnotherBatch` always returns true (SPARK-50180). With the default trigger, the query therefore runs empty micro-batches back to back, each writing checkpoint files. The query is also never "idle", so `processAllAvailable()` never returns.

The sink therefore uses `Trigger.ProcessingTime(job.ordering.trigger-interval)`, default 10 seconds. Idle batches run once per interval, and those idle batches are what fire the blocked-check timer when no input arrives.

- *Alternative:* `TimeMode.None()` with no timers. Rejected: the blocked alert must fire without new input.
- *Alternative:* disable no-data batches. Rejected for the same reason.

Tests wait on query progress reaching the added offset instead of calling `processAllAvailable()`.

### D13. Classic-only `transformWithState` (found during implementation)

Spark 4.0 declares the Scala `transformWithState` API `private[sql]` on the base `KeyValueGroupedDataset`. It is public only on `org.apache.spark.sql.classic.KeyValueGroupedDataset`. The grouped dataset is matched to the classic type, and a clear error is raised otherwise, for example under Spark Connect.

## Risks / Trade-offs

- **[One stuck transaction halts all output]** → This is intentional, for correctness. Mitigated by the blocked alert, with head key and progress, and by the completeness fixes that remove the known causes: duplicates, a delete-count mismatch between marker and data, and lost markers. Recovery is operational: fix the source, or deliberately restart with a new checkpoint.
- **[Single-task throughput]** → Assembly is serial. Per-event work is O(1) state access plus a small `pos` scan. If this becomes the bottleneck, the next step is a separate change. Ordering would only be relaxed per order ID, which this change doesn't do.
- **[Unbounded buffers if a marker is lost]** → Data for a transaction whose marker never arrives stays buffered forever. It can't block emission, because it isn't in the queue, but it consumes state. Reported as a known limitation. TTL is not applied, because it could expire data for a marker that is merely late.
- **[Marker order not CSN-nondecreasing]** → A genuinely new marker would be dropped as a replay. Logged at WARN per D4.
- **[Kafka source delivers a batch's marker rows out of offset order]** → Handled by sorting accepted markers by `marker_offset` within each batch (D2).
- **[Placeholder rows in `orders_current`]** → An order first seen through a child-only change has null order attributes until its order row changes. Consumers must tolerate null `version`. Documented in `SCD_TYPE1_README.md`.
- **[Idle batches every trigger interval]** → Each idle batch writes a small checkpoint entry. At 10 s this is about 8,640 batches a day; checkpoint metadata retention (`spark.sql.streaming.minBatchesToRetain`) bounds it. See D12.
- **[`transformWithState` in Spark 4.0]** → Requires the RocksDB provider, and its state schema is checked at restart. Any later change to the state classes needs a schema-compatible evolution or a new checkpoint.

## Migration Plan

1. Stop `TransactionalCdcProcessingApp` and `ScdType1MergeApp`.
2. Stop Extract `ext04` and add `IGNOREDELETES` (D14), then restart it. This affects only trail records written from then on. Stop replicat `rep3` and apply its config changes (`IGNOREDELETES` removed, `blockingSend=true`, `acks=all`, `enable.idempotence=true`).
3. Delete and recreate `dev.transaction_metadata_json` with 1 partition. Reposition `rep3` to the same trail position as `kc`, or to the trail start, so markers cover every data event that will be replayed. Start `rep3`.
4. Deploy the new app with a new checkpoint location and `startingOffsets=earliest`. Optionally truncate `order_stream` and `orders_current`; otherwise the replay produces duplicate history rows. That is harmless for `orders_current` because of the version and CSN guards, but `order_stream` shows them.
5. Start `ScdType1MergeApp`. If `order_stream` was truncated, also use a new checkpoint for it.

**Rollback:** redeploy the previous jar with its original checkpoint location, which is untouched. The GoldenGate config changes are backward compatible with the old app.

## Open Questions

- Should the blocked alert also be exported as a metric (for example, via `StreamingQueryListener` custom progress) in addition to the error log? This can be added later without changing the specs.
