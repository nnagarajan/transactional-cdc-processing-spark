# Comparison with Stripe's Flink CDC Transaction Aggregation

This compares this pipeline with the design in Stripe's Flink Forward 2022 talk, *Squirreling Away $640 Billion: How Stripe Leverages Flink for Change Data Capture* ([slides](https://www.slideshare.net/slideshow/squirreling-away-640-billion-how-stripe-leverages-flink-for-change-data-capture/252469093)).

> **Source caveat:** The deck has 103 slides and no speaker notes or transcript. The slide text was read directly up to about slide 66. Points about the later slides (the final trigger design, watermark issues, onboarding) come from a summary of those slides, not their verbatim text.

## Summary

Both systems solve the same problem: rebuild whole source transactions from per-table CDC streams. Both decide that a transaction is complete when the buffered events match per-table counts on a transaction metadata event. Our marker-with-per-table-counts design is the same production pattern, and it also matches Debezium's transaction metadata topic.

They differ in how they finish a transaction:

- **Stripe** keys by transaction id. Each transaction completes on its own and can time out. Output is fresh, but it can be incomplete and is not in commit order.
- **This pipeline** has one global queue in commit order and never skips a transaction. Output is complete and ordered, but one incomplete transaction stalls everything after it.

## Where the designs match

| | Stripe (Flink) | This pipeline (Spark) |
|---|---|---|
| Inputs | Many change streams (one per table) and **one** transaction metadata stream | Three data topics and **one** metadata topic (`dev.transaction_metadata_json`) |
| Change event | `before`/`after`, `operation`, `transaction.id`, `global_position`, `source_position` | GoldenGate envelope: `before`/`after`, `op_type`, `xid`, `csn`, `pos` |
| Transaction marker | COMMIT event with `total_events` and `per_source_event_counts` (`[{source: "keyspace.table", total_events}]`) | Marker with `event_count` and `data_collections` (per-table counts) |
| Completeness rule | Buffer matches the per-source counts | `TransactionState.isComplete`: exact per-table counts |
| Combining streams | `connect` plus a custom `WrappedEvent` with a shared key, then `keyBy` | Parse to one row shape, `union`, then group by one key |
| Duplicates and late events | Listed as a requirement; incomplete aggregates still occur | Dedup on `pos`; replays dropped and written to `dropped_events` |
| Bounded state | Stated as a requirement ("Don't result in infinite state growth") | Marker-less buffers purged once a higher CSN is emitted |

## Where the designs differ

### 1. Keying and ordering

Stripe keys by transaction id. Transactions complete independently and are emitted in whatever order they finish. There is no commit-order guarantee across transactions and no head-of-line blocking.

This pipeline uses one global key and a queue in marker (commit) order. Output is strictly in CSN order, and an incomplete head blocks all later transactions. We give up throughput and liveness to get ordering, which the SCD Type 1 and planned SCD Type 2 stages depend on.

### 2. Giving up on a transaction

Stripe uses a global window with a custom `TransactionBoundaryTrigger`. It fires when the counts match, or when an event-time timer expires. A timed-out transaction is emitted incomplete or sent to a side output. The slides return several times to the trade-off "Freshness vs Correctness" and admit that incomplete aggregates still happen.

This pipeline never skips. An incomplete head blocks output and is alerted on after `job.ordering.blocked-alert-threshold`. That is stricter than Stripe's approach.

### 3. Time and completeness signals

Stripe relies on event-time watermarks across many sources. The problems they report:

- idle subtasks on low-volume streams holding back the watermark
- event-time timers firing only sometimes
- a source "idleness" setting they call "not perfect"

This pipeline avoids all of these. It runs in processing-time mode and decides completeness by matching counts only. (The Flink version of this project uses LSN-based watermarks instead.)

### 4. BEGIN markers

Stripe emits both BEGIN and COMMIT markers, and the markers count toward `total_events`. This pipeline has only the commit-side marker from the `rep3` replicat. Counts alone decide completeness, so a BEGIN marker isn't needed.

### 5. Output shape

Stripe emits one aggregated event per transaction, holding the raw `data[]` array. It does no denormalization.

This pipeline goes further. Stage 1 emits records denormalized by root key (`order_stream`), and stage 2 merges them into `orders_current`. The proposed `configurable-cdc-models` change moves closer to Stripe's split: the sequencer emits a generic `TRANSACTION` (xid, csn, events), and shaping happens in the sink.

### 6. Onboarding new tables

Stripe subscribes to topics with a regex and adds new datasets through Flink broadcast state, without redeploying. They note that a redeploy "blows away Freshness SLO's error budget."

Here, adding a table needs a job config change and a restart (and a new checkpoint for a new job). That is fine for config-driven jobs, but it is a gap if a table has to be added to a running job.

## What the Stripe deck doesn't cover

The readable text says nothing about:

- large transactions or head-of-line blocking
- backfills or schema evolution
- state backend, checkpointing, exactly-once delivery or sinks
- scale (the "$640 billion" in the title isn't explained)

This pipeline is already more explicit than the deck about drop auditing (`dropped_events`) and idempotent sinks (Delta `txnAppId` / `txnVersion`). The deck offers nothing on large transactions; for those we rely on chunked bulk loads at the source (see below).

## Takeaway

This pipeline follows an established, production-proven pattern. The main design choice is the one where Stripe went the other way:

| Choice | Freshness | Completeness | Commit order | Failure mode |
|---|---|---|---|---|
| Per-transaction key with timeout (Stripe) | High | Can be incomplete | Not guaranteed | Silent partial aggregates |
| One global ordered queue, never skip (this pipeline) | Lower | Complete | Guaranteed | Visible stall plus alert |

For current-state and history tables, where commit order matters, the global ordered queue is the right fit. If more liveness is needed later, the way forward is partitioning by root key (per-key markers, or a control/payload split), not a timeout that emits partial transactions.

Large transactions are handled operationally. Bulk loads commit in chunks (about 100 orders, never splitting an order) so that each XID stays small. The marker's `event_count` can be used to warn about oversized transactions that bypass this rule.
