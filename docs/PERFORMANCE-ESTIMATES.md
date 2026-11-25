# Performance Estimates

How long an event takes to travel from an Oracle commit to the Delta tables, and whether the pipeline keeps up at **100,000 events per minute**.

> **These are estimates, not measurements.** They come from how the code and configuration work, not from a load test. Treat them as order of magnitude, and confirm them with the measurement steps at the end.

## Summary

| Target | Estimated latency (steady state) |
|---|---|
| Oracle commit → `order_stream` | **about 10–20 s** |
| Oracle commit → `orders_current` | **about 15–40 s** |

At 100,000 events per minute the pipeline should keep up, with roughly twice the capacity it needs. **`GROUPTRANSOPS 1` on `rep3`** can change that completely. **Very large transactions** delay the transactions behind them for as long as they take to arrive, typically tens of seconds rather than hours (see [What would break the estimate](#what-would-break-the-estimate)).

## Assumptions

- About **5.5 events per transaction**, as in the sample data: 1 order, 1 order detail, and about 3.5 line items. That makes about **1,670 events and about 300 transactions per second**.
- Everything runs on one machine: Oracle, GoldenGate, Kafka, Spark (`local[2]`), and the Unity Catalog server.
- The sink trigger is `job.ordering.trigger-interval = 10 seconds`. Each 10-second batch holds about **16,700 events and 3,000 markers**.
- `rep3` runs with `GROUPTRANSOPS 1000` and the per-source-transaction marker formatter (`gg-formatter/`).

## Latency per stage

| Stage | Estimate | What drives it |
|---|---|---|
| Oracle commit → trail (Extract `ext04`) | 1–3 s | Integrated capture reading redo |
| Trail → Kafka (`kc` for data, `rep3` for markers) | 0.5–2 s | `GROUPTRANSOPS 1000`; `rep3` sends about 2 grouped marker messages per second |
| Waiting for the next Spark trigger | 0–10 s (about 5 on average) | Processing-time trigger of 10 s |
| Spark batch: parse, sequence, join, Delta commit through UC | about 3–6 s | Single sequencer task, the Delta write, and the catalog commit |
| **Oracle → `order_stream`** | **about 10–20 s** | |
| `ScdType1MergeApp` | +5–20 s | No trigger is set, so batches run back to back. The MERGE joins each batch against the whole `orders_current` table and slows as the table grows |
| **Oracle → `orders_current`** | **about 15–40 s** | |

## Throughput

A 10-second batch of about 16,700 events should take about 3–6 seconds to process, so it finishes before the next trigger fires. The rough ceiling is **150,000–300,000 events per minute**.

Adding executors won't raise that ceiling. All rows share one grouping key, so `TransactionOrderingProcessor` runs as a single task.

## What would break the estimate

### 1. Large transactions delay the transactions behind them

Buffering costs time linear in a transaction's size. Each event is stored as its own state entry, next to a small per-transaction summary. For every accepted event, `TransactionSequencer` does:

- 2 small reads: the summary, and a `(table, pos)` lookup for duplicates
- 3 writes: the event, its position entry, and the summary

On emission, each event is read once and its two entries are removed. Completeness, duplicate and blocked-progress checks read no events. A transaction of n events therefore costs about 5n small state writes, and `TransactionSequencerTest` asserts exactly that for 5 and 55,000 events.

Before this layout, each event rewrote the whole buffered transaction (about n²/2 event writes). The comparison below assumes one old event rewrite is about 6.7 µs (550 events in about a second), and the new cost is about 1.5 of those units per event.

| Transaction size | Previous layout | Now (sequencer only) | Effect |
|---|---|---|---|
| 5 events | negligible | negligible | none |
| 550 events (the sample insert) | about 1 s | about 5 ms | none |
| 55,000 events (a 10,000-order bulk load) | about 2.8 h | about 0.5 s | the transaction arrives in one or two triggers, then emits |
| 75,000–150,000 events (sizes seen in the `rep3` logs) | about 5–21 h | about 0.75–1.5 s | a few triggers, since each source reads at most `job.kafka.max-offsets-per-trigger` (50,000) offsets per batch |

Output is still in strict commit order, so a large transaction holds back every transaction behind it until all its events have arrived. That is now roughly 15–30 s for a 55,000-event transaction, dominated by trigger intervals, `OrderJoiner` and the Delta commit. Emission also still builds the whole transaction in memory once.

See `cost-analysis.md` in the `fix-large-transaction-buffering` OpenSpec change (under `openspec/changes/`, or `openspec/changes/archive/` once archived) for the derivation and worked examples.

### 2. `GROUPTRANSOPS 1` on `rep3` could fall behind

With `GROUPTRANSOPS 1`, every source transaction becomes its own synchronous Kafka send (blocking send with `acks=all`), plus a Replicat checkpoint. At 300 transactions per second and 2–5 ms each, that's 0.6–1.5 seconds of work for every second of input, which is at or over capacity. Markers would lag, and since output waits for markers, so would the pipeline.

With the new formatter, `GROUPTRANSOPS 1000` sends about 2 messages per second. **Keep `GROUPTRANSOPS 1000` at this rate.** It's also an input to open task 4.3 of the `per-source-transaction-markers` change.

### 3. `orders_current` gets slower over time

The SCD Type 1 MERGE has no partition pruning, so its cost grows with the table. Partitioning or Z-ordering on `orderId`, or tuning `delta.targetFileSize`, would help.

### 4. Unity Catalog commits

Every Delta commit to a managed table goes through the catalog server, adding an HTTP round trip per batch. At one commit every 10 seconds this is small. If the UC server is down, batches fail until it returns.

## How to measure

Load-test at the target rate, for example with a PL/SQL loop issuing about 300 small commits per second, and read the progress each streaming query reports:

```scala
query.lastProgress.durationMs              // triggerExecution, addBatch, commitOffsets
query.lastProgress.numInputRows
query.lastProgress.processedRowsPerSecond
```

- The pipeline keeps up if `triggerExecution` stays well under 10,000 ms.
- For end-to-end latency, compare each row's `dwhProcessedTs` in `order_stream` with its source commit time.
- Time one 55,000-event (10,000-order) transaction separately to see the large-transaction cost directly.
- Watch `rep3` lag (`LAG REPLICAT rep3` in GGSCI) to confirm markers aren't the bottleneck.
