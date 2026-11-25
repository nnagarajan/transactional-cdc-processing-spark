# Cost Analysis: Per-Event Buffering

This estimates how much faster stage 1 handles large transactions once the per-transaction `TransactionState` value is replaced by the per-event layout in `design.md` (D1–D3), and works through examples.

> **These are estimates, not measurements.** They come from counting state operations and calibrating against `PERFORMANCE-ESTIMATES.md`. Task 2.5 checks the operation counts. Task 5.1 (a 10,000-order bulk run on the local stack) gives a measured number.

## The difference in one line

The old code rewrites the whole transaction for every event it receives. The new code writes only the event that arrived. The old per-event cost grows with the number of events already buffered; the new cost does not.

## Cost model

**Old.** Event k reads and rewrites the k−1 events already buffered, so a transaction of n events costs about n²/2 event rewrites. `PERFORMANCE-ESTIMATES.md` puts 550 events (about 151,000 rewrites) at around one second, so:

- one rewrite unit: **c ≈ 1/150,000 s ≈ 6.7 µs**
- `T_old ≈ c · n² / 2`

**New.** Each event:
- is written once and read once at emission: about one unit of c
- also causes a summary read and write, a `positions` write and lookup, and two removals at emission

These extra operations are small and fixed in size. They are counted together as about half a unit of c, which is an assumption.

- `T_new ≈ 1.5 · c · n`

**Speedup.** `T_old / T_new ≈ n / 3`

## Sequencer time per transaction

| Transaction | Events (n) | Old | New | Speedup |
|---|---|---|---|---|
| Typical | 5 | 0.08 ms | 0.05 ms | ~2× (no practical change) |
| Sample insert (100 orders) | 550 | ~1 s | ~5.5 ms | ~180× |
| 1,000 orders | 5,500 | ~100 s | ~55 ms | ~1,800× |
| **10,000-order bulk load** | **55,000** | **~2.8 h** | **~0.55 s** | **~18,000×** |
| Largest size seen in the `rep3` logs | 150,000 | ~21 h | ~1.5 s | ~50,000× |

## End-to-end effect

Once buffering is linear, the sequencer is no longer the bottleneck. For a 55,000-event transaction the delay then comes from:

- **Trigger and batching.** Each source reads at most 50,000 offsets per batch (`job.kafka.max-offsets-per-trigger`), and a batch runs every 10 s (`job.ordering.trigger-interval`). The transaction's 10,000 orders, 10,000 details and ~35,000 line items fit in one or two triggers.
- **Emission.** `OrderJoiner` builds 10,000 `OrderStream` rows, and the Delta commit goes through Unity Catalog. Together they take a few seconds.

Oracle commit to `order_stream` for that transaction goes from **about 3 hours to about 15–30 s**, roughly **400–700×** end to end. Every transaction queued behind it now waits seconds instead of hours.

## Examples

### Example 1: a normal 5-event transaction

Transaction `X1:500` has 1 order, 1 order detail and 3 line items. Its marker declares `ORDERS 1, ORDER_DETAILS 1, ORDER_LINE_ITEMS 3`.

**Old: one growing value per transaction.** State holds `buffers["X1:500"] = TransactionState{ orders[], details[], lineItems[] }`.

| Event arrives | Events read | `pos` compares | Events written |
|---|---|---|---|
| order (pos 1) | 0 | 0 | 1 |
| detail (pos 2) | 1 | 0 | 2 |
| line item (pos 3) | 2 | 0 | 3 |
| line item (pos 4) | 3 | 1 | 4 |
| line item (pos 5) | 4 | 2 | 5 |
| **Total** | **10** | **3** | **15** |

**New: one entry per event.**

| Event arrives | Reads | Writes |
|---|---|---|
| order (pos 1) | summary; `positions[X1:500\|ORDERS\|1]` (absent) | `events[X1:500#0]`, position, summary {orders 1, n 1} |
| detail (pos 2) | summary; position (absent) | `events[X1:500#1]`, position, summary {details 1, n 2} |
| line item (pos 3) | summary; position (absent) | `events[X1:500#2]`, position, summary {lineItems 1, n 3} |
| line item (pos 4) | summary; position (absent) | `events[X1:500#3]`, position, summary {lineItems 2, n 4} |
| line item (pos 5) | summary; position (absent) | `events[X1:500#4]`, position, summary {lineItems 3, n 5} |

Every event costs the same: 2 small reads and 3 writes. When the marker arrives, completeness is checked from the summary alone (`1/1, 1/1, 3/3`), without reading any events. Emission reads `#0 … #4` once each and deletes the transaction's entries.

At this size the two layouts cost about the same, so typical traffic sees no change.

### Example 2: the sample insert (550 events)

The old cost grows like a triangle:

```
event 1    █                     writes 1
event 2    ██                    writes 2
event 3    ███                   writes 3
...
event 550  ████████…(550)        writes 550
           total = 550 × 551 / 2 ≈ 151,000 event writes  (≈ 1 s)
```

The new cost grows like a line:

```
event 1    █
event 2    █
...
event 550  █                     total = 550 event writes + 550 reads at emission
                                 ≈ 825 cost units  (≈ 5 ms)
```

That is about 180× faster. The old code still takes only about a second at this size, which is why the problem doesn't show in normal testing.

### Example 3: a 10,000-order bulk load (55,000 events)

The load has 10,000 orders, 10,000 details and 35,000 line items in one Oracle commit.

**Old.** The last line item alone:
- reads and deserializes the 54,999 buffered events (tens of MB)
- compares its `pos` with up to 34,999 buffered line items
- serializes and writes back 55,000 events

Every event before it does the same with fewer events. The total is 55,000² / 2 ≈ **1.5 billion event writes, about 2.8 hours**.

While this runs, the bulk transaction is at the head of the queue. A customer order placed a minute later as a 5-event transaction is complete within seconds, but it waits behind the bulk load for almost 3 hours. So does every other transaction in that window.

**New.** The last line item costs the same as the first:
- read the summary `{orders 10000, details 10000, lineItems 34999, n 54999}`
- look up `positions[X9:900|ORDER_LINE_ITEMS|55000]`: absent
- write `events[X9:900#54999]`, its position, and the summary `{…, lineItems 35000, n 55000}`

The total is about 55,000 event writes plus 55,000 reads at emission, **about half a second**. The customer order behind it waits only for the bulk load to arrive and be written: about 15–30 s.

### Example 4: duplicate detection

Kafka redelivers line item `pos=40,000` of the bulk transaction after 50,000 events are buffered.

| | Old | New |
|---|---|---|
| Work | Deserialize 50,000 events, then compare `pos` against up to ~35,000 line items | One lookup: `positions[X9:900\|ORDER_LINE_ITEMS\|40000]` exists |
| Result | `DuplicatePos`, recorded in `dropped_events` | Same |

### Example 5: a blocked head

The bulk transaction has 50,000 of its 55,000 events, and the alert timer re-checks it every few seconds.

| | Old | New |
|---|---|---|
| Completeness check | Deserialize 50,000 events to count the lists | Read one summary: `lineItems 30000/35000` |
| Alert text | `Orders: 10000/10000, Details: 10000/10000, LineItems: 30000/35000` | Identical |

## Summary

| Events in the transaction | Old (event writes) | New (event writes) | Overall speedup |
|---|---|---|---|
| 5 | 15 | 5 | ~2× |
| 550 | 151,000 | 550 | ~180× |
| 55,000 | 1.5 billion | 55,000 | ~18,000× |

The output is the same in every example: same transactions, same order, same `dropped_events` rows and same alert text.

## Caveats

- **The old cost is probably understated.** Each old step deserializes a value of up to 55,000 events (tens of MB), so garbage-collection pressure adds more time. The `containsPos` scan adds another ~1.5 billion string comparisons, which is minor next to serialization.
- **The new cost depends on RocksDB point operations.** The 1.5 factor assumes reads and writes of small keys cost about as much as encoding one event. Task 5.1 replaces this assumption with a measurement.
- **Peak memory at emission is unchanged.** A transaction is still assembled in memory once to build its `OrderStream` records (design, Non-Goals).
