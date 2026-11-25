# Design

## Context

See proposal.md, Why, for the motivation.

`TransactionOrderingProcessor` runs one `transformWithState` group (key `all`) with three state variables:
- `buffers`: `MapState[String, TransactionState]`, keyed by `xid:csn`
- `queue`: `MapState[Long, String]`
- `cursor`: `ValueState[Cursor]`

`TransactionSequencer` holds the logic and reaches state only through `SequencerStore`. `InMemorySequencerStore` keeps live object references, so a test passes even if the code mutates a state object and forgets to put it back. The Spark store would lose that change.

`TransactionState` does two jobs today:
- the growing per-transaction buffer: three event lists, the marker and the expected counts
- the assembled transaction handed to `OrderJoiner.joinTransaction` on emission

Facts that constrain the layout:
- `DataChangeEvent.pos` may be null. Parsing doesn't require it, and `containsPos` treats a null `pos` as never a duplicate, so every null-`pos` event is buffered and counted.
- De-duplication is per table: the same `pos` in two tables counts twice today.
- Within a table, events are emitted in the order they were buffered.
- `DroppedEvent.ofPurgedBuffer` needs per-table counts for a purged transaction, not the events.

## Goals / Non-Goals

**Goals:**
- Each accepted event costs a fixed number of state operations; completeness, duplicate and blocked-progress checks read no events.
- Same inputs give the same `SequencerResult` (emitted transactions, alerts, drops) as today.
- `TransactionSequencer` stays Spark-free and unit-testable. The in-memory store can count operations and catches missing writes.

**Non-Goals:**
- **In-place checkpoint migration:** converting the old `buffers` into the new layout on startup. It's possible, but it depends on how Spark 4.1 handles state variables being added to an existing `transformWithState` checkpoint. This change uses a new checkpoint instead.
- **Peak memory at emission:** a transaction is still assembled in memory once to build its `OrderStream` records. Splitting output across batches would change the output contract.
- **Scaling out:** the single global key and single task stay.
- **Batching summary writes:** see D5.

## Decisions

### D1. Three state maps, events keyed by buffered index

| State variable | Key | Value | Written |
|---|---|---|---|
| `txs` | `xid:csn` | `TransactionSummary`: xid, csn, marker, received count per tracked table, event count `n` | on each accepted event and marker |
| `events` | `xid:csn#i` (`i` = 0 … n-1, the buffered order) | `DataChangeEvent` | once per event |
| `positions` | `xid:csn\|TABLE\|pos` | `i` (Long) | once per event with a non-null `pos` |

`queue` and `cursor` keep their names, types and meaning.

**This differs from the layout in the request**, which keyed events by `pos` and added an `index` map from `i` to `pos`. Keying events by `i` instead:
- handles null-`pos` events, which have no `pos` key but must still be buffered and counted
- keeps per-table de-duplication exact, because `TABLE` is in the `positions` key
- reads each event at emission with one lookup by an exact key, not two

The writes per event are the same: one event, one index entry (here `positions`), and one summary.

`i` is a global buffered order across all of a transaction's tables. Splitting the events by table while reading `0 … n-1` therefore reproduces today's per-table order.

The key separators `#` and `|` don't appear in numeric CSNs. The purge still parses the CSN only from `txs` keys, using `TransactionSequencer.csnOf` unchanged.

Alternatives considered:
- One map keyed by `xid:csn` holding a list: this is today's problem.
- A Spark `ListState` per transaction: it isn't possible, because state variables are fixed per grouping key and there is only one key.
- Composite struct keys via `Encoders.product`: the extra encoder complexity buys nothing over formatted strings, and string keys stay readable in the state-store reader.

### D2. `TransactionSummary` decides completeness

`TransactionSummary` is a bean with:
- `xid`, `csn`
- `metadata` (the same accessor pair as `TransactionState`'s, so `Encoders.bean` encodes it)
- `receivedOrders`, `receivedOrderDetails`, `receivedOrderLineItems`
- `eventCount`

Expected counts come from the marker, as today.

`isComplete`, `isOverfilled` and `progress` move into a small shared helper that takes expected and received counts. Both `TransactionSummary` and `TransactionState` call it, so the rules, the over-filled error log and the progress text stay identical. `TransactionState` keeps its public API, so `TransactionStateTest` and `OrderJoiner` are unchanged. It is no longer stored in state; it is built only when a transaction is emitted.

### D3. Sequencer operations

- **`acceptData`:**
  1. Replay check.
  2. Resolve the table. An untracked table is dropped as `UnknownTable` before any state access beyond the summary read, preserving today's precedence.
  3. If `pos` is non-null and `positions` contains `xid:csn|TABLE|pos`, the event is a `DuplicatePos` drop.
  4. Otherwise write `events(xid:csn#n)`, write `positions` if `pos` is non-null, increment the table count and `n`, and write the summary.

  That is at most 2 reads and 3 writes per event.
- **`acceptMarker`:** a duplicate is a summary with a non-null marker. Otherwise set the marker on the existing or a new summary, write it, and queue the transaction.
- **`emitReady`:**
  1. Check completeness from the summary.
  2. When complete, read `events(xid:csn#0 … n-1)` once and assemble a `TransactionState`.
  3. Remove each event and its `positions` entry, using the `pos` just read, then remove the summary and the queue entry.

  Keys to remove are collected first, so nothing is removed while a state iterator is open.
- **`purgeBelow`:**
  - Scan `txs` keys, as it scans `buffers` keys today.
  - Build the drop record from the summary's counts, with no event reads.
  - Remove the transaction's entries as on emission. `positions` needs the `pos` values, so each event is read once.
  - `MaxPurgePerStep` still limits the number of transactions removed per step.
- **`trackBlocked`:** progress comes from the head's summary.

### D4. `SequencerStore` reshaped, not replaced

The trait replaces `getBuffer`/`putBuffer`/`removeBuffer`/`bufferKeys` with:
- `getSummary`, `putSummary`, `removeSummary`, `summaryKeys`
- `getEvent(txKey, i)`, `putEvent(txKey, i, e)`, `removeEvent(txKey, i)`
- `positionExists(txKey, table, pos)`, `putPosition(txKey, table, pos, i)`, `removePosition(txKey, table, pos)`

The queue and cursor operations stay. Key formatting lives in the `TransactionSequencer` companion, so the Spark store and the in-memory store can't disagree on key format.

`InMemorySequencerStore` changes:
- It counts each operation by name.
- It copies beans on put and on get, using a Java-serialization round trip (`TransactionSummary`, `DataChangeEvent` and `Cursor` are `Serializable`). A missing put now fails a test instead of passing by accident.

### D5. Write-through summaries

The summary is written on every accepted event. It is small and fixed-size (one marker with a few table counts), so this keeps per-event cost constant.

A step-local cache that writes each summary once per batch would save writes. It adds a second copy of state within a step, which `emitReady`, `purgeBelow` and `trackBlocked` would all have to read consistently. It is left out until profiling shows the summary write matters.

### D6. Fail clearly on an old checkpoint

A new jar started on an old checkpoint must not run silently with state it can't see. Task 3.3 tests what Spark 4.1 does when the declared state variables differ from the checkpoint's.

If Spark doesn't refuse on its own, the processor also declares the legacy `buffers` variable, read-only. On the first step, it fails with a message naming the migration section if either of these holds:
- `buffers` has any entry
- the queue head has no summary

A drained old checkpoint (no buffers, empty queue) would be safe to continue, but the documented path is still a new checkpoint.

### D7. Testing the cost

A new `TransactionSequencerTest` case buffers a marker plus 55,000 line-item events of one transaction, in batches of 5,000, then emits it. It asserts:
- the emitted transaction holds all 55,000 events in buffered order
- `putEvent` calls equal 55,000, and summary writes are at most 55,000 plus one per marker
- total writes are at most 3 per event plus a constant
- no event is read before emission, and each event is read exactly once at emission
- after emission, the store holds no summary, event or position entries

A second case repeats the check at 5 events, to show the writes per event are the same constant. The tests count operations and never time anything, so they are deterministic.

## Risks / Trade-offs

- **[More state entries, about 2 per event instead of 1 object per transaction]** → RocksDB handles many small keys well. Entries live only until emission or purge. `STATE-STORE-DEBUG.md` is updated with queries over the new variables.
- **[A logic slip leaves orphaned `events` or `positions` entries]** → Every removal path (emission, purge) goes through one helper. Tests assert the store is empty after emission and after purge.
- **[A large purge now costs a read per event]** → It is linear, and bounded by `MaxPurgePerStep` transactions per step. Purged history transactions are usually small.
- **[A new checkpoint re-reads Kafka]** → The Migration Plan gives a cut-point start that avoids duplicate `order_stream` rows, with replay from `earliest` as the fallback.
- **[Encoders for the new beans fail at runtime only]** → The integration test runs the real `transformWithState` with RocksDB.

## Migration Plan

Stage 1 only. Stage 2 keeps its checkpoint and keeps running.

1. Stop `TransactionalCdcProcessingApp`. Note `C`, the highest `csn` in `order_stream`.
2. Find the offset of the first metadata message holding a marker with CSN `C` or higher. Starting at `C` rather than above it never skips a transaction that shares CSN `C` with one already written. The cost is that the few transactions in that message are written to `order_stream` again, which is harmless for `orders_current`.
3. Start the new jar on a **new** `job.output.checkpoint-location`, with these starting offsets:
   - `transaction-metadata` at the offset from step 2
   - the data topics at `earliest`, or at offsets known to precede the oldest in-flight transaction

   Data re-read for already-emitted transactions gets no marker. It is buffered, then purged once the first new transaction is emitted, and recorded in `dropped_events` as purged. Apart from the restart message's transactions, no `order_stream` row is written twice.
4. Fallback: start everything from `earliest`, as in the existing "Migrating an existing deployment" section. `order_stream` gets duplicate history rows unless it is truncated first. `orders_current` is unaffected, because the merge is version-aware.
5. Rollback: redeploy the previous jar on its original checkpoint, which this change leaves untouched. Transactions the new jar already emitted are emitted again into `order_stream`. That is harmless for `orders_current`.

`gg-configs/README.md` gets these steps as a new subsection. `CLAUDE.md`'s Known Limitations gains a note that this layout also needs a new checkpoint.
