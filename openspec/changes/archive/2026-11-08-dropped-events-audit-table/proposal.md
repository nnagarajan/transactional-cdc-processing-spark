# Proposal

## Why

The pipeline drops input in several places, by design:
- replayed events and markers below the cursor
- duplicate markers
- duplicate or unknown-table events
- buffers that can never be emitted, removed by the purge from `go-live-cold-start`
- Kafka messages that don't parse, or have no `xid`/`csn`

Today these show up only as log lines at levels from DEBUG to WARN, or not at all: an unparseable metadata message disappears without a trace. When output looks wrong, or a transaction is missing, there's no durable, queryable record of what was thrown away and why. Logs rotate, and DEBUG is off in production.

## What Changes

- **New Delta table `dropped_events`** (Unity Catalog managed, partitioned by drop date), created by a new `db/create_dropped_events_table.deltalake.sql` that `SeedJob` picks up. There is one row per dropped item:
  - a data event
  - a marker
  - a purged buffer, as one summary row per transaction
  - a raw Kafka message that couldn't be used

  Each row has a `reason`, the cursor CSN at the time, the transaction identity and the Kafka coordinates, plus the event, marker, buffer-count or raw-payload columns for its kind.
- **The sequencer reports its drops.** Every drop it already makes, with the existing rules unchanged, is returned alongside the emitted transactions. The existing log lines stay.
- **Unusable messages are kept and recorded.** Messages that fail parsing or have no `xid`/`csn` are passed through the sequencer as drop records, instead of being filtered out or lost in `posexplode`.
- **Kafka coordinates for data events.** Every parsed row carries its topic, partition and offset, so dropped data events can be traced back to Kafka.
- **The sink becomes a `foreachBatch`** that writes `order_stream` first and `dropped_events` second. Both writes are idempotent per batch (Delta `txnAppId`/`txnVersion`), so a retried batch never duplicates rows. Output to `order_stream` is unchanged.
- **Config:**
  - `job.output.dropped-events.table-name` (default `argus1.dropped_events`)
  - `job.output.dropped-events.enabled` (default `true`; `false` keeps today's log-only behaviour)
  - `job.output.dropped-events.replay-detail`: `full` (default) writes one row per replayed event or marker, `summary` writes one row per replayed transaction
- **BREAKING (internal):** `TransactionOrderingProcessor.orderTransactions` returns an envelope type, with an order or a drop record, instead of `OrderStream`. Callers (the app, the integration tests) select the orders from it.

## Capabilities

### New Capabilities
- `dropped-event-audit`: recording every input the pipeline discards, with its reason, in a queryable table, without changing what the pipeline emits.

### Modified Capabilities
- None. There are no main specs yet. The drop rules themselves, in `ordered-transaction-emission` (`transaction-buffering`, `transaction-ordering`) and in `go-live-cold-start` (`buffer-retention`), are unchanged: this change records their effects.

## Impact

- **Depends on** `go-live-cold-start`, whose orphan-buffer purge is one of the recorded drop sources. That code is implemented, but its rehearsal tasks are still open.
- **Code:**
  - `CdcJsonParsing`: source coordinates on every row, a raw `json` column, and rows for unusable messages.
  - `CdcRows`: a new drop-input kind.
  - `TransactionSequencer`: `SequencerResult` gains its drops.
  - `TransactionOrderingProcessor`: envelope output.
  - `TransactionalCdcProcessingApp`: `foreachBatch` sink.
  - New `DroppedEvent` model.
- **Tables:** new `mymanaged.argus1.dropped_events`. `order_stream` is unchanged.
- **Tests:**
  - sequencer tests asserting each drop reason
  - parsing tests for unusable messages
  - an integration test for the two-table write, including idempotence on a replayed batch
- **Checkpoint:** the state layout is unchanged, but the sink changes from `toTable` to `foreachBatch`. That has to be verified on an existing checkpoint before relying on it.
- **Volume:** a large replay after a GoldenGate restart writes a row per replayed event in `full` mode. `summary` mode bounds that.
