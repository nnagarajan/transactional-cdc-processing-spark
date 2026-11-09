# Design

## Context

See proposal.md for motivation and specs/dropped-event-audit/spec.md for required behaviour. Current state:

- **Drop points in `TransactionSequencer`:**
  - `acceptMarker`: replay below or at the cursor, duplicate marker.
  - `acceptData`: replay, and `TransactionState.addEvent` returning false for a duplicate `pos` or an unknown table. The duplicate and unknown-table cases aren't distinguished today.
  - `purgeBelow`, from `go-live-cold-start`.
  - Each drop logs, and `SequencerResult` holds only `emitted` and `alert`.
- **Drops before the sequencer:**
  - `TransactionalCdcProcessingApp` drops rows with a null `xid`/`csn` with `.filter`.
  - `CdcJsonParsing.parseTransactionMetadata` uses `posexplode(from_json(...))`, so a metadata message that fails to parse yields no row at all.
  - Data rows don't keep their Kafka topic, partition or offset. Markers keep partition and offset as `marker_partition` and `marker_offset`.
- **Single stateful operator:** all rows go through one `transformWithState` on one key, and output is `Dataset[OrderStream]`, written with `toTable`. A second query over the same operator would run a second sequencer with its own state, so drop records have to leave through the same operator's output.
- **Integration tests** call `orderTransactions` and collect with `foreachBatch`.

## Goals / Non-Goals

**Goals:**
- One durable row for every drop, written in the same micro-batch as the output it accompanies.
- No change to the sequencer's rules or to `order_stream`.

**Non-Goals:**
- Replaying dropped rows back into the pipeline.
- Recording blocked (not dropped) transactions. They stay in state and are already alerted on.
- Alerting on drop rates. That's a query on the table, left to monitoring.

## Decisions

### D1. Table shape
The table is wide, one row per dropped item, with nullable columns for each kind. It has `droppedCount INT` (1 for a full row, n for a summary row) and is partitioned by `droppedDate`. The DDL is in `db/create_dropped_events_table.deltalake.sql`, Unity Catalog managed (`delta.feature.catalogManaged`), with 30 days of log retention:

| Group | Columns |
|---|---|
| Drop | `droppedTs`, `droppedDate`, `batchId`, `itemKind`, `reason`, `droppedCount`, `lastEmittedCsn` |
| Identity | `xid`, `csn` |
| Kafka | `kafkaTopic`, `kafkaPartition`, `kafkaOffset`, `markerIndex` |
| Data event | `tableName`, `opType`, `opTs`, `pos`, `before`, `after` |
| Marker | `markerEventCount`, `markerTxTs`, `dataCollections` |
| Purged buffer | `bufferedOrders`, `bufferedDetails`, `bufferedLineItems` |
| Raw message | `rawValue` |

**Alternative:** one table per item kind. Rejected, because investigating one transaction would then need a union of four tables.

### D2. Drops flow out of the sequencer in its result
`SequencerResult` gains `dropped: Seq[DroppedEvent]`, filled at each drop point next to the existing log call. `TransactionState.addEvent` returns a small result (`Added`, `DuplicatePos`, `UnknownTable`) instead of a `Boolean`, so the two reasons can be told apart. `batchId` and `droppedTs` are filled by the sink, and the processing time by the processor.

In `summary` replay mode, the sequencer folds the step's `REPLAY_*` drops by `(xid, csn, reason)` into one row with `droppedCount`, keeping the first item's Kafka coordinates.

### D3. Envelope output from the stateful operator
The output type becomes a bean, `SequencerOutput`, with `kind` (`ORDER` | `DROPPED`), a nullable `order: OrderStream` and a nullable `dropped: DroppedEvent`. `Encoders.bean` can't encode a sealed trait, so a wrapper with nullable fields is the workaround.

`TransactionOrderingProcessor.orderTransactions` returns `Dataset[SequencerOutput]`. Helpers `ordersOf(ds)` and `droppedOf(ds)` select each side, so the integration tests change by one line.

**Alternative:** encode drops as `OrderStream` rows with a flag. Rejected: it pollutes the output model, and `order_stream`'s schema would gain audit columns.

### D4. Unusable messages pass through the sequencer
`CdcJsonParsing` keeps the raw `json` string and adds `source_topic`, `source_partition` and `source_offset` to every row.

- **Data topics:** a row whose `xid` or `csn` is null becomes `event_type = RAW_DROP`. Its `drop_reason` is `UNPARSEABLE` if `get_json_object(json, '$')` is null, and `MISSING_IDS` otherwise.
- **Metadata topic:** the message is parsed into an array first. If that fails, it becomes one `RAW_DROP` row instead of vanishing in `posexplode`. Markers inside an array that lack ids become `MISSING_IDS` rows.
- The app's `.filter` is removed. `CdcRows` maps `RAW_DROP` rows to a new `RawDropInput`, which the sequencer turns directly into a drop record without touching state.

`marker_partition` and `marker_offset` stay, because the sequencer's partition check and queue order depend on them. The `source_*` columns are what the audit row uses.

**Alternative:** write raw drops from a separate stateless query. Rejected: a second query means a second checkpoint and a second sink, and it would need its own exactly-once handling. The volume is small, so passing through the single task costs nothing.

### D5. `foreachBatch` sink with idempotent Delta writes
`toTable` is replaced by `foreachBatch((batch, batchId) => ...)`, which does the following:

1. Persists the batch.
2. Appends `ordersOf(batch)` to `order_stream`.
3. Appends `droppedOf(batch)`, stamped with `batchId` and `droppedTs`, to `dropped_events`, when enabled.
4. Unpersists the batch.

Both writes set `txnAppId` and `txnVersion = batchId`, so Delta skips a write that already committed if the batch re-runs. `order_stream` is written first, so output never waits on the audit write.

`txnAppId` must be unique per checkpoint, because a new checkpoint restarts batch ids at 0 and Delta would skip them as already written. It's taken from the query id in `<checkpoint>/metadata`, which Spark writes when the query is created and keeps for the checkpoint's lifetime, with a different suffix per table: `<queryId>-order_stream` and `<queryId>-dropped_events`.

**Alternatives:**
- **A fixed configured `txnAppId`:** rejected. Reusing it after a checkpoint change silently skips every write.
- **A random id per run:** rejected. It breaks idempotence across restarts.

### D6. Disabled mode
When `job.output.dropped-events.enabled = false`, the sequencer still builds drop records. They are cheap, and the code path stays the same. The sink just doesn't write them, and the table isn't resolved at startup.

## Risks / Trade-offs

- **[Changing the sink on an existing checkpoint]:** `toTable` to `foreachBatch` is a sink change. Spark allows it in general, but this pipeline's checkpoint hasn't been tested with it. → A task verifies resume on a copy of `/tmp/spark-checkpoints-ordered` with no gap or duplicate. Fallback: a new checkpoint plus the go-live runbook.
- **[Replay volume]:** in `full` mode, a GoldenGate replay of N events writes N rows. → `summary` mode, and documentation of when to use it.
- **[Purge volume after a go-live from `earliest`]:** up to 100,000 summary rows per batch. → Acceptable. They are summary rows, bounded by the purge cap.
- **[Bigger state task output]:** drop records go through the single task. → Records are small, apart from `before`/`after` on data events. `summary` mode limits the largest case.
- **[The audit write fails after `order_stream` committed]:** the batch fails and is retried. `order_stream` is skipped as already written (D5), and the audit write retries. Output keeps flowing only once the audit table is healthy again. → Document that; set `enabled = false` to unblock if the table is broken.
- **[Raw payloads may contain sensitive column values]:** they're the same data that is in Kafka and `order_stream`. → Same access controls as `order_stream`; retention is set in the DDL.

## Migration Plan

1. Run `SeedJob` (or the new script) to create `mymanaged.argus1.dropped_events`.
2. Deploy the jar. To start with today's behaviour, set `enabled = false`, then enable it.
3. Before using an existing production checkpoint, confirm the resume test (see Risks). Otherwise start on a new checkpoint with the go-live runbook.

Rollback: redeploy the previous jar on the same checkpoint. The previous `toTable` sink resumes, which is the same sink change in reverse and is covered by the same resume test. The audit table is left as is.

## Open Questions

- Retention for `dropped_events` (30 days in the DDL) is a property and can be changed without code changes.
