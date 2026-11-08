# Design

## Context

See proposal.md for motivation. GoldenGate for Big Data 21.4 behaviour, read from the jars in `/opt/oracle/ggbd/ggjava/resources/lib` and the `rep3` logs:

- **One message per delivery:** with `gg.handler.kafkahandler.mode=tx`, `KafkaHandler.transactionCommit` calls `NgFormatter.formatOp(...)` once per operation, each into a fresh `NgFormattedData`. It concatenates the bytes and calls `processData` once, so there is **one Kafka message per delivered `DsTransaction`**. The handler never calls `beginTx` or `endTx`.
- **Delivery ids are the first transaction's:** `DsTransaction.xidStr`/`csnStr` are `private final`, set in the constructor when the delivery begins (log line `New Transaction: ... end: unset, xid: ..., csn: ...`). In a grouped delivery they are the first source transaction's ids.
- **Each operation knows its own ids:** `DsOperation.getXidStr()`/`getCsnStr()` are per operation.
- **At most 2 output slots:** `NgBAOSFormattedData.size()` counts only `streams[0]` and `streams[1]`.
- **Grouping:** `GROUPTRANSOPS` is a minimum. Replicat adds whole source transactions until the group reaches it, and never splits a transaction. `MAXTRANSOPS` does split.
- **Live data so far:** the one marker on `dev.transaction_metadata_json` (offset 0, `1342848513.8.14.5716:344374437`, 100/100/350) matches the data topics exactly. No grouped delivery has occurred.

## Goals / Non-Goals

**Goals:**
- A correct marker for every source transaction under any `GROUPTRANSOPS` value.
- No change to single-transaction output.
- The marker contract testable without a GoldenGate runtime.

**Non-Goals:**
- One Kafka record per xid:csn (see D3).
- Handling `MAXTRANSOPS` splits (see Risks).
- Porting the change to `~/github/goldengate/gg-custom-formatters`.

## Decisions

### D1. Group by each operation's own xid:csn
On the delivery's last operation (the existing `getLastOperation().getOperationSeqno()` check), the formatter does the following:
- Walks `getOperations()`, grouping into a `LinkedHashMap` keyed by `op.getXidStr() + ":" + op.getCsnStr()`, which keeps trail order. Null ids fall back to the delivery's.
- Counts operations per table (`getShortName()`) and the total for each group.

This relies only on per-operation ids, which GoldenGate sets from the trail, not on any guess about how deliveries are formed.

**Alternative:** `DsOperation.getTxState()` (`BEGIN`/`MIDDLE`/`END`/`WHOLE`) as a boundary signal. Rejected because the field defaults to `UNKNOWN` and is set by native code, so its meaning under grouping is unverified.

### D2. Message form: an object for one transaction, an array for several
For a single group, the formatter keeps the original values (the delivery's xid, csn and `getTransactionBeginTime()`) and writes one object, so the output is identical to today's. For several groups, it writes a JSON array of objects, each with its own group's ids and first operation's `getTimestamp()` as `tx_ts`.
- **Why not always an array:** an object for the common case avoids a format change for existing messages and tooling.
- **Why not newline-delimited JSON:** an array survives pretty-printing and is still valid JSON for viewers such as Control Center.

### D3. Stay in `mode=tx`; don't split records
A per-xid:csn Kafka record isn't possible from the formatter in `mode=tx` (one message per delivery, at most 2 output slots).

**Alternatives considered, both deferred:**
- **`mode=op` with running per-transaction counts:** emit on `END`/`WHOLE`. It depends on the unverified `getTxState()`, and a filtered last operation would stall the marker.
- **A custom producer:** `gg.handler.kafkahandler.customKafkaProducer`, a class with a `(Properties)` constructor implementing `org.apache.kafka.clients.producer.Producer`, that splits arrays into records. It's viable, compiled against `kafka_3.3.2`, but adds code to the replication path. It's only needed if other consumers require one record per transaction.

### D4. Spark parses one array schema for both forms
`posexplode(from_json(json, 'array<struct<…>>')) as (marker_index, data)`. Spark's JSON parser maps a lone object onto an array-of-struct schema as a one-element array. Verified on Spark 4.1.1: the object, the array, and an object with a trailing newline all parse. Each marker becomes its own row with `marker_partition`, `marker_offset` and `marker_index`.

Parsing moved from `TransactionalCdcProcessingApp`, which is an `App` object, to `CdcJsonParsing`, so it can be unit-tested.

### D5. Queue order `(offset, index)`
`MarkerInput` gains `index: Int = 0`, and the sequencer sorts each batch's markers by `(offset, index)`. `CdcRows` reads `marker_index` when it's present and non-null, and defaults to 0 otherwise, so existing test rows without the column still work. Replay and duplicate detection are unchanged: they key on xid:csn and CSN.

### D6. Formatter tests drive the handler's call pattern
The tests use mocked `DsTransaction`/`DsOperation` and call `formatOp` for every operation into one `NgBAOSFormattedData`, as `mode=tx` does, then parse the bytes. Run against the original formatter, the 4 grouped-delivery tests fail (array expected, object produced) and the 4 single-transaction tests pass. That shows both the bug and the unchanged single-transaction output.

## Risks / Trade-offs

- **[`MAXTRANSOPS` splits a source transaction]:** each part gets a marker with partial counts. The sequencer keeps the first and ignores the rest as duplicates, and the transaction then overfills and blocks. → `rep3.prm` keeps `MAXTRANSOPS` commented out. The contract says it must stay unset. A fail-fast check based on `getTxState()` is deferred until it's verified live.
- **[Grouped message size]:** about 280 bytes per marker. `GROUPTRANSOPS 1000` caps a group at about 1,000 transactions, about 276 KB uncompressed. That's under the topic's 1,048,588-byte `max.message.bytes`, and gzip is on. → Revisit `max.message.bytes` if `GROUPTRANSOPS` is raised well above 1,000.
- **[Malformed message]:** `from_json` returns null and `posexplode` yields no rows. The marker is silently dropped, and the transaction then blocks with an alert. Before this change the row was dropped by the xid filter instead. → Possible follow-up: count or log unparseable metadata messages.
- **[Build only where GoldenGate is installed]:** the system-scoped jars live at `/opt/oracle/ggbd/...`. → Kept out of the main build; `-Dlib.location` overrides the path.
- **[Unverified live]:** grouping has only been exercised with mocks. → Task 4.2.

## Migration Plan

1. Deploy the Spark pipeline first or second. It reads both forms, so the order doesn't matter.
2. Build: `./mvnw -f gg-formatter/pom.xml clean package`.
3. Copy `gg-formatter/target/gg-custom-formatters-1.0-SNAPSHOT.jar` to `/opt/oracle/ggbd/ggjava/resources/customlib/`. The deployed jar today is the original build from 2025-07-07.
4. Restart `rep3` (`STOP REPLICAT rep3`, `START REPLICAT rep3`).

Rollback: put the original jar back and restart `rep3`. To run safely without grouping on the original jar, set `GROUPTRANSOPS 1` in `rep3.prm`: one source transaction per delivery.

## Open Questions

- Should `rep3.prm` move to `GROUPTRANSOPS 1` as well, as defence in depth? The editor had an unsaved `GROUPTRANSOPS 1`; the file on disk still says 1000. Either choice is correct with the new formatter.
