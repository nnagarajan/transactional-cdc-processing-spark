# Proposal

## Why

The pipeline completes a transaction only when its marker's per-table counts match the data exactly. A wrong marker blocks all output forever. The deployed GoldenGate formatter builds one marker per delivered `DsTransaction`, using that delivery's xid/csn and counting all its operations.

With `GROUPTRANSOPS 1000` in `rep3.prm`, Replicat groups whole source transactions into one delivery whenever several are waiting in the trail, for example after a restart or a backlog. A grouped delivery therefore produces a single marker under the first transaction's id, with the summed counts of all of them.

It hasn't happened yet only because every delivery so far held one transaction. Nothing in the repo tested the formatter, so the contract couldn't be verified.

## What Changes

- Add the marker formatter to this repo as `gg-formatter/` (`TransactionBoundaryEventJsonFormatter`), with its own Maven build against the local GoldenGate jars and JUnit 4 + Mockito tests of its output.
- The formatter emits **one marker per source transaction**. It groups the delivered operations by each operation's own xid:csn, in trail order. A delivery with one source transaction still produces the same single JSON object as before. A delivery with several produces one Kafka message holding a **JSON array** of markers, in commit order.
- The pipeline parses both message forms. Each marker keeps its Kafka offset and its position in the message. Markers are queued by `(offset, index)`, which keeps commit order within a grouped message.
- Document the marker contract:
  - in `gg-formatter/README.md`, `gg-configs/README.md`, `IMPLEMENTATION.md` and `CLAUDE.md`
  - `MAXTRANSOPS` must stay unset in `rep3`, since it splits source transactions and isn't handled

## Capabilities

### New Capabilities
- `transaction-markers`: production and consumption of transaction markers. One marker per source transaction, with exact per-table counts; the single-object and array message forms; and marker order within a message.

### Modified Capabilities
- None. No main specs exist yet. `transaction-ordering`, from the in-progress `ordered-transaction-emission`, still defines marker order across messages. This change only adds order within a message.

## Impact

- **New module:** `gg-formatter/`, containing `pom.xml` (system-scoped jars from `/opt/oracle/ggbd/ggjava/resources/lib`), the formatter, `TransactionMetaDataType`, tests and a README. It isn't part of the main Maven build and builds only where GoldenGate is installed.
- **Spark code:**
  - New `CdcJsonParsing`, with the topic parsing moved out of `TransactionalCdcProcessingApp`. The metadata parse uses `posexplode(from_json(json, 'array<struct<…>>'))` and adds a `marker_index` column.
  - `MarkerInput` gets an `index` field.
  - `TransactionSequencer` sorts markers by `(offset, index)`.
  - `CdcRows` reads `marker_index`, defaulting to 0.
- **Tests:**
  - 8 formatter tests; the 4 grouped-delivery tests fail against the original formatter.
  - `CdcJsonParsingTest` (3).
  - A new `TransactionSequencerTest` case.
  - The Spark suite goes from 33 to 37 tests.
- **Wire format:** the metadata topic may now carry a JSON array. Consumers other than this pipeline must accept both forms. Single-transaction messages are unchanged, so deployment order doesn't matter.
- **Deployment:** copy the formatter jar to `/opt/oracle/ggbd/ggjava/resources/customlib/`, then restart `rep3`.
- **Upstream:** the original formatter lives in `~/github/goldengate/gg-custom-formatters`. Porting the change there is a separate change in that repo.
