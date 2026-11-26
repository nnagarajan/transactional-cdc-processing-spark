# Proposal

## Why

Every test today runs inside Maven against an in-memory store or a local Spark session fed by hand-built rows. Nothing checks the real chain: Oracle → GoldenGate (`ext04`, `kc`, `rep3`) → Kafka → `TransactionalCdcProcessingApp` → `order_stream` → `ScdType1MergeApp` → `orders_current` in Unity Catalog. In particular, nothing has checked that concurrent Oracle sessions produce exactly the committed data in commit order. The latency and throughput figures in `docs/PERFORMANCE-ESTIMATES.md` are explicitly "estimates, not measurements". We need repeatable end-to-end suites before the per-event buffering change (`fix-large-transaction-buffering`) and later releases go live.

## What Changes

- Add an end-to-end test harness (Scala, run by Maven Failsafe under a new `e2e` profile, never part of `./mvnw package`). It drives Oracle through JDBC with one connection per simulated session and reads the Unity Catalog tables through a local Spark session. It runs against the already-running local stack and fails fast if the stack isn't there.
- Every committed test transaction writes a row to an Oracle run log in the same transaction. The log table is not captured by `ext04`, and its `ORA_ROWSCN` (with `ROWDEPENDENCIES`) is the transaction's CSN. The log records the session, the transaction kind, the order ids it touched, per-table event counts and the commit timestamp. It is the ground truth every suite compares against.
- **Suite A, correctness and ordering (`CorrectnessOrderingIT`).**
  - Between 10 and 20 concurrent sessions (seeded random by default, configurable) run a mixed workload: single-order and multi-order inserts, updates of the session's own orders, contended updates of a shared set of "hot" orders, child-only changes, and rolled-back transactions.
  - Once the pipeline has caught up, the suite compares Oracle with `order_stream`, `orders_current` and `dropped_events`. It checks:
    - every committed transaction appears exactly once with the right per-table contents
    - rolled-back work never appears
    - per-order versions rise with CSN
    - emission follows commit order
    - `orders_current` equals the final Oracle state
    - nothing from the run was dropped
- **Suite B, performance (`PerformanceIT`).**
  - Seven transaction flavors, run as a sustained mixed load and as a bulk phase. The flavors include 1 order + 1 detail + 1/3/5/10 line items, a 10-order transaction, 100-order bulk chunks and an update flavor.
  - The default sustained load matches the documented target of 100,000 events per minute, run with 16 sessions for 5 minutes. The suite measures throughput and end-to-end latency percentiles and confirms that every transaction arrived.
- **Latency run (`CommitLatencyIT`, part of suite B).** One session commits 200 orders, one per transaction. For each order, the suite compares the Oracle commit timestamp with `dwhProcessedTs` and with the time the row first became visible in `order_stream` and in `orders_current`. It writes a per-order latency report and a percentile summary.
- Reports (CSV plus a Markdown summary) go to `target/e2e-reports/`. A new `docs/E2E-TESTING.md` explains prerequisites, configuration and how to read the reports. `docs/PERFORMANCE-ESTIMATES.md` links to it as the way to measure.
- No change to the application code, its configuration or its tables.

## Capabilities

### New Capabilities
- `end-to-end-verification`: the end-to-end suites that check the live pipeline against the Oracle source. Covers workload generation across concurrent sessions, the commit ground-truth log, the correctness and ordering comparisons against the Unity Catalog tables, the performance workload and its metrics, and per-order commit-to-output latency reporting.

### Modified Capabilities
<!-- None: the suites verify existing transaction-ordering, current-state-merge and dropped-event-audit requirements without changing them. -->

## Impact

- **Code:** new test sources under `src/test/scala/com/technext/demos/txbuffering/e2e/` and `src/test/resources/e2e.conf`. A new Oracle script under `db/e2e/` creates the run log table and cleans up a run.
- **Build:** `pom.xml` gets a test-scoped `com.oracle.database.jdbc:ojdbc11` dependency and an `e2e` profile that binds Failsafe to `*IT` classes. Normal builds and `./mvnw test` don't change.
- **Systems:** needs the local stack running: Oracle `ORCLPDB` as `APPUSER`, GoldenGate `ext04`/`kc`/`rep3`, Kafka, the UC server on `localhost:7071`, and both streaming apps. Test data is tagged with a run id, so it can be told apart from other data in the source tables and in UC. It is not removed automatically.
- **Docs:** new `docs/E2E-TESTING.md`. Links from `docs/PERFORMANCE-ESTIMATES.md` ("How to measure") and `README.md`.
- **Out of scope:**
  - deletes (the joiner emits no row for a delete, so there is nothing to compare)
  - failure injection such as restarting an app mid-run
  - running the suites in CI
