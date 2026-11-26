# Design

## Context

See proposal.md (Why) and `specs/end-to-end-verification/spec.md` for the required checks. The existing code and stack that shape the approach:

- **Oracle** runs on this host (`/opt/oracle/db_home`, PDB `ORCLPDB`, schema `APPUSER`). The source tables are `orders`, `order_details` and `order_line_items` (`db/oracle-ddl.sql`). `order_id` and `line_item_id` are identity columns. Triggers bump `version` on every update. `order_ref`, `tracking_number` and `(order_id, product_id)` are unique.
- **`ext04`** captures only those three tables (plus the GG heartbeat). It sets `IGNOREDELETES` and `UPDATERECORDFORMAT FULL`, so deletes never reach Kafka and updates carry full before images. A table created in `APPUSER` for the tests is not captured. `db/simulate-grouped-markers.sql` already uses that for its `ROWDEPENDENCIES` log table, whose `ORA_ROWSCN` is the GoldenGate CSN.
- **`order_stream`** holds `xid`, `csn`, `orderId` and `dwhProcessedTs`, with arrays `orders`, `orderDetails` and `lineItems`, each entry carrying a `before` struct. `dwhProcessedTs` is `Instant.now()` taken once per transaction in `OrderJoiner.joinTransaction`, inside the stateful operator, before the Delta write. Numbers are `DOUBLE`. Dates and timestamps are strings in GoldenGate's format.
- **`orders_current`** is maintained by `ScdType1MergeApp`. Its `csn` and `dwhProcessedTs` come from the last transaction that touched the order.
- **Unity Catalog:** all three tables are UC managed Delta tables in `mymanaged.argus1` (`delta.feature.catalogManaged`). They are read through `UCSingleCatalog` on `http://localhost:7071`. `SparkSessionCreator.getSpark(config)` already builds a local session with that catalog from `reference.conf`.
- **Build:** `maven-failsafe-plugin` is declared in `pluginManagement` with the `--add-opens` flags, but no test is bound to it. Spark, Typesafe Config and Jackson are `provided`, so they are on the test classpath.

## Goals / Non-Goals

**Goals:**
- One harness shared by both suites: sessions, ground-truth log, UC reader, wait loop and reports. Each suite is a thin JUnit class on top.
- Every number in a report can be traced to Oracle (log rows) or to UC (table rows and Delta history), never only to the client's view.
- Runs are reproducible from a seed and isolated by run id.

**Non-Goals:**
- Starting or orchestrating the stack (Docker, GGSCI, spark-submit). The suites assume it is running.
- Measuring per-stage latency (Extract, Replicat, Kafka). Only commit → `dwhProcessedTs` and commit → visible in UC are measured.
- Automatic cleanup of test rows from Oracle or UC. A cleanup script for Oracle is provided. UC rows are left in place, because deleting from `order_stream` would rewrite history other checks may rely on.
- Changing GoldenGate settings (for example `GROUPTRANSOPS`) per run.

## Decisions

### D1. Scala JUnit tests run by Failsafe under an `e2e` profile

The suites are `*IT` classes in `src/test/scala/.../e2e/`. A new `e2e` Maven profile binds `maven-failsafe-plugin` (`integration-test` and `verify`) with `<includes>**/e2e/*IT</includes>`. Run them with:

```bash
./mvnw -Pe2e verify -Dit.test=CorrectnessOrderingIT
```

Surefire keeps its default includes (`*Test`), so `./mvnw package` and `./mvnw test` never touch Oracle.

*Alternatives:* PL/SQL plus `sqlplus` plus `spark-sql` shell scripts, like `simulate-grouped-markers.sql`. That needs no new dependency, but concurrent sessions, pacing, value capture and report generation in shell are fragile, and the comparisons would need a second language. A Python harness would add a toolchain the repo doesn't have. Scala reuses the model beans, the config loader and `SparkSessionCreator`.

### D2. Sessions are JDBC connections on a thread pool

Each simulated session is one thread that owns one `java.sql.Connection` with `autoCommit = false`. The Oracle JDBC driver comes from the test-scoped dependency `com.oracle.database.jdbc:ojdbc11`, which is on Maven Central.

The session count defaults to `10 + rnd.nextInt(11)` from `e2e.seed`, which itself defaults to the current time and is printed. Each session gets its own `Random(seed + sessionNo)`, so the sequence of kinds and sizes is reproducible. The interleaving across sessions, and therefore the CSNs, is not reproducible, and the checks don't depend on it.

`e2e.conf` hardcodes the credentials of the local development database: user `appuser`, password `appuser`. The JDBC URL defaults to `jdbc:oracle:thin:@//localhost:1521/ORCLPDB`. All three can be overridden with `E2E_ORACLE_USER` and `E2E_ORACLE_PASSWORD`, or with `-De2e.oracle.*`, for a database with other credentials. The committed password is acceptable only because this is a local, throwaway test schema. Never point the suites at a shared database with these defaults.

### D3. Ground truth: an uncaptured `ROWDEPENDENCIES` log table, written in the same transaction

`db/e2e/e2e-setup.sql` creates two tables:

- `e2e_tx_log (run_id, session_no, tx_seq, kind, local_tx_id, commit_ts_utc, orders_cnt, details_cnt, line_items_cnt) ROWDEPENDENCIES`
- `e2e_tx_orders (run_id, session_no, tx_seq, order_id) ROWDEPENDENCIES`

Just before `COMMIT`, each transaction inserts its rows with:

- `commit_ts_utc = SYS_EXTRACT_UTC(SYSTIMESTAMP)`
- `local_tx_id = DBMS_TRANSACTION.LOCAL_TRANSACTION_ID`

After the commit, `ORA_ROWSCN` on those rows is the commit SCN, which equals the GoldenGate CSN. The CSN is read in bulk at check time (`SELECT ..., ORA_ROWSCN csn FROM e2e_tx_log WHERE run_id = ?`). The client also records `clientAckTs` when `commit()` returns.

A rolled-back transaction rolls back its log rows too. The client keeps a separate in-memory list of rollbacks: the order ids it got from `RETURNING` and a marker value written into `customer_id`. This feeds the no-phantom check.

The join key is the CSN plus the order id. The `xid` is checked exactly: GoldenGate writes it as `<prefix>.<usn>.<slot>.<sqn>` (for example `1342848513.10.25.112767`), and the part after the prefix is `LOCAL_TRANSACTION_ID` (`10.25.112767`). So every `order_stream` row at a CSN must have an `xid` ending in `.` + that transaction's `local_tx_id` (task 1.3).

*Alternatives:*
- `SCN_TO_TIMESTAMP(ORA_ROWSCN)` has a resolution of about 3 seconds, too coarse for latency.
- The GoldenGate `op_ts` is in Kafka but isn't stored in `order_stream`.
- A client-side timestamp alone is later than the commit by the round trip and doesn't give the CSN.

### D4. Expected values are captured inside the transaction

For each DML statement the session captures what the pipeline should show:

- **Inserts:** `RETURNING order_id`/`line_item_id`. Then the session `SELECT`s the inserted rows inside the transaction to get defaulted columns: `version = 1` and `created_ts`.
- **Updates:** `SELECT ... FOR UPDATE` first, which gives the before image, then `UPDATE`, then `SELECT` again, which gives the after image with the trigger-bumped `version`.

The expected `order_stream` row for `(orderId, csn)` is built from those images. Comparisons normalize types:

- numbers are compared as `Double`
- `DATE`/`TIMESTAMP` values are compared after parsing both sides to `LocalDateTime`
- strings are compared exactly

The GoldenGate string formats, read from real `order_stream` rows (task 1.3):

| Oracle type | Example in `order_stream` |
|---|---|
| `DATE` (`order_date`, `estimated_delivery_date`) | `2026-02-17 00:00:00` |
| `TIMESTAMP` (`order_ts`, `created_ts`, `shipped_ts`) | `2026-02-17 10:00:05.000000000` (9 fractional digits) |
| `NUMBER` | `DOUBLE`, for example `336.6666` |
| `CHAR(3)` | as stored, for example `USD` |

`dwhProcessedTs` is an ISO-8601 instant with nanoseconds, for example `2026-11-09T03:11:38.535661571Z`.

Hot-order updates lock the shared orders in ascending `order_id` order, which avoids deadlocks. Lock waits are expected and are the point of the test.

### D5. Run isolation by tag, check scope by order id

The run id is `yyyyMMddHHmmss-<4 hex>`. Every inserted order gets:

- `order_ref = 'E2E-<runId>-S<nn>-<seq>'`
- `customer_id = 'E2E-<runId>'`
- `tracking_number = 'E2E-<runId>-<orderId>'`

`product_id` is `'P-<n>'`, unique within the order. Hot orders are created by a setup transaction (session 0) that is itself in the ground truth. Every UC query filters on `orderId IN (<run order ids>)`, loaded from `e2e_tx_orders`. Rows are processed in chunks of 1,000 ids, or through a temp view joined with a DataFrame of ids for the large performance runs.

### D6. Commit order and visibility from row tracking and Delta history

All three UC managed tables have `delta.enableRowTracking` and `delta.enableInCommitTimestamps` set (task 1.2). So every row exposes `_metadata.row_commit_version`, the version of the commit that last wrote it, and `DESCRIBE HISTORY` gives each version's in-commit `timestamp`. One scan of the current table, filtered to run order ids, gives each run row's commit version. Joining it with the history gives the time it became visible.

- **`order_stream`** is only appended to by the pipeline, so `row_commit_version` is the commit that added the row.
- **`orders_current`** is written by MERGE, so `row_commit_version` is the commit that last changed the row. For the row's current `csn`, that is the commit that made this state visible.

This gives two things:

1. **Commit-order check.** Group run rows by `row_commit_version`, then compare `max(csn)` of each version with `min(csn)` of every later version. The rule is the existing `transaction-ordering` requirement "Commit-level ordering of output".
2. **Visibility time.** A row's visible time is the history `timestamp` of its `row_commit_version`. That is exact to the millisecond, with no polling.

*Why not time travel:* `VERSION AS OF` works through `UCSingleCatalog`, but each version needs its own scan, and every version older than `delta.deletedFileRetentionDuration` (7 days) is refused (task 1.2). A table without row tracking is refused at pre-flight (see Risks).

The per-order version check is a plain query: group run rows by `orderId`, sort by `csn` as a decimal, and require strictly increasing `orders[0].version`. A row without an order entry (a child-only transaction) uses the detail or line-item versions per entity instead.

*Alternative:* checking that `dwhProcessedTs` is non-decreasing in CSN order. It is cheaper, but a weaker check that depends on the wall clock. It is kept as an extra check, not the primary one.

### D7. Catch-up wait

After the workload, the suite loads the ground truth `(orderId, csn)` set and polls `order_stream` every `e2e.poll-interval` until the set is contained. For the correctness suite it also polls `orders_current` until every run order's `csn` equals its highest ground-truth CSN. Each poll logs progress as "found / expected". If `e2e.catch-up-timeout` (default 10 min) passes, the suite fails and lists the missing transactions with session, kind and CSN. The same loop, with a 2-minute default, serves the pre-flight canary.

### D8. Workloads

**Correctness:**
- S sessions (10–20).
- Before the workload, the setup transaction creates `e2e.correctness.hot-orders` (default 5) hot orders with 3 line items each.
- Each session runs `e2e.correctness.tx-per-session` (default 40) transactions, unpaced, drawn with these weights:

| Kind | Weight |
|---|---|
| `INSERT_ONE` | 25% |
| `INSERT_MULTI` (2–5 orders) | 10% |
| `UPDATE_OWN` (order, detail, 1–2 items) | 20% |
| `UPDATE_HOT` (1–2 hot orders) | 20% |
| `CHILD_ONLY` (add or update line items, or detail only) | 15% |
| `ROLLBACK` (any of the above, then rollback) | 10% |

- The first six transactions of each session cycle through every kind once, so every kind appears in every session.
- With 20 sessions this is 800 transactions, about 500 orders and about 50 updates per hot order. Enough to exercise grouped deliveries and contention in about a minute of source time.

**Performance** (flavors F1–F7 as in the spec):

| | Sustained phase | Bulk phase |
|---|---|---|
| Mix | F1 20%, F2 35%, F3 25%, F4 5%, F5 2%, F7 13% (about 6.35 events/tx) | 100 F6 transactions (10,000 orders, 50,000 events) |
| Rate | at 100,000 events/min, about 262 tx/s across 16 sessions | unpaced |
| Duration / sessions | 300 s | 10 sessions |

- In the sustained phase each session paces itself with a fixed-rate scheduler: `tx/s / sessions`, with the next slot computed from the start time, so slow commits don't compound into drift.
- F7 updates orders created earlier in the same run.
- About 79,000 transactions and about 500,000 events in 5 minutes.
- All values come from `e2e.conf`.

**Latency:** one session, 200 F2 transactions, one every `e2e.latency.interval` (default 500 ms), with no other workload. It starts only after the pre-flight canary shows the pipeline is idle and caught up.

### D9. Metrics and reports

For every ground-truth `(orderId, csn)`:

| Latency | Formula |
|---|---|
| `processedLatencyMs` | `dwhProcessedTs − commit_ts_utc` |
| `streamVisibleLatencyMs` | `orderStreamCommitTs − commit_ts_utc` |
| `currentVisibleLatencyMs` | `ordersCurrentCommitTs − commit_ts_utc` (latency run and correctness suite only) |

Percentiles use the nearest-rank method over all orders.

Throughput comes from the ground truth: committed events and transactions per second over the load window. The catch-up time is the last row's visible time minus the end of the load.

Each run writes to `target/e2e-reports/<suite>-<runId>/`:
- `summary.md`: configuration, seed, sessions, verdict, failed checks with up to 50 example transactions each, and the percentile tables
- `transactions.csv`
- `latency.csv` (one row per order: orderId, xid, csn, commitTs, clientAckTs, dwhProcessedTs, streamVisibleTs, currentVisibleTs, and the three latencies)
- `failures.csv`

Optional thresholds `e2e.performance.max-p95-processed-latency` and `e2e.performance.max-catch-up` fail the run only when set.

### D10. Pre-flight

The pre-flight check runs these steps:

1. Open a JDBC connection.
2. Call UC `GET /api/2.1/unity-catalog/tables/mymanaged.argus1.order_stream`.
3. Check that `e2e_tx_log` exists. If it doesn't, the message says to run `db/e2e/e2e-setup.sql`.
4. Commit a one-order `CANARY` transaction and wait for it in `order_stream`, and for the correctness and latency suites also in `orders_current`.

Each failure names the component: Oracle, UC, setup script, `TransactionalCdcProcessingApp`/GoldenGate, or `ScdType1MergeApp`.

## Risks / Trade-offs

- **[Row tracking turned off on a table, or the table recreated without it]** → The pre-flight checks `delta.enableRowTracking` on all three tables and fails, naming the table, if it is off. All tables created by `SeedJob` through UC have it, so no polling fallback is built. Add one only if a table without row tracking has to be supported.
- **[A later MERGE rewrites an `orders_current` row without changing its `csn`]** → `ScdType1Merger` updates only on a higher CSN, so this shouldn't happen. If it did, the visible time would be too late, never too early. The latency run touches each order once, so it isn't affected.
- **[Clock skew between Oracle `SYSTIMESTAMP` and the Spark JVM]** → Both run on this host, so they share a clock. The report records `clientAckTs − commit_ts_utc` as a sanity check: it should be small and positive. If Oracle moves to another host, the doc requires NTP and treats differences under about 50 ms as noise.
- **[`dwhProcessedTs` is taken before the Delta write]** → That is why the commit-to-visible latency from Delta history is reported too. The two together show sequencing delay separately from write delay.
- **[`ORA_ROWSCN` gives the block SCN without `ROWDEPENDENCIES`]** → The setup script creates the log tables with `ROWDEPENDENCIES`. The pre-flight check confirms that the canary's `ORA_ROWSCN` equals the canary's `csn` in `order_stream`.
- **[The sustained load may not reach 262 tx/s on one machine]** → The report states the committed rate next to the target (spec). The rate and session count are configurable.
- **[Test data grows the UC tables and slows the `orders_current` MERGE over many runs]** → It is documented, and `scripts/reset-pipeline.sh` remains the way to start clean. The Oracle cleanup script removes a run's rows by tag.
- **[A pipeline blocked on someone else's stuck transaction makes the suite time out]** → The timeout message suggests checking the blocked-head alert, and `docs/STATE-STORE-DEBUG.md`.
- **[Lock contention on hot orders slows the correctness run]** → Only 5 hot orders, locked in a fixed order, and with short transactions.

## Migration Plan

Nothing to migrate: the change only adds tests, scripts and docs. To start using the suites:

1. Run `db/e2e/e2e-setup.sql` once as `APPUSER`.
2. With the stack running, run `./mvnw -Pe2e verify`. It connects as `appuser`/`appuser` unless `E2E_ORACLE_USER` and `E2E_ORACLE_PASSWORD` are set.

To roll back, remove the profile and the test sources, and drop the two log tables with `db/e2e/e2e-cleanup.sql --drop`.
