# Tasks

## 1. Setup and stack facts

- [x] 1.1 Add the test-scoped `com.oracle.database.jdbc:ojdbc11` dependency and an `e2e` profile that binds `maven-failsafe-plugin` (`integration-test`, `verify`) to `**/e2e/*IT`. Verify that `./mvnw clean package` runs the same 101 tests as before and no `*IT`, and that `./mvnw -Pe2e verify -Dit.test=NoSuchIT -Dfailsafe.failIfNoSpecifiedTests=false` starts Failsafe
- [x] 1.2 Check on the live stack that `DESCRIBE HISTORY` and `VERSION AS OF` work on `mymanaged.argus1.order_stream` and `orders_current` through `UCSingleCatalog` 0.5.0 (D6). Record the result in design.md. If they don't work, switch D6 to the polling fallback before group 3
- [x] 1.3 Read one real `order_stream` row for an insert and one for an update, and pin the string formats of `orderDate`, `orderTs`, `createdTs`, `shippedTs` and `estimatedDeliveryDate`. Record them in design.md (closing the open question) and in a comment on the normalizer from task 2.5
- [x] 1.4 Add `db/e2e/e2e-setup.sql`, which creates `e2e_tx_log` and `e2e_tx_orders` with `ROWDEPENDENCIES`, and `db/e2e/e2e-cleanup.sql`, which deletes one run's source rows by tag, or drops the log tables with `--drop`. Verify that both run cleanly in `sqlplus` as `APPUSER`, and that `ext04` doesn't pick up the new tables: after a test insert into `e2e_tx_log`, no message for them appears on the data topics

## 2. Harness core

- [x] 2.1 Add `src/test/resources/e2e.conf` (includes `reference`; JDBC URL, user `appuser` and password `appuser` hardcoded as defaults, overridable by `E2E_ORACLE_USER`/`E2E_ORACLE_PASSWORD`; seed, session counts, workload weights, rates, timeouts, poll interval, optional thresholds, report directory) and an `E2eConfig` loader. Verify with a unit test (`E2eConfigTest`) that the defaults load, including `appuser`/`appuser`, and that the environment variables and `-D` overrides take precedence
- [x] 2.2 Implement `RunContext`: run id, seed, session count defaulting to 10–20 from the seed, and per-session `Random`. Verify with a unit test that the same seed gives the same session count and the same per-session kind sequence, and that the default count always lies in [10, 20]
- [x] 2.3 Implement `OracleSession`: one JDBC connection, the transaction builders for every kind in D8 (insert one or many, update own or hot with `FOR UPDATE` in `order_id` order, child-only, F1–F7, rollback), capture of before and after images (D4), and the in-transaction log insert (D3). Verify with `OracleSessionIT` against Oracle only: each builder commits, its log rows exist with the expected counts, `ORA_ROWSCN` is set, and a rollback leaves no log row
- [x] 2.4 Implement `GroundTruth`: loads a run's log and order rows with `ORA_ROWSCN` as the CSN, joins them with the client-side images and rollback list, and builds the expected `(orderId, csn)` rows. Verify with a unit test on hand-built inputs, covering a multi-order transaction, a child-only transaction and a rollback
- [x] 2.5 Implement `UcReader` on `SparkSessionCreator` (UI disabled). It reads `order_stream`, `orders_current` and `dropped_events` filtered to run order ids or CSNs, and reads each run row's `_metadata.row_commit_version` joined with the `DESCRIBE HISTORY` commit timestamps (D6). It also implements the type normalizer from D4, using the formats from 1.3. Verify with a unit test on a local Delta table with row tracking (no UC): each row of two appends and a MERGE reports the version and commit timestamp that wrote it, and the normalizer treats GoldenGate and Oracle values as equal
- [x] 2.6 Implement `CatchUp`, the polling wait with progress logs and the missing-transaction listing on timeout (D7), and `Preflight`, which checks Oracle, UC, row tracking on the three tables, the log tables and the canary, plus the canary `ORA_ROWSCN` = `csn` check (D10). Verify with unit tests using a fake reader, covering found, timeout and each failure message
- [x] 2.7 Implement `Reports`: the per-run directory with `summary.md`, `transactions.csv`, `latency.csv` and `failures.csv`; nearest-rank percentiles; and the verdict (D9). Verify with a unit test of the percentiles on known inputs (p50, p95, p99, max) and of the summary listing each failed check

## 3. Suite A: correctness and ordering

- [x] 3.1 Implement the checks as pure functions over the ground truth and the UC rows, one per spec requirement: completeness and content, no phantom output, commit order across versions, per-order versions, current state against Oracle (read through `OracleSession`), no dropped run data, plus the extra check that `dwhProcessedTs` is non-decreasing. Verify with `CorrectnessChecksTest`, which, for each check, gives a passing input and a crafted failing input (missing, duplicate, out-of-order commit, version regression, stale `orders_current`, phantom rollback, dropped row), and asserts the reported transactions
- [ ] 3.2 Implement `CorrectnessOrderingIT`, which runs pre-flight, the setup of the hot orders, S concurrent sessions with the D8 mix (the first six transactions cycle through every kind), the catch-up on both tables, then all checks from 3.1 and the report. Verify with a run on the live stack: the run passes, and the summary shows the session count in [10, 20], the seed, all kinds per session and zero dropped rows
- [ ] 3.3 Verify that the suite catches real faults:
  - Stop `ScdType1MergeApp` before a run, and check that the pre-flight fails and names it.
  - Run with `e2e.catch-up-timeout = 1 s`, and check that the timeout lists the missing transactions.
  - Re-run with the same seed, and check that the kind sequences in `transactions.csv` are the same.

## 4. Suite B: performance and latency

- [x] 4.1 Implement the F1–F7 workload driver with weighted flavors, the fixed-rate pacer and the derivation of the transaction rate from the target events per minute and the mix's average events per transaction (D8). Verify with a unit test: the default mix averages about 6.35 events/tx, 100,000 events/min gives about 262 tx/s, and the pacer's schedule doesn't drift when one commit is slow
- [ ] 4.2 Implement `PerformanceIT`, which runs pre-flight, the sustained phase (default 16 sessions, 300 s), the catch-up and completeness check, the bulk phase (100 × F6 across 10 sessions), the catch-up and completeness check, and then the throughput, catch-up and latency percentile report per phase, plus the optional thresholds. Verify with a run on the live stack: both phases are complete, and the summary shows target versus achieved rate, catch-up time and p50/p95/p99/max for each phase
- [ ] 4.3 Implement `CommitLatencyIT`, which runs pre-flight (pipeline idle), one session with 200 F2 transactions at `e2e.latency.interval`, the catch-up on `order_stream` and `orders_current`, and `latency.csv` with one row per order (commit, client ack, `dwhProcessedTs`, the two visible timestamps, the three latencies), plus a percentile summary that states the visibility resolution. Verify with a run on the live stack: `latency.csv` has 200 rows, every latency is positive, and `clientAckTs − commitTs` is under 1 s for every row

## 5. Documentation

- [ ] 5.1 Write `docs/E2E-TESTING.md`. Cover:
  - prerequisites (the running stack, `db/e2e/e2e-setup.sql`, the default `appuser`/`appuser` login and how to override it)
  - how to run each suite, and every `e2e.conf` setting
  - what each check proves
  - how to read `summary.md` and `latency.csv`
  - cleanup, and the known limits (no deletes, no failure injection, data left in UC)

  Verify that every command in it runs as written on the local stack
- [x] 5.2 Link the new doc from `docs/PERFORMANCE-ESTIMATES.md` ("How to measure") and `README.md`, and list `db/e2e/` in `db/README.md`. Verify that the links resolve, for example with `grep -o '](\S*E2E-TESTING.md)'` and a check that each target path exists

## 6. Integration check

- [ ] 6.1 On a clean pipeline (`scripts/reset-pipeline.sh`), run all three suites in sequence: `./mvnw -Pe2e verify`. Verify that they pass, and attach the latency and throughput figures from the summaries to the PR description, compared with the estimates in `docs/PERFORMANCE-ESTIMATES.md`
