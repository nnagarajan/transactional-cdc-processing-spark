# End-to-end testing

The end-to-end suites commit known work to Oracle and check what the running pipeline writes to Unity Catalog. The full chain is Oracle → GoldenGate → Kafka → `TransactionalCdcProcessingApp` → `order_stream` → `ScdType1MergeApp` → `orders_current`.

| Suite | Class | What it shows |
|---|---|---|
| Correctness and ordering | `CorrectnessOrderingIT` | 10–20 concurrent sessions; every committed transaction arrives once, complete, in commit order, and `orders_current` ends equal to Oracle |
| Performance | `PerformanceIT` | Committed rate, catch-up time and latency percentiles under a sustained mixed load (100,000 events/min) and a bulk load (10,000 orders in 100-order transactions) |
| Commit latency | `CommitLatencyIT` | Latency of each of 200 orders from the Oracle commit to `dwhProcessedTs`, and to the Delta commits that made it visible in `order_stream` and `orders_current` |
| Oracle session | `OracleSessionIT` | The workload's transaction builders against Oracle alone (no pipeline needed) |

The suites run only on request. `./mvnw package` and `./mvnw test` never connect to Oracle.

## Prerequisites

1. **Everything on one machine, running:**
   - Oracle `ORCLPDB`
   - GoldenGate `ext04`, `kc` and `rep3`
   - Kafka on `localhost:9092`
   - the Unity Catalog server on `localhost:7071` (`bin/start-uc-server -p 7071`)
   - `TransactionalCdcProcessingApp` and `ScdType1MergeApp` (see [IMPLEMENTATION.md](IMPLEMENTATION.md) and [SCD_TYPE1_README.md](SCD_TYPE1_README.md))

   The suites don't start or stop any of these. Oracle and Spark must share one clock, which they do on one host, because latencies compare Oracle's `SYSTIMESTAMP` with Spark's `Instant.now()`.

2. **The ground-truth log tables**, created once as `APPUSER`:

   ```bash
   sqlplus appuser/appuser@//localhost:1521/ORCLPDB @db/e2e/e2e-setup.sql
   ```

   `e2e_tx_log` and `e2e_tx_orders` are not captured by `ext04`, which lists only the three order tables, so they never reach Kafka. Every test transaction writes its row there in the same transaction. Their `ORA_ROWSCN` (`ROWDEPENDENCIES`) is the transaction's commit SCN, which is the CSN GoldenGate writes.

3. **The Oracle login.** `src/test/resources/e2e.conf` uses `appuser`/`appuser`, the local development login. For another database, set `E2E_ORACLE_USER` and `E2E_ORACLE_PASSWORD`, or pass `-De2e.oracle.user=... -De2e.oracle.password=...`, which wins over both. Never point the suites at a shared database with the defaults.

## Running

```bash
./mvnw -Pe2e verify -Dit.test=CorrectnessOrderingIT   # about 2–5 minutes
./mvnw -Pe2e verify -Dit.test=CommitLatencyIT         # about 2–3 minutes
./mvnw -Pe2e verify -Dit.test=PerformanceIT           # about 10–15 minutes
./mvnw -Pe2e verify                                   # all of them, OracleSessionIT included
```

Add settings with `-D`, for example a fixed seed and session count:

```bash
./mvnw -Pe2e verify -Dit.test=CorrectnessOrderingIT -De2e.seed=42 -De2e.correctness.sessions=12
```

Each run starts with a **pre-flight**. It checks:

- the Oracle connection
- the three UC tables, and that they have row tracking
- the log tables

Then it commits a one-order canary transaction and waits for it in `order_stream`. The correctness and latency suites also wait for it in `orders_current`. A failure names the component, for example `Pre-flight failed [ScdType1MergeApp]: canary order 1234 (CSN 345463901) did not appear in orders_current within PT3M; is it running?`. No workload starts until the pre-flight passes.

## Settings (`e2e.conf`)

| Key | Default | Meaning |
|---|---|---|
| `e2e.oracle.url` | `jdbc:oracle:thin:@//localhost:1521/ORCLPDB` | Oracle JDBC URL |
| `e2e.oracle.user` / `password` | `appuser` / `appuser` | Login; `E2E_ORACLE_USER` / `E2E_ORACLE_PASSWORD` override it |
| `e2e.tables.*` | `argus1`, `order_stream`, `orders_current`, `dropped_events` | Schema and tables, in the catalog `spark.unity-catalog.catalog-name` (`mymanaged`) |
| `e2e.seed` | `0` | Seed for session counts and workloads; `0` picks one from the clock. The report prints it |
| `e2e.poll-interval` | `2 seconds` | How often the catch-up wait reads the UC tables |
| `e2e.preflight-timeout` | `3 minutes` | How long the canary may take |
| `e2e.catch-up-timeout` | `10 minutes` | How long the pipeline may take to reflect the workload |
| `e2e.report-dir` | `target/e2e-reports` | Where reports go |
| `e2e.correctness.sessions` | `0` | Concurrent sessions; `0` picks 10–20 from the seed |
| `e2e.correctness.tx-per-session` | `40` | Transactions per session |
| `e2e.correctness.hot-orders` | `5` | Orders shared by all sessions |
| `e2e.correctness.weights.*` | 25 / 10 / 20 / 20 / 15 / 10 | Relative weights of `INSERT_ONE`, `INSERT_MULTI`, `UPDATE_OWN`, `UPDATE_HOT`, `CHILD_ONLY`, `ROLLBACK` |
| `e2e.performance.sessions` | `16` | Sessions of the sustained phase |
| `e2e.performance.events-per-minute` | `100000` | Target rate; the transaction rate follows from the mix |
| `e2e.performance.duration` | `300 seconds` | Length of the sustained phase |
| `e2e.performance.mix.*` | F1 20, F2 35, F3 25, F4 5, F5 2, F7 13 | Flavor weights of the sustained phase (about 6.35 events per transaction) |
| `e2e.performance.bulk.*` | `10000` orders, `100` per transaction, `10` sessions | Bulk phase |
| `e2e.performance.max-p95-processed-latency` | not set | When set, a higher p95 fails the run |
| `e2e.performance.max-catch-up` | not set | When set, a longer catch-up fails the run |
| `e2e.latency.orders` / `interval` | `200` / `500 milliseconds` | Latency run size and pacing |

### Workloads

**Correctness.** A setup transaction creates the hot orders. Then every session runs its transactions unpaced. The first six cycle through the kinds, so each kind appears in each session. The rest are drawn by weight.

| Kind | What it does |
|---|---|
| `INSERT_ONE` | One order, its detail and 2–5 line items |
| `INSERT_MULTI` | 2–5 such orders in one transaction |
| `UPDATE_OWN` | Updates an order the session created, its detail and 1–2 line items |
| `UPDATE_HOT` | Locks 1–2 shared orders (in id order, so no deadlocks) and updates them, sometimes their detail and a line item |
| `CHILD_ONLY` | Only adds line items, or only updates a line item or the detail, of an own order |
| `ROLLBACK` | One of the above, then rolled back |

**Performance flavors.**

| Flavor | Orders | Details | Line items per order | Events |
|---|---|---|---|---|
| F1 | 1 | 1 | 1 | 3 |
| F2 | 1 | 1 | 3 | 5 |
| F3 | 1 | 1 | 5 | 7 |
| F4 | 1 | 1 | 10 | 12 |
| F5 | 10 | 10 | 3 | 50 |
| F6 (bulk phase) | 100 | 100 | 3 | 500 |
| F7 | update of an existing order, its detail and 3 line items | | | 5 |

Each sustained-phase session commits on a fixed-rate schedule computed from the start time, so a slow commit makes it late once but doesn't shift later slots.

## What each check proves

| Check | Proves |
|---|---|
| ground truth consistent with the client | The harness itself: every transaction the client committed has its log row, with the same orders and counts |
| every kind in every session | The workload really covered every kind |
| completeness and content | Each committed transaction has exactly one `order_stream` row per touched order at its CSN. Each row holds exactly the entities the transaction changed, with the committed values and before images. Every row's `xid` ends in the transaction's Oracle `LOCAL_TRANSACTION_ID` |
| no phantom output | No run row at a CSN that wasn't committed, and no order that only rolled-back transactions created, in either table |
| commit order across Delta commits | Within the run, every `order_stream` commit holds only CSNs at or above all CSNs of earlier commits (requirement "Commit-level ordering of output") |
| per-order versions rise with CSN | Each order, detail and line item's history in `order_stream` has strictly increasing versions |
| orders_current equals Oracle | After catch-up, `orders_current` holds Oracle's current values, the current set of line items and the highest committed CSN for every run order |
| no dropped run data | `dropped_events` has no row for any of the run's CSNs |
| dwhProcessedTs non-decreasing in CSN order | Emission time follows commit order (an extra check that depends on the clock) |
| every transaction arrived with its counts (performance) | Per phase, each transaction's rows and per-table entity counts match what was committed |
| latency checks (latency run) | All 200 orders were reported, visible in both tables, with positive latencies and a client ack within 1 s of the Oracle timestamp |

Commit versions come from row tracking. Every row exposes `_metadata.row_commit_version`, and `DESCRIBE HISTORY` gives that version's in-commit timestamp. A visible time is therefore the exact time of the Delta commit that wrote the row. It doesn't depend on polling, and it isn't limited by time-travel retention.

## Reading the reports

Each run writes `target/e2e-reports/<suite>-<runId>/`:

| File | Content |
|---|---|
| `summary.md` | Verdict, configuration (run id, seed, sessions), every check with pass or fail, up to 50 examples per failed check, and the workload, rate and latency tables |
| `transactions.csv` | Every transaction the client ran: session, sequence, kind, committed or rolled back, counts, order ids, CSN, `LOCAL_TRANSACTION_ID`, Oracle commit time, client ack time |
| `latency.csv` (`latency-sustained.csv`, `latency-bulk.csv` for performance) | One row per order and CSN: commit, client ack, `dwhProcessedTs`, the two visible times, and the latencies in ms |
| `failures.csv` | Every failure of every failed check |

The latency columns:

| Column | Measures |
|---|---|
| `processed_latency_ms` | Oracle commit → `dwhProcessedTs`. The pipeline assembled and emitted the transaction; this is taken before the Delta write |
| `order_stream_visible_latency_ms` | Oracle commit → the Delta commit that made the row readable in `order_stream` |
| `orders_current_visible_latency_ms` | Oracle commit → the Delta commit of `orders_current` that holds this CSN (latency run only; the correctness and performance runs leave it empty) |
| `client_ack_ms` | Oracle timestamp → `commit()` returning in the client. It should be a few ms; large values mean a clock or connection problem |

The difference between `processed` and `order_stream_visible` is the Delta write and UC commit. The difference between `order_stream_visible` and `orders_current_visible` is `ScdType1MergeApp`. Compare the percentiles with [PERFORMANCE-ESTIMATES.md](PERFORMANCE-ESTIMATES.md), where the estimates are 10–20 s to `order_stream` and 15–40 s to `orders_current`.

## Cleanup

Test orders are tagged: `customer_id = 'E2E-<runId>'` and `order_ref = 'E2E-<runId>-S<session>-<seq>-<n>'`. Remove them from Oracle with:

```bash
sqlplus appuser/appuser@//localhost:1521/ORCLPDB @db/e2e/e2e-cleanup.sql <runId>   # one run
sqlplus appuser/appuser@//localhost:1521/ORCLPDB @db/e2e/e2e-cleanup.sql ALL       # every run
sqlplus appuser/appuser@//localhost:1521/ORCLPDB @db/e2e/e2e-cleanup.sql --drop    # every run, and drop the log tables
```

After the deletes, the script shrinks `orders`, `order_details`, `order_line_items` and the two log tables with their indexes (`ENABLE ROW MOVEMENT`, then `SHRINK SPACE CASCADE`), and prints each table's size before and after. Tables outside an ASSM tablespace are skipped. Shrinking moves rows, so the `ORA_ROWSCN` of the rows that remain changes: a kept run's report can't be rebuilt from `e2e_tx_log` afterwards. Run the cleanup while no suite is running.

`ext04` ignores deletes, so cleanup doesn't touch the pipeline. The rows in `order_stream`, `orders_current` and `dropped_events` stay. Use [`scripts/reset-pipeline.sh`](../scripts/reset-pipeline.sh) to start the pipeline clean. Many runs make `orders_current` larger, and its MERGE slower.

## Known limits

- **No deletes.** `ext04` sets `IGNOREDELETES`, so deletes never reach the pipeline.
- **No failure injection.** No restart of an app or Replicat mid-run.
- **A blocked pipeline times out the suite.** A transaction from outside the run that never completes blocks all output, and the suite then times out. The timeout message points to the blocked-head alert and [STATE-STORE-DEBUG.md](STATE-STORE-DEBUG.md).
- **The sustained phase may fall short of its target on one machine.** The report shows the achieved rate next to the target, and the count of pacer slots missed by more than one interval.
- **Row tracking is required.** All tables created through Unity Catalog have it. The pre-flight refuses a table without it.
