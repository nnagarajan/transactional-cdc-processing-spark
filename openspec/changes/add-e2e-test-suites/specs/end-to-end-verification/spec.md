# Spec Delta

## Purpose

End-to-end suites that commit known work to the Oracle source through concurrent sessions and check the live pipeline's Unity Catalog tables against it. They verify that output is correct and in commit order, and measure throughput and commit-to-output latency.

## ADDED Requirements

### Requirement: On-demand execution against the running stack
The suites SHALL run only when explicitly requested, never as part of the normal build or unit test run. They SHALL run against an already-running stack and SHALL NOT start, stop or reconfigure the pipeline apps, GoldenGate, Kafka or Unity Catalog.

#### Scenario: Normal build
- **WHEN** a developer runs the normal package or unit test build
- **THEN** no end-to-end suite runs and no connection to Oracle is attempted

#### Scenario: Explicit run
- **WHEN** a developer requests the end-to-end suites
- **THEN** the selected suites run against the configured Oracle database and Unity Catalog server

### Requirement: Pre-flight check
Before any workload starts, each suite SHALL commit one canary transaction and wait for it to appear in `order_stream` within a configurable time. If Oracle or Unity Catalog can't be reached, or the canary doesn't appear in time, the suite SHALL fail with a message naming the component that failed, and no workload SHALL start.

#### Scenario: Pipeline app not running
- **WHEN** Oracle and Unity Catalog are reachable but the canary transaction does not appear in `order_stream` within the pre-flight timeout
- **THEN** the suite fails, reporting that the pipeline did not process the canary, and commits no workload transactions

#### Scenario: Oracle unreachable
- **WHEN** the suite can't open a connection to Oracle
- **THEN** the suite fails before committing anything and reports the connection error

### Requirement: Run isolation
Each suite run SHALL generate a unique run id and tag every row it creates with it. Every comparison SHALL consider only the orders the run created or changed, so data from other runs or other sources doesn't affect the result.

#### Scenario: Existing data in the tables
- **WHEN** `order_stream` and `orders_current` already hold rows from earlier runs and seed data
- **THEN** the comparison counts only rows for orders this run created or changed, at CSNs this run committed

### Requirement: Commit ground truth
For every committed workload transaction, a suite SHALL record in Oracle, within the same transaction:
- the run id and session number
- the transaction kind
- the orders it touched
- its per-table event counts
- the commit timestamp in UTC

The record's commit SCN SHALL serve as the transaction's expected CSN. A rolled-back transaction leaves no record. The client SHALL separately track rolled-back transactions and the values they wrote.

#### Scenario: Committed transaction
- **WHEN** a session commits a transaction that inserts one order, one detail and three line items
- **THEN** the ground truth holds one record for it, with counts 1, 1 and 3 and a CSN equal to the transaction's commit SCN

#### Scenario: Rolled-back transaction
- **WHEN** a session rolls back a transaction
- **THEN** no ground-truth record exists for it, and the client keeps the order ids and values it wrote, so the suite can check that they never appear

### Requirement: Concurrent sessions
The correctness suite SHALL run its workload on several concurrent Oracle sessions. By default it chooses between 10 and 20 sessions at random from a seed, and both the session count and the seed SHALL be configurable. The report SHALL state the session count and seed, so a run can be repeated with the same workload.

#### Scenario: Default session count
- **WHEN** the correctness suite runs without a configured session count
- **THEN** it uses between 10 and 20 concurrent sessions and reports the number and the seed

#### Scenario: Repeated run
- **WHEN** the suite is run again with the same seed and session count
- **THEN** each session issues the same sequence of transaction kinds and sizes

### Requirement: Correctness workload mix
The correctness workload SHALL include each of these transaction kinds at least once per session:
- an insert of one order with its detail and line items
- an insert of several orders in one transaction
- an update of an order the session created earlier
- an update of an order in a small set of orders shared by all sessions
- a transaction that changes only line items or only the detail of an existing order
- a transaction that is rolled back

#### Scenario: Contended orders
- **WHEN** several sessions update the same shared order concurrently
- **THEN** Oracle serializes the updates, each committed update is a separate ground-truth record with its own CSN, and the order's version rises by one per update

### Requirement: Completeness and content check
For every committed transaction in the ground truth, `order_stream` SHALL contain exactly one row per touched order at that transaction's CSN. Each row's order, detail and line-item arrays SHALL hold exactly the entities the transaction changed. Their versions and values SHALL equal what was committed, including the before images of updates.

#### Scenario: Multi-order transaction
- **WHEN** one committed transaction inserted three orders
- **THEN** `order_stream` has exactly three rows at that CSN, one per order, each with its own detail and line items

#### Scenario: Missing transaction
- **WHEN** a committed transaction has no row in `order_stream` after the pipeline has caught up
- **THEN** the suite fails and reports the transaction's session, kind, xid and CSN

#### Scenario: Duplicate row
- **WHEN** `order_stream` holds two rows for the same order and CSN
- **THEN** the suite fails and reports the duplicate

### Requirement: No phantom output
`order_stream` SHALL contain no row for a run order at a CSN that isn't in the ground truth. No value written only by a rolled-back transaction SHALL appear in `order_stream` or `orders_current`.

#### Scenario: Rolled-back insert
- **WHEN** a session inserted an order and rolled back
- **THEN** neither `order_stream` nor `orders_current` contains that order

### Requirement: Commit order check
Ordering SHALL be checked in two ways:
- Across the successive Delta commits of `order_stream` made during the run, every run transaction in a commit SHALL have a CSN no lower than every run transaction in earlier commits.
- For each order, the version in `order_stream` SHALL rise strictly with CSN.

#### Scenario: Out-of-order commit
- **WHEN** a Delta commit holds a run transaction with a lower CSN than a run transaction in an earlier commit
- **THEN** the suite fails and reports both transactions and the two commit versions

#### Scenario: Per-order history
- **WHEN** a shared order was updated by five transactions from different sessions
- **THEN** its five `order_stream` rows, sorted by CSN, have strictly increasing versions

### Requirement: Current state check
After the pipeline has caught up, every run order in `orders_current` SHALL match that order's current rows in Oracle:
- the order attributes and version
- the detail
- the set of line items, with their versions and values

The `csn` column SHALL equal the highest ground-truth CSN that touched the order.

#### Scenario: Last writer wins
- **WHEN** two sessions updated the same shared order, and the second commit has the higher CSN
- **THEN** `orders_current` holds the second session's values and its CSN

### Requirement: No dropped run data
`dropped_events` SHALL contain no row for a CSN in the run's ground truth.

#### Scenario: Clean run
- **WHEN** the correctness suite finishes
- **THEN** it reports zero `dropped_events` rows for the run's CSNs, or fails and lists the rows and their reasons

### Requirement: Catch-up wait
A suite SHALL poll the Unity Catalog tables until every ground-truth transaction is reflected in `order_stream`, and for the correctness suite also in `orders_current`, or until a configurable timeout expires. On timeout the suite SHALL fail and list the transactions not yet reflected.

#### Scenario: Pipeline still processing
- **WHEN** the workload has finished but some transactions are not yet in `order_stream`
- **THEN** the suite keeps polling and does not run the comparisons until all are present or the timeout expires

### Requirement: Performance transaction flavors
The performance suite SHALL generate transactions of these flavors:

| Flavor | Orders | Details | Line items per order |
|---|---|---|---|
| F1 | 1 | 1 | 1 |
| F2 | 1 | 1 | 3 |
| F3 | 1 | 1 | 5 |
| F4 | 1 | 1 | 10 |
| F5 | 10 | 10 | 3 |
| F6 | 100 | 100 | 3 |
| F7 | update of an existing order, its detail and 3 of its line items | | |

The mix, the counts and the line-item sizes SHALL be configurable.

#### Scenario: Flavor counts
- **WHEN** the performance suite commits an F3 transaction
- **THEN** its ground-truth record shows 1 order, 1 detail and 5 line items, and `order_stream` has one row for it with 5 line items

### Requirement: Sustained load measurement
The performance suite SHALL run a sustained mixed load of flavors F1–F5 and F7, spread across concurrent sessions and paced to a configurable target rate. The defaults are 100,000 events per minute, 16 sessions and 5 minutes. It SHALL report:
- the event and transaction rates actually committed
- the time from the end of the load until the pipeline caught up
- commit-to-`dwhProcessedTs` latency percentiles (p50, p95, p99, max) for `order_stream`

It SHALL check that every committed transaction arrived.

#### Scenario: Pipeline keeps up
- **WHEN** the sustained load finishes
- **THEN** the report shows the committed rate, the catch-up time and the latency percentiles, and the suite fails only if a committed transaction is missing or a configured threshold is exceeded

#### Scenario: Target rate not reached
- **WHEN** the sessions can't commit at the target rate
- **THEN** the report states the rate actually reached next to the target

### Requirement: Bulk load measurement
The performance suite SHALL run a bulk phase that commits a configurable number of orders, 10,000 by default, as F6 transactions of 100 orders each, spread across sessions. It SHALL report the same metrics as the sustained load.

#### Scenario: Bulk chunks
- **WHEN** the bulk phase commits 10,000 orders
- **THEN** the ground truth holds 100 F6 transactions, and `order_stream` holds 10,000 rows for them, each at its transaction's CSN

### Requirement: Per-order commit latency report
The latency run SHALL commit 200 orders from a single session, one order per transaction, with no other workload running. For each order it SHALL report:
- the order id, xid and CSN
- the Oracle commit timestamp
- the `dwhProcessedTs`
- the time the row was first seen in `order_stream` and in `orders_current`
- the derived latencies in milliseconds

It SHALL also report the p50, p95, p99 and max of each latency.

#### Scenario: Latency report
- **WHEN** the latency run finishes and all 200 orders are in `order_stream` and `orders_current`
- **THEN** the report has 200 rows, each with its commit-to-processed, commit-to-`order_stream`-visible and commit-to-`orders_current`-visible latencies, plus a summary of their percentiles

#### Scenario: Visibility resolution
- **WHEN** visibility times are measured by polling
- **THEN** the report states the polling interval as the resolution of the visibility latencies

### Requirement: Reports
Each suite run SHALL write its results under a per-run directory, as machine-readable CSV and a human-readable summary. The summary SHALL include:
- the run id, configuration and seed
- the pass or fail verdict, with the reason for each failure

Latency and throughput thresholds SHALL be optional. When none are configured, the performance results SHALL be reported without failing the run.

#### Scenario: Failed check
- **WHEN** any correctness check fails
- **THEN** the summary lists each failed check with the affected transactions, and the suite exits with a failure
