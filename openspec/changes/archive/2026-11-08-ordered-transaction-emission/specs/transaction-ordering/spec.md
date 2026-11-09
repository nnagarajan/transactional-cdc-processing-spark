# Spec Delta

## Purpose

Guarantees that complete transactions are written to `order_stream` in source commit order, defines the source delivery contract this depends on, and makes head-of-line blocking visible to operators.

## ADDED Requirements

### Requirement: Emission in marker order
The system SHALL emit transactions to `order_stream` in the order their markers appear on the transaction metadata topic. A transaction SHALL NOT be emitted while any transaction whose marker appears earlier is still unemitted.

#### Scenario: Later transaction completes first
- **WHEN** the marker for T1 precedes the marker for T2, and T2's data events all arrive before T1's
- **THEN** T2 is not emitted until T1 is complete, and T1's records are committed to `order_stream` no later than T2's

#### Scenario: Several transactions complete in the same batch
- **WHEN** T1, T2 and T3 (in marker order) all become complete in the same micro-batch
- **THEN** all three are emitted in that batch

#### Scenario: Data arrives before its marker
- **WHEN** data events for T2 arrive before T2's marker, and T1 (marker earlier) is complete
- **THEN** T1 is emitted, and T2 remains buffered until its marker arrives and it is complete

### Requirement: Commit-level ordering of output
Every Delta commit to `order_stream` SHALL contain only transactions whose markers appear after the markers of all transactions in earlier commits.

#### Scenario: Consecutive batches
- **WHEN** batch N commits transactions T1..T3 and batch N+1 commits T4
- **THEN** T4's marker appears after the markers of T1, T2 and T3 on the metadata topic

### Requirement: Never skip an incomplete transaction
The system SHALL NOT emit or discard an incomplete transaction in order to unblock later ones. Output SHALL resume only when the head-of-line transaction becomes complete.

#### Scenario: Head transaction missing an event indefinitely
- **WHEN** the earliest unemitted transaction never receives one of its declared events
- **THEN** no later transaction is emitted, and all later transactions remain buffered

### Requirement: Head-of-line blocking alert
When no transaction has been emitted for longer than a configured threshold while buffered transactions exist, the system SHALL log an error identifying the blocking transaction's `xid:csn`, its per-table received/expected counts, and the number of transactions queued behind it. The threshold SHALL default to 10 minutes. The check SHALL run even when no new input arrives.

#### Scenario: Blocked beyond threshold with no new input
- **WHEN** the head transaction has been incomplete for 11 minutes with the default threshold, and no new events have arrived in that time
- **THEN** an error is logged naming the head transaction, its progress, and the queue depth

#### Scenario: Blocking clears
- **WHEN** a blocked head transaction becomes complete and is emitted
- **THEN** the blocked-duration measurement resets

### Requirement: Single-partition metadata topic
The system SHALL fail the streaming query with an explanatory error if it reads a marker from any partition of the transaction metadata topic other than partition 0.

#### Scenario: Metadata topic has more than one partition
- **WHEN** a marker is read from partition 1 of `dev.transaction_metadata_json`
- **THEN** the query stops with an error stating that the metadata topic must have exactly one partition

### Requirement: Ordered and lossless marker delivery
The GoldenGate replicat that publishes markers SHALL be configured to publish synchronously and with idempotent, fully acknowledged writes. It SHALL NOT filter delete operations itself: deletes are filtered only at the Extract (see transaction-buffering), so that the marker and data replicats see the same operations.

#### Scenario: Shipped replicat configuration
- **WHEN** the metadata replicat configuration in `gg-configs/replicat` is inspected
- **THEN** it does not contain `IGNOREDELETES`, sets `blockingSend=true`, and its producer config sets `acks=all` and `enable.idempotence=true`

### Requirement: State survives restart
Buffered transactions, the emission queue, and the record of emitted transactions SHALL be restored from the checkpoint on restart, so that ordering and duplicate detection hold across restarts.

#### Scenario: Restart while a transaction is blocked
- **WHEN** the application restarts while T1 is incomplete and T2 is complete behind it
- **THEN** after restart, T2 is still not emitted before T1, and replayed events for previously emitted transactions are dropped
