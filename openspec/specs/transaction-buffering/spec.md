# transaction-buffering Specification

## Purpose

Buffers CDC data events per source transaction and decides exactly when a transaction has received every event its metadata marker declares, so that only complete transactions are emitted.

## Requirements

### Requirement: Transaction identity
The system SHALL identify a source transaction by the pair of its `xid` and `csn`. Two transactions that share a `csn` but differ in `xid` SHALL be buffered and emitted as separate transactions.

#### Scenario: Two transactions committed at the same CSN
- **WHEN** markers and data events arrive for `xid=A, csn=500` and `xid=B, csn=500`
- **THEN** each transaction is buffered separately, completes against its own marker's counts, and is emitted with its own `xid`

### Requirement: Completion against marker counts
A transaction SHALL be complete only when its marker has been received and, for each of ORDERS, ORDER_DETAILS and ORDER_LINE_ITEMS, the number of distinct buffered data events equals the count the marker declares for that table.

#### Scenario: All declared events received
- **WHEN** the marker declares 2 ORDERS, 2 ORDER_DETAILS and 2 ORDER_LINE_ITEMS events and exactly those 6 distinct events have been buffered
- **THEN** the transaction is complete

#### Scenario: Data events received but no marker
- **WHEN** data events for a transaction have been buffered but its marker has not arrived
- **THEN** the transaction is not complete

#### Scenario: Marker received, events missing
- **WHEN** the marker declares 2 ORDER_LINE_ITEMS events and only 1 has been buffered
- **THEN** the transaction is not complete

#### Scenario: Marker declares no events for the tracked tables
- **WHEN** a marker declares zero events for all three tracked tables
- **THEN** the transaction is complete immediately and produces no output records

### Requirement: Deletes are excluded at the source
Delete operations SHALL be filtered out at the GoldenGate Extract, before the trail, so that neither the data topics nor the marker counts contain them and the two always agree. Deletes SHALL NOT be filtered at only one of the publishing replicats.

#### Scenario: Shipped extract configuration
- **WHEN** `gg-configs/extract/ext04.prm` is inspected
- **THEN** it contains `IGNOREDELETES` before its `TABLE` statements

#### Scenario: Transaction with an update and a delete on line items
- **WHEN** a source transaction updates one line item and deletes another
- **THEN** its marker declares 1 ORDER_LINE_ITEMS event, the transaction completes once the update arrives, and the output contains only the updated line item

### Requirement: Stray delete events are counted but not output
A delete event that reaches the pipeline anyway (for example, one already in the trail before Extract filtering was enabled) SHALL be buffered and counted toward completion like any other event, and SHALL NOT produce an entity record in the output.

#### Scenario: Delete event already in the trail
- **WHEN** a transaction's marker declares 2 ORDER_LINE_ITEMS events and the pipeline receives one update and one delete for it
- **THEN** the transaction completes only after both arrive, and the output contains only the updated line item

### Requirement: Duplicate data events are ignored
A data event whose `pos` equals the `pos` of an event already buffered for the same transaction SHALL be ignored and SHALL NOT count toward completion.

#### Scenario: Producer re-sends an event
- **WHEN** line item event `pos=100` arrives twice for a transaction whose marker declares 3 ORDER_LINE_ITEMS events, followed by `pos=101`
- **THEN** the buffered count is 2, the transaction is not complete, and it completes only when a third distinct event arrives

#### Scenario: Two distinct changes to the same row
- **WHEN** a transaction contains two updates to the same line item with different `pos` values
- **THEN** both events are buffered and both count toward completion

### Requirement: Duplicate markers are ignored
A marker for a transaction that already has a marker, either buffered or already emitted, SHALL be ignored.

#### Scenario: Marker re-sent while the transaction is buffered
- **WHEN** a second marker for `xid:csn` arrives while that transaction is still waiting for data events
- **THEN** the second marker has no effect on the transaction's expected counts or its position in emission order

#### Scenario: Marker re-sent after emission
- **WHEN** a marker arrives for a transaction that has already been emitted
- **THEN** the marker is ignored, no new buffered transaction is created, and nothing is emitted

### Requirement: Events for emitted transactions are dropped
A data event belonging to a transaction that has already been emitted SHALL be dropped without creating buffered state.

#### Scenario: Replayed data event after emission
- **WHEN** a data event arrives whose `xid:csn` was emitted in an earlier batch
- **THEN** it is dropped and no buffered state is created for it
