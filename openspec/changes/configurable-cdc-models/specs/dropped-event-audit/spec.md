# Spec Delta

## ADDED Requirements

### Requirement: Dropped rows identify their job
Every dropped-events row SHALL carry the name of the job that wrote it, so several jobs can share one dropped-events table.

#### Scenario: Two jobs sharing the table
- **WHEN** jobs `orders` and `customers` both record drops in `argus1.dropped_events`
- **THEN** each row's job name tells which job dropped the item

## MODIFIED Requirements

### Requirement: Every drop is recorded with a reason
When enabled, each input the pipeline discards SHALL produce a row in the dropped-events table. The row SHALL carry the drop time, the micro-batch id, the item kind (`DATA_EVENT`, `MARKER`, `PURGED_BUFFER`, `RAW_MESSAGE`), the reason, and the last emitted CSN at the time of the drop.

#### Scenario: Replayed data event below the cursor
- **WHEN** the last emitted CSN is 500 and a data event for transaction a:400 arrives
- **THEN** one row is written with item kind `DATA_EVENT`, reason `REPLAY_BELOW_CURSOR`, xid `a`, csn `400`, and last emitted CSN `500`

#### Scenario: Replayed event at the cursor for an emitted transaction
- **WHEN** transaction a:500 has been emitted and another event for a:500 arrives
- **THEN** one row is written with reason `REPLAY_ALREADY_EMITTED`

#### Scenario: Duplicate marker
- **WHEN** a second marker arrives for a transaction that already has one
- **THEN** one row is written with item kind `MARKER`, reason `DUPLICATE_MARKER`, and the marker's declared counts

#### Scenario: Duplicate data event
- **WHEN** an event arrives with the same `pos` as an event already buffered for its transaction
- **THEN** one row is written with reason `DUPLICATE_EVENT`

#### Scenario: Event for an untracked table
- **WHEN** a data event names a table that the job's model does not track
- **THEN** one row is written with reason `UNKNOWN_TABLE` and that table name

### Requirement: Purged buffers are recorded as one summary row each
When a buffered transaction without a marker is removed because the cursor passed its CSN, the system SHALL write one row for that transaction, with item kind `PURGED_BUFFER`, reason `NO_MARKER_BELOW_CURSOR`, and the number of buffered events for each tracked table. It SHALL NOT write a row per buffered event.

#### Scenario: Orphan buffer purged
- **WHEN** an unmarked buffer for h:100 holding 1 order, 1 detail and 3 line-item events is removed after a transaction at CSN 200 is emitted
- **THEN** one row is written for h:100 with buffered counts ORDERS 1, ORDER_DETAILS 1 and ORDER_LINE_ITEMS 3, and last emitted CSN `200`
