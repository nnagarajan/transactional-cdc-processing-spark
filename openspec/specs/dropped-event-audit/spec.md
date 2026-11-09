# dropped-event-audit Specification

## Purpose

Records every input the transactional CDC pipeline discards, together with the reason, in a queryable Delta table, without changing what the pipeline emits to `order_stream`.

## Requirements

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
- **WHEN** a data event names a table other than ORDERS, ORDER_DETAILS or ORDER_LINE_ITEMS
- **THEN** one row is written with reason `UNKNOWN_TABLE` and that table name

### Requirement: Purged buffers are recorded as one summary row each
When a buffered transaction without a marker is removed because the cursor passed its CSN, the system SHALL write one row for that transaction, with item kind `PURGED_BUFFER`, reason `NO_MARKER_BELOW_CURSOR`, and the number of buffered events per table. It SHALL NOT write a row per buffered event.

#### Scenario: Orphan buffer purged
- **WHEN** an unmarked buffer for h:100 holding 1 order, 1 detail and 3 line-item events is removed after a transaction at CSN 200 is emitted
- **THEN** one row is written for h:100 with buffered counts 1, 1 and 3 and last emitted CSN `200`

### Requirement: Unusable Kafka messages are recorded
A Kafka message that is not valid JSON SHALL produce a row with item kind `RAW_MESSAGE`, reason `UNPARSEABLE`, its topic, partition and offset, and its raw payload. A parsed record with no xid or no csn SHALL produce a row with reason `MISSING_IDS`. Neither SHALL reach transaction assembly.

#### Scenario: Malformed metadata message
- **WHEN** offset 12 of the metadata topic holds the text `{"xid":` (truncated JSON)
- **THEN** one row is written with reason `UNPARSEABLE`, the metadata topic, partition 0, offset 12, and that payload, and no marker is queued

#### Scenario: Data event without a CSN
- **WHEN** a data-topic message parses but has no `csn`
- **THEN** one row is written with reason `MISSING_IDS` and the raw payload

### Requirement: Dropped rows identify their Kafka source
Rows for dropped data events, markers and raw messages SHALL carry the Kafka topic, partition and offset they were read from. Markers SHALL also carry their position within a grouped message.

#### Scenario: Dropped line-item event
- **WHEN** a replayed event read from partition 0, offset 371 of the line-items topic is dropped
- **THEN** its row has that topic, partition 0 and offset 371

### Requirement: Replay detail is configurable
With replay detail set to `full` (the default), each replayed event or marker SHALL produce its own row. With replay detail set to `summary`, all replayed items of one transaction in a micro-batch SHALL produce one row for that transaction with the number of items dropped. Other reasons SHALL always produce full rows.

#### Scenario: Summary mode
- **WHEN** replay detail is `summary` and 350 replayed events of transaction a:400 are dropped in one micro-batch
- **THEN** one row is written for a:400 with reason `REPLAY_BELOW_CURSOR` and a dropped count of 350

### Requirement: Writes are exactly once
Each micro-batch's rows SHALL be written to `order_stream` and to the dropped-events table at most once each, including when the batch is retried after a failure. `order_stream` SHALL be written before the dropped-events table.

#### Scenario: Batch retried after the dropped-events write failed
- **WHEN** a micro-batch committed to `order_stream`, the dropped-events write failed, and the batch is re-run after a restart
- **THEN** `order_stream` holds that batch's transactions once, and the dropped-events table then receives that batch's rows once

### Requirement: Order output is unchanged
Recording drops SHALL NOT change which transactions are emitted to `order_stream`, their order, their contents, or the commit-level ordering of output.

#### Scenario: Same input with and without recording
- **WHEN** the same input is processed once with recording enabled and once disabled
- **THEN** `order_stream` receives identical rows in the same commit order

### Requirement: Recording can be disabled
With recording disabled, the system SHALL write nothing to the dropped-events table and SHALL NOT require it to exist. Drops SHALL still be logged as before.

#### Scenario: Disabled
- **WHEN** recording is disabled and the dropped-events table does not exist
- **THEN** the pipeline starts, emits to `order_stream`, and logs drops as before
