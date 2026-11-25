# Spec Delta

## ADDED Requirements

### Requirement: Buffering cost is linear in transaction size
Accepting a data event SHALL take a bounded amount of state work that does not depend on how many events its transaction has already buffered. Each accepted event SHALL be written to state once, and SHALL be read once when its transaction is emitted or its buffer is removed. Deciding whether a transaction is complete SHALL NOT require reading its buffered events. Detecting a duplicate `pos` SHALL NOT require reading the transaction's other buffered events.

#### Scenario: Bulk transaction of 55,000 events
- **WHEN** a transaction's marker declares 55,000 events and all 55,000 distinct events arrive
- **THEN** the transaction is emitted complete, the state writes per accepted event stay at the same constant as for a 5-event transaction, and each event is written to state exactly once

#### Scenario: Duplicate in a large transaction
- **WHEN** 50,000 events of a transaction are buffered and an event arrives with a `pos` already buffered for the same table
- **THEN** the event is ignored as a duplicate without reading any buffered event

#### Scenario: Completeness check on a large blocked head
- **WHEN** the head-of-line transaction has 50,000 of 55,000 declared events buffered and a step re-checks it
- **THEN** it is found incomplete without reading any buffered event, and the blocked-alert progress reports the per-table received and declared counts

#### Scenario: Buffered order is kept
- **WHEN** a transaction's events are buffered in the order A, B, C for one table and the transaction is emitted
- **THEN** the emitted transaction lists that table's events in the order A, B, C, as before this change
