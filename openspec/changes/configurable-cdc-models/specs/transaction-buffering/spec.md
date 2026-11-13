# Spec Delta

## MODIFIED Requirements

### Requirement: Completion against marker counts
A transaction SHALL be complete only when its marker has been received and, for each table tracked by the job's model, the number of distinct buffered data events equals the count the marker declares for that table. A table SHALL match a marker's data collection by its unqualified table name. Counts the marker declares for tables the job does not track SHALL be ignored.

#### Scenario: All declared events received
- **WHEN** the orders job's marker declares 2 ORDERS, 2 ORDER_DETAILS and 2 ORDER_LINE_ITEMS events and exactly those 6 distinct events have been buffered
- **THEN** the transaction is complete

#### Scenario: Data events received but no marker
- **WHEN** data events for a transaction have been buffered but its marker has not arrived
- **THEN** the transaction is not complete

#### Scenario: Marker received, events missing
- **WHEN** the marker declares 2 ORDER_LINE_ITEMS events and only 1 has been buffered
- **THEN** the transaction is not complete

#### Scenario: Marker declares no events for the tracked tables
- **WHEN** a marker declares zero events for every table the job tracks
- **THEN** the transaction is complete immediately and produces no output records

#### Scenario: Marker covers tables of another job
- **WHEN** the orders job receives a marker declaring 1 ORDERS event and 3 CUSTOMERS events, and CUSTOMERS belongs to a different job
- **THEN** the orders job's transaction is complete once its 1 ORDERS event has been buffered

#### Scenario: Model with a single table
- **WHEN** a job tracks only `APPUSER.CUSTOMERS` and the marker declares 2 CUSTOMERS events
- **THEN** the transaction is complete once 2 distinct CUSTOMERS events have been buffered
