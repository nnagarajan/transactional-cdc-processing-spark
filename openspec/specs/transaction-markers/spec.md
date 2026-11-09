# transaction-markers Specification

## Purpose

Defines the transaction markers that GoldenGate publishes to the metadata topic, and how the pipeline reads them. Exactly one marker per source transaction, with exact per-table counts, in either of two message forms, ordered within a message by commit order.

## Requirements

### Requirement: One marker per source transaction
The marker publisher SHALL emit exactly one marker for each source transaction (identified by xid and csn) that changes a tracked table. Its per-table counts SHALL equal that transaction's operations on each table, and its total count SHALL equal their sum. This SHALL hold regardless of how Replicat groups source transactions into deliveries.

#### Scenario: Two source transactions delivered together
- **WHEN** Replicat delivers source transactions X1:500 (100 ORDERS, 100 ORDER_DETAILS, 600 ORDER_LINE_ITEMS) and X2:600 (50, 50, 400) as one 1,300-operation delivery
- **THEN** two markers are published: X1:500 with counts 100/100/600 and total 800, and X2:600 with counts 50/50/400 and total 500

#### Scenario: Two transactions at the same CSN
- **WHEN** a delivery contains source transactions A:500 and B:500
- **THEN** two separate markers are published, one for A:500 and one for B:500, each with its own counts

#### Scenario: Counts add up exactly
- **WHEN** 50 source transactions of 1 to 4 operations each are delivered together
- **THEN** 50 markers are published, and the sum of their totals equals the number of delivered operations

### Requirement: Single-transaction message is unchanged
A delivery holding one source transaction SHALL produce one message containing a single JSON object. It SHALL have the same fields and values as the original formatter's output: `xid`, `csn`, `tx_ts` (the delivery's begin time), `event_count`, and `data_collections` (one entry per table, with its `event_count`).

#### Scenario: Single source transaction
- **WHEN** a delivery holds one source transaction with 2 operations on TABLE1
- **THEN** the message is one JSON object with that transaction's xid and csn, `event_count` 2, and one data collection TABLE1 with `event_count` 2

### Requirement: Grouped delivery is one message holding an array
A delivery holding several source transactions SHALL produce one message containing a JSON array of marker objects, in commit order. Each object SHALL use its own transaction's xid, csn and operation timestamp.

#### Scenario: Three grouped source transactions
- **WHEN** X1:500, X2:600 and X3:700 are delivered together in that order
- **THEN** the message is a JSON array of three markers, in the order X1:500, X2:600, X3:700

### Requirement: Pipeline accepts both message forms
The pipeline SHALL read a metadata message holding either a single marker object or an array of marker objects, and SHALL treat each marker as a separate marker input.

#### Scenario: Object message
- **WHEN** a metadata message holds one marker object
- **THEN** the pipeline reads exactly one marker from it, with that marker's xid, csn and per-table counts

#### Scenario: Array message
- **WHEN** a metadata message holds an array of three marker objects
- **THEN** the pipeline reads three markers from it, each with its own xid, csn and per-table counts

### Requirement: Markers within a message keep commit order
Markers from one message SHALL be queued for emission in their order within the message, after the markers of earlier messages and before those of later messages. This SHALL hold whatever order the markers reach the sequencer in.

#### Scenario: Grouped markers arrive shuffled
- **WHEN** single-marker messages at offsets 4 and 6 surround a grouped message at offset 5 holding g1, g2 and g3, and all five markers reach the sequencer out of order
- **THEN** once all are complete they are emitted in the order: offset-4 marker, g1, g2, g3, offset-6 marker

#### Scenario: Later grouped marker completes first
- **WHEN** g3's events all arrive before g1's
- **THEN** g3 is not emitted until g1 and g2 have been emitted
