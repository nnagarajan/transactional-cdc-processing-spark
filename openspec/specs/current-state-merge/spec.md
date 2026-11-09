# current-state-merge Specification

## Purpose

Maintains `orders_current`, the SCD Type 1 current-state view of each order, from the `order_stream` history so that it reflects the latest version of every order, detail and line item regardless of replay or batching.

## Requirements

### Requirement: Latest version wins per entity
For each order, `orders_current` SHALL hold the order attributes, order detail and line items with the highest `version` seen for each entity key (order ID for orders and details, line item ID for line items).

#### Scenario: Older version arrives after newer
- **WHEN** `orders_current` holds line item A at version 3 and a batch delivers line item A at version 2
- **THEN** line item A remains at version 3

### Requirement: Child-only change for an unknown order is retained
When a batch contains changes only to an order's details or line items and that order does not yet exist in `orders_current`, the system SHALL insert a row for the order holding those children with null order attributes. When the order's own record later arrives, its attributes SHALL be applied and the retained children kept unless superseded by higher versions.

#### Scenario: Line item update for an order captured before CDC began
- **WHEN** a batch contains only a line item update for order 7, and order 7 is not in `orders_current`
- **THEN** a row for order 7 is inserted containing that line item and null order attributes

#### Scenario: Parent arrives later
- **WHEN** a placeholder row for order 7 holds line item A at version 2, and a later batch delivers order 7 at version 1 with line item A at version 1
- **THEN** the row has order 7's attributes at version 1 and line item A at version 2

### Requirement: Transaction context never regresses
The `xid`, `csn` and `dwhProcessedTs` columns of an `orders_current` row SHALL be updated only by a source row whose numeric `csn` is greater than the row's current `csn`.

#### Scenario: Replayed older transaction
- **WHEN** a row has `csn=900` and a batch delivers a change for the same order from `csn=800`
- **THEN** the row's `xid`, `csn` and `dwhProcessedTs` remain those of `csn=900`

#### Scenario: CSN compared numerically
- **WHEN** a row has `csn=99` and a batch delivers a change from `csn=100`
- **THEN** the row's transaction context is updated to `csn=100`

### Requirement: Intra-batch ties resolved by commit order
When a single batch contains several `order_stream` rows for the same order with equal or missing order versions, the system SHALL take order-level attributes and transaction context from the row with the highest numeric `csn`.

#### Scenario: Two rows with the same order version in one batch
- **WHEN** a batch contains two rows for order 7 with order version 4 at `csn=100` and `csn=120`
- **THEN** the merged result uses the row from `csn=120`
