# Spec Delta

## ADDED Requirements

### Requirement: Current table shape follows the model
Each job's current table SHALL hold one row per root key. A row SHALL contain:
- `xid`, `csn` and `dwhProcessedTs`
- the root entity's columns at top level, with its before image as one struct column, named `<root entity>Before` by default and overridable
- one struct per one-to-one child
- one array per one-to-many child

#### Scenario: Orders current shape
- **WHEN** the current table schema is derived for the shipped orders job
- **THEN** it has top-level order columns, `orderBefore`, an `orderDetails` struct and a `lineItems` array, matching the existing `orders_current` table

### Requirement: Entities without a version use commit order
For an entity that has no version column, the entity's change from the transaction with the highest numeric `csn` SHALL count as its latest version. For each such entity, the current table SHALL store the `csn` of the change it holds in a `changeCsn` field: inside the child's struct or element, or at top level for the root. Entities that have a version column SHALL NOT get this field.

#### Scenario: Child table without a VERSION column
- **WHEN** a one-to-many child has no version column, and the current table holds element K from `csn=500` while a batch delivers element K from `csn=400`
- **THEN** element K keeps its values and `changeCsn=500`

#### Scenario: Orders model unaffected
- **WHEN** the current table schema is derived for the shipped orders job, whose entities all have `VERSION`
- **THEN** it contains no `changeCsn` field

## MODIFIED Requirements

### Requirement: Latest version wins per entity
For each root key, a job's current table SHALL hold the root attributes, each one-to-one child and each one-to-many child's elements with the highest `version` seen for each entity key. The root and one-to-one children SHALL be keyed by the root key. Elements of a one-to-many child SHALL be keyed by the child's primary key.

#### Scenario: Older version arrives after newer
- **WHEN** `orders_current` holds line item A at version 3 and a batch delivers line item A at version 2
- **THEN** line item A remains at version 3

### Requirement: Child-only change for an unknown order is retained
When a batch contains changes only to a root's children and that root key does not yet exist in the current table, the system SHALL insert a row for the key holding those children with null root attributes. When the root's own record later arrives, its attributes SHALL be applied and the retained children kept unless superseded by higher versions.

#### Scenario: Line item update for an order captured before CDC began
- **WHEN** a batch contains only a line item update for order 7, and order 7 is not in `orders_current`
- **THEN** a row for order 7 is inserted containing that line item and null order attributes

#### Scenario: Parent arrives later
- **WHEN** a placeholder row for order 7 holds line item A at version 2, and a later batch delivers order 7 at version 1 with line item A at version 1
- **THEN** the row has order 7's attributes at version 1 and line item A at version 2

### Requirement: Transaction context never regresses
The `xid`, `csn` and `dwhProcessedTs` columns of a current-table row SHALL be updated only by a source row whose numeric `csn` is greater than the row's current `csn`.

#### Scenario: Replayed older transaction
- **WHEN** a row has `csn=900` and a batch delivers a change for the same root key from `csn=800`
- **THEN** the row's `xid`, `csn` and `dwhProcessedTs` remain those of `csn=900`

#### Scenario: CSN compared numerically
- **WHEN** a row has `csn=99` and a batch delivers a change from `csn=100`
- **THEN** the row's transaction context is updated to `csn=100`

### Requirement: Intra-batch ties resolved by commit order
When a single batch contains several stream rows for the same root key with equal or missing root versions, the system SHALL take root attributes and transaction context from the row with the highest numeric `csn`.

#### Scenario: Two rows with the same order version in one batch
- **WHEN** a batch contains two rows for order 7 with order version 4 at `csn=100` and `csn=120`
- **THEN** the merged result uses the row from `csn=120`
