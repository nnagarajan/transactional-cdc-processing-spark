# Spec Delta

## Purpose

Defines how a CDC job's data model is described: one job config that states the job's input (tables, topics, keys, source columns) and its output (column names and types, tables) explicitly. Nothing is derived from the Oracle source schema. One jar runs any number of such jobs.

## ADDED Requirements

### Requirement: A job lists its entities explicitly
A job config SHALL list exactly one root entity and zero or more child entities. Each entity SHALL state its output name, its source table (unqualified, as the transaction marker names it), its Kafka topic, and its columns. Together with shared infrastructure defaults, the job config SHALL be enough to run both pipeline stages. The job SHALL NOT read the Oracle data dictionary or connect to Oracle.

#### Scenario: Orders job
- **WHEN** a job config lists root `orders` (table `ORDERS`, topic `dev.appuser.orders.json`) and children `orderDetails` (`ORDER_DETAILS`) and `lineItems` (`ORDER_LINE_ITEMS`), each with its columns, keys and output tables
- **THEN** the stage 1 and stage 2 pipelines start for that model without any further settings

#### Scenario: Topic as configured
- **WHEN** an entity sets its topic to `prod.orders.items`
- **THEN** the job reads that entity from `prod.orders.items`

### Requirement: Columns are mapped explicitly
Each entity column SHALL state its source column name, its output name and its output type as a Spark SQL type. Only declared columns SHALL appear in the output. A column declared without a source column is a derived column: it SHALL appear in the output schema and SHALL be filled in by the job's hand-written transform.

#### Scenario: Renamed and typed column
- **WHEN** entity `orders` declares source `TOTAL_AMOUNT` as output `totalAmount` of type `decimal(20,4)`
- **THEN** the stream and current tables carry `totalAmount` as DECIMAL(20,4) cast from `TOTAL_AMOUNT`

#### Scenario: Source column not declared
- **WHEN** order events carry column `INTERNAL_NOTES` and entity `orders` does not declare it
- **THEN** `INTERNAL_NOTES` appears in neither stage's output

#### Scenario: Derived column
- **WHEN** entity `orders` declares output `totalWithTax` of type `decimal(20,4)` with no source column, and the job names a transform that sets it
- **THEN** the stream and current tables carry `totalWithTax` with the values the transform computed

### Requirement: Keys, cardinality and versions are declared
The root SHALL declare its key as one column or an ordered list of columns. Each child SHALL declare its root key: a mapping from each root key column to the child column that holds its value. When the root key has one column, the child MAY give just its column name. Each child SHALL declare its cardinality, `one` or `many`. A child with cardinality `many` SHALL declare its own key, as one column or an ordered list of columns, which identifies its elements. Any entity MAY declare a version column; an entity that declares none has no version column. Every key, root-key and version setting SHALL name source columns the entity declares.

Root and child events SHALL be grouped under the same root key when, for every root key column, the root's value and the value of the child's mapped column are equal after both are converted to that root key column's declared type.

#### Scenario: Child grouped by its root-key column
- **WHEN** child `lineItems` declares root-key column `ORDER_ID`
- **THEN** line-item events are grouped under the `ORDER_ID` value they carry

#### Scenario: Root key under a different name in the child
- **WHEN** the root declares key `ORDER_ID` and child `orderDetails` declares root-key column `ORD_REF`
- **THEN** an order-detail event with `ORD_REF = 7` is grouped with the order whose `ORDER_ID = 7`

#### Scenario: Compound root key
- **WHEN** the root declares key `[REGION, ORDER_NO]` and child `shipments` maps `REGION` to `SHIP_REGION` and `ORDER_NO` to `ORD_REF`
- **THEN** a shipment event with `SHIP_REGION = EU` and `ORD_REF = 7` is grouped with the order whose `REGION = EU` and `ORDER_NO = 7`, and not with the order whose `REGION = US` and `ORDER_NO = 7`

#### Scenario: Compound key on a one-to-many child
- **WHEN** child `shipments` declares key `[SHIP_REGION, ORD_REF, SHIPMENT_SEQ]`
- **THEN** its elements are identified by the combination of those three values

#### Scenario: Key values converted to the root key's type
- **WHEN** the root declares `ORDER_ID` with type `double`, child `orderDetails` maps it to `ORD_REF` declared with type `string`, and the events carry `ORDER_ID = 7` and `ORD_REF = 7`
- **THEN** both events are grouped under the same root key

#### Scenario: Entity without a version column
- **WHEN** an entity declares no version column
- **THEN** stage 2 orders its changes by commit order (spec `current-state-merge`)

### Requirement: Before images are on by default
Each entity SHALL carry a before image unless its config turns it off.

#### Scenario: Before image turned off
- **WHEN** entity `lineItems` sets its before image off
- **THEN** its stage 1 elements have no `before` field

### Requirement: Output tables are named, checkpoints follow the job name
A job config SHALL name its stage 1 (stream) and stage 2 (current) tables. Each stage's checkpoint SHALL default to `<checkpoint root>/<job name>/<stage>` and SHALL be overridable.

#### Scenario: Two jobs side by side
- **WHEN** jobs `orders` and `customers` run with the same checkpoint root and no checkpoint overrides
- **THEN** their checkpoints are in different directories

### Requirement: Models are a root with direct children
A model SHALL consist of one root entity and zero or more children, each carrying the root key. Children of children SHALL NOT be supported.

#### Scenario: Single-table model
- **WHEN** a job lists only root `customers` and no children
- **THEN** both stages run for that model, with the root's columns as the only entity

### Requirement: Invalid job configuration fails at startup
A job SHALL refuse to start, with an error naming the problem, when any of the following holds. All problems SHALL be reported together.
- there is not exactly one root entity
- an entity lacks a name, table, topic or columns
- a column lacks an output name or a type
- a type is not a valid Spark SQL type
- two entities share an output name or a source table, or two columns of one entity share an output name or a source column
- a key, root-key or version setting names a source column the entity does not declare
- a key lists the same column twice
- a child lacks a root key or a cardinality, or a `many` child lacks its own key
- a child's root-key mapping does not cover exactly the root's key columns, or gives a single column name while the root key has several columns
- a derived column is declared but the job names no transform, or the named transform class cannot be loaded
- a starting-offsets key names neither an entity nor `transaction-metadata`

#### Scenario: Child with no root-key column
- **WHEN** child `orderDetails` declares no root-key column
- **THEN** the job fails before any query starts, naming `orderDetails`

#### Scenario: Key names an undeclared column
- **WHEN** child `lineItems` declares key `LINE_ITEM_IDD`, which none of its columns use as source
- **THEN** the job fails before any query starts, naming `lineItems` and `LINE_ITEM_IDD`

#### Scenario: Root-key map misses a key column
- **WHEN** the root declares key `[REGION, ORDER_NO]` and child `shipments` maps only `ORDER_NO`
- **THEN** the job fails before any query starts, naming `shipments` and `REGION`

#### Scenario: Missing type
- **WHEN** column `orderDate` of entity `orders` has no type
- **THEN** the job fails before any query starts, naming `orders.orderDate`

### Requirement: The shipped orders job reproduces the existing tables
The repository SHALL ship an orders job config whose output matches the existing `order_stream` and `orders_current` schemas exactly: column names, types, nesting and nullability. That includes DOUBLE numeric columns, STRING date and timestamp columns, the `lineItems` array, and the `orderBefore` column.

#### Scenario: Schema equality
- **WHEN** the stage 1 and stage 2 schemas are derived from the orders job config
- **THEN** they equal the schemas of `order_stream` and `orders_current` as created by the existing DDL
