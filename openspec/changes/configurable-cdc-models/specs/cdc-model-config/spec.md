# Spec Delta

## Purpose

Defines how a CDC job's data model is described: a short job config naming a root table and its children, with every other detail taken from conventions, overridable where needed. One jar runs any number of such jobs.

## ADDED Requirements

### Requirement: A job is defined by a root table and its child tables
A job config SHALL name exactly one root source table and zero or more child source tables, all qualified by schema. Together with shared defaults and the job's generated model schema file, that SHALL be enough to run both pipeline stages.

#### Scenario: Minimal orders job
- **WHEN** a job config sets only its job name, root `APPUSER.ORDERS` and children `APPUSER.ORDER_DETAILS` and `APPUSER.ORDER_LINE_ITEMS`, and the model schema file for those tables exists
- **THEN** the stage 1 and stage 2 pipelines start for that model without any further settings

### Requirement: Topics follow the replicat naming convention
Each entity's Kafka topic SHALL default to the shared topic template applied to its schema-qualified table name. The default template SHALL produce `dev.<schema>.<table>.json` in lower case, matching the replicat's topic mapping. An entity SHALL be able to override its topic.

#### Scenario: Default topic
- **WHEN** an entity's table is `APPUSER.ORDER_LINE_ITEMS` and no topic override is set
- **THEN** the job reads that entity from `dev.appuser.order_line_items.json`

#### Scenario: Topic override
- **WHEN** an entity sets its topic to `prod.orders.items`
- **THEN** the job reads that entity from `prod.orders.items`

### Requirement: Names follow camelCase conventions
An entity's output name SHALL default to the camelCase form of its table name, and a column's output name SHALL default to the camelCase form of its column name. Both SHALL be overridable per entity and per column.

#### Scenario: Default names
- **WHEN** table `ORDER_LINE_ITEMS` has column `LINE_ITEM_ID` and no overrides are set
- **THEN** the entity is named `orderLineItems` and the column `lineItemId`

#### Scenario: Entity name override
- **WHEN** the `ORDER_LINE_ITEMS` entity sets its name to `lineItems`
- **THEN** its output array and its references in stage 2 are named `lineItems`

### Requirement: Types follow the model schema with overrides
A column's output type SHALL default to the type mapped from its Oracle type in the model schema file. A job SHALL be able to override the mapping for a whole Oracle type family, such as every `NUMBER` or every `DATE`, and to override a single column's type.

#### Scenario: Family override
- **WHEN** a job maps the `NUMBER` family to `double` and `DATE` and `TIMESTAMP` to `string`
- **THEN** every numeric column is output as DOUBLE and every date or timestamp column as STRING

#### Scenario: Column override wins
- **WHEN** the `NUMBER` family maps to `double` and column `ORDERS.TOTAL_AMOUNT` overrides its type to `decimal(20,4)`
- **THEN** `totalAmount` is DECIMAL(20,4) and the other numeric columns are DOUBLE

### Requirement: Per-entity defaults
Each entity SHALL by default include all its columns, carry a before image, and use column `VERSION` as its version when the table has one, or no version column otherwise. An entity SHALL be able to exclude columns, turn off its before image, and name a different version column.

#### Scenario: Excluded column
- **WHEN** an entity excludes column `INTERNAL_NOTES`
- **THEN** that column appears in neither stage's output for that entity

#### Scenario: Table without a VERSION column
- **WHEN** an entity's table has no `VERSION` column and no version override is set
- **THEN** the entity has no version column

### Requirement: Output tables and checkpoints follow the job name
The stage 1 table SHALL default to `<root entity>_stream` and the stage 2 table to `<root entity>_current`, both in the configured catalog schema. Each stage's checkpoint SHALL default to `<checkpoint root>/<job name>/<stage>`. Each of these SHALL be overridable.

#### Scenario: Two jobs side by side
- **WHEN** jobs `orders` and `customers` run with the same checkpoint root and no overrides
- **THEN** their checkpoints are in different directories and they write to different tables

### Requirement: Models are a root with direct children
A model SHALL consist of one root entity and zero or more children. Every child SHALL carry the root key, either through a foreign key to the root in the model schema file or through a key override in the job config. Children of children SHALL NOT be supported.

#### Scenario: Child keyed by a foreign key
- **WHEN** `ORDER_LINE_ITEMS.ORDER_ID` is a foreign key to `ORDERS`
- **THEN** line-item events are grouped under the `ORDER_ID` value they carry

#### Scenario: Child without a declared foreign key
- **WHEN** a child table has no foreign key to the root and its job config sets its key to `ORDER_ID`
- **THEN** its events are grouped under their `ORDER_ID` value

### Requirement: Invalid job configuration fails at startup
A job SHALL refuse to start, with an error naming the problem, when any of the following holds:
- the model schema file is missing
- a configured table is absent from the model schema file
- a child has no root key
- the root key has more than one column
- two entities share an output name
- an override names an unknown column
- a type is not a valid Spark type

#### Scenario: Child with no root key
- **WHEN** a child table has no foreign key to the root and no key override
- **THEN** the job fails before any query starts, naming that table

#### Scenario: Override names an unknown column
- **WHEN** a column override names `ORDERS.ORDER_REFF`, which the model schema does not contain
- **THEN** the job fails before any query starts, naming that column

### Requirement: The shipped orders job reproduces the existing tables
The repository SHALL ship an orders job config whose output matches the existing `order_stream` and `orders_current` schemas exactly: column names, types, nesting and nullability. That includes DOUBLE numeric columns, STRING date and timestamp columns, the `lineItems` array, and the `orderBefore` column.

#### Scenario: Schema equality
- **WHEN** the stage 1 and stage 2 schemas are derived from the orders job config
- **THEN** they equal the schemas of `order_stream` and `orders_current` as created by the existing DDL
