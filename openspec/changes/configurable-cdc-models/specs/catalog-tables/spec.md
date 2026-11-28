# Spec Delta

## ADDED Requirements

### Requirement: Table schemas are checked at startup
Before its query starts, each stage SHALL compare the schema of each table it writes with the schema derived from the job's model. On any difference in column names, types or nesting it SHALL refuse to start, naming the table and the differing columns. A job SHALL NOT create or alter its stream or current tables.

#### Scenario: Table created from stale DDL
- **WHEN** a job's model includes column `priority`, but its stream table was created before that column was added
- **THEN** stage 1 fails before the query starts, naming the stream table and `priority`

#### Scenario: Missing table
- **WHEN** a job's current table does not exist
- **THEN** stage 2 fails before the query starts, naming the table

### Requirement: Table DDL is written from the job config
A command SHALL write a job's stage 1 and stage 2 CREATE TABLE scripts as `*.deltalake.sql` files that the seed job can run, using only the job config and shared defaults. The scripts SHALL use the `{{CATALOG_SCHEMA}}` placeholder and specify no storage location. The command SHALL NOT connect to Oracle.

#### Scenario: Seeding written DDL
- **WHEN** the seed job runs the scripts written for a job
- **THEN** it creates that job's stream and current tables as managed tables whose schemas equal the schemas the job derives at startup

#### Scenario: Orders DDL
- **WHEN** the command runs for the shipped orders job
- **THEN** the scripts create tables whose schemas equal the existing `order_stream` and `orders_current` schemas

## MODIFIED Requirements

### Requirement: Tables are catalog-managed
Each job's stream and current tables (for the orders job, `order_stream` and `orders_current`) SHALL be Unity Catalog managed Delta tables in the configured catalog and schema (default `mymanaged.argus1`). The catalog SHALL choose each table's storage location under the schema's (or else the catalog's) storage root. Table definitions SHALL NOT specify a storage location.

#### Scenario: Seeding creates managed tables
- **WHEN** the seed job runs against a catalog and schema that have storage roots
- **THEN** the catalog lists `order_stream` and `orders_current` as `MANAGED` tables whose locations are under the schema's storage root

### Requirement: Tables are referenced by schema-qualified name
The applications SHALL read and write each job's tables by the schema-qualified names its config resolves to (for the orders job, `argus1.order_stream` and `argus1.orders_current`), against the configured default catalog. They SHALL NOT depend on a table's storage path.

#### Scenario: Pipeline writes the change table
- **WHEN** the orders job's stage 1 pipeline emits a transaction
- **THEN** its records are committed to the `order_stream` table in schema `argus1` of the default catalog

#### Scenario: Unqualified name
- **WHEN** a table is referenced without its schema
- **THEN** it resolves to the default catalog's `default` schema, which does not hold the pipeline's tables
