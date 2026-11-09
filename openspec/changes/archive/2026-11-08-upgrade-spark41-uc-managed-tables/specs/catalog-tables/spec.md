# Spec Delta

## Purpose

Defines where the pipeline's Delta tables live and how they are created, named and recreated in Unity Catalog. Covers the constraints of catalog-managed tables: availability of the catalog server for writes, and session-scoped views.

## ADDED Requirements

### Requirement: Tables are catalog-managed
`order_stream` and `orders_current` SHALL be Unity Catalog managed Delta tables in the configured catalog and schema (default `mymanaged.argus1`). The catalog SHALL choose each table's storage location under the schema's (or else the catalog's) storage root. Table definitions SHALL NOT specify a storage location.

#### Scenario: Seeding creates managed tables
- **WHEN** the seed job runs against a catalog and schema that have storage roots
- **THEN** the catalog lists `order_stream` and `orders_current` as `MANAGED` tables whose locations are under the schema's storage root

### Requirement: Tables are referenced by schema-qualified name
The applications SHALL read and write the tables by schema-qualified name (`argus1.order_stream`, `argus1.orders_current`), resolved against the configured default catalog. They SHALL NOT depend on a table's storage path.

#### Scenario: Pipeline writes the change table
- **WHEN** the transaction pipeline emits a transaction
- **THEN** its records are committed to the `order_stream` table in schema `argus1` of the default catalog

#### Scenario: Unqualified name
- **WHEN** a table is referenced without its schema
- **THEN** it resolves to the default catalog's `default` schema, which does not hold the pipeline's tables

### Requirement: Reseeding recreates empty tables
The seed job SHALL drop and recreate each table it defines. Because dropping a managed table deletes its data, a reseed SHALL leave both tables empty.

#### Scenario: Seed job rerun after data was written
- **WHEN** the seed job runs again after the pipeline has written records
- **THEN** both tables exist and contain no records

### Requirement: Writes require the catalog server
Commits to the managed tables SHALL go through the Unity Catalog server. When the server is unreachable, a write SHALL fail instead of being written outside the catalog. Once the server is back, a restarted application SHALL resume from its checkpoint.

#### Scenario: Catalog server stopped while the pipeline runs
- **WHEN** the Unity Catalog server is unreachable during a micro-batch commit
- **THEN** the commit fails, and no data is written to the table's storage location outside the catalog

#### Scenario: Server back and application restarted
- **WHEN** the server is reachable again and the application is restarted with the same checkpoint
- **THEN** processing resumes from the last committed batch

### Requirement: Convenience views are session-scoped
Derived views over the tables (`order_stream_current`, `order_stream_changes`, `orders_current_view`) SHALL be provided as session-scoped temporary views, outside the seed job. The seed job SHALL NOT create catalog views, which the connector does not support on the current runtime.

#### Scenario: Seed job on the current runtime
- **WHEN** the seed job runs
- **THEN** it creates both tables and completes without attempting to create a view

#### Scenario: Analyst session
- **WHEN** a Spark session runs `db/views.sql`
- **THEN** the three views can be queried by name for the rest of that session
