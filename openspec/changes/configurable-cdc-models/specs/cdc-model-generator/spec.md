# Spec Delta

## Purpose

Generates and checks a job's model schema file and table DDL from the Oracle data dictionary, so that column lists, types, keys and relationships are never written by hand and the running job never needs access to Oracle.

## ADDED Requirements

### Requirement: Generate the model schema file from Oracle
Given a job config and Oracle connection settings, the generator SHALL read the configured tables from the Oracle data dictionary and write the job's model schema file. The file SHALL contain each table's columns in table order with their Oracle types and nullability, its primary key, its foreign keys, the root key, and each child's cardinality. It SHALL record the source schema and the generation time.

#### Scenario: Orders model generated
- **WHEN** the generator runs for the orders job against a database containing the orders DDL in `db/oracle-ddl.sql`
- **THEN** the schema file lists every column of the three tables, the primary keys `ORDERS(ORDER_ID)`, `ORDER_DETAILS(ORDER_ID)` and `ORDER_LINE_ITEMS(LINE_ITEM_ID)`, and root key `ORDER_ID`

#### Scenario: Configured table missing in Oracle
- **WHEN** a configured table does not exist in the data dictionary
- **THEN** the generator fails with an error naming the table and writes nothing

### Requirement: Cardinality is derived from keys
A child whose primary key consists exactly of its root-key columns SHALL be recorded as one-to-one. Any other child SHALL be recorded as one-to-many.

#### Scenario: One-to-one child
- **WHEN** `ORDER_DETAILS` has primary key `ORDER_ID`, which is also its foreign key to `ORDERS`
- **THEN** it is recorded as one-to-one

#### Scenario: One-to-many child
- **WHEN** `ORDER_LINE_ITEMS` has primary key `LINE_ITEM_ID` and foreign key `ORDER_ID` to `ORDERS`
- **THEN** it is recorded as one-to-many

### Requirement: Default Oracle type mapping
Unless overridden, Oracle types SHALL map to output types as follows:

| Oracle type | Output type |
|---|---|
| `NUMBER(p,0)` with p ≤ 9 | `int` |
| `NUMBER(p,0)` with p ≤ 18 | `bigint` |
| `NUMBER(p,0)` with p > 18 | `decimal(p,0)` |
| `NUMBER(p,s)` with s > 0 | `decimal(p,s)` |
| `NUMBER` without precision | `decimal(38,10)` |
| `FLOAT` and `BINARY_DOUBLE` | `double` |
| `VARCHAR2`, `NVARCHAR2`, `CHAR` and `CLOB` | `string` |
| `DATE` and every `TIMESTAMP` variant | `timestamp` |
| `RAW` and `BLOB` | `binary` |

An unmapped type SHALL make the generator fail, naming the column.

#### Scenario: Orders type mapping
- **WHEN** `ORDERS` has `ORDER_ID NUMBER(19,0)`, `VERSION NUMBER(10,0)`, `TOTAL_AMOUNT NUMBER(20,4)` and `ORDER_DATE DATE`
- **THEN** with no overrides they map to `bigint`, `bigint`, `decimal(20,4)` and `timestamp`

#### Scenario: Unsupported type
- **WHEN** a configured table has an `XMLTYPE` column and no type override covers it
- **THEN** the generator fails, naming the table and column

### Requirement: Generate table DDL
The generator SHALL write the job's stage 1 and stage 2 CREATE TABLE scripts as `*.deltalake.sql` files that the seed job can run. The scripts SHALL apply the job's naming, type and exclusion overrides, use the `{{CATALOG_SCHEMA}}` placeholder, and specify no storage location.

#### Scenario: Seeding generated DDL
- **WHEN** the seed job runs the scripts generated for a job
- **THEN** it creates that job's stream and current tables as managed tables whose schemas equal the schemas the job derives at startup

### Requirement: Check for schema drift
The generator SHALL provide a check mode that compares the Oracle data dictionary and the job config with the checked-in model schema file and generated DDL. It SHALL exit non-zero and list each difference (added, removed or retyped columns, and changed keys) when they disagree. It SHALL write nothing.

#### Scenario: Column added in Oracle
- **WHEN** column `ORDERS.PRIORITY` has been added in Oracle and the schema file was not regenerated
- **THEN** check mode exits non-zero and reports `ORDERS.PRIORITY` as added

#### Scenario: No drift
- **WHEN** the schema file and DDL were generated from the current Oracle schema and job config
- **THEN** check mode exits zero

### Requirement: Only the generator connects to Oracle
The streaming jobs SHALL NOT connect to Oracle or require the Oracle JDBC driver. The job jar SHALL NOT bundle the driver.

#### Scenario: Job started without the driver
- **WHEN** a stage 1 or stage 2 job is submitted without the Oracle JDBC driver on its classpath
- **THEN** it starts and runs normally
