# Proposal

## Why

The pipeline only handles one data model: orders, order details and order line items. That model is hard-coded in about ten places: the topic list, the per-table buffers in `TransactionState`, the `Order*` bean classes and their `@JsonProperty` mappings, `OrderJoiner`, the hand-written column lists in `ScdType1Merger`, the DDL, and the `dropped_events` counts. Adding a second data model (for example customers or accounts) would mean copying all of it. We want to run several jobs from one jar. Each job should be described by a short config file, with columns, types, keys and relationships derived from the Oracle source schema instead of written by hand.

## What Changes

- A job is defined by a **job config** naming a root table and its child tables. Topics, entity and column names, before images, version columns, output table names and checkpoint paths come from conventions in shared defaults. Any of them can be overridden per job, entity or column.
- A **model generator** (`cdc-model gen`) reads the Oracle data dictionary once and writes a checked-in **model schema file** for the job. The file holds each table's columns, Oracle types, primary key and foreign keys, the root key, and each child's cardinality (1:1 or 1:N). It also writes the Delta DDL for the job's tables. `cdc-model check` fails when the Oracle schema no longer matches the checked-in file. The Spark job never connects to Oracle.
- The stage 1 pipeline (`TransactionalCdcProcessingApp`) reads its topics, tracked tables and output shape from the job model instead of fixed code. The sequencer buffers events per tracked table and checks completeness against the marker count for every tracked table. Output records are shaped by config-derived Spark projections instead of bean classes and Jackson.
- The stage 2 merge (`ScdType1MergeApp`) is generated from the same model:
  - the root's columns are flattened, with its before image as one struct column
  - a 1:1 child becomes a struct, kept at its highest version
  - a 1:N child becomes an array, deduplicated by the child's primary key and version
  - transaction context keeps its CSN guard
- The existing orders job is expressed as `jobs/orders.conf` plus overrides that **reproduce today's `order_stream` and `orders_current` schemas exactly**: DOUBLE numbers, STRING dates and timestamps, the `lineItems` array name, and the `orderBefore` column. Existing tables keep working.
- **BREAKING**: the sequencer's state layout changes from three fixed per-table lists to per-table maps, so the job needs a new checkpoint location. The tables themselves are unaffected.
- **BREAKING (additive schema)**: `dropped_events` gains a `jobName` column and a per-table `bufferedCounts` array. `bufferedOrders`, `bufferedDetails` and `bufferedLineItems` stay in the table but are no longer written.
- The `Order*` bean classes and `OrderJoiner` are removed, along with the order-specific code in `TransactionState` and `ScdType1Merger`.

## Capabilities

### New Capabilities
- `cdc-model-config`: how a job's data model is defined. Covers the job config, conventions and defaults, overrides, the root-plus-children model shape, the rule that every child carries the root key, and validation at startup.
- `cdc-model-generator`: generating and checking a job's model schema file and table DDL from the Oracle data dictionary, including the Oracle-to-Spark type mapping and the derivation of keys and cardinality.
- `stream-record-shaping`: how a complete transaction becomes stage 1 output records. Covers per-key grouping, one array per entity, typed columns cast from source values, before images, the handling of deletes and unmapped columns, and values that fail to cast.

### Modified Capabilities
- `transaction-buffering`: completion is checked against the job's tracked tables instead of the fixed ORDERS, ORDER_DETAILS and ORDER_LINE_ITEMS.
- `current-state-merge`: the SCD Type 1 merge applies to any job model. Entity keys and versions come from the model, and the column shapes follow the root, 1:1 and 1:N rules.
- `dropped-event-audit`: `UNKNOWN_TABLE` is relative to the job's tracked tables. Purged-buffer rows record counts per tracked table. Rows carry the job name.
- `stream-startup`: Kafka sources are the job's metadata topic plus one topic per entity, not four fixed topics. Starting offsets are keyed by entity.
- `catalog-tables`: each job's stream and current tables are named by its config and created from generated DDL. A job checks at startup that its tables match the model and refuses to start on a mismatch.

## Impact

- **Code:**
  - `TransactionalCdcProcessingApp`, `CdcJsonParsing` (only the event-type tag changes), `TransactionState`, `TransactionSequencer`, `TransactionOrderingProcessor` and `SequencerOutput` are changed.
  - `CdcOutputSink`, `DroppedEvent`, `ScdType1MergeApp`, `ScdType1Merger`, `KafkaSourceOptions` and `AppConfig` are changed.
  - `OrderJoiner` and `model/order/*` are removed.
  - New code: a model package for the spec, conventions, schema derivation and DDL, and a `ModelGenerator` entry point.
- **Config:**
  - New `jobs/` and `models/` directories.
  - `transactional-cdc-processing.conf` and `scd-type1-merge.conf` are replaced by job configs.
  - Run commands take `-Dconfig.file=jobs/<job>.conf`.
- **Dependencies:** the Oracle JDBC driver, used only by the generator. It is `provided` scope and not bundled in the job jar.
- **Data:**
  - New checkpoint locations for both stages.
  - `dropped_events` gains columns.
  - `order_stream` and `orders_current` are unchanged.
- **Tests:**
  - Tests that build `Order*` beans or `TransactionState` lists are rewritten against the generic model.
  - `ScdType1MergerTest` and the integration test must pass unchanged against the orders job model, as the compatibility check.
- **Docs:** the CLAUDE.md architecture and run sections, and `db/README.md`.
