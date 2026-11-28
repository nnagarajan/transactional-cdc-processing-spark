# Proposal

## Why

The pipeline only handles one data model: orders, order details and order line items. That model is hard-coded in about ten places: the topic list, the per-table counters in `TransactionSummary`, the `Order*` bean classes and their `@JsonProperty` mappings, `OrderJoiner`, the hand-written column lists in `ScdType1Merger`, the DDL, and the `dropped_events` counts. Adding a second data model (for example customers or accounts) would mean copying all of it. We want to run several jobs from one jar. Each job should be described by one config file that states its input and its output explicitly: which tables and topics it reads, which source columns it maps to which output columns and types, its keys, and the tables it writes. Anything a plain rename and cast cannot express is written by hand in Scala.

## What Changes

- A job is defined by a **job config** that lists its entities explicitly: one root and zero or more children. For each entity it states:
  - the input: source table, Kafka topic, root key, its own key (for a one-to-many child), version column, and cardinality (one-to-one or one-to-many)
  - the column mapping: each source column with its output name and output type

  Keys may be compound: the root key, and a one-to-many child's own key, can each list several columns. A child maps each root key column to the child column that holds it, so a child may store the root key under different column names.

  The config also names the stream and current tables. Only infrastructure settings (metadata topic, catalog schema, checkpoint root, timestamp format, dropped-events settings) come from shared defaults. Nothing is read from Oracle, and the Spark job never connects to it.
- A **hand-written transform** handles anything beyond rename and cast. A job may name a Scala class implementing `StreamTransform`. It runs on the shaped stage 1 records in `foreachBatch`, and its output must match the declared stream schema. A column declared without a source is filled in by the transform.
- A **DDL command** (`cdc-model ddl`) writes the Delta CREATE TABLE scripts for a job's stream and current tables from its config alone.
- The stage 1 pipeline (`TransactionalCdcProcessingApp`) reads its topics, tracked tables and output shape from the job model instead of fixed code. The sequencer counts events per tracked table and checks completeness against the marker count for every tracked table. Output records are shaped by config-derived Spark projections instead of bean classes and Jackson.
- The stage 2 merge (`ScdType1MergeApp`) is generated from the same model:
  - the root's columns are flattened, with its before image as one struct column
  - a 1:1 child becomes a struct, kept at its highest version
  - a 1:N child becomes an array, deduplicated by the child's declared key and version
  - transaction context keeps its CSN guard
- The existing orders job is expressed as `jobs/orders.conf`, which **reproduces today's `order_stream` and `orders_current` schemas exactly**: DOUBLE numbers, STRING dates and timestamps, the `lineItems` array name, and the `orderBefore` column. Existing tables keep working.
- **BREAKING**: `TransactionSummary`'s three fixed per-table counters become per-table maps, so its state encoding changes and the job needs a new checkpoint location. The tables themselves are unaffected.
- **BREAKING (additive schema)**: `dropped_events` gains a `jobName` column and a per-table `bufferedCounts` array. `bufferedOrders`, `bufferedDetails` and `bufferedLineItems` stay in the table but are no longer written.
- The `Order*` bean classes and `OrderJoiner` are removed, along with the order-specific code in `TransactionSummary`, `TransactionState`, `TransactionCompletion` and `ScdType1Merger`.

## Capabilities

### New Capabilities
- `cdc-model-config`: how a job's data model is defined. Covers the explicit entity and column mapping, keys, cardinality and versions, output tables, the root-plus-children model shape, and validation at startup.
- `stream-record-shaping`: how a complete transaction becomes stage 1 output records. Covers per-key grouping, one array per entity, typed columns cast from source values, before images, the handling of deletes and undeclared columns, values that fail to cast, and the hand-written transform.

### Modified Capabilities
- `transaction-buffering`: completion is checked against the job's tracked tables instead of the fixed ORDERS, ORDER_DETAILS and ORDER_LINE_ITEMS.
- `current-state-merge`: the SCD Type 1 merge applies to any job model. Entity keys and versions come from the model, and the column shapes follow the root, 1:1 and 1:N rules.
- `dropped-event-audit`: `UNKNOWN_TABLE` is relative to the job's tracked tables. Purged-buffer rows record counts per tracked table. Rows carry the job name.
- `stream-startup`: Kafka sources are the job's metadata topic plus one topic per entity, not four fixed topics. Starting offsets are keyed by entity.
- `catalog-tables`: each job's stream and current tables are named by its config and created from DDL written from the config. A job checks at startup that its tables match the model and refuses to start on a mismatch.

## Impact

- **Code:**
  - `TransactionalCdcProcessingApp`, `CdcJsonParsing` (only the event-type tag changes), `TransactionSummary`, `TransactionState`, `TransactionCompletion`, `TransactionSequencer`, `TransactionOrderingProcessor` and `SequencerOutput` are changed.
  - `CdcOutputSink`, `DroppedEvent`, `ScdType1MergeApp`, `ScdType1Merger`, `KafkaSourceOptions` and `AppConfig` are changed.
  - `OrderJoiner` and `model/order/*` are removed.
  - New code: a model package for the config, schema derivation and DDL, the `StreamTransform` trait, and a `cdc-model ddl` entry point.
- **Config:**
  - New `jobs/` directory.
  - `transactional-cdc-processing.conf` and `scd-type1-merge.conf` are replaced by job configs.
  - Run commands take `-Dconfig.file=jobs/<job>.conf`.
- **Dependencies:** none added.
- **Data:**
  - New checkpoint locations for both stages.
  - `dropped_events` gains columns.
  - `order_stream` and `orders_current` are unchanged.
- **Tests:**
  - Tests that build `Order*` beans or order-specific counters are rewritten against the generic model.
  - `ScdType1MergerTest`, the integration test and the e2e `CorrectnessOrderingIT` must pass unchanged against the orders job model, as the compatibility check.
- **Docs:** the CLAUDE.md architecture and run sections, and `db/README.md`.
