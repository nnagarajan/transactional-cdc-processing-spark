# Design

## Context

The order-specific parts of today's code (see proposal.md, Why) sit at two ends of a generic core:

```
 Kafka x4 --> CdcJsonParsing --> union --> TransactionSequencer --> OrderJoiner --> CdcOutputSink
 [fixed       [generic: GG        [generic   [TransactionSummary:   [beans +        [order_stream]
  topic list]  envelope ->          queue,     3 fixed counters;    Jackson]
               map<string,string>]  cursor,    TransactionState:
                                    per-event  3 fixed lists at
                                    state]     emission]
```

The sequencer already buffers per event (`txs`, `events`, `positions`, `queue` map states). Only the per-table counters in `TransactionSummary`, the per-table lists that `TransactionState` assembles at emission, and the completion rules in `TransactionCompletion` name the three order tables.

Constraints that shape the approach:

- **Encoders.** `transformWithState` state and output use `Encoders.bean`, which is fixed at compile time. A schema known only from config cannot be encoded there as typed beans.
- **Single task.** The sequencer runs as one task with one global key. Anything moved out of it gets cheaper, and anything added to it slows the whole pipeline.
- **Source values.** GoldenGate values arrive as strings in `map<string,string>`, because the replicat sets `schemas.enable=false`. Messages carry no type information.
- **Compatibility.** The orders job must reproduce `order_stream` and `orders_current` exactly (spec `cdc-model-config`, "The shipped orders job reproduces the existing tables").

## Goals / Non-Goals

**Goals:**
- The sequencer and its state contain nothing specific to a model. A new model that needs only renames and casts needs no code change.
- A job's input and output are stated explicitly in its config, so reading the config tells exactly what is read and what is written.
- All schema-dependent work happens in plain DataFrame code inside `foreachBatch`, built from one in-memory model.
- Stage 1 schemas, stage 2 schemas, written DDL and startup checks all come from the same derivation code, so they cannot drift apart.

**Non-Goals:**
- Reading the Oracle data dictionary, or any tooling that compares the config with Oracle.
- Expressing transformations in config beyond rename and cast. They are written by hand (D6).
- Grandchildren, or children that don't carry every root key column. Children joined to root columns other than the root key.
- CDC formats other than GoldenGate JSON. The envelope parser is unchanged and isn't made pluggable yet.
- Automatic Delta schema evolution. Jobs never `ALTER` or create their stream or current tables.
- Migrating sequencer state across the layout change. As in previous state changes, upgrades start a new checkpoint (Migration Plan).
- Cross-job atomicity for a source transaction that touches tables of two jobs.

## Decisions

### D1. The model is plain Scala data, built from the job config

A new package `com.technext.demos.txbuffering.model.spec` holds immutable case classes. It has no dependency on a Spark session; it only uses `StructType`/`DataType`.

```
 cdc-defaults.conf --+
                     +--> JobConfig --> ModelResolver --> JobModel --+--> streamSchema: StructType
 jobs/X.conf --------+                  (parse types,                +--> currentSchema: StructType
                                         checkpoint defaults,        +--> shaping exprs (stage 1)
                                         validation)                 +--> merge exprs (stage 2)
                                                                     +--> DDL text
```

`JobModel` holds, for each entity:
- name and source table
- topic
- role and cardinality
- for the root, its ordered key columns; for a child, its root-key mapping, an ordered list of `(root key column, child column)` pairs in root key order
- for a one-to-many child, its ordered own key columns
- version column, or none
- whether it has a before image
- ordered `ModelColumn(source: Option[String], outputName, outputType)`; `source = None` marks a derived column

Every other consumer reads only `JobModel`. `ModelResolver` parses each type with `DataType.fromDDL`, applies the checkpoint defaults, and runs every startup validation from spec `cdc-model-config`. Failures are collected and reported together, rather than stopping at the first one.

*Alternative:* derive columns, types, keys and cardinality from the Oracle data dictionary with a generator that writes a checked-in schema file. Rejected: it needs Oracle access and credentials in the build, a type-mapping table with family and column overrides, and drift tooling. An explicit config is longer, but it is the whole truth in one place, and the job warns about source columns it does not declare (spec `stream-record-shaping`).

### D2. Config files and loading

- `src/main/resources/cdc-defaults.conf` holds only infrastructure defaults: timestamp format, catalog schema, checkpoint root, metadata topic, and dropped-events settings. It is included by `reference.conf`.
- `jobs/<job>.conf` is in the repository, outside the jar, and is passed with `-Dconfig.file`.
- The orders job conf is also bundled as a classpath resource and used when `-Dconfig.file` isn't given, so the existing run command keeps working.
- `AppConfig.load` keeps system properties as the top layer (`-Djob...` overrides still work), then the job file, then defaults.

Both apps take the same job conf. Stage 2 settings live under `job.output.current`.

Every column states `source`, `name` and `type`; there is no default type. Keys and versions name source columns. A `key` is one column name or a list. A child's `root-key` is a map from each root key column to the child column holding it, or a single column name when the root key has one column. Orders job (abridged; the full file lists every column):

```hocon
job.name = orders
job.model.entities = [
  { name = orders, role = root, table = ORDERS, topic = "dev.appuser.orders.json"
    key = ORDER_ID, version = VERSION
    columns = [
      { source = ORDER_ID,     name = orderId,     type = double }
      { source = VERSION,      name = version,     type = double }
      { source = ORDER_DATE,   name = orderDate,   type = string }
      { source = TOTAL_AMOUNT, name = totalAmount, type = double }
      # ...
    ] }
  { name = orderDetails, table = ORDER_DETAILS, topic = "dev.appuser.order_details.json"
    cardinality = one, root-key = ORDER_ID, version = VERSION
    columns = [ ... ] }
  { name = lineItems, table = ORDER_LINE_ITEMS, topic = "dev.appuser.order_line_items.json"
    cardinality = many, root-key = ORDER_ID, key = LINE_ITEM_ID, version = VERSION
    columns = [ ... ] }
]
job.output {
  stream.table        = argus1.order_stream
  current.table       = argus1.orders_current
  current.root-before = orderBefore
  # transform = com.technext.demos.txbuffering.jobs.OrdersTransform   (optional, D6)
}
```

A compound root key, with a child that stores it under different names:

```hocon
{ name = orders, role = root, table = ORDERS, ..., key = [REGION, ORDER_NO], ... }
{ name = shipments, table = SHIPMENTS, ..., cardinality = many
  root-key { REGION = SHIP_REGION, ORDER_NO = ORD_REF }
  key = [SHIP_REGION, ORD_REF, SHIPMENT_SEQ]
  columns = [ { source = SHIP_REGION, name = shipRegion, type = string } ... ] }
```

### D3. Sequencer state counts per table; output becomes a generic transaction

The per-event state layout (`events`, `positions`, `queue`, cursor) stays; the summary map is renamed `transactions`. Only the model-specific parts change:
- `TransactionSummary` replaces `receivedOrders`, `receivedOrderDetails` and `receivedOrderLineItems` with `received: java.util.Map[String, Integer]`, keyed by unqualified table name. Expected counts come from the marker as today, restricted to tracked tables.
- `TransactionCompletion` compares expected and received counts over the tracked-table set.
- `TransactionState`, assembled once at emission, groups events in a `java.util.Map[String, java.util.List[DataChangeEvent]]`.

The tracked tables are passed to `TransactionSequencer` at construction. A marker's counts for untracked tables are ignored, and an event for an untracked table returns `UnknownTable`. `isComplete`, `progress` and the purged-buffer counts iterate over the tracked set. Because the summary's encoding changes, a new checkpoint is required.

The sequencer no longer joins anything. For each emitted transaction it outputs one `SequencerOutput` of kind `TRANSACTION`, carrying:
- `xid`, `csn`
- `processedTs`, an `Instant.now().toString` taken at emission, as `OrderJoiner` did
- `events`, the buffered events in table order and then buffered order

`SequencerOutput`'s encoder therefore stays fixed for every model. A transaction is emitted as a single row, so a very large transaction becomes one large row (see Risks).

*Alternative:* emit one row per event (`xid`, `csn`, `processedTs`, `idx`, `event`) and regroup in `foreachBatch`. Rejected: one row per transaction keeps each emitted transaction a single unit in the operator's output and keeps `SequencerOutput` unchanged in shape.

*Alternative:* shape inside the operator with `Encoders.row(streamSchema)`. Rejected:
- it moves per-record casting and grouping into the single-task operator
- it couples the operator's output schema, and so its checkpoint, to the model
- `Encoders.row` output with a `transformWithState` processor written in Scala is awkward to test without Spark

### D4. Stage 1 shaping is a generated DataFrame projection in `foreachBatch`

`StreamShaper(model).shape(transactions: DataFrame): DataFrame` takes the operator's `TRANSACTION` rows and runs these steps:

1. `posexplode` the events, keeping the index for ordering. Keep events whose `after` is non-null, so deletes produce no element. Add `table`.
2. For each entity, build `named_struct(<cols>, 'before', <cols over before>)`. A column of type `string` is `after['SRC']`. A `timestamp` or `date` column uses `try_to_timestamp(after['SRC'], fmt)`. Any other type is `try_cast(after['SRC'] AS type)`. A derived column is `CAST(NULL AS type)`. `before` is `CASE WHEN before IS NULL THEN NULL ELSE … END`.
3. Compute one key part per root key column `k`. For a root event it reads `after['k']`; for a child event it reads the child column mapped to `k`. Each part is converted to `k`'s declared type with the step 2 rules, so a child that declares the column with another type still groups with its root. A `CASE table WHEN …` expression selects the source column per table. The parts are named as the root's key output columns.
4. Group by `(xid, csn, processedTs, k1, …, kn)`. For each entity, collect `struct(idx, elem)` filtered to that table, then `array_sort` and `transform(…, x -> x.elem)`. This keeps the buffered order, and an entity with no events gives an empty array.
5. Select the columns in `streamSchema` order.
6. If the job names a transform, apply it (D6).

Cast failures are counted in the same pass: `SRC is not null AND try_cast(...) is null`, summed per table and column, and logged once per batch. Because there are no extra jobs, the batch DataFrame is cached for the duration of `writeBatch`. Undeclared source columns are detected with `map_keys(after)` minus the entity's declared sources, collected per distinct table, and warned about once per run, tracked in a driver-side set.

*Alternative:* keep Jackson with dynamically generated classes. Rejected because of runtime codegen and because it would stay inside the operator.

*Compatibility note:* `OrderJoiner` grouped by the converted `Double` key with Jackson coercion. `try_cast(string AS double)` yields the same values for well-formed numbers, and the golden test (task 1.1) guards this.

### D5. Stage 2 merge is generated from the same model

`ScdType1Merger` becomes `CurrentStateMerger(model)`. It keeps today's algorithm, which already follows the root / 1:1 / 1:N pattern and takes transaction context from each key's highest-CSN row, and replaces the literals:

| Today | Generated from |
|---|---|
| `"orders"` array, `orderId` key | root entity name, root key output columns |
| `target.orderId = source.orderId` merge condition | `AND` over every root key column |
| 13-entry `updateExpr` map | root columns + root-before column |
| `orderDetails` struct merge | each 1:1 child |
| `lineItems` array dedup by `lineItemId` | each 1:N child, by a struct of its declared key columns |
| `.version` comparisons | `orderingExpr(entity)`: `version`, or `CAST(changeCsn AS DECIMAL(38,0))` when versionless |

For a versionless entity, `transformSourceBatch` first adds `changeCsn = row.csn` to each of its elements, or at top level for the root, before deduplication. That matches the spec's `changeCsn` field. Derived columns are ordinary columns here: stage 2 merges whatever stage 1 wrote.

The automatic `saveAsTable(overwrite)` that today creates a missing target table is removed, per spec `catalog-tables`.

### D6. Hand-written transforms

Config covers rename and cast only. Anything else, such as derived values, parsing a composite source value, or normalizing codes, is Scala:

```scala
trait StreamTransform extends Serializable {
  def apply(records: DataFrame, model: JobModel): DataFrame
}
```

`job.output.transform` names the class, which needs a no-argument constructor. It is loaded once at startup, so a bad class name fails validation. In `foreachBatch` it runs after shaping (D4 step 6) on the batch's shaped records, before the write. Its output schema is compared with `streamSchema` using the D7 rules. A mismatch fails the batch before any write, so Delta's `txnVersion` idempotency is unaffected on retry. Derived columns, declared without `source`, arrive as typed nulls for the transform to fill in.

The transform runs on the stage 1 record shape (nested entity arrays). Changing a nested element uses `transform(array, x -> x.withField(...))`. Stage 2 has no transform hook: it merges the stage 1 columns as they are.

*Alternative:* per-column SQL expressions in config. Rejected for now. It turns the config into a second language that's hard to test, while a Scala class gets unit tests and an IDE. It can be added later as a built-in transform without changing the hook.

### D7. Startup schema check and DDL command

Before starting its query, each stage compares `spark.table(name).schema` with the derived schema. Names, types and nesting must match, with field order included and nullability and metadata ignored. On a mismatch it throws, listing the differing paths. This replaces the implicit table creation done by today's `saveAsTable` append and merger.

`CdcModelTool ddl jobs/<job>.conf` is a small entry point in the main module, run with `scripts/cdc-model.sh ddl jobs/<job>.conf` or `spark-submit --class com.technext.demos.txbuffering.CdcModelTool`. (The script uses `exec:exec` with the test classpath: `exec:java` conflicts with the pom's existing `exec:exec` arguments, and Spark's runtime dependencies are only on the test classpath because Spark is `provided`.) `CdcModelTool check` validates a job config without running it. It writes `db/<job>_stream.deltalake.sql` and `db/<job>_current.deltalake.sql` from the same `JobModel` the jobs derive, so seeded tables pass the startup check. It needs no Spark session and no Oracle access. For the orders job, its output replaces `db/create_table.deltalake.sql` and `db/create_scd1_table.deltalake.sql`, keeping the existing seed order: the dropped-events table first, then the stream and current tables.

### D8. dropped_events

`DroppedEvent` gains:
- `jobName`
- `bufferedCounts: java.util.List[DataCollectionCount]`, which reuses the marker's per-table count struct

The legacy columns `bufferedOrders`, `bufferedDetails` and `bufferedLineItems` stay in the bean and table, but they stay null. The migration script is `db/alter_dropped_events_add_job.sql`, which runs `ALTER TABLE ADD COLUMNS`. The base DDL is updated for new installs. `checkDroppedEventsTable` also checks for the new columns, naming the script if they are missing.

### D9. Kafka sources keyed by entity

`KafkaSourceOptions.forTopic` takes the entity name, or `transaction-metadata`, as its key. Unknown keys under `job.kafka.starting-offsets` fail validation. The orders job's keys change from `order-details` / `order-line-items` to `orderDetails` / `lineItems`. The go-live runbook in `gg-configs/README.md` is updated to match.

## Risks / Trade-offs

- [The golden output differs subtly, for example in key typing, array order or empty-array versus null] → Capture golden `order_stream` and `orders_current` output from the current code before refactoring (task 1.1), and assert equality afterwards. The integration test, `ScdType1MergerTest` and the e2e `CorrectnessOrderingIT` must pass unchanged in substance.
- [The config falls behind Oracle DDL, for example a new column is never declared] → Undeclared source columns are warned about once per run and table, and missing declared columns become null, so the job keeps running. There is no automated drift check against Oracle; this is the price of not probing it.
- [Explicit configs are long: the orders job lists about 30 columns] → It is written once per job, and every line is one source-to-output mapping. Startup validation catches typos in keys, types and duplicates.
- [A hand-written transform changes the output schema] → The per-batch schema comparison fails the batch before writing, naming the columns.
- [An extra shuffle per batch for the group-by in `foreachBatch`] → Batches are bounded by `max-offsets-per-trigger`, and the work moves out of the single-task operator, so throughput should improve overall. Check with the existing performance estimate and `PerformanceIT` after implementing.
- [A whole transaction is one `TRANSACTION` row, so a 55,000-event transaction is one row of tens of MB, built and passed in memory in one piece] → Accepted. Buffering stays linear because of the per-event state, and today's emission already assembles the whole transaction in memory once (CLAUDE.md, Known Limitations). Measure with `PerformanceIT`'s bulk phase after implementing; if it becomes a problem, switching to per-event rows touches only `SequencerOutput` and the first step of `StreamShaper`.
- [A shared metadata topic means every job queues every marker] → Markers for unrelated transactions complete immediately and are removed. Their cost is one queue entry per marker. This is documented in Known Limitations.
- [A key part is null or fails to convert, so events group under a null key part and the merge condition, which uses `=`, never matches that row] → Key conversion failures are counted with the other cast failures each batch. Source keys are normally `NOT NULL` primary and foreign keys. The orders job keeps today's behavior because its key is the same single column.
- [A child maps a root key column to the wrong child column] → The mapping is explicit and validated against declared columns, but values cannot be checked. Wrong mappings show as children in rows whose root never arrives. The "Adding a new job" doc tells you to check this on first run.
- [`try_cast` hides malformed values] → Cast failures are counted per column every batch, and the spec requires the log. A metric can be added later.
- [Upgrading replays history into `order_stream`] → This follows the existing migration precedent: truncate or accept duplicate history rows. `orders_current` is unaffected because its merge is version-aware.

## Migration Plan

1. Write the orders DDL with `scripts/cdc-model.sh ddl jobs/orders.conf`, and check that it equals today's schemas.
2. Stop both apps. Run `db/alter_dropped_events_add_job.sql`.
3. Deploy the new jar. Start stage 1 with `-Dconfig.file=jobs/orders.conf` on a **new** stream checkpoint, starting from `earliest` as in `gg-configs/README.md`, "Migrating an existing deployment". Optionally truncate `order_stream` first.
4. Start stage 2. Use a new checkpoint if `order_stream` was truncated; otherwise keep the old one by overriding `job.output.current.checkpoint` to its path.
5. Rollback: redeploy the previous jar with its original checkpoints. The added `dropped_events` columns are ignored by the old code.

## Open Questions

- Whether to expose cast-failure counts as a Spark metric or query progress metric, in addition to the log, can be added later without changing the specs.
