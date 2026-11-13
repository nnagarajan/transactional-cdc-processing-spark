# Design

## Context

The order-specific parts of today's code (see proposal.md, Why) sit at two ends of a generic core:

```
 Kafka x4 --> CdcJsonParsing --> union --> TransactionSequencer --> OrderJoiner --> CdcOutputSink
 [fixed       [generic: GG        [generic   [TransactionState:    [beans +        [order_stream]
  topic list]  envelope ->          queue,     3 fixed lists,       Jackson]
               map<string,string>]  cursor]    3 fixed counts]
```

Constraints that shape the approach:

- **Encoders.** `transformWithState` state and output use `Encoders.bean`, which is fixed at compile time. A schema known only from config cannot be encoded there as typed beans.
- **Single task.** The sequencer runs as one task with one global key. Anything moved out of it gets cheaper, and anything added to it slows the whole pipeline.
- **Source values.** GoldenGate values arrive as strings in `map<string,string>`, because the replicat sets `schemas.enable=false`. Messages carry no type information.
- **Compatibility.** The orders job must reproduce `order_stream` and `orders_current` exactly (spec `cdc-model-config`, "The shipped orders job reproduces the existing tables").

## Goals / Non-Goals

**Goals:**
- The sequencer and its state contain nothing specific to a model. A new model needs no code change.
- All schema-dependent work happens in plain DataFrame code inside `foreachBatch`, built from one in-memory model.
- Stage 1 schemas, stage 2 schemas, generated DDL and startup checks all come from the same derivation code, so they cannot drift apart.

**Non-Goals:**
- Grandchildren, or children that don't carry the root key. Composite root keys.
- CDC formats other than GoldenGate JSON. The envelope parser is unchanged and isn't made pluggable yet.
- Automatic Delta schema evolution. Jobs never `ALTER` or create their stream or current tables.
- Migrating sequencer state across the layout change. As in previous state changes, upgrades start a new checkpoint (Migration Plan).
- Cross-job atomicity for a source transaction that touches tables of two jobs.

## Decisions

### D1. The model is plain Scala data, derived in layers

A new package `com.technext.demos.txbuffering.model.spec` holds immutable case classes. It has no dependency on a Spark session; it only uses `StructType`/`DataType`.

```
 defaults.conf --+
 jobs/X.conf ----+--> JobConfig ---------+
                                         +--> ModelResolver --> JobModel --+--> streamSchema: StructType
 models/X.schema.conf --> SourceSchema --+    (conventions,               +--> currentSchema: StructType
                      (generated: Oracle      overrides,                  +--> shaping exprs (stage 1)
                       columns/types/keys)    type mapping,               +--> merge exprs (stage 2)
                                              validation)                 +--> DDL text
```

`JobModel` holds, for each entity:
- name and source table
- topic
- role and cardinality
- primary key and root-key column
- version column, or none
- whether it has a before image
- ordered `ModelColumn(sourceName, outputName, outputType)`

Every other consumer reads only `JobModel`. `ModelResolver` applies the precedence column override > family override > default mapping, and runs every startup validation from spec `cdc-model-config`. Failures are collected and reported together, rather than stopping at the first one.

*Alternative:* resolve conventions lazily in each consumer. Rejected, because the stream DDL, the stream shaper and the startup check would each re-implement naming rules and could disagree.

### D2. Config files and loading

- `src/main/resources/cdc-defaults.conf` holds the shared defaults: topic template, naming, type mapping, timestamp format, catalog schema, checkpoint root, metadata topic, and dropped-events settings. It is included by `reference.conf`.
- `jobs/<job>.conf` is in the repository, outside the jar, and is passed with `-Dconfig.file`.
- `models/<job>.schema.conf` is generated. Its location is `job.model.schema-file`, which defaults to `models/<job.name>.schema.conf` relative to the working directory. A `classpath:` prefix loads it from the jar.
- The orders job conf and schema are also bundled as classpath resources and used when `-Dconfig.file` isn't given, so the existing run command keeps working.
- `AppConfig.load` keeps system properties as the top layer (`-Djob...` overrides still work), then the job file, then defaults.

Both apps take the same job conf. Stage 2 settings live under `job.current`.

Example orders job (overrides only):

```hocon
job.name = orders
model {
  root     = APPUSER.ORDERS
  children = [APPUSER.ORDER_DETAILS, APPUSER.ORDER_LINE_ITEMS]
  types { NUMBER = double, DATE = string, TIMESTAMP = string }   # legacy shape
  entities.ORDER_LINE_ITEMS.name = lineItems
}
output.stream.table   = argus1.order_stream     # default would be orders_stream
current.table         = argus1.orders_current
current.root-before   = orderBefore
```

### D3. Sequencer state becomes per-table maps; output becomes a generic transaction

`TransactionState` becomes:
- `events: java.util.Map[String, java.util.List[DataChangeEvent]]`, keyed by unqualified table name
- `expected: java.util.Map[String, Integer]`

Both are bean-encodable. The tracked tables are passed to `TransactionSequencer` at construction. `setMetadata` records expected counts for tracked tables only, and `addEvent` returns `UnknownTable` for any other table. `isComplete`, `progress` and the purged-buffer counts iterate over the tracked set.

The sequencer no longer joins anything. For each emitted transaction it outputs one `SequencerOutput` of kind `TRANSACTION`, carrying:
- `xid`, `csn`
- `processedTs`, an `Instant.now().toString` taken at emission, as `OrderJoiner` did
- `events`, the buffered events in table order and then buffered order

`SequencerOutput`'s encoder therefore stays fixed for every model.

*Alternative:* shape inside the operator with `Encoders.row(streamSchema)`. Rejected:
- it moves per-record casting and grouping into the single-task operator
- it couples the operator's output schema, and so its checkpoint, to the model
- `Encoders.row` output with a `transformWithState` processor written in Scala is awkward to test without Spark

### D4. Stage 1 shaping is a generated DataFrame projection in `foreachBatch`

`StreamShaper(model).shape(transactions: DataFrame): DataFrame` takes the operator's `TRANSACTION` rows and runs these steps:

1. `posexplode` the events, keeping the index for ordering. Keep events whose `after` is non-null, so deletes produce no element. Add `table`.
2. For each entity, build `named_struct(<cols>, 'before', <cols over before>)`. Each column is `try_cast(after['SRC'] AS type)`. Timestamp and date columns use `try_to_timestamp(after['SRC'], fmt)`. `before` is `CASE WHEN before IS NULL THEN NULL ELSE … END`. With the legacy `string` overrides, the expression is just `after['SRC']`.
3. Compute the key as `try_cast(after['<root key src>'] AS keyType)`.
4. Group by `(xid, csn, processedTs, key)`. For each entity, collect `struct(idx, elem)` filtered to that table, then `array_sort` and `transform(…, x -> x.elem)`. This keeps the buffered order, and an entity with no events gives an empty array.
5. Select the columns in `streamSchema` order.

Cast failures are counted in the same pass: `SRC is not null AND try_cast(...) is null`, summed per table and column, and logged once per batch. Because there are no extra jobs, the batch DataFrame is cached for the duration of `writeBatch`. Unmapped source columns are detected with `map_keys(after)` minus the model's columns, collected per distinct table, and warned about once per run, tracked in a driver-side set.

*Alternative:* keep Jackson with dynamically generated classes. Rejected because of runtime codegen and because it would stay inside the operator.

*Compatibility note:* `OrderJoiner` grouped by the converted `Double` key with Jackson coercion. `try_cast(string AS double)` yields the same values for well-formed numbers, and the golden test (task 1.1) guards this.

### D5. Stage 2 merge is generated from the same model

`ScdType1Merger` becomes `CurrentStateMerger(model)`. It keeps today's algorithm, which already follows the root / 1:1 / 1:N pattern, and replaces the literals:

| Today | Generated from |
|---|---|
| `"orders"` array, `orderId` key | root entity name, root key output name |
| 13-entry `updateExpr` map | root columns + root-before column |
| `orderDetails` struct merge | each 1:1 child |
| `lineItems` array dedup by `lineItemId` | each 1:N child, by its primary key |
| `.version` comparisons | `orderingExpr(entity)`: `version`, or `CAST(changeCsn AS DECIMAL(38,0))` when versionless |

For a versionless entity, `transformSourceBatch` first adds `changeCsn = row.csn` to each of its elements, or at top level for the root, before deduplication. That matches the spec's `changeCsn` field.

The automatic `saveAsTable(overwrite)` that today creates a missing target table is removed, per spec `catalog-tables`.

### D6. Generator: same codebase, separate entry point, Oracle driver `provided`

`ModelGenerator` (`gen` / `check`) lives in the main module so it shares `ModelResolver`, the schema derivation and the DDL writer. That sharing is what lets `check` and the startup check agree. `com.oracle.database.jdbc:ojdbc11` is added with `provided` scope: it is on the compile classpath, but not in the jar and not on the Spark classpath. The generator runs with:

```
./mvnw -Pmodel-gen exec:java -Dexec.args="gen jobs/orders.conf"
```

The `model-gen` profile configures `exec-maven-plugin` with `classpathScope=compile`.

Connection settings are `generator.oracle.{url,user}`. The password comes from the env var `ORACLE_PASSWORD` and is never stored in config.

Dictionary queries run against `ALL_TAB_COLUMNS` (ordered by `COLUMN_ID`), `ALL_CONSTRAINTS` (types `P` and `R`) and `ALL_CONS_COLUMNS`, filtered by owner and table. Two pieces are derived from them:
- **Root key:** the child's `R` constraint columns that reference the root's primary key.
- **Cardinality:** a child whose primary-key column set equals its root-key column set is one-to-one; any other child is one-to-many.

Outputs are:
- `models/<job>.schema.conf`, written with deterministic ordering so diffs stay clean
- `db/<job>_stream.deltalake.sql` and `db/<job>_current.deltalake.sql`

For the orders job, the generated DDL replaces `db/create_table.deltalake.sql` and `db/create_scd1_table.deltalake.sql`. Their file names keep the existing seed order: the dropped-events table first, then the stream and current tables.

*Alternative:* a separate Maven project, like `gg-formatter/`. Rejected because it would need to duplicate or publish the derivation code.

*Alternative:* read the dictionary at job startup. Rejected in exploration: Oracle credentials would be needed in the job, and the schema could shift silently on restart.

### D7. Startup schema check

Before starting its query, each stage compares `spark.table(name).schema` with the derived schema. Names, types and nesting must match, with field order included and nullability and metadata ignored. On a mismatch it throws, listing the differing paths. This replaces the implicit table creation done by today's `saveAsTable` append and merger.

### D8. dropped_events

`DroppedEvent` gains:
- `jobName`
- `bufferedCounts: java.util.List[DataCollectionCount]`, which reuses the marker's per-table count struct

The legacy columns `bufferedOrders`, `bufferedDetails` and `bufferedLineItems` stay in the bean and table, but they stay null. The migration script is `db/alter_dropped_events_add_job.sql`, which runs `ALTER TABLE ADD COLUMNS`. The base DDL is updated for new installs. `checkDroppedEventsTable` also checks for the new columns, naming the script if they are missing.

### D9. Kafka sources keyed by entity

`KafkaSourceOptions.forTopic` takes the entity name, or `transaction-metadata`, as its key. Unknown keys under `job.kafka.starting-offsets` fail validation. The orders job's keys change from `order-details` / `order-line-items` to `orderDetails` / `lineItems`. The go-live runbook in `gg-configs/README.md` is updated to match.

## Risks / Trade-offs

- [The golden output differs subtly, for example in key typing, array order or empty-array versus null] → Capture golden `order_stream` and `orders_current` output from the current code before refactoring (task 1.1), and assert equality afterwards. The integration test and `ScdType1MergerTest` must pass unchanged in substance.
- [An extra shuffle per batch for the group-by in `foreachBatch`] → Batches are bounded by `max-offsets-per-trigger`, and the work moves out of the single-task operator, so throughput should improve overall. Check with the existing performance estimate after implementing.
- [A bean encoder over `Map[String, List[DataChangeEvent]]` changes RocksDB value size] → The state holds the same events, plus table-name keys. The test fixtures exercise large transactions.
- [A shared metadata topic means every job queues every marker] → Markers for unrelated transactions complete immediately and are removed. Their cost is one queue entry per marker. This is documented in Known Limitations.
- [The schema file goes stale when Oracle DDL changes] → `cdc-model check` runs in CI where Oracle is reachable. At runtime, unmapped columns warn and missing columns become null, so the job keeps running.
- [`try_cast` hides malformed values] → Cast failures are counted per column every batch, and the spec requires the log. A metric can be added later.
- [Upgrading replays history into `order_stream`] → This follows the existing migration precedent: truncate or accept duplicate history rows. `orders_current` is unaffected because its merge is version-aware.

## Migration Plan

1. Build and seed: run the generator for `jobs/orders.conf`. Check that `cdc-model check` passes and that the generated DDL equals today's schemas.
2. Stop both apps. Run `db/alter_dropped_events_add_job.sql`.
3. Deploy the new jar. Start stage 1 with `-Dconfig.file=jobs/orders.conf` on a **new** stream checkpoint, starting from `earliest` as in `gg-configs/README.md`, "Migrating an existing deployment". Optionally truncate `order_stream` first.
4. Start stage 2. Use a new checkpoint if `order_stream` was truncated; otherwise keep the old one by overriding `current.checkpoint` to its path.
5. Rollback: redeploy the previous jar with its original checkpoints. The added `dropped_events` columns are ignored by the old code.

## Open Questions

- Where the generator runs in CI (which runner can reach Oracle) can be decided when CI is set up. Locally it runs against the Docker Oracle used for `db/oracle-ddl.sql`.
- Whether to expose cast-failure counts as a Spark metric or query progress metric, in addition to the log, can be added later without changing the specs.
