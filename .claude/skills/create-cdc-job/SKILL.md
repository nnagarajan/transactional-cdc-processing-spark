---
name: create-cdc-job
description: Create a new CDC job config (jobs/<job>.conf) for this pipeline by interviewing the user about the Oracle source tables, Kafka topics, keys, column mapping and Delta output tables, then writing, validating and (optionally) generating DDL for it. Use when the user wants to add a job, onboard a new table or set of tables, write a <job>.conf, or says "create a job config" or "/create-cdc-job".
---

# Create a CDC job config

Interview the user, then write `jobs/<job>.conf`, validate it with `scripts/cdc-model.sh check`,
and write its DDL with `scripts/cdc-model.sh ddl`. The config is the whole data model: nothing is
read from Oracle at runtime, so every column, type and key the job needs must be in the file.

Read these first if you haven't in this session. They are the source of truth and win over this
skill if they disagree:
- `jobs/orders.conf`: the complete, working example (root + 1:1 child + 1:N child)
- `docs/ADDING-A-JOB.md`: user-facing guide and settings reference
- `src/main/resources/cdc-defaults.conf`: shared defaults (Kafka servers, metadata topic,
  checkpoint root, dropped events, ordering); only override what differs
- `src/main/scala/com/technext/demos/txbuffering/model/spec/ModelResolver.scala`: every
  validation rule

## The model, in brief

- A **job** is one **root** table plus zero or more **direct children**. No grandchildren. Every
  child must carry every root key column (it may name them differently).
- Each entity has: `name` (output name, camelCase), `table` (Oracle table name **without schema**,
  as the transaction marker names it), `topic` (Kafka topic GoldenGate publishes it to), `columns`.
- Root: `role = root`, `key` (one column or a list = compound key).
- Child: `cardinality = one` (a struct in the current table) or `many` (an array merged per
  element key); `root-key` (the child's column holding the root key, or a map
  `{ ROOT_COL = CHILD_COL, ... }` for a compound or differently named key); `key` required for
  `many` (the child's own row identity; may be compound).
- Optional per entity: `version` (a column that increases on every change; without it, the change
  with the highest CSN wins and a `changeCsn` field is added to the current table), `before = false`
  (drop the before image).
- Columns: `{ source = ORACLE_COL, name = outputName, type = <Spark SQL type> }`. `type` is
  required. A column with no `source` is **derived** and needs `job.output.transform` (a hand-written
  `StreamTransform` class). Keys, `version` and `root-key` refer to **source** names, and each must
  be the source of a declared column of that entity.
- Output: `job.output.stream.table` (stage 1, SCD2-style one row per transaction and root key) and
  `job.output.current.table` (stage 2, one row per root key). Both fully qualified
  (`catalog.schema.table`).

## Step 1: Gather the answers

Use `AskUserQuestion` for choices (up to 4 questions per call), and plain questions for free text
like names and column lists. Don't ask what you can derive; propose a default and let the user
correct it. Ask in this order, one round per group:

**A. Job and tables**
1. Job name (lowercase, used for `job.name`, the DDL file names, the checkpoint path and
   `dropped_events.jobName`). Check that `jobs/<job>.conf` doesn't already exist; if it does, ask
   whether to overwrite or pick another name.
2. The Oracle root table, and the child tables (name, and for each: 1:1 or 1:N with the root).
   Table names without schema (`ORDERS`, not `APPUSER.ORDERS`). Each Oracle table may appear in one
   job only; check `jobs/*.conf` for tables already used and warn.
3. The Oracle schema (only to propose topic names and look up columns; it is not written into the
   config).

**B. Kafka**
4. Topic per table. Propose the existing convention, `dev.<schema lowercase>.<table lowercase>.json`
   (e.g. `dev.appuser.orders.json`), and ask to confirm or edit.
5. Only if different from `cdc-defaults.conf`: bootstrap servers, metadata topic
   (default `dev.transaction_metadata_json`, shared by all jobs).

**C. Columns, per table.** Ask how to get the column list (offer these options):
- **Paste Oracle DDL or `DESC` output** (most reliable for types).
- **Read a sample message from the topic**: with the user's OK, run
  `kafka-console-consumer --bootstrap-server <servers> --topic <topic> --from-beginning --max-messages 1 --timeout-ms 15000`
  and take the keys of `after` (or `before`). This gives names but not Oracle types, so ask about
  types for anything that isn't obviously a string. Don't do this without asking.
- **Copy from an existing Databricks/UC table** (see D): derive names and types from it.
- **Type it in.**

Never connect to Oracle yourself unless the user explicitly asks and gives the connection to use.

Then, per table, confirm:
6. Which columns to include (default: all). Undeclared source columns are left out at runtime with
   a warning, so leaving some out is fine.
7. Output names. Default: camelCase of the source (`ORDER_ID` -> `orderId`,
   `SHIPPING_ADDRESS_ID` -> `shippingAddressId`). Names must be unique within an entity, and the
   root's names must not collide with `xid`, `csn`, `dwhProcessedTs`, the entity names, or the
   root-before column.
8. Types. Propose from the Oracle type, then confirm:

   | Oracle | Spark type to propose |
   |---|---|
   | `NUMBER(p,0)`, `p <= 18` / `INTEGER` | `bigint` (or `int` if `p <= 9`) |
   | `NUMBER(p,0)`, `p > 18` | `decimal(p,0)` |
   | `NUMBER(p,s)` | `decimal(p,s)` |
   | `NUMBER` (no precision) | `decimal(38,10)` or `double`; ask |
   | `FLOAT`, `BINARY_DOUBLE` | `double` |
   | `VARCHAR2`, `NVARCHAR2`, `CHAR`, `CLOB` | `string` |
   | `DATE`, `TIMESTAMP` | `timestamp` (parsed with `job.source.timestamp-format`), or `date`, or `string` to keep the GoldenGate text |

   Note for the user: the orders job uses `double` and `string` dates only to reproduce the old
   tables; new jobs should prefer `bigint`/`decimal`/`timestamp`. A value that fails to convert
   becomes null and is counted in the batch log.
9. Root key column(s) (`key`). Propose the Oracle primary key if known.
10. For each child: which of its columns holds each root key column (`root-key`). If the names are
    equal and the key is single, write `root-key = COL`; otherwise the map form. The child must
    declare those columns. Values are converted to the root key's type before grouping, so a
    `string` child column still joins a `bigint` root key.
11. For each `many` child: its own key (`key`), usually its primary key.
12. Version column per table (`version`), if the table has one that increases on every update
    (e.g. `VERSION`). Explain the fallback (CSN order, `changeCsn`) if not.
13. Before images: keep (default) or drop per table (`before = false`).
14. Derived columns (computed in Scala): any? If yes, name, type, and the transform class name
    (`job.output.transform`). Offer to scaffold the class (see Step 4).

**D. Output tables**
15. Catalog and schema (default from the existing jobs: `mymanaged.argus1`).
16. Stream and current table names. Propose `<job>_stream` and `<job>_current`.
17. Do the tables already exist? Ask with options: "No, generate DDL" (default) / "Yes, the config
    must match them".
    - If they exist, get their schemas: ask the user to paste `DESCRIBE TABLE` output, or, with
      their OK, query the UC REST API (local stack:
      `curl -s http://localhost:7071/api/2.1/unity-catalog/tables/<catalog.schema.table>` and read
      `columns[].name` / `type_text`; on Databricks: `databricks tables get <catalog.schema.table>`).
      Use them to set column names and types so the config reproduces the tables. The apps refuse
      to start if a table differs from the config in column names, types or nesting (nullability
      and comments are ignored).
18. Root before-image column name in the current table (`job.output.current.root-before`): default
    `<rootName>Before`; ask only if matching an existing table.
19. Checkpoints: default `/tmp/spark-checkpoints/<job>/stream` and `.../current`. Ask only whether
    the defaults are fine.

## Step 2: Show the plan, then write the file

Before writing, show the user a compact summary: entities (table, topic, cardinality, keys,
version), column count per entity, output tables, anything unusual (compound keys, renamed key
columns, derived columns, no version). Ask for confirmation.

Write `jobs/<job>.conf` in the style of `jobs/orders.conf`:
- A header comment: what the job is (root and children with cardinality), how to run it with
  `-Dconfig.file=jobs/<job>.conf`, and how to write its DDL.
- The root first, then children in the order the user listed them (this is the order of the arrays
  in the stream table).
- Column lists aligned in columns, one per line.
- Only override shared settings the user changed; leave defaults to `cdc-defaults.conf`. Keep the
  commented `checkpoint` lines as orders.conf does.
- Quote topic names and table names; quote types containing parentheses (`"decimal(20,4)"`).

Skeleton:

```hocon
job {
  name = <job>

  model.entities = [
    {
      name = <rootName>
      role = root
      table = <ROOT_TABLE>
      topic = "<topic>"
      key = <KEY_COL>                     # or [COL_A, COL_B]
      version = <VERSION_COL>             # omit if none
      columns = [
        { source = <KEY_COL>, name = <keyName>, type = bigint }
        ...
      ]
    }
    {
      name = <childName>
      table = <CHILD_TABLE>
      topic = "<topic>"
      cardinality = many                  # or one
      root-key = <CHILD_COL>              # or { ROOT_COL_A = CHILD_COL_A, ROOT_COL_B = CHILD_COL_B }
      key = <CHILD_PK>                    # many only
      version = <VERSION_COL>
      columns = [ ... ]
    }
  ]

  output {
    stream {
      table = "<catalog>.<schema>.<job>_stream"
      # checkpoint = "/tmp/spark-checkpoints/<job>/stream"     (default)
    }
    current {
      table = "<catalog>.<schema>.<job>_current"
      # checkpoint = "/tmp/spark-checkpoints/<job>/current"    (default)
    }
    # transform = com.example.<Job>Transform                   (only with derived columns)
  }
}
```

## Step 3: Validate and write DDL

1. Run `scripts/cdc-model.sh check jobs/<job>.conf`. It lists every problem at once. Fix each in
   the config (asking the user when the fix is a decision, not a typo) and re-run until it prints
   `Job <job> is valid`. If a transform class is named but not written yet, the check fails on it;
   that's expected until Step 4 is done.
2. If the tables **don't exist**: run `scripts/cdc-model.sh ddl jobs/<job>.conf`, which writes
   `db/<job>_stream.deltalake.sql` and `db/<job>_current.deltalake.sql`. Show the user the two
   CREATE TABLE statements.
3. If the tables **exist**: write the DDL to the scratchpad instead
   (`scripts/cdc-model.sh ddl jobs/<job>.conf <scratch dir>`) and compare its columns and types with
   the existing schemas from Step 1 D17. Report every difference and fix the config until there are
   none (ignore nullability and comments).

## Step 4: Optional transform scaffold

If the job has derived columns, offer to create the transform class under
`src/main/scala/<package path>/`, implementing
`com.technext.demos.txbuffering.model.spec.StreamTransform`:
`def apply(records: DataFrame, model: JobModel): DataFrame`. It receives stream records in the
stream table's schema with derived columns as typed nulls, and must return exactly that schema.
It needs a no-argument constructor. Use the example in `docs/ADDING-A-JOB.md`
("Hand-written transforms"). Leave the computation as a clearly marked TODO if the user hasn't
described it.

## Step 5: Hand over

Tell the user what was written and what's left, without doing any of it:
- **Create the tables** by running only the two new `db/<job>_*.deltalake.sql` files
  (`spark-sql -f` after replacing `{{CATALOG_SCHEMA}}`). Warn that `SeedJob` runs every
  `db/*.deltalake.sql`, including the orders scripts, which drop and recreate `order_stream` and
  `orders_current`.
- **GoldenGate**: the data replicat (`kc`) must publish each table to its topic. Markers need no
  change; every job reads the shared metadata topic and counts only its own tables.
- **Run** both apps with `-Dconfig.file=jobs/<job>.conf` (VM option in IntelliJ). Optional
  per-source starting offsets: `job.kafka.starting-offsets.<entity name>` and
  `...transaction-metadata`.
- **After the first transactions**, check that children land in rows with their root (the query in
  `docs/ADDING-A-JOB.md`, "Check the mapping on the first run"). Validation can't check that a
  `root-key` mapping holds the right values.

## Guardrails

- Don't run `SeedJob`, create or alter tables, start or stop the apps, or delete checkpoints.
- Don't connect to Oracle, Kafka or UC without the user's OK for that specific read.
- Don't overwrite an existing `jobs/*.conf` or `db/*.deltalake.sql` without asking.
- Don't commit.
- Credentials (Oracle, Kafka, UC tokens) never go into the config file.
