# Adding a New Job

One jar runs any number of CDC jobs. A job is one data model, a root table with zero or more child
tables, described by one config file, `jobs/<job>.conf`. The same file drives both stages:

- `TransactionalCdcProcessingApp` (stage 1) reads one Kafka topic per table plus the shared
  transaction metadata topic, and writes `<stream table>`, one row per transaction and root key.
- `ScdType1MergeApp` (stage 2) merges the stream table into `<current table>`, one row per root key.

The config states the job's input and output explicitly: which tables and topics it reads, which
source column becomes which output column and type, its keys, and the tables it writes. Nothing
is read from Oracle. Anything a rename and a cast can't express is written by hand in Scala
([Hand-written transforms](#hand-written-transforms)).

`jobs/orders.conf` is the complete example for a root with two children. This page builds a
single-table job, then adds a child.

With Claude Code, `/create-cdc-job` (`.claude/skills/create-cdc-job/`) asks for the tables,
topics, keys, columns and output tables, then writes the config, checks it and writes its DDL.

## 1. Write the job config

`jobs/customers.conf`:

```hocon
job {
  name = customers

  model.entities = [
    {
      name = customers               # output name: the stream array, and stage 2 references
      role = root                    # exactly one entity is the root
      table = CUSTOMERS              # as the transaction marker names it (no schema)
      topic = "dev.appuser.customers.json"
      key = CUSTOMER_ID              # one column, or a list for a compound key
      version = VERSION              # optional; without it stage 2 uses commit order
      columns = [
        { source = CUSTOMER_ID, name = customerId, type = bigint }
        { source = VERSION,     name = version,    type = bigint }
        { source = NAME,        name = name,       type = string }
        { source = CREATED_TS,  name = createdTs,  type = timestamp }
        { source = BALANCE,     name = balance,    type = "decimal(20,4)" }
      ]
    }
  ]

  output {
    stream.table = "mymanaged.argus1.customers_stream"
    current.table = "mymanaged.argus1.customers_current"
  }
}
```

Every column states `source`, `name` and `type`. Types are Spark SQL types. GoldenGate sends every
value as a string, so:
- `string` copies the value as it is
- `timestamp` and `date` parse it with `job.source.timestamp-format`
  (default `yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]`)
- any other type is a cast

A value that doesn't convert becomes null and is counted in the batch's log. Source columns that
the config doesn't declare are left out, with one warning per run naming them.

Shared settings come from `cdc-defaults.conf` in the jar, and any of them can be set in the job
config or with `-D`: Kafka servers, metadata topic, checkpoint root, dropped events, and ordering.
Each stage's checkpoint defaults to `/tmp/spark-checkpoints/<job>/stream` and `.../current`.

## 2. Check it

```bash
scripts/cdc-model.sh check jobs/customers.conf
```

This lists every problem at once, for example a type that isn't a Spark type, a key naming an
undeclared column, or two columns with one name. Both apps run the same validation at startup.

## 3. Write and seed the tables

```bash
scripts/cdc-model.sh ddl jobs/customers.conf     # writes db/customers_stream.deltalake.sql
                                                 #    and db/customers_current.deltalake.sql
```

Then run `SeedJob`, which runs every `db/*.deltalake.sql`. Note that the orders scripts drop and
recreate `order_stream` and `orders_current`, so on a deployment with data, run only the new files
(for example with `spark-sql -f`, after replacing `{{CATALOG_SCHEMA}}`). The apps never create or
alter their tables, and each refuses to start when its table doesn't match the job config. When
the config changes, write the DDL again and recreate (or `ALTER`) the table.

## 4. GoldenGate

- The data replicat (`kc`) must publish the table to the topic the config names.
- Markers need no change: `rep3` publishes one marker per source transaction with counts for every
  table in it. Every job reads the shared metadata topic and counts only its own tables, so a
  transaction touching `CUSTOMERS` and `ORDERS` completes in each job once that job's events have
  arrived.

## 5. Run both stages

```bash
spark-submit \
  --class com.technext.demos.txbuffering.TransactionalCdcProcessingApp \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_4.1_2.13:4.2.0,io.unitycatalog:unitycatalog-spark_4.1_2.13:0.5.0,com.typesafe:config:1.4.3,org.apache.logging.log4j:log4j-api-scala_2.13:13.1.0 \
  --driver-java-options "-Dconfig.file=jobs/customers.conf" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.mymanaged=io.unitycatalog.spark.UCSingleCatalog \
  --conf spark.sql.catalog.mymanaged.uri=http://localhost:7071 \
  --conf spark.sql.catalog.mymanaged.auth.type=static \
  --conf spark.sql.catalog.mymanaged.auth.token=dummy \
  --conf spark.sql.defaultCatalog=mymanaged \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar
```

Then run the same with `--class com.technext.demos.txbuffering.ScdType1MergeApp`. In an IDE, set
the VM option `-Dconfig.file=jobs/customers.conf`; without it, both apps run the bundled orders job.

On a new checkpoint each source starts at `kafka.starting-offsets` (default `earliest`), or at a
per-source position keyed by entity name: `job.kafka.starting-offsets.customers`, or
`...transaction-metadata` for the markers. For the cut point at go-live, see "Go-live" in
[gg-configs/README.md](../gg-configs/README.md).

## Adding a child

A child is a table that carries the root key. Add it to `model.entities`:

```hocon
{
  name = addresses
  table = CUSTOMER_ADDRESSES
  topic = "dev.appuser.customer_addresses.json"
  cardinality = many                 # one: a struct in the current table; many: an array
  root-key = CUST_ID                 # the child's column holding the root key
  key = ADDRESS_ID                   # many only: identifies the child's elements
  version = VERSION
  columns = [
    { source = ADDRESS_ID, name = addressId,  type = bigint }
    { source = CUST_ID,    name = customerId, type = bigint }
    { source = VERSION,    name = version,    type = bigint }
    { source = CITY,       name = city,       type = string }
  ]
}
```

The child's column may have any name (`CUST_ID` here, `CUSTOMER_ID` in the root). Values are
matched after conversion to the root key's declared type, so a child that declares the column as
`string` still groups with a `bigint` root key.

**Compound keys.** A root key with several columns is a list, and each child maps every one of
them to its own column:

```hocon
# root
key = [REGION, ORDER_NO]
# child
root-key { REGION = SHIP_REGION, ORDER_NO = ORD_REF }
key = [SHIP_REGION, ORD_REF, SHIPMENT_SEQ]          # a many child's own key may be compound too
```

The stream and current tables then get one top-level column per root key column (for example
`region` and `orderNo`), and stage 2 matches rows on all of them.

**Check the mapping on the first run.** Validation can only check that the mapped columns are
declared, not that they hold the root key. A wrong mapping shows up as children in rows whose root
never arrives. After the first transactions, this must return no rows:

```sql
-- child-only rows whose root key also has a root row
SELECT c.region, c.orderNo
FROM mymanaged.argus1.regional_current c
WHERE c.version IS NULL                -- no root attributes: child-only placeholder
  AND EXISTS (SELECT 1 FROM mymanaged.argus1.regional_stream s
              WHERE size(s.orders) > 0 AND s.region = c.region AND s.orderNo = c.orderNo);
```

## Hand-written transforms

For logic beyond rename and cast, declare the output column without a `source` (a *derived*
column) and name a transform class:

```hocon
columns = [
  ...
  { name = balanceWithInterest, type = "decimal(20,4)" }    # derived
]
...
job.output.transform = com.example.CustomersTransform
```

```scala
package com.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, transform}
import com.technext.demos.txbuffering.model.spec.{JobModel, StreamTransform}

class CustomersTransform extends StreamTransform {
  override def apply(records: DataFrame, model: JobModel): DataFrame =
    records.withColumn("customers", transform(col("customers"),
      c => c.withField("balanceWithInterest", (c.getField("balance") * 1.02).cast("decimal(20,4)"))))
}
```

The transform gets each micro-batch's stream records, in the stream table's schema, with derived
columns as typed nulls. It must return exactly that schema; otherwise the batch fails before
anything is written, naming the columns that differ. It needs a no-argument constructor, and
startup fails if the class can't be loaded. Stage 2 has no transform: it merges whatever stage 1
wrote, derived columns included.

## Settings reference

| Setting | Required | Meaning |
|---|---|---|
| `job.name` | yes | Job name; default checkpoints, DDL file names, `dropped_events.jobName` |
| `job.model.entities[].name` | yes | Output name of the entity (stream array, current column) |
| `...role = root` | one entity | Marks the root |
| `...table` | yes | Source table, as the marker names it (no schema) |
| `...topic` | yes | Kafka topic of the table |
| `...columns[]` | yes | `{ source, name, type }`; no `source` = derived column |
| `...key` | root, `many` children | Column or list of columns identifying rows |
| `...root-key` | children | Child column holding the root key, or a map from each root key column |
| `...cardinality` | children | `one` or `many` |
| `...version` | no | Version column; without it, the change with the highest CSN wins (stored as `changeCsn`) |
| `...before` | no | `false` drops the entity's before image (default `true`) |
| `job.output.stream.table` | yes | Stage 1 table |
| `job.output.current.table` | yes | Stage 2 table |
| `job.output.current.root-before` | no | Name of the root's before-image column (default `<root>Before`) |
| `job.output.stream.checkpoint`, `...current.checkpoint` | no | Default `<checkpoint-root>/<job>/stream`, `.../current` |
| `job.output.transform` | with derived columns | `StreamTransform` class |
| `job.kafka.starting-offsets.<entity>` | no | Starting position on a new checkpoint |

## Limits

- A model is a root and its direct children. Grandchildren, and children that don't carry every
  root key column, aren't supported.
- Column lists are maintained by hand. There's no check against Oracle: a column added in Oracle
  is left out (with a warning) until the config declares it.
- Every job reads the whole metadata topic, so each marker costs every job one queue entry.
- A table belongs to one job. A source transaction that touches two jobs' tables is emitted by each
  job independently, with no atomicity across jobs.
