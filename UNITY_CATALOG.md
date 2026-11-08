# Unity Catalog Setup

How to install and run a local open-source Unity Catalog (UC) server and use it as the catalog for
this project's Delta tables (`order_stream`, `orders_current`). The tables are **UC managed**:
Unity Catalog chooses where their files live, so the create scripts have no `LOCATION`.

## What This Project Expects

| Item | Value | Configured in |
|---|---|---|
| UC server URL | `http://localhost:7071` | `src/main/resources/reference.conf` (`spark.unity-catalog.uri`) |
| Catalog | `mymanaged`, storage root `file:///opt/delta/warehouse/managed` | `reference.conf` (`spark.unity-catalog.catalog-name`) |
| Schema | `argus1`, storage root `file:///opt/delta/warehouse/managed/argus1` | `src/main/resources/seed-job.conf` (`job.catalog-schema = "mymanaged.argus1"`) |
| Tables | `argus1.order_stream`, `argus1.orders_current` (managed Delta tables) | `transactional-cdc-processing.conf`, `scd-type1-merge.conf` |
| Auth | static token `dummy` (server authorization is disabled) | `reference.conf` |
| Versions | Spark 4.1.1, Delta `delta-spark_4.1_2.13:4.2.0`, connector `unitycatalog-spark_4.1_2.13:0.5.0` | `pom.xml` |

```
 TransactionalCdcProcessingApp / ScdType1MergeApp / SeedJob
        |  Spark catalog "mymanaged" (UCSingleCatalog)
        v
 Unity Catalog server :7071  ----- metadata in etc/db/h2db (H2 file DB)
        |                          (every Delta commit to a managed table goes through the server)
        v  storage root of schema argus1
 /opt/delta/warehouse/managed/argus1/__unitystorage/schemas/<schema-id>/tables/<table-id>  (Delta files)
```

UC stores the metadata, and for managed tables it also coordinates every Delta commit. **The
server must be running whenever the apps write.** The Delta data files live under the schema's
storage root, in directories named by generated IDs.

## 1. Prerequisites

- JDK 17 on the `PATH` (`java -version`). Build UC with `JAVA_HOME` pointing at JDK 17 as well
  (see Troubleshooting).
- Spark 4.1.x to run the apps with `spark-submit` (for example `/opt/spark/spark-4.1.1`)
- `git`
- No separate sbt install: the UC repo ships `build/sbt`
- Ports **7071 and 7072** free. The server listens on the port you give it and runs its
  API one port higher.

## 2. Install (build from source)

```bash
cd ~/github
git clone https://github.com/unitycatalog/unitycatalog.git
cd unitycatalog
build/sbt clean package
```

The first build downloads dependencies and takes several minutes. It produces
`server/target/unitycatalog-server-*.jar` and `server/target/classpath`.

> **Already cloned?** Run `git pull`, then rebuild with `build/sbt clean package`.
> `bin/start-uc-server` reuses whatever server jar it finds in `server/target` and only builds
> when none exists, so an old build keeps running after you pull new source.

## 3. Start the Server

```bash
cd ~/github/unitycatalog
bin/start-uc-server -p 7071
```

- Use `-p` (or `--port`). A bare `7071` argument is ignored, and the server starts on the
  default port **8080**.
- The server prints the Unity Catalog banner when it's up. Leave this terminal running.
- Metadata is stored in `etc/db/h2db.mv.db`, so catalogs and tables survive a restart.
- Server settings are in `etc/conf/server.properties`. The defaults (`server.authorization=disable`,
  managed tables enabled, no cloud credentials) are what this project expects.

To run it in the background:

```bash
nohup bin/start-uc-server -p 7071 > /tmp/uc-server.log 2>&1 &
```

**Stop:** Ctrl+C in the server terminal, or `pkill -f io.unitycatalog.server.UnityCatalogServer`.

## 4. Verify with the CLI

The CLI defaults to `http://localhost:8080`, so always pass `--server`:

```bash
cd ~/github/unitycatalog
bin/uc --server http://localhost:7071 catalog list
```

A new server lists the built-in `unity` catalog.

## 5. Prepare the Storage Root

Managed table data goes under the storage roots set in the next step. The directory must exist
and be writable by the user that runs Spark:

```bash
sudo mkdir -p /opt/delta/warehouse/managed
sudo chown -R "$USER" /opt/delta/warehouse
```

## 6. Create the Catalog and Schema with Storage Roots

```bash
cd ~/github/unitycatalog
bin/uc --server http://localhost:7071 catalog create --name mymanaged \
    --storage_root file:///opt/delta/warehouse/managed --comment "Managed tables (storage root set)"
bin/uc --server http://localhost:7071 schema create --catalog mymanaged --name argus1 \
    --storage_root file:///opt/delta/warehouse/managed/argus1

bin/uc --server http://localhost:7071 catalog get --name mymanaged
bin/uc --server http://localhost:7071 schema get --full_name mymanaged.argus1
```

Both should show a `STORAGE_ROOT`. A storage root can't be added to an existing catalog or
schema later; to change it, delete the schema (and catalog) and create them again.

| Object | Storage root | Where Unity Catalog places data |
|---|---|---|
| Catalog `mymanaged` | `file:///opt/delta/warehouse/managed` | `.../managed/__unitystorage/catalogs/<catalog-id>/...` |
| Schema `mymanaged.argus1` | `file:///opt/delta/warehouse/managed/argus1` | `.../managed/argus1/__unitystorage/schemas/<schema-id>/tables/<table-id>` |

A schema's root takes precedence over the catalog's root. If a schema has no root, its tables go
under the catalog's root.

> Type the full command each time, or use a shell function such as
> `uc() { ~/github/unitycatalog/bin/uc --server http://localhost:7071 "$@"; }`. Don't store the
> command in a variable like `UC="bin/uc --server ..."`: zsh doesn't split `$UC` into words, so
> it fails with `no such file or directory`.

## 7. Create the Tables (SeedJob)

`SeedJob` runs every `db/*.deltalake.sql` script, replacing `{{CATALOG_SCHEMA}}` with
`mymanaged.argus1`. Each script drops and creates its table with no `LOCATION` and with
`TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')`, which the connector requires
for a managed table.

> **Rerunning SeedJob deletes the data.** Each script starts with `DROP TABLE IF EXISTS`, and
> dropping a managed table deletes its files. Also delete the apps' checkpoint directories
> (`/tmp/spark-checkpoints-ordered`, `/tmp/spark-checkpoints-scd4`) after a reseed.

From the project root (the scripts directory `db` is resolved relative to the current directory):

```bash
./mvnw -Dquick clean package
java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED \
     -cp "target/classes:$(./mvnw -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)" \
     com.technext.demos.txbuffering.SeedJob
```

Or run `SeedJob` from the IDE with the project root as the working directory.

When it isn't launched by `spark-submit`, `SparkSessionCreator` uses its **dev configuration**,
which wires in Unity Catalog from `reference.conf`. Check the result:

```bash
bin/uc --server http://localhost:7071 table list --catalog mymanaged --schema argus1
```

Expected: `order_stream` and `orders_current`, both `MANAGED`, located under
`/opt/delta/warehouse/managed/argus1/__unitystorage/...`.

The create scripts don't create views. The connector supports views only on Spark 4.2 and
later; on Spark 4.1 it fails with `Catalog ... does not support views`. The views are in
`db/views.sql` as session-scoped `TEMPORARY VIEW`s. `SeedJob` doesn't run that file; run it at
the start of a `spark-sql` or Zeppelin session.

## 8. Connecting Spark to Unity Catalog

### Dev mode (IDE or plain `java`): automatic

`SparkSessionCreator` sets these from `reference.conf`:

```
spark.sql.catalog.mymanaged                     = io.unitycatalog.spark.UCSingleCatalog
spark.sql.catalog.mymanaged.uri                 = http://localhost:7071
spark.sql.catalog.mymanaged.auth.type           = static
spark.sql.catalog.mymanaged.auth.token          = dummy
spark.sql.defaultCatalog                        = mymanaged
```

`credential-vending = false` is also passed, as `spark.sql.catalog.mymanaged.credentialVending`.
The connector doesn't read that option, so it has no effect. Local file tables need no
credentials.

### `spark-submit`: pass the settings yourself

On the `spark-submit` path, `SparkSessionCreator` calls `getOrCreate()` without the dev settings,
so the catalog has to be configured on the command line. Use Spark 4.1.x:

```bash
spark-submit \
  --class com.technext.demos.txbuffering.TransactionalCdcProcessingApp \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_4.1_2.13:4.2.0,io.unitycatalog:unitycatalog-spark_4.1_2.13:0.5.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.mymanaged=io.unitycatalog.spark.UCSingleCatalog \
  --conf spark.sql.catalog.mymanaged.uri=http://localhost:7071 \
  --conf spark.sql.catalog.mymanaged.auth.type=static \
  --conf spark.sql.catalog.mymanaged.auth.token=dummy \
  --conf spark.sql.defaultCatalog=mymanaged \
  target/transactional-cdc-processing-spark-1.0.0-SNAPSHOT.jar
```

Use the same `--packages` and `--conf` lines for `ScdType1MergeApp` and `SeedJob`.

### Querying with `spark-sql`

```bash
spark-sql \
  --packages io.delta:delta-spark_4.1_2.13:4.2.0,io.unitycatalog:unitycatalog-spark_4.1_2.13:0.5.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.mymanaged=io.unitycatalog.spark.UCSingleCatalog \
  --conf spark.sql.catalog.mymanaged.uri=http://localhost:7071 \
  --conf spark.sql.catalog.mymanaged.auth.type=static \
  --conf spark.sql.catalog.mymanaged.auth.token=dummy \
  --conf spark.sql.defaultCatalog=mymanaged \
  -e "SHOW TABLES IN mymanaged.argus1; SELECT count(*) FROM mymanaged.argus1.order_stream;"
```

## 9. Table Names in the Apps

The apps use schema-qualified names, resolved against the default catalog `mymanaged`. An
unqualified name such as `order_stream` would resolve to `mymanaged.default`, which doesn't exist.

| File | Key | Value |
|---|---|---|
| `src/main/resources/transactional-cdc-processing.conf` | `job.output.delta-table-name` | `"argus1.order_stream"` |
| `src/main/resources/scd-type1-merge.conf` | `job.source-table-name` | `"argus1.order_stream"` |
| `src/main/resources/scd-type1-merge.conf` | `job.target-table-name` | `"argus1.orders_current"` |

`mymanaged.argus1.<table>` also works.

## 10. Run Order

```
 1. bin/start-uc-server -p 7071                  (Unity Catalog; keep it running)
 2. bin/uc ... catalog/schema create             (once, with storage roots)
 3. SeedJob                                      (once; rerunning deletes the data)
 4. Kafka + GoldenGate replicats                 (see gg-configs/README.md)
 5. TransactionalCdcProcessingApp                (writes argus1.order_stream)
 6. ScdType1MergeApp                             (writes argus1.orders_current)
```

## Managed vs External Tables

| | Managed (this project) | External (with `LOCATION`) |
|---|---|---|
| Who picks the data path | Unity Catalog, under the schema's or catalog's storage root | You, with `LOCATION` in `CREATE TABLE` |
| How Delta commits | Through the Unity Catalog server (catalog-managed commits) | Writes `_delta_log` files directly |
| Delta version | Delta 4.2.0 or later | Any |
| Needs the UC server to write | Yes | No |
| Readable without the UC server | No | Yes, by path |
| `DROP TABLE` | Deletes the data as well | Removes only the metadata; files stay |

The apps read and write by table name in both cases. Only `CREATE TABLE` differs.

Requirements for a managed table:

1. **Storage root** on the catalog or the schema (section 6).
2. **Managed tables enabled on the server.** `server.managed-table.enabled` in
   `etc/conf/server.properties` is enabled by default in current source. Older releases shipped
   `server.managed-table.enabled=false`.
3. **`TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')`** on the `CREATE TABLE`. The
   connector rejects a managed `CREATE TABLE` without it. The older name
   `delta.feature.catalogOwned-preview` is also accepted. Don't add either to an external table.

**Tested on this machine** (UC server built from source, `mymanaged.argus1`):

| Spark / Delta / connector | Result |
|---|---|
| 4.0.0 / 4.0.0 / 0.4.0 (previous project versions) | `CREATE TABLE` without `LOCATION` fails: `DELTA_UNSUPPORTED_CATALOG_OWNED_TABLE_CREATION` |
| 4.1.1 / 4.2.0 / 0.5.0 (current project versions, as in Databricks Runtime 18) | `CREATE`, `INSERT`, `MERGE`, a streaming append (`toTable`, 1 s trigger) and a `foreachBatch` Delta MERGE all work. `SeedJob` creates both tables as `MANAGED` |
| 4.1.1 / 4.4.1 / 0.7.0 | `CREATE`, `INSERT`, `MERGE` work |

Not yet tested end to end: the full pipeline from Kafka through both apps (task 6.3 of the
`ordered-transaction-emission` change).

To go back to external tables, register a catalog without a storage root, replace each
`TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')` line in `db/*.deltalake.sql`
with `LOCATION '<path>/<table>'`, and point `reference.conf` and `seed-job.conf` at that catalog.

## Optional: Web UI

The UC repo includes a web UI in `ui/`. It needs Node.js and yarn:

```bash
cd ~/github/unitycatalog/ui
yarn install
yarn start        # http://localhost:3000
```

The UI expects the server on port 8080 by default. To use it with this setup, run the server on
8080, or change the UI's proxy target (`ui/package.json`, `proxy` field) to
`http://localhost:7071`.

## Optional: Docker

`compose.yaml` in the UC repo starts the server and UI in containers:

```bash
cd ~/github/unitycatalog
docker compose up
```

It publishes the server on **8080** (`8080:8080`). To use it with this project, change
`spark.unity-catalog.uri` to `http://localhost:8080`, or remap the port to `7071:8080`.
Table locations such as `/opt/delta/warehouse` must then be paths that both the container
and your Spark jobs can reach.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Build: `value writeString is not a member of object java.nio.file.Files` | sbt is running on Java 8 (`JAVA_HOME` points to a 1.8 JDK) | `export JAVA_HOME=/usr/lib/jvm/java-17`, then `build/sbt clean package` |
| CLI: connection refused | Server not running, or CLI talking to port 8080 | Start the server with `-p 7071`; pass `--server http://localhost:7071` |
| Server started on 8080 instead of 7071 | Port passed without `-p` | `bin/start-uc-server -p 7071` |
| `Address already in use` | 7071 or 7072 already taken | `ss -ltnp \| grep -E ':707[12]'`, then stop that process or choose another port (update `reference.conf`) |
| Server behaves like an old version after `git pull` | Script reused an old jar in `server/target` | `build/sbt clean package` |
| Spark: `Catalog 'mymanaged' not found` | Catalog not configured (common under `spark-submit`) or not created in UC | Add the `--conf spark.sql.catalog.mymanaged...` lines; run `catalog create` (section 6) |
| Spark: `TABLE_OR_VIEW_NOT_FOUND` for `order_stream` | Unqualified name resolves to `mymanaged.default` | Use `argus1.order_stream` (section 9) |
| SeedJob: `Scripts directory does not exist: db` | Started from the wrong working directory | Run from the project root |
| SeedJob: `DELTA_UNSUPPORTED_FEATURES_IN_CONFIG ... delta.feature.catalogmanaged` | Running on Delta 4.0.0, which doesn't know the feature | Use the project's Delta 4.2.0; external tables don't need the property at all |
| `DELTA_UNSUPPORTED_CATALOG_OWNED_TABLE_CREATION` | `CREATE TABLE` without `LOCATION` (managed table) on Delta 4.0.0 | Use Delta 4.2.0 or later; see [Managed vs External Tables](#managed-vs-external-tables) |
| `Catalog mymanaged does not support views` | The connector supports views only on Spark 4.2+ | Use the temporary views in `db/views.sql` |
| `Altering a table is not supported yet` | Connector 0.4.0 has no `ALTER TABLE`. 0.5.0 passes it to Delta, except `RENAME COLUMN` | Use the project's connector 0.5.0, or set table properties in `CREATE TABLE` |
| Errors writing under `/opt/delta/warehouse/managed` | Storage root directory missing or not writable | Section 5 |
| Managed `CREATE TABLE` fails asking for a storage root or managed location | Catalog and schema were created without `--storage_root` | Recreate them with storage roots (section 6) |
| Writes fail with connection errors to `localhost:7071` | UC server stopped; managed-table commits go through it | Start the server, then restart the app (it resumes from its checkpoint) |
| Tables empty after rerunning SeedJob | `DROP TABLE` on a managed table deletes its data | Expected. Delete the app checkpoints and replay from Kafka |
| Need a clean slate | Old catalogs and tables in metadata | Stop the server and delete `~/github/unitycatalog/etc/db/h2db.mv.db` (this deletes **all** UC metadata, but not the Delta files). Then repeat sections 6 and 7. |

## Reference

- Unity Catalog repository and docs: https://github.com/unitycatalog/unitycatalog
- Spark connector option names (0.4.0 / 0.5.0): `uri`, `token` (legacy) or `auth.type` / `auth.token`,
  `warehouse`, `renewCredential.enabled`, `serverSidePlanning.enabled`
