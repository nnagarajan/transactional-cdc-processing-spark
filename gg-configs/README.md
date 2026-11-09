## Extract Scipts

```sh
dblogin useridalias c##ggadmin       
register extract ext04 database container (ORCLPDB);
add extract ext04, integrated translog, begin now;
ALTER EXTTRAIL /opt/oracle/gg/gg_home/dirdat/e4, EXTRACT EXT04, MEGABYTES 100

add trandata orclpdb.appuser.ORDERS
add trandata orclpdb.appuser.ORDER_DETAILS
add trandata orclpdb.appuser.ORDER_LINE_ITEMS
```


## Replicat Scripts

```sh
delete replicat kc
add replicat KC,exttrail /opt/oracle/gg/gg_home/dirdat/e4
alter replicat KC extseqno 14 extrba 110968
```


## Troubleshooting

A
SEND REPLICAT REP2 GETLAG                                                                                
SEND REPLICAT REP2 GETLOGEND

SEND EXTRACT EXT04 GETLAG
SEND EXTRACT EXT04 LOGEND
```

## Transaction Metadata Topic Contract

The Spark pipeline emits transactions in the order their markers appear on
`dev.transaction_metadata_json`. That order is only the commit order if:

- The topic has **exactly one partition**. The app fails the query if it reads a
  marker from any other partition.
- `rep3` publishes synchronously with idempotent, fully acknowledged writes:
  - `gg.handler.kafkahandler.blockingSend=true` in `rep3.props`
  - `acks=all` and `enable.idempotence=true` in `kp.properties`
- Deletes are filtered **only at the Extract**: `ext04.prm` sets `IGNOREDELETES`, and `rep3`
  does not. Deletes never reach the trail, so the `kc` data topics and the `rep3` marker
  counts always see the same operations. Filtering at only one replicat would make
  marker counts disagree with the data, and transactions would complete too early or never.
- `rep3` uses the formatter from [`gg-formatter/`](../gg-formatter/README.md), which emits
  **exactly one marker per source transaction**, even when `GROUPTRANSOPS` groups several
  source transactions into one delivery. A grouped delivery becomes one Kafka message holding a
  JSON array of markers. Deploy that build to `customlib/`; the earlier formatter merged grouped
  transactions into one marker with wrong counts.

Create the topic:

```sh
kafka-topics --bootstrap-server localhost:9092 --create \
  --topic dev.transaction_metadata_json --partitions 1 --replication-factor 1
```

### Go-live: first start with markers

Use this when the data topics already hold history and the metadata topic is switched on for the
first time. It takes one cut point in the trail, shared by `kc` and `rep3`, so that:

- every marker `rep3` publishes refers to a transaction whose data events come after the offsets
  the pipeline starts from, and
- no data event after those offsets lacks a marker.

If either half is wrong, a marker can refer to data the pipeline skipped, and that transaction
blocks all output until the pipeline is restarted on a new checkpoint.

`kc` stops at a transaction boundary, so its checkpoint is the cut point. `kc` pauses for as long
as steps 2 to 5 take, usually seconds. Extract `ext04` keeps running.

1. Create the metadata topic with one partition (see above). If it exists and holds test markers,
   delete and recreate it. Make sure `rep3` is stopped:
   ```sh
   ggsci> STOP REPLICAT rep3
   ```
2. Stop `kc` and wait until `INFO REPLICAT kc` reports `STOPPED`:
   ```sh
   ggsci> STOP REPLICAT kc
   ggsci> INFO REPLICAT kc
   ```
3. Record `kc`'s trail position: the `Sequence #` and `RBA` under **Current Checkpoint (position
   of last record read in the data source)**:
   ```sh
   ggsci> INFO REPLICAT kc, SHOWCH
   ```
4. Record the end offset of every partition of the three data topics. Run it twice, a few seconds
   apart, and check that nothing changed. If an offset moved, `kc` was still writing: wait and
   repeat.
   ```sh
   for t in dev.appuser.orders.json dev.appuser.order_details.json dev.appuser.order_line_items.json; do
     kafka-get-offsets --bootstrap-server localhost:9092 --topic $t --time -1
   done
   # prints topic:partition:offset, for example dev.appuser.orders.json:0:113
   ```
5. Position `rep3` at `kc`'s trail position, then start both:
   ```sh
   ggsci> ALTER REPLICAT rep3 EXTSEQNO <sequence> EXTRBA <rba>
   ggsci> START REPLICAT kc
   ggsci> START REPLICAT rep3
   ```
6. Set the pipeline's starting offsets to the recorded end offsets (partition to offset), and
   leave the metadata topic at `earliest`. In `transactional-cdc-processing.conf`:
   ```hocon
   job.kafka.starting-offsets {
     orders = """{"0":113}"""
     order-details = """{"0":109}"""
     order-line-items = """{"0":372}"""
     transaction-metadata = "earliest"
   }
   ```
   The app logs each source's starting position at startup, and refuses to start if a value is
   invalid.
7. Start `TransactionalCdcProcessingApp` on a **new** `job.output.checkpoint-location`. Starting
   offsets apply only to a new checkpoint; on an existing one, Spark resumes from its own offsets.
8. Verify: after the next source transaction commits, its row appears in `order_stream`, and the
   app logs no `has blocked emission` error. In the state store (see `STATE-STORE-DEBUG.md`),
   `seq_buffers` holds only in-flight transactions.

After the first start the offsets block is ignored. It can stay, or be removed.

#### Fallback: start from `earliest`

If a clean cut point isn't possible, start the data topics from `earliest` (the default) after
step 5, with `rep3` positioned at or after `kc`. Then:

- The pipeline reads the retained history in batches of at most
  `job.kafka.max-offsets-per-trigger` offsets per source (default 50,000).
- History without markers is buffered, and removed once the first transaction after the cut point
  is emitted. The app logs `Removed <n> buffered transaction(s) without a marker below CSN <csn>`,
  at most 100,000 per batch.
- `rep3` must still start at a transaction boundary. A `rep3` started in the middle of a
  transaction (`BEGIN NOW`, or a hand-picked RBA) publishes a marker with partial counts, which
  blocks forever.

The cost is a slower first start, and state that holds the history until the purge clears it.

### Migrating an existing deployment

1. Stop `TransactionalCdcProcessingApp` and `ScdType1MergeApp`.
2. Stop Extract `ext04`, add `IGNOREDELETES`, and restart it (this affects trail records
   written from then on). Stop replicat `rep3` and apply its config changes above.
3. Delete and recreate `dev.transaction_metadata_json` with 1 partition:
   ```sh
   kafka-topics --bootstrap-server localhost:9092 --delete --topic dev.transaction_metadata_json
   kafka-topics --bootstrap-server localhost:9092 --create \
     --topic dev.transaction_metadata_json --partitions 1 --replication-factor 1
   ```
4. Reposition `rep3` to the same trail position as `kc` (or the trail start) so
   markers cover every data event that will be replayed, then start it:
   ```sh
   alter replicat rep3 extseqno <seqno> extrba <rba>
   start replicat rep3
   ```
5. Deploy the new app with a **new checkpoint location** and
   `startingOffsets=earliest`. Optionally truncate `order_stream` and
   `orders_current` first; otherwise the replay adds duplicate history rows to
   `order_stream` (harmless for `orders_current`).
6. Start `ScdType1MergeApp`, with a new checkpoint if `order_stream` was truncated.

Rollback: redeploy the previous jar with its original checkpoint location. The
replicat config changes are compatible with the old app.
