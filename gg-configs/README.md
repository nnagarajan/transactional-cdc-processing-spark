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

Create the topic:

```sh
kafka-topics --bootstrap-server localhost:9092 --create \
  --topic dev.transaction_metadata_json --partitions 1 --replication-factor 1
```

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
