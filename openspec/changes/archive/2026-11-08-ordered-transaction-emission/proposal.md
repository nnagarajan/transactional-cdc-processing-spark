# Proposal

## Why

The pipeline cannot guarantee that a transaction committed at csn1 is written to `order_stream` before one committed at csn2. Each `xid:csn` group is processed in isolation, so emission follows completion order, not commit order. Consumers of `order_stream` need commit order. The completeness check can also fire early: duplicate events count toward the expected totals, and marker counts leave out deletes that the data topics still carry. Fixing ordering without fixing completeness would emit wrong transactions in the right order.

## What Changes

- **BREAKING** Replace per-transaction grouping (`xid:csn` + `flatMapGroupsWithState`) with a single ordered sequencer built on `transformWithState`. Transactions are emitted to `order_stream` strictly in the order of their markers on the transaction metadata topic, which is commit order. Emission stops at the first incomplete transaction and never skips past it. The state schema changes, so a new checkpoint location is required.
- Alert, by error log, when the head-of-line transaction has been blocked longer than a configurable threshold (default 10 minutes). Report its key, its progress, and the queue depth behind it.
- Validate at runtime that the metadata topic has a single partition, and fail the query if it doesn't.
- Make completeness detection exact:
  - Deduplicate data events on `pos` within a transaction.
  - Deduplicate markers on `xid:csn`.
  - Drop events and markers that belong to already-emitted transactions.
- GoldenGate/Kafka delivery settings for the metadata replicat (`rep3`):
  - Move delete filtering to the Extract (`IGNOREDELETES` in `ext04.prm`) and remove it from `rep3`, so the data topics and marker counts see the same operations. Deletes no longer reach the pipeline at all.
  - Set `acks=all` and `enable.idempotence=true`.
  - Set `blockingSend=true`.
  - Document the single-partition requirement for `dev.transaction_metadata_json`.
- `ScdType1MergeApp` becomes robust to replay and to orders first seen through a child-only change:
  - A child-only change for an unknown order is kept as a placeholder row instead of being dropped.
  - Transaction context columns (`xid`, `csn`, `dwhProcessedTs`) never move to a lower CSN.
  - Ties within a batch are broken by CSN rather than processing timestamp.
- Ensure the RocksDB state store provider is set on the spark-submit path as well as the dev path (`transformWithState` requires it).

## Capabilities

### New Capabilities
- `transaction-buffering`: Buffering CDC events per transaction and deciding exactly when a transaction is complete, including duplicate and late-event handling.
- `transaction-ordering`: Emitting complete transactions to `order_stream` in commit order, the source-ordering contract that makes this possible, and head-of-line blocking/alerting.
- `current-state-merge`: How `ScdType1MergeApp` maintains `orders_current` from `order_stream`, including placeholder rows, CSN-monotonic transaction context, and intra-batch tie-breaking.

### Modified Capabilities
- None. No specs exist yet.

## Impact

- **Code:**
  - `TransactionalCdcProcessingApp`: stream assembly and the stateful operator.
  - New `StatefulProcessor` implementation.
  - `TransactionState`: dedup.
  - `ScdType1MergeApp`: merge clauses and tie-breaking.
  - `SparkSessionCreator`: state store config.
  - `transactional-cdc-processing.conf`: new settings for the alert threshold.
- **GoldenGate config:** `gg-configs/extract/ext04.prm`, `gg-configs/replicat/rep3.prm`, `rep3.props`, `kp.properties`, `gg-configs/README.md`.
- **Deletes:** Oracle deletes on the tracked tables are no longer captured, so they never appear in `order_stream` or `orders_current`. This matches today's output, which never emitted delete records.
- **Kafka:** `dev.transaction_metadata_json` must be (re)created with exactly 1 partition.
- **Operations:**
  - Deploying needs a new checkpoint location. In-flight state from the old operator is discarded and topics are replayed from `earliest`.
  - A permanently missing event or marker now halts all output until an operator intervenes. This is intentional.
- **Performance:** transaction assembly runs as a single task, losing the parallelism of today's 4 shuffle partitions.
- **Tests:** no test sources exist today. This change adds `src/test/scala` with processor and merge tests.
- **Docs:** `README.md`, `CLAUDE.md` (Known Limitations), `SCD_TYPE1_README.md`.
