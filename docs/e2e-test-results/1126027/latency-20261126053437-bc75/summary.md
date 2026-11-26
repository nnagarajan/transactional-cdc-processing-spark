# latency 20261126053437-bc75

**Verdict: PASS**

## Configuration

| Setting | Value |
|---|---|
| run id | 20261126053437-bc75 |
| seed | 416431749460 |
| sessions | 1 |
| orders | 200 |
| interval | PT0.5S |
| flavor | F2 (1 order, 1 detail, 3 line items), one order per transaction |

## Checks

| Check | Result | Note |
|---|---|---|
| ground truth consistent with the client | pass | 201 committed transactions |
| all 200 orders reported | pass |  |
| every order visible in both tables | pass |  |
| every latency positive | pass |  |
| client ack within 1 s of the Oracle commit timestamp | pass |  |

## Latency per order

| Latency (ms) | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| commit → `dwhProcessedTs` | 200 | 10130 | 14667 | 15177 | 15665 |
| commit → visible in `order_stream` | 200 | 11112 | 15630 | 16442 | 16641 |
| commit → visible in `orders_current` | 200 | 12546 | 17216 | 17816 | 18193 |
| commit → client ack (sanity) | 200 | 7 | 12 | 13 | 13 |

Visibility resolution: exact. A visible time is the in-commit timestamp of the Delta commit that wrote the row (`_metadata.row_commit_version` joined with `DESCRIBE HISTORY`), not a polling time. Per-order values are in `latency.csv`.
