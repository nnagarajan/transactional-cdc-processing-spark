# correctness 20261126053036-8ad4

**Verdict: PASS**

## Configuration

| Setting | Value |
|---|---|
| run id | 20261126053036-8ad4 |
| seed | 176115872869 |
| sessions | 17 |
| tx per session | 40 |
| hot orders | 5 |
| weights | CHILD_ONLY=15 INSERT_MULTI=10 INSERT_ONE=25 ROLLBACK=10 UPDATE_HOT=20 UPDATE_OWN=20 |
| orders | 473 |
| events | 3526 |

## Checks

| Check | Result | Note |
|---|---|---|
| every kind in every session | pass |  |
| ground truth consistent with the client | pass | 599 committed transactions |
| completeness and content | pass | 883 expected rows, 599 transactions |
| no phantom output | pass | 83 rolled-back transactions |
| commit order across Delta commits | pass | 2 Delta commits |
| per-order versions rise with CSN | pass | 2657 entities |
| orders_current equals Oracle | pass | 473 orders |
| no dropped run data | pass |  |
| dwhProcessedTs non-decreasing in CSN order | pass |  |

## Workload

| Session | INSERT_ONE | INSERT_MULTI | UPDATE_OWN | UPDATE_HOT | CHILD_ONLY | ROLLBACK |
|---|---|---|---|---|---|---|
| 1 | 9 | 4 | 11 | 8 | 5 | 3 |
| 2 | 8 | 6 | 11 | 6 | 6 | 3 |
| 3 | 9 | 6 | 7 | 8 | 7 | 3 |
| 4 | 11 | 6 | 6 | 11 | 3 | 3 |
| 5 | 7 | 6 | 11 | 7 | 3 | 6 |
| 6 | 8 | 5 | 6 | 8 | 6 | 7 |
| 7 | 13 | 2 | 7 | 6 | 6 | 6 |
| 8 | 11 | 3 | 6 | 6 | 8 | 6 |
| 9 | 11 | 4 | 6 | 8 | 5 | 6 |
| 10 | 8 | 9 | 1 | 9 | 5 | 8 |
| 11 | 10 | 5 | 9 | 8 | 5 | 3 |
| 12 | 17 | 2 | 5 | 6 | 5 | 5 |
| 13 | 11 | 6 | 7 | 6 | 5 | 5 |
| 14 | 10 | 4 | 9 | 8 | 5 | 4 |
| 15 | 6 | 4 | 8 | 10 | 5 | 7 |
| 16 | 10 | 5 | 7 | 8 | 5 | 5 |
| 17 | 8 | 6 | 8 | 7 | 8 | 3 |

599 committed and 83 rolled-back transactions in 0.8 s.

## Latency (informational)

| Latency (ms) | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| commit → `dwhProcessedTs` | 883 | 16079 | 16430 | 16481 | 16596 |
| commit → visible in `order_stream` | 883 | 17267 | 17632 | 17684 | 17800 |
| commit → client ack (sanity) | 883 | 7 | 10 | 12 | 17 |
