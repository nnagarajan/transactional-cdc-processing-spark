# performance 20261126054934-99d3

**Verdict: PASS**

## Configuration

| Setting | Value |
|---|---|
| run id | 20261126054934-99d3 |
| seed | 214392108579 |
| sessions | 16 |
| target events/min | 100000 |
| mix | F1=20 F2=35 F3=25 F4=5 F5=2 F7=13 |
| mix events/tx | 6.35 |
| target tx/s | 262.5 |
| sustained duration | PT5M |
| bulk | 10000 orders as 100 F6 transactions on 10 sessions |

## Checks

| Check | Result | Note |
|---|---|---|
| ground truth consistent with the client | pass | 78741 committed transactions |
| Sustained phase: every transaction arrived with its counts | pass | 78740 transactions |
| ground truth consistent with the client | pass | 78841 committed transactions |
| Bulk phase: every transaction arrived with its counts | pass | 100 transactions |

## Sustained phase

| Metric | Value |
|---|---|
| transactions committed | 78740 |
| events committed | 503282 |
| commit window | 300.0 s |
| target | 100,000 events/min (262.5 tx/s) |
| achieved | 100,658 events/min (262.5 tx/s) |
| catch-up after last commit | 16814 ms |
| pacer slots missed by more than one interval | 538 |

| Latency (ms) | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| commit → `dwhProcessedTs` | 93518 | 10949 | 15507 | 16251 | 16974 |
| commit → visible in `order_stream` | 93518 | 11984 | 16584 | 17341 | 17841 |
| commit → client ack (sanity) | 93518 | 3 | 22 | 39 | 299 |


## Bulk phase

| Metric | Value |
|---|---|
| transactions committed | 100 |
| events committed | 50000 |
| commit window | 0.9 s |
| target | unpaced |
| achieved | 3,325,942 events/min (110.9 tx/s) |
| catch-up after last commit | 16515 ms |

| Latency (ms) | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| commit → `dwhProcessedTs` | 10000 | 15673 | 15912 | 16023 | 16024 |
| commit → visible in `order_stream` | 10000 | 16562 | 16878 | 16998 | 16999 |
| commit → client ack (sanity) | 10000 | 14 | 24 | 27 | 33 |

