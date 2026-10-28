# Benchmark results

Generated 2026-10-28 from `benchmark.py` on this Windows machine. Table numbers were measured. The painful-sample projection is labeled as a calculation.

Server profile for the table: latency 0 ms, failure rate 0, rate limit off, page size 1,000. Deep offset is still slow because SQLite walks skipped rows. That is the real effect, not a sleep.

Wall-clock seconds include landing the pages and writing Parquet. Retries are 0 in every row because injected failures and the rate limit were off. Validation found 0 duplicate ids on every complete strategy.

## quick: 100,000 records

| strategy | seconds | requests | records/sec | retries | duplicates | missing |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| naive | 0.29 | 2 | 3447.5 | 0 | 0 | 99000 |
| offset | 3.91 | 100 | 25551.0 | 0 | 0 | 0 |
| cursor | 3.30 | 100 | 30336.1 | 0 | 0 | 0 |
| keyset | 3.30 | 100 | 30309.5 | 0 | 0 | 0 |
| parallel_offset | 4.47 | 100 | 22357.7 | 0 | 0 | 0 |
| partitioned | 5.63 | 104 | 17776.0 | 0 | 0 | 0 |
| resilient | 3.80 | 100 | 26325.4 | 0 | 0 | 0 |
| incremental | 4.13 | 101 | 24217.3 | 0 | 0 | 0 |
| bulk_export | 4.19 | 4 | 23841.2 | 0 | 0 | 0 |

## full: 1,000,000 records

| strategy | seconds | requests | records/sec | retries | duplicates | missing |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| naive | 0.25 | 2 | 4006.9 | 0 | 0 | 999000 |
| offset | 91.20 | 1000 | 10964.8 | 0 | 0 | 0 |
| cursor | 35.04 | 1000 | 28539.1 | 0 | 0 | 0 |
| keyset | 35.27 | 1000 | 28350.6 | 0 | 0 | 0 |
| parallel_offset | 51.37 | 1000 | 19466.4 | 0 | 0 | 0 |
| partitioned | 57.11 | 1000 | 17510.4 | 0 | 0 | 0 |
| resilient | 33.34 | 1000 | 29992.8 | 0 | 0 | 0 |
| incremental | 32.56 | 1001 | 30709.2 | 0 | 0 | 0 |
| bulk_export | 38.57 | 17 | 25928.2 | 0 | 0 | 0 |

Offset is 2.6x slower than keyset (91.20s vs 35.27s) with no artificial latency. A direct SQLite probe on the same file, measured separately, was 126.4 ms for `OFFSET 900000` and 0.3 ms for `WHERE id > 900000`.

Parallel offset did not win. With latency injection off, the network is not the bottleneck, and several deep `OFFSET` queries at once make SQLite work harder. Partitioned keyset has the same shape here: the queries are cheap, and writing a million rows to Parquet dominates. Concurrency pays when each request costs hundreds of milliseconds and you are still under the rate limit. It does not pay when the rate limit is already the ceiling.

## Painful sample (default 300 ms latency)

Measured 20 offset pages at 300 ms configured latency: 6.96s total, 0.348s per page, 20,000 records.

Calculated, not measured, full sequential pull of 1,000,000 records at that per-page cost and page size 1,000: **348.2 seconds** (5.8 minutes), before deep-offset penalty and retries.

Calculated, not measured, same per-request cost at page size 100 (10,000 requests): **3,480 seconds** (58 minutes). That is the hour-long pull. The fix is a larger page, a keyset or a file, or a higher rate limit. It is not "add threads" until you have checked the limit.

## Demo contrasts (separate small servers)

From `results/demo.json`, also measured:

- Incremental: first load 5,000 rows, then 40 rows touched, second load 40 rows in 0.251s.
- Live data: offset landed 286 raw / 272 unique (14 duplicates). Keyset on the already-mutated table landed 286 unique and 0 duplicates.
- Hard kill after 5 checkpointed pages, then rerun: 2,000 unique ids, 0 duplicates.
- Server limit 5 requests/second: 20-way parallel offset had 30 retries. A client limiter at 4/second had 0 retries. Both landed 2,000 rows.
