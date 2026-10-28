# LinkedIn post

150-250 words, first person. Links assume the published repo.

## Post

I watched a pull come back with 1,000 rows and then take about an hour once we tried to get the rest. The instinct was to add threads. That would have been the wrong fix.

I timed it. A million synthetic rows, page size 1,000, no artificial latency: offset pagination took 91 seconds. Keyset pagination, which seeks to the last id instead of skipping rows, took 35. Skipping 900,000 rows in SQLite cost 126 ms. Seeking past that same id cost 0.3 ms.

The hour is arithmetic. One thousand pages at a measured 348 ms each is about 6 minutes. The same per-request cost at page size 100 is about 58 minutes. If the rate limit is already the ceiling, threads only create 429s. Against a 5-per-second cap, 20 concurrent callers retried 30 times. A client limiter at 4 per second retried zero times and still landed every row.

Offset pagination while rows were moving duplicated 14 ids. A keyset walk duplicated none. A hard-killed run resumed from a checkpoint with zero duplicate ids.

FDIC BankFind was the other lesson. Documented max page size 10,000, 27,835 institutions, 3 requests, 2.9 seconds.

Repo: https://github.com/Manzoor25/api-pagination-lab
Walkthrough: https://manzoor25.github.io/api-pagination-lab/

## Alternate hooks

1. Our API pull took an hour. The API was not the slow part.
2. I timed nine ways to pull a million rows. Adding threads was not the winner.
3. Offset pagination did not just get slow. While the data moved, it repeated 14 ids and skipped others.
