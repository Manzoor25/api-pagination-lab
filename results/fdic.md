# FDIC BankFind pull

Measured 2026-10-28 against the live public API. No API key.

Docs: https://api.fdic.gov/banks/docs/
Spec: https://api.fdic.gov/banks/docs/swagger.yaml

## What the docs and the live API agreed on

- `GET https://api.fdic.gov/banks/institutions`
- Pagination is `limit` + `offset`
- Default limit is 10. Maximum is 10,000. `limit=10001` returned HTTP 400.
- Total is `meta.total`. Rows are wrapped in `data[].data`. Primary key is `CERT`.
- An API key is documented and not currently required.
- Every call returned `X-Ratelimit-Limit: 20`. The header does not name the window. Across separate calls, `X-Ratelimit-Remaining` stayed at 19, so the header alone is not a reason to assume "20 per second" or "20 per minute".

## Diagnose (3 calls, page size 1,000, fields CERT,NAME)

| sample | latency | records | decoded bytes | content-encoding | rate limit |
| --- | ---: | ---: | ---: | --- | --- |
| 1 | 314 ms | 1,000 | 78,963 | gzip | 20 remaining 19 |
| 2 | 122 ms | 1,000 | 84,725 | gzip | 20 remaining 19 |
| 3 | 129 ms | 1,000 | 87,251 | gzip | 20 remaining 19 |

Average latency 188 ms. `meta.total` was 27,835, so page size 1,000 means 28 requests. Page size 10, the default, means 2,784 requests. Page size 10,000 means 3 requests.

httpx decoded gzip before the byte count, so those sizes are the JSON, not the wire size. The `Content-Encoding: gzip` header is what shows compression is on.

## Full pull

Command:

```
python fetch.py --adapter fdic --strategy offset --page-size 10000 --rate-limit-rps 2 --run-id fdic-institutions
```

| | |
| --- | --- |
| records landed | 27,835 |
| pages | 3 |
| elapsed | 2.9 s |
| retries | 0 |
| duplicate ids | 0 |
| unique ids | 27,835 |

`validate.py` reported OK. The 2.9 seconds includes a client-side limit of 2 requests/second, which is slower than the network. The point of that limit is to be a polite guest, not to win a race.

Using the default page size of 10 would be 2,784 requests. At the measured ~188 ms for a much larger page, that is already several minutes before any rate-limit window. If the unpublished window is tight, it becomes the hour-long job. The fix here was not threads. It was reading the maximum page size.
