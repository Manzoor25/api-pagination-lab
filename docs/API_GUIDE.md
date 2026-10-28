# How to read an API's docs before you write a loop

Your teammates lost an hour because the first successful call looked like "the API works" and nobody measured the next thousand calls. Spend ten minutes in the docs first. This is the checklist, with the two public APIs this lab actually checked.

## 1. One example that returns two rows

Copy a request from the docs and run it. You want the real JSON, not the marketing page. Note:

- the base URL
- the path
- whether a key is required
- the HTTP status

FDIC, verified 2026-10-28 against the live service and [the OpenAPI spec](https://api.fdic.gov/banks/docs/swagger.yaml):

```
GET https://api.fdic.gov/banks/institutions?limit=2&offset=0&fields=CERT,NAME
```

No API key was required. The docs say a key exists and is recommended. The response wrapped each row: `data[].data`, and the count was `meta.total`.

## 2. Name the pagination style

| What you see in the docs | What it really is | Can you parallelize it? |
| --- | --- | --- |
| `limit` + `offset`, `$limit` + `$offset` | offset | Yes, but rows can move under you |
| `startIndex` + `resultsPerPage` | offset with different names | Same caveat |
| `page` + `pageSize` | offset, plus an off-by-one risk | Same caveat |
| `cursor`, `page_token`, `continuationToken` | cursor | No. Page N needs page N-1's token |
| `after_id`, `since_id`, `search_after` | keyset | Yes, by id or time ranges |
| `Link: <url>; rel="next"` | usually cursor or offset, hidden in a header | Only if the URL is guessable, which it often is not |
| a file, dump, or `/export` | bulk | You do not page |

If the docs do not say, request two pages and see whether the second request needs a token from the first. If it does, you have a cursor.

## 3. Find the maximum page size by reading, then by one rejected call

FDIC's spec says the default is 10 and the maximum is 10,000. A live call confirmed both: `limit=10000` returned 10,000 rows, `limit=10001` returned HTTP 400.

Using the default of 10 against 27,835 institutions is 2,784 requests. Using 10,000 is 3 requests. That single number is often the whole hour.

## 4. Find the rate limit, including the window

A header that says `20` is not a plan. 20 per second and 20 per minute differ by 60x. Look for:

- `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `Retry-After`
- a sentence in the docs: "5 requests in a rolling 30 seconds"
- a higher number if you send a key

On 2026-10-28 every FDIC institutions call returned `X-Ratelimit-Limit: 20`. The header does not name the window. `diagnose.py --preset fdic` records the header and refuses to invent the window. Eight quick calls in about one second all returned 200, so the anonymous limit is not "1 per second", but you should still pace yourself and stop if you see 429.

NVD's CVE API is the strict government pattern this lab wants you to recognize. The [official vulnerabilities page](https://nvd.nist.gov/developers/vulnerabilities) documents `startIndex` and `resultsPerPage`, plus `totalResults`. A live call on 2026-10-28:

```
GET https://services.nvd.nist.gov/rest/json/cves/2.0?startIndex=0&resultsPerPage=1
```

returned HTTP 200 with `startIndex=0`, `resultsPerPage=1`, and `totalResults=415517`. That response did not include an `X-RateLimit-*` header. The limit is documented on the developers page (anonymous calls are throttled on a rolling window, and a free API key raises it). Read that page the day you build the pull. If the limit is a handful of requests per 30 seconds, threads will not help. Sleep, get a key, or ask for a feed.

## 5. Find the filter that makes tomorrow's pull small

Search the docs for `updated_since`, `modifiedSince`, `lastModified`, `DATEUPDT`, or a date range. That parameter is the incremental load. The first pull is the slow one. The daily pull should be the rows that changed.

FDIC filters use Elasticsearch query-string syntax, documented on the same docs page, for example `DATEUPDT:[2020-01-01 TO *]`. Dates in filters are `yyyy-mm-dd` even when a field in the body is displayed as `MM/DD/YYYY`. That mismatch is a classic way to pull zero rows and think the API is empty.

## 6. Ask about a file

Search for export, download, dump, bulk, csv, and feed. FDIC can return CSV via the `Accept` header, and the institutions dataset is small enough that 3 JSON pages are already a bulk pull in practice. A nightly file still wins when the dataset is millions of rows.

## 7. Then write the adapter

Copy [TEMPLATE_adapter.py](../real_api/TEMPLATE_adapter.py). Fill in the base URL, auth header, pagination style, page-size parameter, total field, and next-page field. Run one page. Compare its count with `meta.total` or `totalResults`. Only then start the loop.
