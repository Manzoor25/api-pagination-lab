"""Fill-in adapter. Point this lab at your team's API in a few minutes.

Copy this file to real_api/my_api.py and replace every FILL_IN value. Then
run:

    python fetch.py --adapter my_api --strategy offset

If your API is not offset/limit, do not force it. Match the style the docs
describe. The comments below are the checklist for reading those docs.

How to read any API's docs
--------------------------
1. Find the base URL and the path that returns the collection (not a single
   record). Write down an example request that returns 2 records.
2. Find the pagination style. Look for these parameter names:
   - offset + limit, or start + count, or startIndex + resultsPerPage
   - page + pageSize, or page[number] + page[size]
   - cursor, next, page_token, continuationToken
   - after, since_id, search_after (keyset)
   - a Link header with rel="next"
3. Find the maximum page size. It is often in the parameter description
   ("maximum is 10000") or you discover it by getting HTTP 400. Never guess
   and then loop: one rejected call is cheaper than a silent cap.
4. Find the total count field (meta.total, totalResults, count) and the array
   that holds the rows (data, results, items). Some APIs wrap each row again
   (FDIC uses data[].data).
5. Find the primary key. You need it for dedup and for keyset pagination.
6. Find rate limits: X-RateLimit-Limit, X-RateLimit-Remaining, Retry-After,
   or a sentence like "5 requests per 30 seconds". The window matters more
   than the number. 20 per second and 20 per minute are different problems.
7. Find filters, especially updated_since / modifiedSince / lastModified.
   That is your incremental load.
8. Find a bulk export, dump, or file feed. If it exists, use it. Paging is
   the fallback.
9. Check whether an API key is required, optional, or changes the rate limit.
   Government APIs often allow anonymous calls at a strict limit and a higher
   limit with a free key.

Government APIs that use startIndex / resultsPerPage
----------------------------------------------------
This is offset pagination with different names. startIndex is how many
records to skip (usually zero-based; confirm, because some APIs start at 1).
resultsPerPage is the page size.

The NVD CVE API 2.0 is the clearest public example:
https://nvd.nist.gov/developers/vulnerabilities

    GET https://services.nvd.nist.gov/rest/json/cves/2.0?startIndex=0&resultsPerPage=20

The response includes resultsPerPage, startIndex, and totalResults. The next
page is startIndex + resultsPerPage. NVD publishes a strict rolling rate
limit and a higher limit if you send an API key. Read the current numbers on
that developers page before you code a sleep. Do not assume they are still
the numbers you remember. If the limit is a handful of requests per 30
seconds, concurrency will not help. It will only create 429s. Sleep for the
window, request a key, or ask whether a bulk feed exists.

Other federal APIs rhyme with this even when the names differ:

- FDIC BankFind (this repo's working adapter): limit + offset, max 10000,
  key optional. See real_api/fdic.py.
- Socrata portals (data.cms.gov and many city open-data sites): $limit and
  $offset. The cap is in the dataset or the portal docs, often 1000 or 50000.
- Regulations.gov: page[number] and page[size], API key required, documented
  hourly limits.
- Congress.gov: offset and limit, API key required, max limit published in
  the API docs (do not assume 10000).

The loop is the same either way:

    start = 0
    while True:
        page = get(startIndex=start, resultsPerPage=page_size)
        write(page)
        if start + len(page) >= total:
            break
        start += len(page)

Stop on an empty page or on start >= total. Advance by the number of rows
you actually received. And keep a checkpoint, because a strict rate limit
means this loop may run for an hour and you will not want to start over.
"""

from __future__ import annotations

import json

import httpx

# --- fill these in from the docs ---
BASE_URL = "https://FILL_IN.example/api"  # no trailing path that already includes the collection
COLLECTION_PATH = "/FILL_IN"  # for example /v1/orders
AUTH_HEADER = "Authorization"  # or "X-Api-Key", or "" if the API is anonymous
AUTH_VALUE = ""  # "Bearer FILL_IN" or the raw key. Prefer an env var. Do not commit a key.
PAGE_STYLE = "offset"  # offset | page | cursor | keyset | link | startIndex
PAGE_SIZE_PARAM = "limit"  # resultsPerPage, page[size], $limit, ...
PAGE_SIZE = 100  # the documented maximum, not a number you hope works
OFFSET_PARAM = "offset"  # startIndex, $offset, page, ...
CURSOR_PARAM = "cursor"  # only used when PAGE_STYLE is cursor
TOTAL_FIELD = "meta.total"  # dotted path, for example totalResults
DATA_FIELD = "data"  # dotted path to the array of rows
ID_FIELD = "id"  # primary key inside each row
UPDATED_FIELD = "updated_at"  # used for incremental loads and dedup
UPDATED_SINCE_PARAM = "updated_since"  # empty string if the API has no such filter
NEXT_FIELD = "next_cursor"  # body field, or "header:Link" if the next page is a header
RATE_LIMIT_RPS = 1.0  # set from the docs. If the docs say "5 per 30 seconds", this is 5/30.
# --- end of fill-ins ---


def _dig(payload: dict, dotted: str):
    cursor = payload
    for part in dotted.split("."):
        if isinstance(cursor, dict) and part in cursor:
            cursor = cursor[part]
        else:
            return None
    return cursor


def _headers() -> dict[str, str]:
    headers = {"Accept": "application/json", "User-Agent": "api-pagination-lab-template/1.0"}
    if AUTH_HEADER and AUTH_VALUE:
        headers[AUTH_HEADER] = AUTH_VALUE
    return headers


def fetch_page(client: httpx.Client, offset: int) -> dict:
    """One page, already unwrapped into the lab's record shape.

    Replace the param names if PAGE_STYLE is not offset. The rest of the lab
    (landing, checkpoint, validate) only needs id and updated_at.
    """
    if "FILL_IN" in BASE_URL or "FILL_IN" in COLLECTION_PATH:
        raise SystemExit("Fill in BASE_URL and COLLECTION_PATH in real_api/TEMPLATE_adapter.py first.")
    params: dict[str, object] = {PAGE_SIZE_PARAM: PAGE_SIZE}
    if PAGE_STYLE in {"offset", "startIndex"}:
        params[OFFSET_PARAM] = offset
    elif PAGE_STYLE == "page":
        # Many page-number APIs are 1-based. Confirm before you trust this.
        params[OFFSET_PARAM] = (offset // PAGE_SIZE) + 1
    response = client.get(COLLECTION_PATH, params=params)
    if response.status_code == 429:
        # Honor Retry-After. Adding threads here will not raise the limit.
        raise RuntimeError(f"rate limited, Retry-After={response.headers.get('Retry-After')}")
    response.raise_for_status()
    payload = response.json()
    rows = _dig(payload, DATA_FIELD) or []
    records = []
    for row in rows:
        item = row.get("data", row) if isinstance(row, dict) else {"value": row}
        records.append(
            {
                "id": int(item[ID_FIELD]),
                "updated_at": str(item.get(UPDATED_FIELD) or ""),
                "category": "",
                "amount": 0.0,
                "payload": json.dumps(item, default=str),
            }
        )
    total = _dig(payload, TOTAL_FIELD)
    return {"records": records, "total": int(total) if total is not None else None}


def main() -> None:
    """Tiny standalone loop so you can test the adapter before wiring fetch.py.

    A production pull should use fetcher.landing.LandingZone and the retry
    helper, not a list. This function is only a smoke test.
    """
    client = httpx.Client(base_url=BASE_URL.rstrip("/"), headers=_headers(), timeout=60.0)
    try:
        first = fetch_page(client, 0)
    finally:
        client.close()
    print(
        f"page_size={PAGE_SIZE} got={len(first['records'])} total={first['total']} "
        f"style={PAGE_STYLE} rate_limit_rps={RATE_LIMIT_RPS}"
    )
    print("If total is much larger than got, loop. If a 429 appears, sleep. Do not add threads yet.")


if __name__ == "__main__":
    main()
