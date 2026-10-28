"""Measure a few calls and say where the time actually goes.

What this file teaches
----------------------
Before you add threads, measure. A pull is slow for one of four reasons:

- each request is slow (latency)
- each page is small (too many round trips)
- the server only allows a few requests per window (rate limit)
- the query itself gets slower as you go deeper (deep offset)

Concurrency helps only the first two, and only until you hit the rate limit.
If the rate limit is the ceiling, more threads just create more 429 errors.

    python diagnose.py --preset mock
    python diagnose.py --preset fdic
    python diagnose.py --base-url https://api.fdic.gov/banks --preset fdic
"""

from __future__ import annotations

import argparse
import math
import time
from statistics import mean

import httpx

PRESETS = {
    "mock": {
        "base_url": "http://127.0.0.1:8000",
        "path": "/offset",
        "page_param": "limit",
        "offset_param": "offset",
        "page_size": 1000,
        "total_path": "/count",
        "total_keys": ("total",),
        "data_keys": ("data",),
        "probe_rate_limit": True,
    },
    "fdic": {
        "base_url": "https://api.fdic.gov/banks",
        "path": "/institutions",
        "page_param": "limit",
        "offset_param": "offset",
        "page_size": 1000,
        "total_path": "/institutions",
        "total_keys": ("meta", "total"),
        "data_keys": ("data",),
        # Do not burst a public API just to force a 429. Read the headers.
        "probe_rate_limit": False,
        "extra_params": {"fields": "CERT,NAME", "sort_by": "CERT", "sort_order": "ASC"},
    },
}


def _dig(payload: dict, keys: tuple[str, ...]):
    cursor = payload
    for key in keys:
        if not isinstance(cursor, dict) or key not in cursor:
            return None
        cursor = cursor[key]
    return cursor


def _header(headers: httpx.Headers, name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def _timed_get(
    client: httpx.Client,
    path: str,
    params: dict,
    headers: dict | None = None,
) -> tuple[httpx.Response, float]:
    started = time.perf_counter()
    response = client.get(path, params=params, headers=headers)
    elapsed = time.perf_counter() - started
    return response, elapsed


def diagnose(args: argparse.Namespace) -> dict:
    preset = PRESETS.get(args.preset, {})
    base_url = args.base_url or preset.get("base_url")
    path = args.path or preset.get("path")
    page_param = args.page_param or preset.get("page_param", "limit")
    offset_param = args.offset_param or preset.get("offset_param", "offset")
    page_size = args.page_size or preset.get("page_size", 100)
    extra = dict(preset.get("extra_params") or {})
    if not base_url or not path:
        raise SystemExit("Pass --preset mock|fdic or --base-url and --path")

    client = httpx.Client(
        base_url=base_url.rstrip("/"),
        timeout=args.timeout,
        follow_redirects=True,
        headers={"Accept": "application/json", "User-Agent": "api-pagination-lab-diagnose/1.0"},
    )
    samples = []
    try:
        print(f"[diagnose] {client.base_url}{path} page_size={page_size}", flush=True)
        for index in range(args.samples):
            params = {page_param: page_size, offset_param: index * page_size, **extra}
            response, elapsed = _timed_get(client, path, params)
            response.raise_for_status()
            payload = response.json()
            data = _dig(payload, preset.get("data_keys", ("data",)))
            if data is None and isinstance(payload, dict):
                data = payload.get("data")
            count = len(data) if isinstance(data, list) else None
            samples.append(
                {
                    "elapsed_s": elapsed,
                    "status": response.status_code,
                    "records": count,
                    "bytes": len(response.content),
                    "content_encoding": _header(response.headers, "content-encoding"),
                    "rate_limit": _header(response.headers, "x-ratelimit-limit"),
                    "rate_remaining": _header(response.headers, "x-ratelimit-remaining"),
                    "retry_after": _header(response.headers, "retry-after"),
                    "query_delay_ms": _header(response.headers, "x-query-delay-ms"),
                }
            )
            print(
                f"[diagnose] sample {index + 1}: {elapsed * 1000:.0f} ms, "
                f"records={count}, bytes={len(response.content):,}, "
                f"encoding={samples[-1]['content_encoding']}, "
                f"rate_limit={samples[-1]['rate_limit']} remaining={samples[-1]['rate_remaining']}",
                flush=True,
            )

        # Gzip versus identity. httpx decodes gzip, so compare body sizes by
        # asking for identity on the second call. A smaller identity body means
        # field selection matters more than compression; a gzip content-encoding
        # on the first call means the server can compress.
        identity, identity_s = _timed_get(
            client,
            path,
            {page_param: page_size, offset_param: 0, **extra},
            headers={"Accept-Encoding": "identity"},
        )
        gzip_supported = any(sample["content_encoding"] for sample in samples if sample["content_encoding"])
        total = None
        total_path = preset.get("total_path")
        if total_path:
            total_response, _ = _timed_get(client, total_path, {page_param: 1, offset_param: 0, **extra})
            if total_response.status_code == 200:
                total = _dig(total_response.json(), preset.get("total_keys", ("total",)))
                if total is not None:
                    total = int(total)

        rate_limit_header = samples[0]["rate_limit"]
        saw_429 = False
        retry_after = None
        if args.probe_rate_limit or (preset.get("probe_rate_limit") and args.probe_rate_limit is not False):
            if args.preset == "fdic" and not args.force_probe:
                print("[diagnose] skipping 429 probe on FDIC. Pass --force-probe to override.", flush=True)
            else:
                print("[diagnose] probing rate limit with a short burst", flush=True)
                for _ in range(args.burst):
                    burst_response, _ = _timed_get(client, path, {page_param: 1, offset_param: 0, **extra})
                    if burst_response.status_code == 429:
                        saw_429 = True
                        retry_after = _header(burst_response.headers, "retry-after")
                        print(f"[diagnose] HTTP 429 Retry-After={retry_after}", flush=True)
                        break
    finally:
        client.close()

    latencies = [sample["elapsed_s"] for sample in samples]
    avg_latency = mean(latencies) if latencies else 0.0
    records_per_page = next((sample["records"] for sample in samples if sample["records"]), page_size)
    pages = math.ceil(total / records_per_page) if total and records_per_page else None
    sequential = pages * avg_latency if pages else None
    parallel = sequential / args.concurrency if sequential else None
    rps = _parse_rps(rate_limit_header)
    rate_time = pages / rps if pages and rps else None
    bottleneck = _bottleneck(avg_latency, records_per_page, rps, saw_429, sequential, parallel, rate_time)
    report = {
        "base_url": base_url,
        "path": path,
        "samples": len(samples),
        "avg_latency_s": round(avg_latency, 4),
        "min_latency_s": round(min(latencies), 4) if latencies else None,
        "max_latency_s": round(max(latencies), 4) if latencies else None,
        "records_per_page": records_per_page,
        "response_bytes": samples[0]["bytes"] if samples else None,
        "identity_bytes": len(identity.content) if identity.status_code == 200 else None,
        "identity_latency_s": round(identity_s, 4),
        "gzip_content_encoding_seen": bool(gzip_supported),
        "total_records": total,
        "pages": pages,
        "rate_limit_header": rate_limit_header,
        "saw_429": saw_429,
        "retry_after": retry_after,
        "assumed_rps": rps,
        "concurrency": args.concurrency,
        "estimated_sequential_s": None if sequential is None else round(sequential, 1),
        "estimated_parallel_s": None if parallel is None else round(parallel, 1),
        "estimated_rate_limit_s": None if rate_time is None else round(rate_time, 1),
        "bottleneck": bottleneck,
    }
    _print_recommendation(report)
    return report


def _parse_rps(header: str | None) -> float | None:
    """Headers rarely include the window. Return the number only as a hint.

    X-Ratelimit-Limit: 20 might mean 20/second or 20/minute. diagnose refuses
    to invent the window. Callers that know the window can pass nothing here;
    the recommendation says so out loud.
    """
    if not header:
        return None
    token = header.split(",")[0].strip()
    try:
        return float(token)
    except ValueError:
        return None


def _bottleneck(avg_latency, records_per_page, rps, saw_429, sequential, parallel, rate_time) -> str:
    if saw_429:
        return "rate_limit"
    if rate_time is not None and parallel is not None and rate_time > parallel * 1.1:
        return "rate_limit_if_header_is_per_second"
    if avg_latency >= 0.25 and records_per_page <= 1000:
        return "latency_and_page_size"
    if sequential and sequential > 60 and records_per_page < 5000:
        return "too_many_round_trips"
    return "measure_more"


def _print_recommendation(report: dict) -> None:
    print("", flush=True)
    print("[diagnose] ---- where the time goes ----", flush=True)
    print(
        f"[diagnose] average latency {report['avg_latency_s'] * 1000:.0f} ms, "
        f"{report['records_per_page']} records/page, "
        f"response {report['response_bytes']} bytes "
        f"(identity {report['identity_bytes']} bytes), "
        f"gzip content-encoding seen: {report['gzip_content_encoding_seen']}",
        flush=True,
    )
    if report["total_records"] is not None:
        print(f"[diagnose] total records reported by the API: {report['total_records']:,}", flush=True)
        print(f"[diagnose] pages at this page size: {report['pages']}", flush=True)
    print(
        "[diagnose] formula: total_time ~= (total_records / page_size) x latency / concurrency, "
        "then take the max of that and (pages / rate_limit)",
        flush=True,
    )
    print(
        f"[diagnose] estimate sequential={report['estimated_sequential_s']}s "
        f"parallel@{report['concurrency']}={report['estimated_parallel_s']}s "
        f"rate_limit_header={report['rate_limit_header']}",
        flush=True,
    )
    if report["saw_429"] or report["bottleneck"] == "rate_limit":
        print(
            "[diagnose] Your bottleneck is the rate limit. Concurrency will NOT help. "
            "More threads only produce more 429s. Ask for a higher limit, a bulk export, "
            "or pull incrementally so each run is a few pages.",
            flush=True,
        )
    elif report["bottleneck"] == "rate_limit_if_header_is_per_second":
        print(
            "[diagnose] A rate-limit header is present, but it does not say the window. "
            "If that number is per second, concurrency may already be bumping into it. "
            "If it is per minute, concurrency will make things worse. Confirm the window "
            "before adding threads.",
            flush=True,
        )
    elif report["bottleneck"] == "too_many_round_trips":
        print(
            "[diagnose] Your bottleneck is the number of round trips. Raise the page size "
            "to the documented maximum, drop unused fields, and turn on gzip. Then consider "
            "keyset or partitioned pulls. Do not start with more threads.",
            flush=True,
        )
    else:
        print(
            "[diagnose] Latency per request is the first lever. Reuse connections, raise the "
            "page size, select fields, and only then add a capped number of concurrent "
            "partitioned keyset loops. If you start seeing 429, stop adding threads.",
            flush=True,
        )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Find the bottleneck in a paginated API")
    parser.add_argument("--preset", choices=sorted(PRESETS), default=None)
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--path", default=None)
    parser.add_argument("--page-param", default=None)
    parser.add_argument("--offset-param", default=None)
    parser.add_argument("--page-size", type=int, default=None)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--burst", type=int, default=30)
    parser.add_argument("--probe-rate-limit", action="store_true", default=None)
    parser.add_argument("--force-probe", action="store_true")
    args = parser.parse_args(argv)
    if args.preset is None and args.base_url is None:
        args.preset = "mock"
    diagnose(args)


if __name__ == "__main__":
    main()
