"""Pull a dataset with one named strategy.

What this file teaches
----------------------
This is the single entry point. Pick a strategy, point it at the mock API
(or later at FDIC), and watch the progress lines. Each strategy is implemented
in fetcher/strategies.py and explained in docs/LEARNING.md.

Examples (from the repo root, with the mock API already running):

    python fetch.py --strategy naive
    python fetch.py --strategy offset --page-size 1000
    python fetch.py --strategy keyset
    python fetch.py --strategy parallel_offset --concurrency 8
    python fetch.py --strategy resilient --run-id resume-demo --rate-limit-rps 8
    python fetch.py --adapter fdic --strategy offset --page-size 10000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from fetcher.models import ClientConfig
from fetcher.strategies import STRATEGIES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pull records from a paginated API")
    parser.add_argument("--strategy", required=True, choices=sorted(STRATEGIES))
    parser.add_argument("--adapter", default="mock", choices=["mock", "fdic"])
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-retries", type=int, default=6)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--partition-by", default="id", choices=["id", "date"])
    parser.add_argument("--rate-limit-rps", type=float, default=0.0)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--out", default="data/landing")
    parser.add_argument("--fields", default=None)
    parser.add_argument("--crash-after-pages", type=int, default=None)
    return parser


def config_from_args(args: argparse.Namespace) -> ClientConfig:
    base_url = args.base_url
    fields = args.fields
    page_size = args.page_size
    if args.adapter == "fdic":
        # Verified against https://api.fdic.gov/banks/docs/swagger.yaml :
        # institutions uses limit/offset, maximum limit 10000, no key required.
        base_url = "https://api.fdic.gov/banks"
        if args.strategy not in {"offset", "parallel_offset", "naive"}:
            print(
                f"[fetch] FDIC BankFind paginates with limit/offset. "
                f"Strategy '{args.strategy}' does not apply. Use offset, "
                "parallel_offset, or naive.",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if page_size > 10_000:
            print("[fetch] FDIC maximum page size is 10000. Clamping.", flush=True)
            page_size = 10_000
        if not fields:
            # A short field list keeps the public API response small. CERT is
            # the institution primary key. Pulling every column is allowed,
            # but it is rude when you do not need them.
            fields = "CERT,NAME,STALP,CITY,STNAME,ACTIVE,DATEUPDT,ASSET,DEP"
    return ClientConfig(
        base_url=base_url,
        page_size=page_size,
        timeout_s=args.timeout,
        max_retries=args.max_retries,
        concurrency=args.concurrency,
        partitions=args.partitions,
        rate_limit_rps=args.rate_limit_rps,
        run_id=args.run_id,
        out_dir=Path(args.out),
        fields=fields,
        crash_after_pages=args.crash_after_pages,
        partition_by=args.partition_by,
        adapter=args.adapter,
    )


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    config = config_from_args(args)
    if args.adapter == "fdic":
        from real_api.fdic import run_fdic

        result = run_fdic(args.strategy, config)
    else:
        result = STRATEGIES[args.strategy](config)
    print(
        f"[fetch] strategy={result.strategy} records={result.records:,} "
        f"pages={result.pages} retries={result.retries} "
        f"elapsed={result.elapsed_s:.1f}s dir={result.run_dir}",
        flush=True,
    )


if __name__ == "__main__":
    main()
