"""Time every strategy and write the results down.

What this file teaches
----------------------
A benchmark that invents numbers is useless. This script starts the mock API,
runs each strategy, validates the landing zone, and writes:

- results/benchmark.md
- results/benchmark.json
- results/benchmark.png

Two modes:

- quick: 100,000 records, so you can see the shape of the table in a minute
- full:  1,000,000 records, the number the README quotes

The measurement profile turns injected failures and the default 300 ms latency
off. Those are real behaviors, and diagnose.py / the painful sample measure
them. Mixing a 2% failure rate into a timing table hides the pagination cost
under retries. The settings used are printed in the report so the numbers
cannot be mistaken for the default painful server.

    python benchmark.py --mode quick
    python benchmark.py --mode full
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import httpx
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from fetcher.models import ClientConfig
from fetcher.strategies import STRATEGIES
from validate import build_report

ROOT = Path(__file__).resolve().parent
ORDER = [
    "naive",
    "offset",
    "cursor",
    "keyset",
    "parallel_offset",
    "partitioned",
    "resilient",
    "incremental",
    "bulk_export",
]


def _wait_for(url: str, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    last_error = "not started"
    while time.time() < deadline:
        try:
            response = httpx.get(url, timeout=2.0)
            if response.status_code == 200:
                return
            last_error = f"HTTP {response.status_code}"
        except httpx.HTTPError as exc:
            last_error = str(exc)
        time.sleep(0.2)
    raise RuntimeError(f"server did not become healthy: {last_error}")


def start_server(port: int, records: int, db: Path, extra: list[str]) -> subprocess.Popen:
    if records != 1_000_000 or not db.exists():
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "mock_api.seed",
                "--db",
                str(db),
                "--records",
                str(records),
            ],
            cwd=ROOT,
        )
    cmd = [
        sys.executable,
        "-m",
        "mock_api.server",
        "--db",
        str(db),
        "--records",
        str(records),
        "--port",
        str(port),
        "--host",
        "127.0.0.1",
        "--latency-ms",
        "0",
        "--failure-rate",
        "0",
        "--timeout-rate",
        "0",
        "--rate-limit-rps",
        "0",
        "--deep-offset-ms-per-1k",
        "0",
        *extra,
    ]
    print("[benchmark] " + " ".join(cmd), flush=True)
    log_path = ROOT / "data" / f"server-{port}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log_handle, stderr=subprocess.STDOUT)
    proc._lab_log = log_handle  # closed by the caller after wait
    return proc


def run_mode(mode: str, port: int, only: list[str] | None = None) -> dict:
    records = 100_000 if mode == "quick" else 1_000_000
    db = ROOT / "data" / f"bench_{mode}.db"
    if mode == "full":
        db = ROOT / "data" / "mock.db"
    proc = start_server(port, records, db, [])
    base = f"http://127.0.0.1:{port}"
    rows = []
    try:
        _wait_for(f"{base}/health")
        out = ROOT / "data" / "benchmark" / mode
        # A leftover watermark would turn "incremental" into an empty delta
        # and the table would lie about the full load.
        watermark = out / "state" / "watermark.json"
        if watermark.exists():
            watermark.unlink()
        for name in (only or ORDER):
            print(f"\n[benchmark] === {name} ===", flush=True)
            config = ClientConfig(
                base_url=base,
                page_size=1000,
                timeout_s=120,
                max_retries=4,
                concurrency=8,
                partitions=8,
                rate_limit_rps=0,
                run_id=f"{mode}-{name}",
                out_dir=out,
            )
            started = time.perf_counter()
            result = STRATEGIES[name](config)
            elapsed = time.perf_counter() - started
            report = build_report(
                Path(result.run_dir),
                records if name != "naive" else None,
                assume_dense=name != "naive",
            )
            if name == "naive":
                # Naive is supposed to land one page. Do not call that a failure.
                report["ok"] = result.records == 1000 or result.records == min(1000, records)
                report["missing_records"] = records - result.records
            rps = result.records / elapsed if elapsed else 0
            row = {
                "strategy": name,
                "elapsed_s": round(elapsed, 2),
                "pages": result.pages,
                "records": result.records,
                "requests": result.requests,
                "retries": result.retries,
                "records_per_second": round(rps, 1),
                "duplicates": report["duplicate_count"],
                "missing": report["missing_records"] if name != "naive" else records - result.records,
                "unique_ids": report["unique_ids"],
                "ok": report["ok"] if name != "naive" else True,
                "run_dir": result.run_dir,
                "notes": result.notes,
            }
            rows.append(row)
            print(
                f"[benchmark] {name}: {row['elapsed_s']}s records={row['records']:,} "
                f"rps={row['records_per_second']} dupes={row['duplicates']} missing={row['missing']}",
                flush=True,
            )
    finally:
        _stop(proc)
    return {
        "mode": mode,
        "records": records,
        "page_size": 1000,
        "concurrency": 8,
        "partitions": 8,
        "server": {
            "latency_ms": 0,
            "failure_rate": 0,
            "timeout_rate": 0,
            "rate_limit_rps": 0,
            "deep_offset_ms_per_1k": 0,
            "note": (
                "Injected latency and failures are off so the table measures "
                "pagination and SQLite, not time.sleep. Deep OFFSET is still "
                "slow because SQLite walks skipped rows. See the painful sample "
                "for the default 300 ms server."
            ),
        },
        "rows": rows,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def write_outputs(payload: dict) -> None:
    results = ROOT / "results"
    results.mkdir(exist_ok=True)
    (results / "benchmark.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = [
        "# Benchmark results",
        "",
        "Generated "
        + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        + " by `benchmark.py`. Table numbers were measured. The painful-sample projection is labeled as a calculation.",
        "",
        "Server profile for the table: latency 0 ms, failure rate 0, rate limit off, page size 1,000.",
        "Deep offset is still slow because SQLite has to walk skipped rows. That is the real effect, not a sleep.",
        "",
    ]
    for mode_name in ("quick", "full"):
        block = payload.get(mode_name)
        if not block:
            continue
        lines.append(f"## {mode_name}: {block['records']:,} records")
        lines.append("")
        lines.append("| strategy | seconds | requests | records/sec | retries | duplicates | missing |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for row in block["rows"]:
            lines.append(
                f"| {row['strategy']} | {row['elapsed_s']:.2f} | {row['requests']} | "
                f"{row['records_per_second']:.1f} | {row['retries']} | {row['duplicates']} | {row['missing']} |"
            )
        lines.append("")
    if payload.get("painful_sample"):
        sample = payload["painful_sample"]
        lines.append("## Painful sample (default 300 ms latency)")
        lines.append("")
        lines.append(
            f"Measured {sample['pages']} offset pages at {sample['latency_ms']} ms configured latency: "
            f"{sample['elapsed_s']:.2f}s total, {sample['per_page_s']:.3f}s per page."
        )
        lines.append("")
        lines.append(
            f"Calculated, not measured, full sequential pull of {sample['projected_records']:,} records "
            f"at that per-page cost and page size {sample['page_size']}: "
            f"**{sample['projected_seconds']:.0f} seconds** "
            f"({sample['projected_seconds'] / 60:.1f} minutes), before deep-offset penalty and retries."
        )
        lines.append("")
    (results / "benchmark.md").write_text("\n".join(lines), encoding="utf-8")
    _chart(payload, results / "benchmark.png")
    print(f"[benchmark] wrote {results / 'benchmark.md'}", flush=True)


def _chart(payload: dict, path: Path) -> None:
    block = payload.get("full") or payload.get("quick")
    if not block:
        return
    # Naive is a one-page failure on purpose. Leaving it on the chart squashes
    # the interesting bars, so it is omitted from the picture and kept in the table.
    rows = [row for row in block["rows"] if row["strategy"] != "naive"]
    names = [row["strategy"] for row in rows]
    times = [row["elapsed_s"] for row in rows]
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(names, times, color="#1f6feb")
    ax.set_ylabel("Seconds")
    ax.set_title(f"Pull time for {block['records']:,} records (measured)")
    ax.tick_params(axis="x", rotation=30)
    for bar, value in zip(bars, times):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:.1f}",
            ha="center",
            va="bottom",
            fontsize=8,
        )
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def _stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
    log_handle = getattr(proc, "_lab_log", None)
    if log_handle is not None:
        log_handle.close()


def measure_painful(port: int = 8770) -> dict:
    """20 real pages against a 300 ms server. Small on purpose.

    The product (per-page time × 1,000 pages) is labeled as a calculation.
    It is not a measured pull of a million rows at 300 ms.
    """
    db = ROOT / "data" / "painful.db"
    subprocess.check_call(
        [sys.executable, "-m", "mock_api.seed", "--db", str(db), "--records", "20000"],
        cwd=ROOT,
    )
    cmd = [
        sys.executable,
        "-m",
        "mock_api.server",
        "--db",
        str(db),
        "--records",
        "20000",
        "--port",
        str(port),
        "--latency-ms",
        "300",
        "--failure-rate",
        "0",
        "--timeout-rate",
        "0",
        "--rate-limit-rps",
        "0",
        "--deep-offset-ms-per-1k",
        "0",
        "--max-page-size",
        "1000",
    ]
    proc = subprocess.Popen(cmd, cwd=ROOT)
    try:
        base = f"http://127.0.0.1:{port}"
        _wait_for(f"{base}/health")
        config = ClientConfig(
            base_url=base,
            page_size=1000,
            out_dir=ROOT / "data" / "benchmark" / "painful",
            run_id="painful-offset",
            max_retries=2,
        )
        # Only 20 pages: stop by using a filter? The server has 20,000 rows so
        # page size 1000 is exactly 20 pages. Perfect.
        started = time.perf_counter()
        result = STRATEGIES["offset"](config)
        elapsed = time.perf_counter() - started
        per_page = elapsed / result.pages if result.pages else 0
        projected_pages = math_ceil(1_000_000 / 1000)
        return {
            "pages": result.pages,
            "records": result.records,
            "elapsed_s": round(elapsed, 2),
            "per_page_s": round(per_page, 3),
            "latency_ms": 300,
            "page_size": 1000,
            "projected_records": 1_000_000,
            "projected_pages": projected_pages,
            "projected_seconds": round(per_page * projected_pages, 1),
            "label": "calculated from measured per-page time, not a full 1,000,000-row timed run",
        }
    finally:
        _stop(proc)


def math_ceil(value: float) -> int:
    return int(value) if value == int(value) else int(value) + 1


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Benchmark every fetch strategy")
    parser.add_argument("--mode", choices=["quick", "full", "both"], default="both")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--skip-painful", action="store_true")
    parser.add_argument("--only", default="", help="Comma-separated strategies to run")
    args = parser.parse_args(argv)
    only = [part for part in args.only.split(",") if part] or None
    payload: dict = {}
    modes = ["quick", "full"] if args.mode == "both" else [args.mode]
    for index, mode in enumerate(modes):
        payload[mode] = run_mode(mode, args.port + index, only=only)
    if not args.skip_painful:
        print("\n[benchmark] === painful 300 ms sample ===", flush=True)
        payload["painful_sample"] = measure_painful()
        print(json.dumps(payload["painful_sample"], indent=2), flush=True)
    write_outputs(payload)


if __name__ == "__main__":
    main()
