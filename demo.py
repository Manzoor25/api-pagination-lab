"""Short teaching run: every strategy, a real kill/resume, and live-data drift.

This is what `run.ps1 demo` and `make demo` call. It uses a few thousand
rows so you can watch the ideas without waiting for the million-row benchmark.
The benchmark is a separate command and is the source of the README table.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import httpx

from fetcher.models import ClientConfig
from fetcher.strategies import STRATEGIES
from validate import build_report

ROOT = Path(__file__).resolve().parent


def _start(port: int, records: int, **flags: str) -> tuple[subprocess.Popen, str]:
    db = ROOT / "data" / f"demo-{port}.db"
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
        "--failure-rate",
        "0",
        "--timeout-rate",
        "0",
    ]
    for key, value in flags.items():
        flag = "--" + key.replace("_", "-")
        if value == "true":
            cmd.append(flag)
        else:
            cmd.extend([flag, value])
    print("[demo] " + " ".join(cmd), flush=True)
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            if httpx.get(f"{url}/health", timeout=1.0).status_code == 200:
                return proc, url
        except httpx.HTTPError:
            time.sleep(0.1)
    proc.kill()
    raise RuntimeError(f"demo server on {port} did not start")


def _stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()


def _ids(run_dir: Path) -> list[int]:
    found = []
    for path in sorted((run_dir / "pages").glob("*.ndjson")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line:
                found.append(json.loads(line)["id"])
    return found


def main() -> None:
    report: dict = {"steps": []}
    out = ROOT / "data" / "demo"
    proc, url = _start(
        8091,
        5000,
        latency_ms="20",
        rate_limit_rps="0",
        deep_offset_ms_per_1k="0",
    )
    try:
        for name in ("naive", "offset", "cursor", "keyset", "parallel_offset", "partitioned", "bulk_export"):
            config = ClientConfig(
                base_url=url,
                page_size=500,
                out_dir=out / "basic",
                run_id=name,
                concurrency=4,
                partitions=4,
                max_retries=3,
            )
            started = time.perf_counter()
            result = STRATEGIES[name](config)
            report["steps"].append(
                {
                    "name": name,
                    "records": result.records,
                    "pages": result.pages,
                    "elapsed_s": round(time.perf_counter() - started, 3),
                    "retries": result.retries,
                    "notes": result.notes,
                }
            )
        state = out / "incremental" / "state" / "watermark.json"
        if state.exists():
            state.unlink()
        first = STRATEGIES["incremental"](
            ClientConfig(base_url=url, page_size=500, out_dir=out / "incremental", run_id="first")
        )
        touched = httpx.post(f"{url}/admin/touch", params={"n": 40}, timeout=30.0)
        touched.raise_for_status()
        second = STRATEGIES["incremental"](
            ClientConfig(base_url=url, page_size=500, out_dir=out / "incremental", run_id="second")
        )
        report["incremental"] = {
            "first_records": first.records,
            "touched": touched.json()["touched"],
            "second_records": second.records,
            "second_elapsed_s": round(second.elapsed_s, 3),
        }
        print(
            f"[demo] incremental first={first.records} second={second.records} "
            f"(touched {touched.json()['touched']})",
            flush=True,
        )
    finally:
        _stop(proc)

    live, live_url = _start(
        8092,
        300,
        latency_ms="0",
        rate_limit_rps="0",
        deep_offset_ms_per_1k="0",
        live_data="true",
        live_every="1",
        max_page_size="20",
    )
    try:
        offset = STRATEGIES["offset"](
            ClientConfig(base_url=live_url, page_size=20, out_dir=out / "live", run_id="offset")
        )
        keyset = STRATEGIES["keyset"](
            ClientConfig(base_url=live_url, page_size=20, out_dir=out / "live", run_id="keyset")
        )
        offset_ids = _ids(Path(offset.run_dir))
        keyset_ids = _ids(Path(keyset.run_dir))
        drift = httpx.get(f"{live_url}/drift", timeout=30.0).json()
        report["live"] = {
            "offset_raw": len(offset_ids),
            "offset_unique": len(set(offset_ids)),
            "offset_duplicates": len(offset_ids) - len(set(offset_ids)),
            "keyset_raw": len(keyset_ids),
            "keyset_unique": len(set(keyset_ids)),
            "keyset_duplicates": len(keyset_ids) - len(set(keyset_ids)),
            "drift_events": len(drift["events"]),
        }
        print(f"[demo] live drift {json.dumps(report['live'])}", flush=True)
    finally:
        _stop(live)

    resume_proc, resume_url = _start(
        8093,
        2000,
        latency_ms="0",
        rate_limit_rps="0",
        deep_offset_ms_per_1k="0",
        max_page_size="100",
    )
    fetch_cmd = [
        sys.executable,
        "fetch.py",
        "--strategy",
        "resilient",
        "--base-url",
        resume_url,
        "--page-size",
        "100",
        "--run-id",
        "kill-demo",
        "--out",
        str(out / "resume"),
        "--rate-limit-rps",
        "0",
    ]
    killed = subprocess.Popen(fetch_cmd, cwd=ROOT)
    checkpoint = out / "resume" / "resilient" / "kill-demo" / "checkpoint.json"
    deadline = time.time() + 30
    while time.time() < deadline:
        if checkpoint.exists():
            saved = json.loads(checkpoint.read_text(encoding="utf-8"))
            if saved["pages"] >= 3:
                break
        time.sleep(0.05)
    # Hard kill. No KeyboardInterrupt handler runs. The checkpoint on disk is
    # the only reason the next process can continue.
    killed.kill()
    killed.wait(timeout=10)
    before = json.loads(checkpoint.read_text(encoding="utf-8"))
    resumed = subprocess.check_output(fetch_cmd, cwd=ROOT, text=True)
    after_ids = _ids(out / "resume" / "resilient" / "kill-demo")
    report["resume"] = {
        "killed_after_pages": before["pages"],
        "killed_after_id": before["last_id"],
        "resumed_unique": len(set(after_ids)),
        "resumed_raw": len(after_ids),
        "duplicates": len(after_ids) - len(set(after_ids)),
        "tail": resumed.strip().splitlines()[-3:],
    }
    print(f"[demo] resume {json.dumps(report['resume'])}", flush=True)
    _stop(resume_proc)

    limited, limited_url = _start(
        8094,
        2000,
        latency_ms="0",
        rate_limit_rps="5",
        deep_offset_ms_per_1k="0",
        max_page_size="100",
    )
    try:
        rushed = STRATEGIES["parallel_offset"](
            ClientConfig(
                base_url=limited_url,
                page_size=100,
                out_dir=out / "ratelimit",
                run_id="rushed",
                concurrency=20,
                rate_limit_rps=0,
                max_retries=8,
            )
        )
        polite = STRATEGIES["resilient"](
            ClientConfig(
                base_url=limited_url,
                page_size=100,
                out_dir=out / "ratelimit",
                run_id="polite",
                rate_limit_rps=4,
                max_retries=8,
            )
        )
        report["rate_limit"] = {
            "server_rps": 5,
            "rushed_concurrency": 20,
            "rushed_retries": rushed.retries,
            "rushed_records": rushed.records,
            "polite_client_rps": 4,
            "polite_retries": polite.retries,
            "polite_records": polite.records,
        }
        print(f"[demo] rate limit {json.dumps(report['rate_limit'])}", flush=True)
    finally:
        _stop(limited)

    # Confirm the basic offset pull matched the server count.
    offset_step = next(step for step in report["steps"] if step["name"] == "offset")
    report["offset_validation_note"] = (
        f"offset landed {offset_step['records']} of 5000 on the quiet server"
    )
    quiet = build_report(out / "basic" / "offset" / "offset", 5000, assume_dense=True)
    report["quiet_offset_ok"] = quiet["ok"]
    dest = ROOT / "results" / "demo.json"
    dest.parent.mkdir(exist_ok=True)
    dest.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"[demo] wrote {dest}", flush=True)


if __name__ == "__main__":
    main()
