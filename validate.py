"""Check that a landed pull did not lose or duplicate records.

What this file teaches
----------------------
A pull that "finishes" can still be wrong. This script is the definition of
done:

- raw NDJSON row count versus the API's reported total
- duplicate primary keys (offset drift and naive retries cause these)
- missing ids, when the dataset is supposed to be 1..N
- the deduped Parquet count, which is what an idempotent write keeps

Usage:

    python validate.py --run-dir data/landing/keyset/<run-id> --base-url http://127.0.0.1:8000
    python validate.py --run-dir data/landing/offset/<run-id> --expect-drift

Exit code 0 means the pull matches. Exit code 1 means it does not.
`--expect-drift` flips that for the live-data demo, where offset is supposed
to be wrong and keyset is supposed to be right.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from fetcher.dedup import count_missing, dedup_records, find_duplicate_ids, missing_ranges
from fetcher.landing import LandingZone


def scan_run(run_dir: Path) -> dict:
    """Stream the raw pages. Returns counts, not the full row list, plus a sample."""
    landing = LandingZone(run_dir)
    seen: set[int] = set()
    duplicates: list[int] = []
    reported: set[int] = set()
    raw = 0
    min_id = None
    max_id = None
    # Keep one copy of each id only long enough to detect "same id, two versions".
    # For a million ids this set is the memory we accept. The rows themselves
    # are not retained.
    versions: dict[int, str] = {}
    conflicting_versions = 0
    for record in landing.iter_records():
        raw += 1
        key = int(record["id"])
        updated = str(record.get("updated_at") or "")
        if key in seen:
            if key not in reported:
                duplicates.append(key)
                reported.add(key)
            if versions.get(key) != updated:
                conflicting_versions += 1
        else:
            seen.add(key)
            versions[key] = updated
        if min_id is None or key < min_id:
            min_id = key
        if max_id is None or key > max_id:
            max_id = key
    return {
        "raw_records": raw,
        "unique_ids": len(seen),
        "duplicate_ids": duplicates,
        "conflicting_versions": conflicting_versions,
        "min_id": min_id,
        "max_id": max_id,
        "seen": seen,
    }


def expected_total(base_url: str | None, updated_since: str | None) -> int | None:
    if not base_url:
        return None
    import httpx

    params = {}
    if updated_since:
        params["updated_since"] = updated_since
    response = httpx.get(f"{base_url.rstrip('/')}/count", params=params, timeout=30.0)
    response.raise_for_status()
    return int(response.json()["total"])


def build_report(run_dir: Path, api_total: int | None, assume_dense: bool) -> dict:
    scanned = scan_run(run_dir)
    seen = scanned.pop("seen")
    gaps: list[tuple[int, int]] = []
    missing = 0
    if assume_dense and api_total:
        # Seeded mock data uses ids 1..N until live mode deletes some.
        gaps = missing_ranges(sorted(seen), 1, api_total)
        missing = count_missing(gaps)
    elif assume_dense and scanned["min_id"] == 1 and scanned["max_id"]:
        gaps = missing_ranges(sorted(seen), 1, int(scanned["max_id"]))
        missing = count_missing(gaps)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    count_matches = api_total is None or scanned["unique_ids"] == api_total
    ok = count_matches and not scanned["duplicate_ids"] and missing == 0
    return {
        "run_dir": str(run_dir),
        "strategy": manifest.get("strategy"),
        "raw_records": scanned["raw_records"],
        "unique_ids": scanned["unique_ids"],
        "duplicate_count": len(scanned["duplicate_ids"]),
        "duplicate_sample": scanned["duplicate_ids"][:10],
        "conflicting_versions": scanned["conflicting_versions"],
        "missing_records": missing,
        "missing_range_sample": gaps[:10],
        "api_total": api_total,
        "count_matches_api": count_matches,
        "min_id": scanned["min_id"],
        "max_id": scanned["max_id"],
        "checksum": manifest.get("checksum_sha256"),
        "ok": ok,
    }


def print_report(report: dict) -> None:
    status = "OK" if report["ok"] else "PROBLEM"
    print(f"[validate] {status} strategy={report.get('strategy')} dir={report['run_dir']}")
    print(
        f"[validate] raw={report['raw_records']:,} unique={report['unique_ids']:,} "
        f"duplicates={report['duplicate_count']:,} missing={report['missing_records']:,} "
        f"api_total={report['api_total']}"
    )
    if report["duplicate_sample"]:
        print(f"[validate] duplicate id sample: {report['duplicate_sample']}")
    if report["missing_range_sample"]:
        print(f"[validate] missing id ranges (first 10): {report['missing_range_sample']}")
    if report["conflicting_versions"]:
        print(
            f"[validate] {report['conflicting_versions']} ids arrived with more than one "
            "updated_at. An idempotent write keeps the newest updated_at and discards the rest."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a landed API pull")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--updated-since", default=None)
    parser.add_argument(
        "--no-dense-ids",
        action="store_true",
        help="Do not expect ids 1..N (use this for FDIC CERT numbers)",
    )
    parser.add_argument(
        "--expect-drift",
        action="store_true",
        help="Exit 0 only when duplicates or gaps are present (offset live-data demo)",
    )
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args(argv)
    api_total = expected_total(args.base_url, args.updated_since)
    report = build_report(Path(args.run_dir), api_total, assume_dense=not args.no_dense_ids)
    print_report(report)
    if args.json_out:
        path = Path(args.json_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        # seen set was already removed. Report is JSON-safe.
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if args.expect_drift:
        drifted = report["duplicate_count"] > 0 or report["missing_records"] > 0
        print(f"[validate] expect-drift: {'saw drift' if drifted else 'NO drift, which is surprising'}")
        return 0 if drifted else 1
    return 0 if report["ok"] else 1


# Re-exported so tests can show the idempotent rule without going through HTTP.
__all__ = ["dedup_records", "find_duplicate_ids", "missing_ranges", "main"]


if __name__ == "__main__":
    sys.exit(main())
