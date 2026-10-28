"""Idempotent dedup and missing-id scans.

Idempotent means you can apply the same page twice and end at the same rows.
The rule used everywhere in this lab: primary key is `id`; if two rows share
an id, keep the one with the greater `updated_at` (string order works because
we store ISO-8601 UTC). That is how a retry, a resume, or an incremental
overlap stays safe.
"""

from __future__ import annotations


def dedup_records(records: list[dict]) -> list[dict]:
    """Keep the newest row for each id. Input order is not preserved."""
    best: dict[int, dict] = {}
    for record in records:
        key = int(record["id"])
        current = best.get(key)
        if current is None or str(record.get("updated_at") or "") >= str(current.get("updated_at") or ""):
            best[key] = record
    return list(best.values())


def find_duplicate_ids(records: list[dict]) -> list[int]:
    """Return ids that appear more than once, in first-seen order."""
    seen: set[int] = set()
    duplicates: list[int] = []
    reported: set[int] = set()
    for record in records:
        key = int(record["id"])
        if key in seen and key not in reported:
            duplicates.append(key)
            reported.add(key)
        seen.add(key)
    return duplicates


def missing_ranges(sorted_ids: list[int], start: int, end: int) -> list[tuple[int, int]]:
    """Inclusive gaps between start and end given a sorted list of seen ids.

    Example: seen [1, 2, 5] over 1..6 returns [(3, 4), (6, 6)].
    """
    if end < start:
        return []
    gaps: list[tuple[int, int]] = []
    cursor = start
    for value in sorted_ids:
        if value < start:
            continue
        if value > end:
            break
        if value > cursor:
            gaps.append((cursor, value - 1))
        cursor = max(cursor, value + 1)
    if cursor <= end:
        gaps.append((cursor, end))
    return gaps


def count_missing(gaps: list[tuple[int, int]]) -> int:
    return sum(stop - start + 1 for start, stop in gaps)
