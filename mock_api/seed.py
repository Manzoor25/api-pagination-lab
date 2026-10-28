"""Build the synthetic dataset the mock API serves.

What this file teaches
----------------------
A pagination lab is useless if the data changes every run, so seeding is
deterministic: the same seed and the same row count always produce the same
ids, timestamps, categories, amounts, and text payloads.

One million small rows is enough to make deep OFFSET painful and to make
"hold everything in a list" a bad idea, without needing a real external API.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Eight stable categories. Row i always lands in the same bucket.
CATEGORIES = (
    "billing",
    "support",
    "shipping",
    "fraud",
    "signup",
    "refund",
    "audit",
    "notice",
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY,
    updated_at TEXT NOT NULL,
    category TEXT NOT NULL,
    amount REAL NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS drift_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    action TEXT NOT NULL,
    record_id INTEGER,
    detail TEXT
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open SQLite with settings that are safe for many short page queries."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL lets a reader (a page query) and a writer (live-data mode) coexist.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def _date_prefixes() -> list[str]:
    """Precompute YYYY-MM-DD strings so seeding a million rows stays cheap."""
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    # 2024 is a leap year, so 366 prefixes cover every day-of-year we use.
    return [(base + timedelta(days=d)).strftime("%Y-%m-%d") for d in range(366)]


def row_values(i: int, dates: list[str]) -> tuple[int, str, str, float, str]:
    """One deterministic record. No RNG calls, so a million rows stay fast."""
    category = CATEGORIES[i % len(CATEGORIES)]
    # Spread updates across 2024. String order matches time order because the
    # format is ISO-8601 UTC. That matters for updated_since filters.
    updated_at = (
        f"{dates[i % 366]}T{(i // 366) % 24:02d}:{(i // 17) % 60:02d}:{(i // 13) % 60:02d}Z"
    )
    amount = ((i * 37) % 1_000_000) / 100.0
    payload = (
        f"synthetic-record-{i} category={category} seed=42 "
        f"payload-for-pagination-lab padding={i % 9973}"
    )
    return (i, updated_at, category, amount, payload)


def seed_database(
    db_path: str | Path,
    count: int = 1_000_000,
    seed: int = 42,
    force: bool = False,
    batch_size: int = 5_000,
) -> int:
    """Create the SQLite file if needed and return the row count.

    `seed` is stored in the meta table. The row formula does not call
    random(), but we still record the seed so a future change can refuse to
    reuse a database built with different parameters.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        conn = connect(path)
        try:
            existing = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]
            meta_seed = conn.execute("SELECT value FROM meta WHERE key = 'seed'").fetchone()
            meta_count = conn.execute("SELECT value FROM meta WHERE key = 'count'").fetchone()
        except sqlite3.Error:
            existing = -1
            meta_seed = None
            meta_count = None
        finally:
            conn.close()
        same = (
            existing == count
            and meta_seed is not None
            and int(meta_seed["value"]) == seed
            and meta_count is not None
            and int(meta_count["value"]) == count
        )
        if same:
            print(f"[seed] reusing {path} ({existing:,} records, seed={seed})")
            return existing
        print(f"[seed] {path} does not match count={count} seed={seed}; rebuilding")

    if path.exists():
        path.unlink()
    for suffix in ("-wal", "-shm"):
        extra = Path(str(path) + suffix)
        if extra.exists():
            extra.unlink()

    print(f"[seed] writing {count:,} records to {path} (seed={seed})")
    started = time.perf_counter()
    conn = connect(path)
    try:
        # synchronous=OFF is safe here: if seeding crashes we just rebuild.
        conn.execute("PRAGMA synchronous=OFF")
        conn.executescript(SCHEMA)
        dates = _date_prefixes()
        insert = (
            "INSERT INTO records (id, updated_at, category, amount, payload) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        batch: list[tuple[int, str, str, float, str]] = []
        for i in range(1, count + 1):
            batch.append(row_values(i, dates))
            if len(batch) >= batch_size:
                conn.executemany(insert, batch)
                batch.clear()
                if i % 100_000 == 0 or i == count:
                    elapsed = time.perf_counter() - started
                    print(f"[seed] {i:,}/{count:,} rows in {elapsed:.1f}s")
        if batch:
            conn.executemany(insert, batch)
        conn.commit()
        # Index after the load. Building it on the way down is slower.
        print("[seed] creating updated_at index")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_records_updated ON records(updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_records_category ON records(category)")
        conn.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            (
                ("seed", str(seed)),
                ("count", str(count)),
                ("built_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
            ),
        )
        conn.commit()
        # Turn durability back on for the server process that reuses this file.
        conn.execute("PRAGMA synchronous=NORMAL")
        total = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]
    finally:
        conn.close()
    elapsed = time.perf_counter() - started
    print(f"[seed] done: {total:,} records in {elapsed:.1f}s")
    return int(total)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Seed the mock API database")
    parser.add_argument("--db", default="data/mock.db")
    parser.add_argument("--records", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    seed_database(args.db, count=args.records, seed=args.seed, force=args.force)
