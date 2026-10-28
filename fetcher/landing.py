"""Land each page on disk immediately, then compact to Parquet.

Why we never hold the full dataset in memory
---------------------------------------------
A million small JSON objects is hundreds of megabytes of Python objects, and
the next dataset will be larger. If the process dies at page 900, a giant list
dies with it. Streaming each page to its own NDJSON file means:

- memory stays flat (one page at a time)
- a crash keeps every page already written
- validation can scan the files without a second pull

Parquet is the compact form analysts want. We write it in batches from the
NDJSON files, still without building one giant list. Dedup happens here, on
primary key, keeping the newest updated_at. The raw NDJSON is left in place
so validation can still see duplicates the API actually sent.
"""

from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

COLUMNS = ("id", "updated_at", "category", "amount", "payload")

ARROW_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("updated_at", pa.string()),
        ("category", pa.string()),
        ("amount", pa.float64()),
        ("payload", pa.string()),
    ]
)


def normalize_record(record: dict) -> dict:
    """Keep a stable column set even if the API omitted a field."""
    return {
        "id": int(record["id"]),
        "updated_at": str(record.get("updated_at") or ""),
        "category": str(record.get("category") or ""),
        "amount": float(record["amount"]) if record.get("amount") not in (None, "") else 0.0,
        "payload": str(record.get("payload") or record.get("text") or ""),
    }


class LandingZone:
    """A directory for one run: pages/*.ndjson, data.parquet, manifest.json."""

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = Path(run_dir)
        self.page_dir = self.run_dir / "pages"
        self.page_dir.mkdir(parents=True, exist_ok=True)
        self.raw_records = 0
        self.page_files: list[str] = []
        # Parallel strategies write different page files at the same time.
        # The files themselves do not overlap; this lock only protects the
        # bookkeeping lists.
        self._lock = threading.Lock()

    def write_page(self, key: str, records: list[dict]) -> Path:
        """Write one page. The key must be stable across a resume (offset or id)."""
        safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in key)
        path = self.page_dir / f"page_{safe}.ndjson"
        lines = []
        for record in records:
            normalized = normalize_record(record)
            lines.append(json.dumps(normalized, separators=(",", ":")))
        # Overwrite, do not append. A resumed page must not duplicate itself.
        existed = path.exists()
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        rel = str(path.relative_to(self.run_dir)).replace("\\", "/")
        with self._lock:
            if rel not in self.page_files:
                self.page_files.append(rel)
            if not existed:
                self.raw_records += len(records)
        return path

    @property
    def checksum(self) -> str:
        """Hash page files in name order so parallel writes still agree."""
        digest = hashlib.sha256()
        for path in sorted(self.page_dir.glob("page_*.ndjson")):
            digest.update(path.name.encode("utf-8"))
            digest.update(path.read_bytes())
        return digest.hexdigest()

    def iter_records(self):
        """Stream every landed record. Used by compaction and validation."""
        for path in sorted(self.page_dir.glob("page_*.ndjson")):
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        yield json.loads(line)

    def compact_parquet(self, dedup: bool = True, batch_size: int = 5_000) -> tuple[Path, int]:
        """Write data.parquet. Returns (path, row_count_written)."""
        path = self.run_dir / "data.parquet"
        written = 0
        seen: dict[int, dict] = {}
        if dedup:
            # One million integer keys plus the winning row is acceptable, and
            # it is still far smaller than holding every duplicate page too.
            # We then stream the winners out in batches.
            for record in self.iter_records():
                current = seen.get(record["id"])
                if current is None or str(record["updated_at"]) >= str(current["updated_at"]):
                    seen[record["id"]] = record
            records = seen.values()
        else:
            records = self.iter_records()

        def batches(source):
            batch: list[dict] = []
            for record in source:
                batch.append(record)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

        writer: pq.ParquetWriter | None = None
        try:
            for batch in batches(records):
                table = pa.Table.from_pylist(batch, schema=ARROW_SCHEMA)
                if writer is None:
                    writer = pq.ParquetWriter(path, ARROW_SCHEMA, compression="zstd")
                writer.write_table(table)
                written += len(batch)
        finally:
            if writer is not None:
                writer.close()
        if writer is None:
            # No rows. Still leave a valid empty file so downstream tools do not
            # have to special-case a missing path.
            pq.write_table(ARROW_SCHEMA.empty_table(), path, compression="zstd")
        return path, written

    def write_manifest(self, payload: dict) -> Path:
        body = {
            "written_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            **payload,
            "checksum_sha256": self.checksum,
            "page_files": self.page_files,
        }
        path = self.run_dir / "manifest.json"
        path.write_text(json.dumps(body, indent=2), encoding="utf-8")
        return path
