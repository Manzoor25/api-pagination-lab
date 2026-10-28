"""Checkpoint files so a killed run can resume without duplicating pages.

The important idea: write the page file first, then write the checkpoint.
If the process dies between those two steps, the next run overwrites the same
page file (the name is the cursor, not a counter) and continues. Hard-killing
the process still works, because we do not depend on a clean Ctrl+C handler.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class Checkpoint:
    strategy: str
    last_id: int
    pages: int
    records: int
    requests: int
    retries: int
    run_dir: str

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        payload = json.dumps(asdict(self), indent=2)
        temporary.write_text(payload, encoding="utf-8")
        # os.replace is atomic on the same volume, so a crash mid-write cannot
        # leave a half-written checkpoint that the next run trusts. On Windows
        # a scanner can briefly lock the destination, so retry, then fall back
        # to a direct write. Losing one checkpoint rename must not abort a
        # million-row pull.
        for attempt in range(6):
            try:
                temporary.replace(path)
                return
            except PermissionError:
                time.sleep(0.05 * (attempt + 1))
        path.write_text(payload, encoding="utf-8")
        try:
            temporary.unlink()
        except OSError:
            pass

    @classmethod
    def load(cls, path: Path) -> Checkpoint | None:
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            strategy=data["strategy"],
            last_id=int(data["last_id"]),
            pages=int(data["pages"]),
            records=int(data["records"]),
            requests=int(data.get("requests", 0)),
            retries=int(data.get("retries", 0)),
            run_dir=data["run_dir"],
        )
