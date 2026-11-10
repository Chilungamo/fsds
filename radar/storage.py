"""Raw filing documents, kept gzipped on disk so every parse can be re-run offline
("raw first, derived second", design doc §3.3)."""

from __future__ import annotations

import gzip
import os
from pathlib import Path

from radar.edgar.urls import normalize_accession

DEFAULT_DIR = Path.home() / ".cache" / "restatement-radar" / "raw"


class RawStore:
    def __init__(self, root: str | Path | None = None) -> None:
        self.root = Path(root or os.environ.get("RADAR_RAW_DIR") or DEFAULT_DIR)

    def path(self, cik: int, accession: str, name: str) -> Path:
        safe = Path(name).name  # never let a document name escape the store
        return self.root / str(int(cik)) / normalize_accession(accession) / f"{safe}.gz"

    def get(self, cik: int, accession: str, name: str) -> bytes | None:
        p = self.path(cik, accession, name)
        return gzip.decompress(p.read_bytes()) if p.exists() else None

    def put(self, cik: int, accession: str, name: str, data: bytes) -> Path:
        p = self.path(cik, accession, name)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_bytes(gzip.compress(data, mtime=0))
        tmp.replace(p)  # atomic: readers never see a half-written file
        return p
