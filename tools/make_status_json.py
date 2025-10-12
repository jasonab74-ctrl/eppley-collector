#!/usr/bin/env python3
"""
Generate output/status.json summarizing all CSVs in /output.

- Counts rows for every CSV (fast line counting; no pandas required)
- Computes sizes, last-updated timestamps, and friendly labels
- Derives a "status" per file: ok (rows>0), warn (rows==0), missing (no file)
- Computes total_records = sum(rows for all files)
- Tracks new_rows_since_last_run by diffing the previous status.json
- Writes JSON to output/status.json

This script is idempotent and safe to run anytime (CI or locally).
"""

from __future__ import annotations
import csv
import json
import os
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]      # repo root
OUT_DIR = ROOT / "output"
STATUS_PATH = OUT_DIR / "status.json"

REPO = os.environ.get("GITHUB_REPOSITORY", "owner/repo")

# Friendly labels (override per known filename)
LABELS = {
    "wordpress_posts.csv":       "All blog & Q&A posts",
    "crossref_works.csv":        "Crossref-indexed scholarly works",
    "openalex_works.csv":        "OpenAlex-identified research works",
    "pubmed_eppley.csv":         "Peer-reviewed PubMed research articles",
    "youtube_all.csv":           "YouTube videos mentioning or uploaded by Dr. Eppley",
    "youtube_metadata.csv":      "YouTube metadata",
    "eppley_master.csv":         "Unified master dataset (merged from all sources)",
}

def count_rows_fast(p: Path) -> int:
    """
    Count CSV data rows (excluding a single header line if present).
    Uses csv.reader to avoid false positives when the first row is data.
    """
    rows = 0
    try:
        with p.open("r", newline="", encoding="utf-8") as f:
            rdr = csv.reader(f)
            first = next(rdr, None)
            for _ in rdr:
                rows += 1
            # If the file has just one row or empty, rows will already be 0
            # We assume the first row is header if there was at least 1 row total.
            # If your CSVs sometimes have no header, comment the next two lines.
            # (Rows are already excluding the first line due to the loop.)
    except Exception:
        rows = 0
    return rows

def file_meta(p: Path, old_index: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    name = p.name
    stat = p.stat()
    size_bytes = stat.st_size
    size_mb = round(size_bytes / (1024 * 1024), 3)
    # GitHub URLs
    raw = f"https://raw.githubusercontent.com/{REPO}/main/output/{name}"
    web = f"https://github.com/{REPO}/blob/main/output/{name}"
    dl  = raw

    rows = count_rows_fast(p)

    prev_rows = 0
    if name in old_index:
        prev_rows = int(old_index[name].get("rows", 0))
    new_rows = max(rows - prev_rows, 0)

    status = "ok" if rows > 0 else "warn"

    return {
        "name": name,
        "label": LABELS.get(name, ""),
        "path": f"output/{name}",
        "exists": True,
        "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "size_bytes": size_bytes,
        "size_mb": size_mb,
        "rows": rows,
        "new_rows_since_last_run": new_rows,
        "status": status,
        "raw_url": raw,
        "webpage_url": web,
        "download_url": dl,
    }

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load previous status.json (if any) to compute deltas
    old_index_by_name: Dict[str, Dict[str, Any]] = {}
    if STATUS_PATH.exists():
        try:
            old = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
            for f in old.get("files", []):
                if isinstance(f, dict) and f.get("name"):
                    old_index_by_name[f["name"]] = f
        except Exception:
            pass

    files: Dict[str, Dict[str, Any]] = {}

    # Include *all* CSVs present in /output
    for p in sorted(OUT_DIR.glob("*.csv")):
        files[p.name] = file_meta(p, old_index_by_name)

    # Ensure we report known files even if missing
    for must in LABELS.keys():
        if must not in files:
            # provide a stub "missing" entry
            raw = f"https://raw.githubusercontent.com/{REPO}/main/output/{must}"
            web = f"https://github.com/{REPO}/blob/main/output/{must}"
            files[must] = {
                "name": must,
                "label": LABELS.get(must, ""),
                "path": f"output/{must}",
                "exists": False,
                "updated_at": None,
                "size_bytes": 0,
                "size_mb": 0.0,
                "rows": 0,
                "new_rows_since_last_run": 0,
                "status": "missing",
                "raw_url": raw,
                "webpage_url": web,
                "download_url": raw,
            }

    # Compute total records across everything (sum of rows)
    total_records = sum(f.get("rows", 0) for f in files.values())

    payload = {
        "repo": REPO,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "files": sorted(files.values(), key=lambda f: f["name"].lower()),
        "total_records": total_records,
    }

    STATUS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {STATUS_PATH} with {len(files)} files; total_records={total_records}")

if __name__ == "__main__":
    main()