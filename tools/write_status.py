"""
Creates output/status.json with:
{
  "updated_at": "2025-10-10T23:11:18Z",
  "total_records": 2887,
  "files": {
    "wordpress_posts.csv": {"rows": 312, "status": "ok"},
    ...
    "eppley_corpus.jsonl": {"exists": true, "status": "ok"}
  }
}
Rules:
- CSV present with rows > 0  -> status = "ok"
- CSV present with rows == 0 -> status = "warn"
- CSV missing                -> status = "skipped"
- corpus JSONL present       -> status = "ok", with exists=true
"""

from __future__ import annotations
import json, os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

OUTPUT = Path("output")
STATUS_F = OUTPUT / "status.json"

# These are the files your UI expects (and in the order you show them)
CSV_FILES = [
    "wordpress_posts.csv",
    "pubmed_eppley.csv",
    "crossref_works.csv",
    "openalex_works.csv",
    "youtube_all.csv",
    "eppley_master.csv",
]

CORPUS_FILE = "eppley_corpus.jsonl"

def csv_rows(path: Path) -> int:
    try:
        if not path.exists():
            return -1  # sentinel for missing
        # Fast path: count lines - 1 header
        # But robustly handle empty files by falling back to pandas when small
        size = path.stat().st_size
        if size > 0:
            # Count lines quickly
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                n = sum(1 for _ in f)
            return max(0, n - 1)
        return 0
    except Exception:
        # Fallback to pandas if needed
        try:
            df = pd.read_csv(path)
            return int(len(df))
        except Exception:
            return 0

def compute_total(master_path: Path) -> int:
    if not master_path.exists():
        return 0
    try:
        # Prefer fast line count
        with master_path.open("r", encoding="utf-8", errors="ignore") as f:
            n = sum(1 for _ in f)
        return max(0, n - 1)
    except Exception:
        try:
            df = pd.read_csv(master_path)
            return int(len(df))
        except Exception:
            return 0

def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)

    files = {}
    for name in CSV_FILES:
        p = OUTPUT / name
        r = csv_rows(p)
        if r == -1:
            status = "skipped"
            r = 0
        else:
            status = "ok" if r > 0 else "warn"
        files[name] = {"rows": r, "status": status}

    # Special case: corpus JSONL is not a CSV (no row count), but we note presence
    corpus_path = OUTPUT / CORPUS_FILE
    files[CORPUS_FILE] = {
        "exists": corpus_path.exists(),
        "status": "ok" if corpus_path.exists() else "skipped"
    }

    total = compute_total(OUTPUT / "eppley_master.csv")
    payload = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_records": total,
        "files": files,
    }

    with STATUS_F.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"write_status: wrote {STATUS_F} with total_records={total}")

if __name__ == "__main__":
    main()