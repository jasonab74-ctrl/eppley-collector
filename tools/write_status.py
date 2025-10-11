from datetime import datetime, timezone
from pathlib import Path
import json
import pandas as pd

OUTDIR = Path("output")
STATUS_F = OUTDIR / "status.json"

FILES = [
    "wordpress_posts.csv",
    "crossref_works.csv",
    "openalex_works.csv",
    "pubmed_eppley.csv",
    "youtube_all.csv",
    "eppley_master.csv",
]

def rows_of(path: Path) -> int:
    if not path.exists():
        return -1
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            count = sum(1 for _ in f)
        return max(0, count - 1)
    except Exception:
        try:
            return len(pd.read_csv(path))
        except Exception:
            return 0

def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    files_status = {}
    for name in FILES:
        p = OUTDIR / name
        r = rows_of(p)
        files_status[name] = {
            "rows": 0 if r < 0 else r,
            "status": "skipped" if r < 0 else ("ok" if r > 0 else "warn")
        }
    total_records = rows_of(OUTDIR / "eppley_master.csv")
    if total_records < 0:
        total_records = 0
    payload = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_records": total_records,
        "files": files_status,
    }
    with STATUS_F.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[status] wrote {STATUS_F}")

if __name__ == "__main__":
    main()