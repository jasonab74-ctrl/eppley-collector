from datetime import datetime, timezone
from pathlib import Path
import json, pandas as pd

OUTDIR = Path("output")
STATUS = OUTDIR / "status.json"

FILES = [
    "wordpress_posts.csv",
    "crossref_works.csv",
    "openalex_works.csv",
    "pubmed_eppley.csv",
    "youtube_all.csv",
    "eppley_master.csv",
]

def rows_of(p: Path) -> int:
    if not p.exists():
        return -1
    try:
        with p.open("r", encoding="utf-8", errors="ignore") as f:
            n = sum(1 for _ in f)
        return max(0, n - 1)
    except Exception:
        try:
            return len(pd.read_csv(p))
        except Exception:
            return 0

def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    files = {}
    for name in FILES:
        p = OUTDIR / name
        r = rows_of(p)
        if r == -1:
            files[name] = {"rows": 0, "status": "skipped"}
        else:
            files[name] = {"rows": r, "status": "ok" if r > 0 else "warn"}
    total = rows_of(OUTDIR / "eppley_master.csv")
    payload = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_records": max(0, total),
        "files": files,
    }
    with STATUS.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[status] wrote {STATUS}")

if __name__ == "__main__":
    main()