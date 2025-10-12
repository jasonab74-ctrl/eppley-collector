from datetime import datetime, timezone
from pathlib import Path
import json
import os
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
        # Fast counter (lines - 1 for header)
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
    print(json.dumps(payload, indent=2))

    # Optional hard-stop to prevent publishing an all-zero dataset.
    if os.getenv("FAIL_IF_ALL_ZERO") == "1":
        only_csvs = [k for k in files_status.keys() if k.endswith(".csv")]
        all_zero = all(files_status[k]["rows"] == 0 for k in only_csvs)
        if all_zero:
            raise SystemExit("[status] All datasets are empty; failing run to avoid publishing zeros.")

if __name__ == "__main__":
    main()
