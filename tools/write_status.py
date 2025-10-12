from datetime import datetime, timezone
from pathlib import Path
import json
import os

OUT = Path("output")
STATUS = OUT / "status.json"

FILES = [
    "wordpress_posts.csv",
    "crossref_works.csv",
    "openalex_works.csv",
    "pubmed_eppley.csv",
    "youtube_all.csv",
    "eppley_master.csv",
]

RAW_BASE = "https://raw.githubusercontent.com/{repo}/main/output/".format(
    repo=os.getenv("GITHUB_REPOSITORY", "jasonab74-ctrl/eppley-collector")
)

def fast_row_count(p: Path) -> int:
    if not p.exists():
        return -1
    # Count lines minus header
    try:
        with p.open("r", encoding="utf-8", errors="ignore") as f:
            n = sum(1 for _ in f)
        return max(0, n - 1)
    except Exception:
        return 0

def main():
    OUT.mkdir(parents=True, exist_ok=True)

    files_map = {}
    for name in FILES:
        p = OUT / name
        rows = fast_row_count(p)
        exists = p.exists()
        size = p.stat().st_size if exists else 0
        status = "skipped" if not exists else ("ok" if rows > 0 else "warn")
        files_map[name] = {
            "rows": max(0, rows),
            "status": status,
            "exists": exists,
            "size_bytes": size,
            "download": RAW_BASE + name
        }

    total = files_map.get("eppley_master.csv", {}).get("rows", 0)
    payload = {
        # preferred by page
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_records": total,
        "files": files_map,
        # extra fields some of your older status consumers expect
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "repo": os.getenv("GITHUB_REPOSITORY", ""),
        "schema": "map"
    }

    with STATUS.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[status] wrote {STATUS}")

if __name__ == "__main__":
    main()
