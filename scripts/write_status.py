f#!/usr/bin/env python3
import csv
import json
from pathlib import Path
from datetime import datetime, timezone

OUTPUT = Path("output")
OUTPUT.mkdir(exist_ok=True, parents=True)

# Files we show on the site (order matters for the table)
MANIFEST = [
    {"name": "wordpress_posts.csv", "label": "All blog+Q&A posts"},
    {"name": "crossref_works.csv",   "label": "Crossref-indexed scholarly works"},
    {"name": "openalex_works.csv",   "label": "OpenAlex-identified research works"},
    {"name": "pubmed_eppley.csv",    "label": "Publications (PubMed)"},
    {"name": "youtube_metadata.csv", "label": "YouTube metadata"},
    {"name": "eppley_master.csv",    "label": "Unified master dataset (merged from all sources)"},
]

def count_rows(csv_path: Path) -> int:
    """Count data rows in a CSV (skip header if present)."""
    try:
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
            if not rows:
                return 0
            # If the first row looks like headers (strings), subtract one
            # else, return total length
            if rows and all(isinstance(x, str) for x in rows[0]):
                return max(0, len(rows) - 1)
            return len(rows)
    except Exception:
        return 0

def iso_now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def build_status():
    files = []
    total_records = 0

    for item in MANIFEST:
        name = item["name"]
        label = item["label"]
        p = OUTPUT / name
        exists = p.exists()

        entry = {
            "name": name,
            "label": label,
            "path": str(p),
            "exists": exists,
            "updated_at": None,
            "size_bytes": 0,
            "size_mb": 0.0,
            "rows": 0,
            "new_rows_since_last_run": 0,  # placeholder (we keep simple)
            "raw_url": f"https://raw.githubusercontent.com/jasonab74-ctrl/eppley-collector/main/output/{name}",
            "webpage_url": f"https://github.com/jasonab74-ctrl/eppley-collector/blob/main/output/{name}",
            "download_url": f"https://raw.githubusercontent.com/jasonab74-ctrl/eppley-collector/main/output/{name}",
        }

        if exists:
            try:
                entry["size_bytes"] = p.stat().st_size
                entry["size_mb"] = round(entry["size_bytes"] / (1024 * 1024), 3)
                entry["rows"] = count_rows(p)
                entry["updated_at"] = datetime.utcfromtimestamp(p.stat().st_mtime).replace(tzinfo=timezone.utc).isoformat(timespec="seconds")
                # Only count into total if it's not the master
                if name != "eppley_master.csv":
                    total_records += entry["rows"]
            except Exception:
                pass

        files.append(entry)

    status = {
        "repo": "jasonab74-ctrl/eppley-collector",
        "generated_at": iso_now_utc(),
        "files": files,
        "totals": {
            "records_excluding_master": total_records
        }
    }

    out_path = OUTPUT / "status.json"
    out_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
    print(f"Wrote {out_path} with {len(files)} file entries.")

if __name__ == "__main__":
    build_status()