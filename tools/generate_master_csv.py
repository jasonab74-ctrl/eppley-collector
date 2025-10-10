#!/usr/bin/env python3
"""
Merge all source CSVs into output/eppley_master.{csv,json}
Also writes output/status.json (timestamp, totals per file).
Prefers enriched PubMed abstracts when available.
"""
import csv, json, pathlib
from datetime import datetime

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

ALLOWED = [
    "pubmed_eppley_with_abstracts.csv",  # preferred
    "pubmed_eppley.csv",
    "wordpress_posts.csv",
    "crossref_works.csv",
    "openalex_works.csv",
    "clinical_trials.csv",
    "orcid_profiles.csv",
    "orcid_works.csv",
    "youtube_all.csv",
]

FIELDS = ["source","title","summary","date","link","authors","journal","type","keywords"]

MASTER_CSV  = OUT / "eppley_master.csv"
MASTER_JSON = OUT / "eppley_master.json"
STATUS_JSON = OUT / "status.json"

def normalize(source: str, row: dict) -> dict:
    return {
        "source": source,
        "title":   row.get("title") or row.get("BriefTitle") or row.get("display_name") or "",
        "summary": (row.get("abstract") or row.get("description") or row.get("content") or "")[:2000],
        "date":    row.get("year") or row.get("publication_date") or row.get("StartDate") or "",
        "link":    row.get("url") or row.get("openalex_url") or row.get("link") or "",
        "authors": row.get("authors") or row.get("authorships") or row.get("author") or "",
        "journal": row.get("journal") or row.get("host_venue_name") or row.get("institution") or "",
        "type":    row.get("type") or row.get("type_display_name") or "",
        "keywords":row.get("concepts") or row.get("tags") or "",
    }

def count_rows(path: pathlib.Path) -> int:
    try:
        with path.open(encoding="utf-8", newline="") as f:
            return max(0, sum(1 for _ in f) - 1)  # minus header
    except FileNotFoundError:
        return 0

def merge():
    records = []
    per_file = {}
    for name in ALLOWED:
        p = OUT / name
        if not p.exists():
            per_file[name] = 0
            continue
        with p.open(encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                records.append(normalize(name.replace(".csv",""), row))
        per_file[name] = count_rows(p)

    with MASTER_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(records)

    with MASTER_JSON.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    status = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "records": len(records),
        "files": per_file,
    }
    with STATUS_JSON.open("w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    print(f"[MERGE] {len(records)} rows → {MASTER_CSV.name}, {MASTER_JSON.name}")
    print(f"[STATUS] wrote {STATUS_JSON.name}: {status}")

if __name__ == "__main__":
    merge()
