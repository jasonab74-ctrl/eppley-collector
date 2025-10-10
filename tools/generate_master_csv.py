#!/usr/bin/env python3
"""
Generate unified eppley_master.csv and eppley_master.json
from all collector output files in /output.

- Prefers enriched files like pubmed_eppley_with_abstracts.csv if present.
- Normalizes columns into a standard schema.
- Writes output/eppley_master.csv and output/eppley_master.json.
"""

import csv, json, pathlib
from datetime import datetime

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

ALLOWED_FILES = [
    "pubmed_eppley_with_abstracts.csv",
    "pubmed_eppley.csv",
    "wordpress_posts.csv",
    "crossref_works.csv",
    "openalex_works.csv",
    "clinical_trials.csv",
    "orcid_profiles.csv",
    "orcid_works.csv",
    "youtube_all.csv",
]

MASTER_CSV = OUT / "eppley_master.csv"
MASTER_JSON = OUT / "eppley_master.json"

FIELDS = [
    "source",
    "title",
    "summary",
    "date",
    "link",
    "authors",
    "journal",
    "type",
    "keywords",
]

def normalize_row(source: str, row: dict) -> dict:
    """Normalize heterogeneous row structures into unified schema."""
    return {
        "source": source,
        "title": row.get("title") or row.get("BriefTitle") or row.get("display_name") or "",
        "summary": (
            row.get("abstract")
            or row.get("description")
            or row.get("content")
            or ""
        )[:2000],
        "date": row.get("year") or row.get("publication_date") or row.get("StartDate") or "",
        "link": row.get("url") or row.get("openalex_url") or row.get("link") or "",
        "authors": row.get("authors") or row.get("authorships") or row.get("author") or "",
        "journal": row.get("journal") or row.get("host_venue_name") or row.get("institution") or "",
        "type": row.get("type") or row.get("type_display_name") or "",
        "keywords": row.get("concepts") or row.get("tags") or "",
    }

def merge():
    records = []
    for f in ALLOWED_FILES:
        path = OUT / f
        if not path.exists():
            continue
        source = f.replace(".csv", "")
        with path.open(encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                records.append(normalize_row(source, row))

    # Write CSV
    with MASTER_CSV.open("w", encoding="utf-8", newline="") as outcsv:
        writer = csv.DictWriter(outcsv, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)

    # Write JSON
    with MASTER_JSON.open("w", encoding="utf-8") as outjson:
        json.dump(records, outjson, ensure_ascii=False, indent=2)

    print(f"[OK] Merged {len(records)} rows into {MASTER_CSV.name} and {MASTER_JSON.name}")

    # Also write a lightweight status.json for the site
    status = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "records": len(records),
        "sources": [f for f in ALLOWED_FILES if (OUT / f).exists()],
    }
    with (OUT / "status.json").open("w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

if __name__ == "__main__":
    merge()
