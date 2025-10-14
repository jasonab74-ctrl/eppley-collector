"""
ORCID Works Collector for Dr. Barry Eppley
------------------------------------------
Fetches all publications from the ORCID Public API and writes to output/orcid_works.csv
Schema: source,title,year,journal,type,DOI,URL,authors
"""

import csv
import requests
from pathlib import Path
from datetime import datetime

OUT = Path("output/orcid_works.csv")
ORCID_ID = "0000-0001-6815-1551"  # Barry Eppley’s verified ORCID ID
BASE = f"https://pub.orcid.org/v3.0/{ORCID_ID}/works"
FIELDS = ["source", "title", "year", "journal", "type", "DOI", "URL", "authors"]

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    headers = {"Accept": "application/json"}
    rows = []

    try:
        resp = requests.get(BASE, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        for group in data.get("group", []):
            summary = group.get("work-summary", [])[0]
            title = (summary.get("title", {}).get("title", {}).get("value") or "").strip()
            year = None
            if summary.get("publication-date"):
                year = summary["publication-date"].get("year", {}).get("value")
            doi = summary.get("external-ids", {}).get("external-id", [])
            doi_val = ""
            url_val = ""
            for d in doi:
                if d.get("external-id-type") == "doi":
                    doi_val = d.get("external-id-value", "")
                if d.get("external-id-type") == "uri":
                    url_val = d.get("external-id-value", "")
            rows.append({
                "source": "orcid",
                "title": title,
                "year": year,
                "journal": "",
                "type": summary.get("type", ""),
                "DOI": doi_val,
                "URL": url_val or summary.get("url", {}).get("value", ""),
                "authors": "Barry L. Eppley"
            })
    except Exception as e:
        print(f"[orcid_works] error: {e}")

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[orcid_works] wrote {len(rows)} rows to {OUT}")

if __name__ == "__main__":
    run()