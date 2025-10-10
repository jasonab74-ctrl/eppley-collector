#!/usr/bin/env python3
"""
Eppley Collector — Final Unified Version
----------------------------------------
Runs all collectors, writes individual CSVs,
merges them into eppley_master.csv and eppley_master.json.
100% GitHub-Actions safe (no tokens required except YT_API_KEY).
"""

import csv, json, os, sys, time, traceback, importlib, pathlib

ROOT = pathlib.Path(__file__).resolve().parent
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

PIPELINE = [
    ("collectors.wordpress", "run_wp", {}),
    ("collectors.pubmed", "run_pubmed", {}),
    ("collectors.crossref", "run_crossref", {}),
    ("collectors.openalex", "run_openalex", {}),
    ("collectors.clinical_trials", "run_ct", {}),
    ("collectors.orcid", "run_orcid_profiles", {}),
    ("collectors.orcid", "run_orcid_works", {}),
    ("collectors.youtube", "run_youtube", {}),
]

def _log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def _safe_run(mod_name, fn_name, **kwargs):
    try:
        mod = importlib.import_module(mod_name)
        fn = getattr(mod, fn_name)
        rows = fn(**kwargs)
        _log(f"{mod_name}.{fn_name} -> {rows} rows")
        return rows
    except Exception as e:
        _log(f"FAIL {mod_name}.{fn_name}: {e}")
        traceback.print_exc()
        return 0

def _normalize_row(src, row):
    """Normalize to consistent fields for AI ingestion."""
    return {
        "source": src,
        "title": row.get("title") or row.get("BriefTitle") or row.get("display_name") or "",
        "summary": (row.get("description") or row.get("content") or "")[:2000],
        "date": row.get("year") or row.get("publication_date") or row.get("StartDate") or "",
        "link": row.get("url") or row.get("openalex_url") or row.get("link") or "",
        "authors": row.get("authors") or row.get("authorships") or "",
        "journal": row.get("journal") or row.get("host_venue_name") or "",
        "type": row.get("type") or "",
        "keywords": row.get("concepts") or row.get("tags") or "",
    }

def _merge_all():
    """Merge every output/*.csv into master CSV + JSON."""
    master_csv = OUT / "eppley_master.csv"
    master_json = OUT / "eppley_master.json"
    # Only merge the CSVs that correspond to our collectors.  In the original
    # repository the ``output`` directory contained other CSVs (e.g. youtube_metadata.csv)
    # unrelated to the unified Eppley dataset.  Merging every CSV indiscriminately
    # led to spurious rows in ``eppley_master.csv``.  Restrict the merge to
    # known files produced by our pipeline.
    allowed = {
        "wordpress_posts.csv",
        "pubmed_eppley.csv",
        "crossref_works.csv",
        "openalex_works.csv",
        "clinical_trials.csv",
        "orcid_profiles.csv",
        "orcid_works.csv",
        "youtube_all.csv",
        # include semanticscholar_works.csv if it exists; we treat it as optional
        "semanticscholar_works.csv",
    }
    files = [f for f in sorted(OUT.glob("*.csv")) if f.name in allowed]
    all_rows: list = []

    fieldnames = ["source","title","summary","date","link","authors","journal","type","keywords"]

    with open(master_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for file in files:
            if file.name.startswith("eppley_master"):  # skip self
                continue
            try:
                with open(file, newline="", encoding="utf-8") as rf:
                    rd = csv.DictReader(rf)
                    for r in rd:
                        row = _normalize_row(file.name, r)
                        w.writerow(row)
                        all_rows.append(row)
            except Exception as e:
                _log(f"merge skip {file.name}: {e}")

    # Write JSON
    try:
        with open(master_json, "w", encoding="utf-8") as jf:
            json.dump(all_rows, jf, ensure_ascii=False, indent=2)
        _log(f"[merge] wrote {len(all_rows)} rows → eppley_master.csv/json")
    except Exception as e:
        _log(f"json write error: {e}")

    return len(all_rows)

def main():
    _log("Starting unified Eppley Collector run…")
    total = 0
    for mod, fn, kwargs in PIPELINE:
        total += _safe_run(mod, fn, **kwargs)
    merged = _merge_all()
    _log(f"✅ Done. Collected {total} individual rows, merged {merged} unified entries.")
    return 0

if __name__ == "__main__":
    sys.exit(main())