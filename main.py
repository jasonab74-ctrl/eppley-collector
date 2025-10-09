#!/usr/bin/env python3
"""
Main orchestrator for Eppley Collector.
Runs each collector, guarantees headers get written, logs row counts,
and never crashes the workflow on one-source failure.

Outputs live in: output/*.csv
"""

import os, sys, traceback, importlib, pathlib, time

ROOT = pathlib.Path(__file__).resolve().parent
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

# Collectors to run (module, function, kwargs)
# Keep names stable so swapping implementations is painless.
PIPELINE = [
    ("collectors.wordpress", "run_wp", {}),            # if you have it
    ("collectors.pubmed", "run_pubmed", {}),           # if you have it
    ("collectors.crossref", "run_crossref", {}),       # if you have it
    ("collectors.openalex", "run_openalex", {}),       # provided
    ("collectors.clinical_trials", "run_ct", {}),      # provided
    ("collectors.orcid", "run_orcid_profiles", {}),    # provided
    ("collectors.orcid", "run_orcid_works", {}),       # provided
    ("collectors.youtube", "run_youtube", {}),         # provided
]

def _log(msg:str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[{ts} UTC] {msg}", flush=True)

def _ensure_csv(path: pathlib.Path, header_line: str):
    """If a collector failed before writing, ensure file exists with header only."""
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(header_line + "\n")
        _log(f"created empty CSV with header: {path}")

def main():
    _log("Eppley Collector starting…")

    # Run collectors one by one; failure of one does not stop others.
    for mod_name, fn_name, kwargs in PIPELINE:
        # Skip missing optional collectors gracefully
        try:
            mod = importlib.import_module(mod_name)
        except ModuleNotFoundError:
            _log(f"SKIP: {mod_name} not found.")
            continue

        fn = getattr(mod, fn_name, None)
        if not callable(fn):
            _log(f"SKIP: {mod_name}.{fn_name} not present.")
            continue

        _log(f"RUN: {mod_name}.{fn_name}…")
        try:
            rows = fn(**kwargs)  # collector returns row count (excluding header)
            _log(f"DONE: {mod_name}.{fn_name} -> {rows} rows")
        except SystemExit as e:
            # e.g., youtube collector missing YT_API_KEY
            _log(f"FAIL (non-fatal): {mod_name}.{fn_name}: {e}")
        except Exception as e:
            _log(f"FAIL (non-fatal): {mod_name}.{fn_name}: {e}")
            traceback.print_exc()

    # Minimum safety: ensure core CSVs exist with headers so site never breaks
    # (These headers should match each collector’s writer)
    headers = {
        "wordpress_posts.csv": "title,date,url,tags,content",
        "pubmed_eppley.csv": "pmid,doi,title,year,journal,authors,url",
        "crossref_works.csv": "doi,title,year,journal,authors,type,url",
        "openalex_works.csv": "openalex_id,doi,title,publication_date,type,cited_by_count,host_venue_name,host_venue_publisher,authorships,concepts,openalex_url",
        "clinical_trials.csv": "nct_id,title,condition,intervention,sponsor,status,start_date,completion_date,study_type,phase,last_update,primary_outcome,first_post_date,country,city,responsible_party",
        "orcid_profiles.csv": "orcid,given_names,family_name,credit_name,institutions,countries,keywords,num_works,last_modified",
        "orcid_works.csv": "orcid,put_code,title,type,journal,year,external_ids,source",
        "youtube_all.csv": "video_id,title,description,channel_title,channel_id,published_at,view_count,like_count,comment_count,duration,tags,definition,license,live_broadcast,link",
    }
    for name, header in headers.items():
        _ensure_csv(OUT / name, header)

    _log("All collectors attempted. Exiting OK.")
    return 0

if __name__ == "__main__":
    sys.exit(main())