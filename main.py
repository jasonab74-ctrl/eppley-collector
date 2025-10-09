#!/usr/bin/env python3
"""
Unified Eppley Collector
Runs all collectors, merges all into a master CSV for NotebookLM.
Never fails hard — always leaves usable outputs.
"""

import csv, os, sys, traceback, importlib, pathlib, time

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
    except Exception as e:
        _log(f"FAIL: {mod_name}.{fn_name}: {e}")
        traceback.print_exc()
        rows = 0
    return rows

def _merge_all():
    master_path = OUT / "eppley_master.csv"
    files = list(OUT.glob("*.csv"))
    fieldnames = ["source","title","summary","date","link","authors","journal","type","keywords","extra"]
    written = 0
    with open(master_path,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f,fieldnames=fieldnames)
        w.writeheader()
        for file in files:
            if file.name=="eppley_master.csv": continue
            try:
                with open(file,newline="",encoding="utf-8") as rf:
                    rd = csv.DictReader(rf)
                    for row in rd:
                        title = row.get("title") or row.get("BriefTitle") or row.get("display_name") or ""
                        link = row.get("url") or row.get("openalex_url") or row.get("link") or ""
                        w.writerow({
                            "source": file.name,
                            "title": title[:300],
                            "summary": (row.get("description") or row.get("content") or "")[:1000],
                            "date": row.get("year") or row.get("publication_date") or row.get("StartDate") or "",
                            "link": link,
                            "authors": row.get("authors") or row.get("authorships") or "",
                            "journal": row.get("journal") or row.get("host_venue_name") or "",
                            "type": row.get("type") or "",
                            "keywords": row.get("concepts") or row.get("tags") or "",
                            "extra": ";".join([f"{k}:{v}" for k,v in list(row.items())[:5]])
                        })
                        written+=1
            except Exception as e:
                _log(f"merge skip {file.name}: {e}")
    _log(f"[merge] wrote {written} unified rows -> {master_path}")
    return written

def main():
    _log("Eppley unified collector started.")
    for mod, fn, kwargs in PIPELINE:
        _safe_run(mod, fn, **kwargs)
    _merge_all()
    _log("✅ Done.")
    return 0

if __name__=="__main__":
    sys.exit(main())