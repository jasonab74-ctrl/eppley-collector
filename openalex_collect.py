#!/usr/bin/env python3
"""
Robust OpenAlex collector with null-safe field access.
Writes: output/openalex_works.csv  and  output/openalex_works.jsonl
"""

import csv, json, pathlib, time, requests
from datetime import datetime, timezone
from typing import Dict, Any, Iterable, List

OUTDIR = pathlib.Path("output")
CSV_PATH = OUTDIR / "openalex_works.csv"
JSONL_PATH = OUTDIR / "openalex_works.jsonl"

API = "https://api.openalex.org/works"
UA  = "eppley-collector/openalex-1.1"

def utc_now(): return datetime.now(timezone.utc).isoformat(timespec="seconds")

def get_json(url: str, params: Dict[str, Any], retries=4, backoff=0.7):
    headers = {"User-Agent": UA, "Accept": "application/json"}
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff * (i + 1))
                continue
            # hard error
            return None
        except requests.RequestException:
            time.sleep(backoff * (i + 1))
    return None

def g(d: Any, *path, default=""):
    """null-safe getter for nested dicts/lists"""
    cur = d
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p, None)
        elif isinstance(cur, list) and isinstance(p, int):
            cur = cur[p] if 0 <= p < len(cur) else None
        else:
            return default
        if cur is None:
            return default
    return cur

def iter_results(params: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    page = 1
    while True:
        p = params.copy()
        p["page"] = page
        j = get_json(API, p)
        if not j or not j.get("results"):
            break
        for it in j["results"]:
            yield it
        meta = j.get("meta", {})
        if page >= int(meta.get("last_page", page)):
            break
        page += 1
        time.sleep(0.25)

def normalize(it: Dict[str, Any]) -> Dict[str, Any]:
    # authors string
    auths = []
    for a in g(it, "authorships", default=[]):
        name = g(a, "author", "display_name", default="")
        if name: auths.append(name)
    oa_host = g(it, "primary_location", "source", "display_name", default="")
    oa_url  = g(it, "primary_location", "source", "host_organization", default="")

    # external IDs
    ids = g(it, "ids", default={}) or {}
    doi = (ids.get("doi") or "").replace("https://doi.org/", "").strip()

    return {
        "title": g(it, "title", default=""),
        "year": g(it, "publication_year", default=""),
        "journal": g(it, "host_venue", "display_name", default="") or oa_host,
        "venue": g(it, "host_venue", "display_name", default=""),
        "openalex_id": g(it, "id", default=""),
        "doi": doi,
        "pmid": g(it, "ids", "pmid", default="").replace("https://pubmed.ncbi.nlm.nih.gov/",""),
        "url": g(it, "primary_location", "source", "url", default="") or g(it, "primary_location", "landing_page_url", default=""),
        "authors": ", ".join(auths),
        "cited_by_count": g(it, "cited_by_count", default=""),
        "source": "openalex",
        "collected_at": utc_now(),
    }

def write_outputs(rows: List[Dict[str, Any]]):
    OUTDIR.mkdir(parents=True, exist_ok=True)
    fields = ["title","year","journal","venue","authors","doi","pmid","url","openalex_id","cited_by_count","source","collected_at"]
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for r in rows: w.writerow({k: r.get(k, "") for k in fields})
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[openalex] wrote {len(rows)} rows")

def main():
    # Default, but allow config.yaml names if present
    names = ["Barry L. Eppley", "Barry Eppley", "Eppley BL"]
    try:
        import yaml, pathlib
        cfgp = pathlib.Path("config.yaml")
        if cfgp.exists():
            with open(cfgp, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
                names = cfg.get("names") or names
    except Exception:
        pass

    rows = []
    for n in names:
        q = f'author.display_name.search:"{n}"'
        params = {
            "search": q,
            "per_page": 200,
            "sort": "publication_year:desc",
        }
        for it in iter_results(params):
            rows.append(normalize(it))
    # dedupe by DOI → title
    dedup = {}
    for r in rows:
        k = r.get("doi") or r.get("title")
        if k and k not in dedup:
            dedup[k] = r
    write_outputs(list(dedup.values()))

if __name__ == "__main__":
    main()