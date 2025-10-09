#!/usr/bin/env python3
"""
Semantic Scholar collector (public API, no key required for modest use)

- Finds author IDs by name (config 'names')
- For each author, fetches papers with metadata
- Writes: output/semanticscholar_works.csv + .jsonl

API docs: https://api.semanticscholar.org/api-docs/graph
"""

import csv, json, pathlib, time, requests
from datetime import datetime, timezone
from typing import List, Dict

OUTDIR = pathlib.Path("output")
CSV_PATH = OUTDIR / "semanticscholar_works.csv"
JSONL_PATH = OUTDIR / "semanticscholar_works.jsonl"

def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def get_json(url, params=None, retries=3, backoff=0.5, ua="eppley-collector/ss-1.0"):
    headers = {"User-Agent": ua, "Accept": "application/json"}
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff * (i + 1))
                continue
        except requests.RequestException:
            time.sleep(backoff * (i + 1))
    return None

def search_author_ids(names: List[str]) -> List[str]:
    ids = []
    base = "https://api.semanticscholar.org/graph/v1/author/search"
    for name in names:
        j = get_json(base, params={"query": name, "limit": 5, "fields": "name,aliases"})
        if not j: 
            continue
        for res in j.get("data", []):
            aid = str(res.get("authorId") or "")
            if aid and aid not in ids:
                ids.append(aid)
    return ids

def collect_author_papers(author_id: str) -> List[Dict]:
    rows = []
    base = f"https://api.semanticscholar.org/graph/v1/author/{author_id}/papers"
    # fields list kept compact but useful
    fields = ",".join([
        "paperId","title","year","venue","publicationTypes",
        "externalIds","url","openAccessPdf","citationCount","authors"
    ])
    offset = 0
    page_size = 200
    while True:
        j = get_json(base, params={"fields": fields, "limit": page_size, "offset": offset})
        if not j or not j.get("data"): 
            break
        for p in j["data"]:
            eid = p.get("externalIds") or {}
            rows.append({
                "paper_id": p.get("paperId",""),
                "title": p.get("title",""),
                "year": p.get("year",""),
                "venue": p.get("venue",""),
                "types": ", ".join(p.get("publicationTypes") or []),
                "doi": (eid.get("DOI") or "") if isinstance(eid, dict) else "",
                "pmid": (eid.get("PubMed") or "") if isinstance(eid, dict) else "",
                "url": p.get("url",""),
                "open_access_pdf": (p.get("openAccessPdf") or {}).get("url","") if isinstance(p.get("openAccessPdf"), dict) else "",
                "citation_count": p.get("citationCount",""),
                "authors": ", ".join([a.get("name","") for a in (p.get("authors") or [])]),
                "author_id": author_id,
                "source": "semanticscholar",
                "collected_at": utc_now()
            })
        offset += page_size
        if offset >= (j.get("total", offset) or offset):
            break
        time.sleep(0.2)  # be polite
    return rows

def write_outputs(rows: List[Dict]):
    OUTDIR.mkdir(parents=True, exist_ok=True)
    fields = [
        "paper_id","title","year","venue","types","doi","pmid","url",
        "open_access_pdf","citation_count","authors","author_id","source","collected_at"
    ]
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k:r.get(k,"") for k in fields})
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def main():
    # very small, safe reader for names from config.yaml (optional)
    names = ["Barry L. Eppley","Barry Eppley","Eppley BL"]
    try:
        import yaml, pathlib
        cfgp = pathlib.Path("config.yaml")
        if cfgp.exists():
            with open(cfgp, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
                if cfg.get("names"):
                    names = cfg["names"]
                # allow explicit author ids if provided
                if cfg.get("semanticscholar_author_ids"):
                    aids = [str(x) for x in cfg["semanticscholar_author_ids"]]
                else:
                    aids = search_author_ids(names)
        else:
            aids = search_author_ids(names)
    except Exception:
        aids = search_author_ids(names)

    all_rows = []
    for aid in aids:
        all_rows.extend(collect_author_papers(aid))
    write_outputs(all_rows)
    print(f"[semanticscholar] authors={len(aids)} rows={len(all_rows)}")

if __name__ == "__main__":
    main()