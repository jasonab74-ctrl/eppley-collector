"""
OpenAlex Deep Linking Enricher (Additive & Safe)
------------------------------------------------
Reads output/eppley_master.csv and creates output/eppley_openalex.csv
with extra metadata from OpenAlex for rows that have DOI/PMID/title.

- Non-destructive: only writes a separate CSV
- Rate-limit friendly: caching + small sleeps
- Robust: if OpenAlex is down, script exits gracefully and CI continues

Columns written:
  key, doi, pmid, title, year,
  openalex_id, cited_by_count, concepts, authorships, host_venue, oa_url
"""

from __future__ import annotations
import csv
import json
import re
import time
from pathlib import Path
from typing import Dict, Any, Optional
import urllib.parse as up
import requests

BASE = "https://api.openalex.org/works"
MASTER = Path("output/eppley_master.csv")
OUT = Path("output/eppley_openalex.csv")
CACHE = Path("output/cache/openalex_cache.json")
OUT.parent.mkdir(parents=True, exist_ok=True)
CACHE.parent.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "EppleyCollector/1.0 (mailto:site@example.com)"}

def load_cache() -> Dict[str, Any]:
    if CACHE.exists():
        try:
            return json.loads(CACHE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache: Dict[str, Any]) -> None:
    CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def norm_doi(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.strip()
    s = re.sub(r"^https?://(dx\.)?doi\.org/", "", s, flags=re.I)
    return s.lower() if s else None

def extract_ids(row: Dict[str, str]) -> Dict[str, Optional[str]]:
    url = (row.get("url") or "").strip()
    title = (row.get("title") or "").strip()
    year = (row.get("year") or "").strip()
    doi = row.get("doi") or None
    pmid = row.get("pmid") or None

    # Try DOI in URL if not present as a column
    if not doi and "doi.org/" in url:
        m = re.search(r"doi\.org/([^?\s#]+)", url, flags=re.I)
        if m:
            doi = m.group(1)

    # Try PMID in URL (PubMed canonical)
    if not pmid:
        m = re.search(r"pubmed\.ncbi\.nlm\.nih\.gov/(\d+)", url, flags=re.I)
        if not m:
            m = re.search(r"ncbi\.nlm\.nih\.gov/pubmed/(\d+)", url, flags=re.I)
        if m:
            pmid = m.group(1)

    doi = norm_doi(doi)
    pmid = pmid.strip() if pmid else None
    return {"doi": doi, "pmid": pmid, "title": title, "year": year}

def get_json(url: str, retries: int = 3, sleep: float = 0.6) -> Optional[Dict[str, Any]]:
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(sleep * (i + 1))
    return None

def pick_best_oa(work: Dict[str, Any]) -> Optional[str]:
    # best_oa_location or first location with a URL
    best = (work.get("best_oa_location") or {}) if isinstance(work, dict) else {}
    url = best.get("url")
    if url:
        return url
    for loc in (work.get("oa_locations") or []):
        if loc.get("url"):
            return loc["url"]
    return None

def compact_authorships(work: Dict[str, Any]) -> str:
    out = []
    for a in (work.get("authorships") or []):
        name = a.get("author", {}).get("display_name")
        affs = [aff.get("display_name") for aff in (a.get("institutions") or []) if aff.get("display_name")]
        if name:
            if affs:
                out.append(f"{name} ({'; '.join(affs)})")
            else:
                out.append(name)
    return "; ".join(out)

def compact_concepts(work: Dict[str, Any], k: int = 5) -> str:
    cs = sorted((work.get("concepts") or []), key=lambda c: c.get("score", 0), reverse=True)
    names = []
    for c in cs[:k]:
        nm = c.get("display_name")
        if nm:
            names.append(nm)
    return "; ".join(names)

def lookup_openalex(doi: Optional[str], pmid: Optional[str], title: str, year: str, cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # 1) DOI direct
    if doi:
        key = f"doi:{doi}"
        if key in cache: return cache[key]
        j = get_json(f"{BASE}/doi:{up.quote(doi, safe='')}")
        cache[key] = j or {}
        return j

    # 2) PMID direct
    if pmid:
        key = f"pmid:{pmid}"
        if key in cache: return cache[key]
        j = get_json(f"{BASE}/pmid:{pmid}")
        cache[key] = j or {}
        return j

    # 3) Conservative title search (year-filtered if possible)
    ttl = title.strip()
    if not ttl:
        return None
    query = f"{BASE}?search={up.quote(ttl)}&per_page=5"
    if year and str(year).isdigit():
        query += f"&filter=from_publication_date:{year}-01-01,to_publication_date:{year}-12-31"
    key = f"title:{ttl}|year:{year or ''}"
    if key in cache: return cache[key]
    j = get_json(query)
    # pick best exact-ish match
    best = None
    if j and isinstance(j, dict):
        for it in (j.get("results") or []):
            wtitle = (it.get("title") or "").strip().lower()
            if wtitle and wtitle == ttl.lower():
                best = it
                break
        if not best and (j.get("results") or []):
            best = j["results"][0]
    cache[key] = best or {}
    return best

def run():
    if not MASTER.exists():
        print("[openalex] master not found; skipping")
        OUT.write_text("", encoding="utf-8")
        return

    cache = load_cache()
    out_rows = []

    with MASTER.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ids = extract_ids(row)
            doi, pmid = ids["doi"], ids["pmid"]
            title, year = ids["title"], ids["year"]

            work = lookup_openalex(doi, pmid, title, year, cache)
            if not work:
                continue

            # some lookups cache {} when not found
            if not isinstance(work, dict) or not work:
                continue

            wid = work.get("id")
            host = (work.get("host_venue") or {}).get("display_name") if isinstance(work.get("host_venue"), dict) else None
            out_rows.append({
                "key": f"doi:{doi}" if doi else (f"pmid:{pmid}" if pmid else f"title:{title}|year:{year}"),
                "doi": doi or "",
                "pmid": pmid or "",
                "title": title or "",
                "year": year or "",
                "openalex_id": wid or "",
                "cited_by_count": work.get("cited_by_count", ""),
                "concepts": compact_concepts(work),
                "authorships": compact_authorships(work),
                "host_venue": host or "",
                "oa_url": pick_best_oa(work) or "",
            })
            time.sleep(0.25)  # be polite

    # write outputs + cache
    with OUT.open("w", encoding="utf-8", newline="") as f:
        cols = ["key", "doi", "pmid", "title", "year", "openalex_id", "cited_by_count", "concepts", "authorships", "host_venue", "oa_url"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    save_cache(cache)
    print(f"[openalex] wrote {len(out_rows)} rows to {OUT}")

if __name__ == "__main__":
    run()