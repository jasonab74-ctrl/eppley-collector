#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Eppley Collector — full, resilient collectors with safe fallbacks.

Outputs (full files only; always overwrite):
  - output/wordpress_posts.csv
  - output/pubmed_eppley.csv
  - output/crossref_works.csv
  - output/openalex_works.csv
  - output/clinical_trials.csv
  - output/orcid_profiles.csv
  - output/orcid_works.csv
  - output/youtube_all.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import os
import re
import subprocess
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple

import requests

# --------------------------- basics & helpers ---------------------------

OUT_DIR = "output"
os.makedirs(OUT_DIR, exist_ok=True)

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

def session_with_retries() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json, */*;q=0.8"})
    s.timeout = 20
    return s

def save_csv(path: str, rows: List[Dict], fieldnames: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            # Normalize any None to empty strings for CSV readability
            out = {k: ("" if r.get(k) is None else r.get(k)) for k in fieldnames}
            w.writerow(out)

def dt_utcnow_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def jget(d: Optional[dict], *path, default=None):
    cur = d or {}
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
        if cur is None:
            return default
    return cur

def safe_json(resp: requests.Response) -> dict:
    try:
        return resp.json()
    except Exception:
        return {}

def backoff_sleep(i: int):
    time.sleep(min(1.5 * (i + 1), 8))

# --------------------------- WordPress ---------------------------

WP_SITES = [
    # Add more seeds if you want
    "https://exploreplasticsurgery.com",
]

def wp_rest_collect(site: str, s: requests.Session) -> List[Dict]:
    """Use WordPress REST API with pagination."""
    base = site.rstrip("/")
    url = f"{base}/wp-json/wp/v2/posts"
    rows: List[Dict] = []
    page = 1
    per_page = 100
    while True:
        for i in range(3):
            try:
                r = s.get(
                    url,
                    params={
                        "per_page": per_page,
                        "page": page,
                        "_fields": "id,slug,date,link,title,excerpt,categories,tags",
                    },
                    timeout=25,
                )
                if r.status_code == 404:
                    return []  # REST not available
                r.raise_for_status()
                data = r.json()
                break
            except Exception:
                if i == 2:
                    return rows
                backoff_sleep(i)

        if not data:
            break

        for p in data:
            rows.append(
                {
                    "id": p.get("id"),
                    "slug": p.get("slug"),
                    "date": p.get("date"),
                    "url": p.get("link"),
                    "title": html.unescape(jget(p, "title", "rendered", default="")).strip(),
                    "excerpt": re.sub(r"<[^>]+>", "", jget(p, "excerpt", "rendered", default="")).strip(),
                    "site": base,
                }
            )
        if len(data) < per_page:
            break
        page += 1
    return rows

def wp_sitemap_collect(site: str, s: requests.Session) -> List[Dict]:
    """Very light sitemap collector as fallback."""
    base = site.rstrip("/")
    rows: List[Dict] = []
    for sm in ("sitemap.xml", "sitemap_index.xml"):
        try:
            r = s.get(f"{base}/{sm}", timeout=20)
            if r.status_code == 404:
                continue
            txt = r.text
            urls = re.findall(r"<loc>(.*?)</loc>", txt)
            # crude: keep post-like urls
            for u in urls:
                if "/20" in u or "/blog" in u or "/your-questions" in u:
                    rows.append({"id": u, "slug": u.rstrip("/").split("/")[-1], "date": "", "url": u, "title": "", "excerpt": "", "site": base})
            if rows:
                break
        except Exception:
            pass
    return rows

def collect_wp() -> None:
    s = session_with_retries()
    all_rows: List[Dict] = []
    for site in WP_SITES:
        rows = wp_rest_collect(site, s)
        if not rows:
            rows = wp_sitemap_collect(site, s)
        all_rows.extend(rows)

    # stable sort by date then slug
    def dparse(x):
        try:
            return dt.datetime.fromisoformat(x.get("date","").replace("Z",""))
        except Exception:
            return dt.datetime.min
    all_rows.sort(key=lambda r: (dparse(r), r.get("slug","")))

    save_csv(
        os.path.join(OUT_DIR, "wordpress_posts.csv"),
        all_rows,
        ["id", "slug", "date", "url", "title", "excerpt", "site"],
    )
    print(f"[wp] wrote {len(all_rows)} rows → wordpress_posts.csv")

# --------------------------- PubMed (Entrez eutils) ---------------------------

def collect_pubmed() -> None:
    # Simple PubMed fetch: author name variants
    s = session_with_retries()
    terms = [
        'Eppley BL[Author]',
        '"Barry L Eppley"[Author]',
        '"Barry Eppley"[Author]'
    ]
    query = " OR ".join(terms)
    esearch = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    esummary = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    ids: List[str] = []
    for i in range(3):
        try:
            r = s.get(esearch, params={"db": "pubmed", "retmode": "json", "term": query, "retmax": 10000}, timeout=30)
            r.raise_for_status()
            js = r.json()
            ids = js.get("esearchresult", {}).get("idlist", [])
            break
        except Exception:
            if i == 2:
                ids = []
            backoff_sleep(i)

    rows: List[Dict] = []
    if ids:
        for i in range(3):
            try:
                r = s.get(esummary, params={"db": "pubmed", "retmode": "json", "id": ",".join(ids)}, timeout=40)
                r.raise_for_status()
                js = r.json().get("result", {})
                for pmid in ids:
                    rec = js.get(pmid, {})
                    rows.append(
                        {
                            "pmid": pmid,
                            "title": rec.get("title"),
                            "journal": rec.get("fulljournalname"),
                            "pubdate": rec.get("pubdate"),
                            "authors": "; ".join([a.get("name","") for a in rec.get("authors", [])]),
                            "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                        }
                    )
                break
            except Exception:
                if i == 2:
                    break
                backoff_sleep(i)

    save_csv(os.path.join(OUT_DIR, "pubmed_eppley.csv"), rows,
             ["pmid", "title", "journal", "pubdate", "authors", "url"])
    print(f"[pubmed] wrote {len(rows)} rows → pubmed_eppley.csv")

# --------------------------- Crossref ---------------------------

def collect_crossref() -> None:
    s = session_with_retries()
    query = "Barry L. Eppley"
    url = "https://api.crossref.org/works"
    rows: List[Dict] = []
    cursor = "*"
    for _ in range(10):  # up to ~1k items
        try:
            r = s.get(url, params={"query.author": query, "rows": 100, "cursor": cursor, "cursor_max": 1000}, timeout=30)
            r.raise_for_status()
            js = r.json()
        except Exception:
            break

        items = js.get("message", {}).get("items", [])
        for it in items:
            rows.append(
                {
                    "doi": it.get("DOI"),
                    "title": "; ".join(it.get("title") or []),
                    "container": it.get("container-title", [""])[0] if it.get("container-title") else "",
                    "published": "-".join(map(str, (jget(it,"published","date-parts",[[""]])[0]))),
                    "type": it.get("type"),
                    "url": it.get("URL"),
                }
            )
        next_cursor = js.get("message", {}).get("next-cursor")
        if not next_cursor or not items:
            break
        cursor = next_cursor

    save_csv(os.path.join(OUT_DIR, "crossref_works.csv"), rows,
             ["doi", "title", "container", "published", "type", "url"])
    print(f"[crossref] wrote {len(rows)} rows → crossref_works.csv")

# --------------------------- OpenAlex ---------------------------

def collect_openalex() -> None:
    s = session_with_retries()
    # Fetch works for author
    # First: find author id
    aid = None
    try:
        r = s.get("https://api.openalex.org/authors", params={"search": "Barry L. Eppley", "per_page": 1})
        js = r.json()
        if jget(js, "results"):
            aid = js["results"][0]["id"]
    except Exception:
        aid = None

    rows: List[Dict] = []
    if aid:
        cursor = "*"
        for _ in range(20):
            try:
                r = s.get("https://api.openalex.org/works",
                          params={"filter": f"authorships.author.id:{aid}", "per_page": 200, "cursor": cursor},
                          timeout=30)
                r.raise_for_status()
                js = r.json()
            except Exception:
                break

            for it in js.get("results", []):
                pl = it.get("primary_location") or {}
                source = pl.get("source") or {}
                rows.append(
                    {
                        "id": it.get("id"),
                        "doi": it.get("doi"),
                        "title": it.get("title"),
                        "publication_year": it.get("publication_year"),
                        "type": it.get("type"),
                        "venue": source.get("display_name",""),
                        "open_access": jget(it, "open_access", "is_oa", default=False),
                        "landing_url": pl.get("landing_page_url") or it.get("primary_location", {}) or "",
                    }
                )
            if not js.get("results") or not js.get("meta", {}).get("next_cursor"):
                break
            cursor = js["meta"]["next_cursor"]

    save_csv(os.path.join(OUT_DIR, "openalex_works.csv"), rows,
             ["id", "doi", "title", "publication_year", "type", "venue", "open_access", "landing_url"])
    print(f"[openalex] wrote {len(rows)} rows → openalex_works.csv")

# --------------------------- ClinicalTrials.gov ---------------------------

CT_FIELDS = [
    "NCTId", "Condition", "BriefTitle", "OverallStatus",
    "StartDate", "CompletionDate", "LastUpdatePostDate", "EnrollmentCount",
    "LocationCountry", "InterventionName"
]

def collect_clinicaltrials() -> None:
    s = session_with_retries()
    fields = ",".join(CT_FIELDS)  # <-- fixed
    url = "https://clinicaltrials.gov/api/query/study_fields"
    params = {
        "expr": "Eppley",
        "fields": fields,
        "min_rnk": 1,
        "max_rnk": 1000,
        "fmt": "json",
    }
    rows: List[Dict] = []
    try:
        r = s.get(url, params=params, timeout=35)
        r.raise_for_status()
        js = safe_json(r)  # tolerate non-JSON
        studies = jget(js, "StudyFieldsResponse", "StudyFields", default=[])
        for st in studies:
            rows.append({
                "nct_id": ";".join(st.get("NCTId", [])),
                "title": ";".join(st.get("BriefTitle", [])),
                "status": ";".join(st.get("OverallStatus", [])),
                "start": ";".join(st.get("StartDate", [])),
                "complete": ";".join(st.get("CompletionDate", [])),
                "updated": ";".join(st.get("LastUpdatePostDate", [])),
                "enrollment": ";".join(st.get("EnrollmentCount", [])),
                "country": ";".join(st.get("LocationCountry", [])),
                "intervention": ";".join(st.get("InterventionName", [])),
            })
    except Exception:
        # leave rows empty but still write a file
        pass

    save_csv(os.path.join(OUT_DIR, "clinical_trials.csv"), rows,
             ["nct_id", "title", "status", "start", "complete", "updated", "enrollment", "country", "intervention"])
    print(f"[ctgov] wrote {len(rows)} rows → clinical_trials.csv")

# --------------------------- ORCID ---------------------------

def collect_orcid() -> None:
    s = session_with_retries()
    # Search ORCID for profile
    rows_profiles: List[Dict] = []
    rows_works: List[Dict] = []
    orcid_id = None
    try:
        r = s.get("https://pub.orcid.org/v3.0/expanded-search/",
                  headers={"Accept": "application/json"},
                  params={"q": 'given-names:"Barry" AND family-name:"Eppley"', "rows": 5},
                  timeout=25)
        js = r.json()
        if js.get("expanded-result"):
            first = js["expanded-result"][0]
            orcid_id = first.get("orcid-id")
            rows_profiles.append({
                "orcid": orcid_id,
                "given": first.get("given-names"),
                "family": first.get("family-names"),
                "credit-name": first.get("credit-name"),
                "institution": first.get("institution-name"),
            })
    except Exception:
        pass

    if orcid_id:
        try:
            r = s.get(f"https://pub.orcid.org/v3.0/{orcid_id}/works", headers={"Accept": "application/json"}, timeout=25)
            js = r.json()
            for g in js.get("group", []):
                work = jget(g, "work-summary", 0, default={})
                rows_works.append({
                    "orcid": orcid_id,
                    "put-code": work.get("put-code"),
                    "title": jget(work, "title", "title", "value", default=""),
                    "type": work.get("type", ""),
                    "year": jget(work, "publication-date", "year", "value", default=""),
                    "external-id": jget(work, "external-ids", "external-id", 0, "external-id-value", default=""),
                })
        except Exception:
            pass

    save_csv(os.path.join(OUT_DIR, "orcid_profiles.csv"), rows_profiles,
             ["orcid", "given", "family", "credit-name", "institution"])
    save_csv(os.path.join(OUT_DIR, "orcid_works.csv"), rows_works,
             ["orcid", "put-code", "title", "type", "year", "external-id"])
    print(f"[orcid] wrote {len(rows_profiles)} profile rows → orcid_profiles.csv")
    print(f"[orcid] wrote {len(rows_works)} work rows → orcid_works.csv")

# --------------------------- YouTube ---------------------------

YOUTUBE_CHANNEL_URLS = [
    # If you know a specific channel/playlist, put urls here (e.g., channel or playlist URL)
    # "https://www.youtube.com/@ExploringPlasticSurgery/videos",
]

def _yt_using_ytdlp_flat(url: str) -> List[Dict]:
    """Use yt-dlp in flat mode to list items without downloading."""
    try:
        res = subprocess.run(
            ["yt-dlp", "--flat-playlist", "--dump-json", url],
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode != 0:
            return []
        rows: List[Dict] = []
        for line in res.stdout.splitlines():
            try:
                js = json.loads(line)
            except Exception:
                continue
            rows.append({
                "id": js.get("id"),
                "title": js.get("title"),
                "uploader": js.get("uploader"),
                "duration": js.get("duration"),
                "url": f'https://www.youtube.com/watch?v={js.get("id")}' if js.get("id") else "",
            })
        return rows
    except FileNotFoundError:
        return []

def collect_youtube() -> None:
    rows: List[Dict] = []
    # Strategy:
    # 1) If channel URLs are provided, try yt-dlp flat listing.
    # 2) Otherwise, leave a placeholder row so audit sees the file and you can fill channels later.
    for u in YOUTUBE_CHANNEL_URLS:
        items = _yt_using_ytdlp_flat(u)
        rows.extend(items)
    if not rows:
        # Placeholder row makes it clear where to configure channels
        rows = [{
            "id": "",
            "title": "Configure YOUTUBE_CHANNEL_URLS in main.py to enumerate videos",
            "uploader": "",
            "duration": "",
            "url": "",
        }]

    save_csv(os.path.join(OUT_DIR, "youtube_all.csv"), rows,
             ["id", "title", "uploader", "duration", "url"])
    print(f"[yt] wrote {len(rows)} rows → youtube_all.csv")

# --------------------------- CLI / Orchestration ---------------------------

def main():
    p = argparse.ArgumentParser(description="Eppley collector")
    p.add_argument("--only", choices=["wp","pubmed","crossref","openalex","ct","orcid","yt","all"], default="all")
    args = p.parse_args()

    started = dt_utcnow_iso()
    print(f"[start] {started}")

    if args.only in ("wp","all"):
        collect_wp()
    if args.only in ("pubmed","all"):
        collect_pubmed()
    if args.only in ("crossref","all"):
        collect_crossref()
    if args.only in ("openalex","all"):
        collect_openalex()
    if args.only in ("ct","all"):
        collect_clinicaltrials()
    if args.only in ("orcid","all"):
        collect_orcid()
    if args.only in ("yt","all"):
        collect_youtube()

    print(f"[done] {dt_utcnow_iso()}")

if __name__ == "__main__":
    main()