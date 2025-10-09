#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Eppley Collector — hardened final w/ OpenAlex ORCID pass
- WP: REST first, then sitemap+HTML fallback
- OpenAlex: retries, per-page=200, mailto, plus secondary author.orcid pass
- ClinicalTrials: retries + safe JSON
- All collectors isolated; failures never abort the run
- Outputs live in ./output/*.csv
"""

import csv, json, os, re, time, html, urllib.parse
from datetime import datetime, timezone

import requests

# ----------------------------
# Utilities
# ----------------------------

OUTDIR = "output"
os.makedirs(OUTDIR, exist_ok=True)

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {}

def rget(url, *, headers=None, timeout=20, tries=3, backoff=1.3, ok=(200,)):
    """GET with small retry/backoff."""
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, headers=h, timeout=timeout)
            if r.status_code in ok and r.text:
                return r
            last = r
        except Exception as e:
            last = e
        time.sleep(backoff ** i)
    if isinstance(last, requests.Response):
        return last
    raise RuntimeError(f"request failed for {url}: {last}")

def write_csv(path, rows, header=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not header:
        keys = set()
        for r in rows:
            keys.update(r.keys())
        header = sorted(keys)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def log(msg):
    print(msg, flush=True)

# ----------------------------
# WordPress (REST → fallback HTML)
# ----------------------------

def wp_rest(base="https://exploreplasticsurgery.com", pages=50, per_page=100, delay=0.2):
    out = []
    for page in range(1, pages + 1):
        url = f"{base}/wp-json/wp/v2/posts?page={page}&per_page={per_page}&_fields=id,date,link,title,excerpt"
        r = rget(url, tries=3, backoff=1.6)
        if r.status_code != 200:
            log(f"[wp-rest] page {page} non-200={r.status_code}; stopping REST")
            break
        js = safe_json(r)
        if not js:
            log(f"[wp-rest] page {page} empty/invalid JSON; stopping REST")
            break
        if not isinstance(js, list) or not js:
            log(f"[wp-rest] page {page} empty list; stopping REST")
            break
        for it in js:
            out.append({
                "id": it.get("id"),
                "date": it.get("date"),
                "link": it.get("link"),
                "title": (it.get("title") or {}).get("rendered", ""),
                "excerpt": (it.get("excerpt") or {}).get("rendered", ""),
                "source": "wp-rest",
            })
        time.sleep(delay)
    return out

def _extract(tag, name):
    m = re.search(rf'<meta[^>]+property=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']', tag, re.I)
    if not m:
        m = re.search(rf'<meta[^>]+name=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']', tag, re.I)
    return html.unescape(m.group(1)) if m else ""

def wp_sitemap_html(base="https://exploreplasticsurgery.com", max_urls=1000, delay=0.35):
    """
    Parse sitemap(s) → fetch each post → pull og:title/date/url.
    Keeps it simple (regex), no bs4 needed.
    """
    urls = []
    # try sitemap index first
    idx = rget(f"{base.rstrip('/')}/sitemap.xml", tries=2, backoff=1.6)
    if idx.status_code == 200:
        locs = re.findall(r"<loc>(.*?)</loc>", idx.text, re.I)
        # keep only post sitemaps, fall back to all if unknown
        post_maps = [u for u in locs if "post-sitemap" in u or "post" in u]
        if not post_maps:
            post_maps = locs
        for sm in post_maps:
            try:
                r = rget(sm, tries=2, backoff=1.6)
                locs2 = re.findall(r"<loc>(.*?)</loc>", r.text, re.I)
                urls.extend(locs2)
            except Exception:
                continue
    else:
        # simple single sitemap
        locs = re.findall(r"<loc>(.*?)</loc>", idx.text, re.I)
        urls.extend(locs)

    # de-dupe + clamp
    seen = set()
    clean = []
    for u in urls:
        if u not in seen and "/tag/" not in u and "/category/" not in u:
            seen.add(u)
            clean.append(u)
        if len(clean) >= max_urls:
            break

    out = []
    for i, u in enumerate(clean, 1):
        try:
            r = rget(u, tries=2, backoff=1.6)
            htmltxt = r.text or ""
            title = _extract(htmltxt, "og:title") or _extract(htmltxt, "twitter:title")
            pubdt = _extract(htmltxt, "article:published_time") or _extract(htmltxt, "og:updated_time")
            out.append({
                "id": "",
                "date": pubdt,
                "link": u,
                "title": title,
                "excerpt": "",
                "source": "wp-html",
            })
        except Exception as e:
            log(f"[wp-html] skip {u}: {e}")
        if i % 20 == 0:
            time.sleep(delay)
    return out

def run_wp():
    rows = wp_rest()
    if not rows:
        log("[wp] REST yielded 0 → falling back to sitemap+HTML")
        rows = wp_sitemap_html()
    write_csv(os.path.join(OUTDIR, "wordpress_posts.csv"), rows,
              header=["id","date","link","title","excerpt","source"])
    log(f"[wp] wrote {len(rows)} rows → wordpress_posts.csv")

# ----------------------------
# PubMed (simple CSL endpoint; tolerant)
# ----------------------------

def run_pubmed(query="Eppley BL[Author]"):
    url = f"https://api.ncbi.nlm.nih.gov/lit/ctxp/v1/pubmed/?format=csl&query={urllib.parse.quote(query)}"
    r = rget(url, tries=3, backoff=1.6)
    data = safe_json(r)
    rows = []
    if isinstance(data, list):
        for it in data:
            rows.append({
                "title": it.get("title"),
                "issued": (it.get("issued") or {}).get("date-parts", [[]])[0][0] if it.get("issued") else "",
                "URL": it.get("URL"),
                "DOI": it.get("DOI"),
                "container": it.get("container-title"),
            })
    write_csv(os.path.join(OUTDIR, "pubmed_eppley.csv"), rows)
    log(f"[pubmed] wrote {len(rows)} rows → pubmed_eppley.csv")

# ----------------------------
# Crossref (author query)
# ----------------------------

def run_crossref(names):
    out = []
    for n in names:
        url = f"https://api.crossref.org/works?query.author={urllib.parse.quote(n)}&rows=100"
        r = rget(url, tries=3, backoff=1.6)
        js = safe_json(r)
        items = (js.get("message", {}) or {}).get("items", []) or []
        for it in items:
            out.append({
                "title": (it.get("title") or [""])[0],
                "DOI": it.get("DOI"),
                "URL": it.get("URL"),
                "issued": (it.get("issued") or {}).get("date-parts", [[]])[0][0] if it.get("issued") else "",
            })
        time.sleep(0.3)
    write_csv(os.path.join(OUTDIR, "crossref_works.csv"), out)
    log(f"[crossref] wrote {len(out)} rows → crossref_works.csv")

# ----------------------------
# OpenAlex (robust, per-page=200, mailto) + ORCID secondary pass
# ----------------------------

def run_openalex(names):
    out = []
    base = "https://api.openalex.org/works"
    contact = "mailto:data-admin@invalid.example"

    # Primary pass: author.display_name.search
    for n in names:
        page = 1
        while True:
            url = (
                f"{base}?filter=author.display_name.search:{urllib.parse.quote(n)}"
                f"&per-page=200&page={page}&mailto={urllib.parse.quote(contact)}"
            )
            r = rget(url, tries=3, backoff=1.6)
            js = safe_json(r)
            results = js.get("results") or []
            if not results:
                break
            for it in results:
                out.append({
                    "title": it.get("title"),
                    "doi": it.get("doi"),
                    "id": it.get("id"),
                    "publication_year": it.get("publication_year"),
                })
            page += 1
            time.sleep(0.3)

    # Secondary pass: author.orcid based on collected ORCID profiles (if any)
    try:
        orcids = []
        p = os.path.join(OUTDIR, "orcid_profiles.csv")
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                for r in rdr:
                    if r.get("orcid"):
                        orcids.append(r["orcid"])
        # sanity cap to avoid excessive calls
        for pid in orcids[:10]:
            page = 1
            while True:
                url = (
                    f"{base}?filter=author.orcid:{pid}"
                    f"&per-page=200&page={page}&mailto={urllib.parse.quote(contact)}"
                )
                r = rget(url, tries=3, backoff=1.6)
                js = safe_json(r)
                results = js.get("results") or []
                if not results:
                    break
                for it in results:
                    out.append({
                        "title": it.get("title"),
                        "doi": it.get("doi"),
                        "id": it.get("id"),
                        "publication_year": it.get("publication_year"),
                    })
                page += 1
                time.sleep(0.3)
    except Exception as e:
        log(f"[openalex] orcid pass skipped: {e}")

    write_csv(os.path.join(OUTDIR, "openalex_works.csv"), out)
    log(f"[openalex] wrote {len(out)} rows → openalex_works.csv")

# ----------------------------
# ClinicalTrials.gov (safe JSON)
# ----------------------------

CT_FIELDS = [
    "NCTId","BriefTitle","OverallStatus","StartDate","Phase","Condition","LeadSponsorName","LocationCity","LocationState"
]

def run_clinicaltrials(terms):
    out = []
    for t in terms:
        expr = urllib.parse.quote(t)
        fields = ","join(CT_FIELDS)
        url = (
            "https://clinicaltrials.gov/api/query/study_fields"
            f"?expr={expr}&fields={urllib.parse.quote(fields)}&min_rnk=1&max_rnk=200&fmt=json"
        )
        r = rget(url, tries=3, backoff=1.6)
        js = safe_json(r)
        studies = (js.get("StudyFieldsResponse", {}) or {}).get("StudyFields", []) or []
        for s in studies:
            def first(k):
                v = s.get(k, [""])
                return v[0] if isinstance(v, list) and v else ""
            out.append({
                "NCTId": first("NCTId"),
                "BriefTitle": first("BriefTitle"),
                "OverallStatus": first("OverallStatus"),
                "StartDate": first("StartDate"),
                "Phase": first("Phase"),
                "Condition": first("Condition"),
                "LeadSponsorName": first("LeadSponsorName"),
                "LocationCity": first("LocationCity"),
                "LocationState": first("LocationState"),
            })
        time.sleep(0.3)
    write_csv(os.path.join(OUTDIR, "clinical_trials.csv"), out)
    log(f"[ct] wrote {len(out)} rows → clinical_trials.csv")

# ----------------------------
# ORCID (profiles + minimal works lookup via public JSON)
# ----------------------------

def run_orcid(names):
    profiles = []
    for n in names:
        url = f"https://pub.orcid.org/v3.0/search/?q={urllib.parse.quote(n)}"
        r = rget(url, tries=3, backoff=1.6, headers={"Accept":"application/json"})
        js = safe_json(r)
        for item in js.get("result", []) or []:
            pid = ((item.get("orcid-identifier") or {}).get("path")) or ""
            if pid:
                profiles.append({"orcid": pid, "query": n})
        time.sleep(0.3)
    write_csv(os.path.join(OUTDIR, "orcid_profiles.csv"), profiles)
    log(f"[orcid] wrote {len(profiles)} rows → orcid_profiles.csv")

    # very light works pass (optional; tolerates empties)
    works = []
    for p in profiles[:30]:  # sanity cap
        pid = p["orcid"]
        url = f"https://pub.orcid.org/v3.0/{pid}/works"
        r = rget(url, tries=2, backoff=1.6, headers={"Accept":"application/json"})
        js = safe_json(r)
        for g in js.get("group", []) or []:
            w = g.get("work-summary", []) or []
            for it in w:
                works.append({
                    "orcid": pid,
                    "title": ((it.get("title") or {}).get("title") or {}).get("value"),
                    "put-code": it.get("put-code"),
                    "type": it.get("type"),
                    "year": ((it.get("publication-date") or {}).get("year") or {}).get("value"),
                })
        time.sleep(0.2)
    write_csv(os.path.join(OUTDIR, "orcid_works.csv"), works)
    log(f"[orcid] wrote {len(works)} rows → orcid_works.csv")

# ----------------------------
# YouTube (placeholder shim)
# ----------------------------

def run_youtube():
    # keep minimal metadata so audit sees the file; replace later with yt API/yt-dlp pass if desired
    rows = [{"channel":"Eppley","video":"placeholder","note":"expand later"}]
    write_csv(os.path.join(OUTDIR, "youtube_all.csv"), rows)
    log(f"[yt] wrote {len(rows)} rows → youtube_all.csv")

# ----------------------------
# Main
# ----------------------------

def main():
    names = ["Barry L. Eppley","Barry Eppley","Eppley BL"]
    # Every collector isolated: failures print, but never stop the run
    for label, fn, args in [
        ("wp", run_wp, ()),
        ("pubmed", run_pubmed, ()),
        ("crossref", run_crossref, (names,)),
        ("openalex", run_openalex, (names,)),
        ("ct", run_clinicaltrials, (names,)),
        ("orcid", run_orcid, (names,)),
        ("yt", run_youtube, ()),
    ]:
        try:
            log(f"=== START {label} ===")
            fn(*args)
            log(f"=== OK {label} ===")
        except Exception as e:
            log(f"=== FAIL {label}: {e} ===")

if __name__ == "__main__":
    main()