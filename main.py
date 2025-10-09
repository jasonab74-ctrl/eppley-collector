#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Eppley Collector — full, resilient runner.

Sources supported (null-safe):
- WordPress      → output/wordpress_posts.csv
- PubMed         → output/pubmed_eppley.csv
- Crossref       → output/crossref_works.csv
- OpenAlex       → output/openalex_works.csv
- ClinicalTrials → output/clinical_trials.csv
- ORCID          → output/orcid_profiles.csv, output/orcid_works.csv
- YouTube (shim) → calls youtube_collect.py if present

CLI:
  python main.py --only all
  python main.py --only wp --wp-mode auto --wp-max 600 --save-every 150 --delay 3.0
"""

import argparse, csv, json, os, sys, time, math, pathlib, re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

import requests

ROOT   = pathlib.Path(__file__).resolve().parent
OUTDIR = ROOT / "output"
OUTDIR.mkdir(parents=True, exist_ok=True)

UA = "eppley-collector/2.1 (+https://github.com/jasonab74-ctrl/eppley-collector)"

# ----------------------------- utils ---------------------------------

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def g(d: Any, *path, default: Any = "") -> Any:
    """Null-safe nested getter."""
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

def load_config() -> Dict[str, Any]:
    import yaml
    cfg_path = ROOT / "config.yaml"
    if not cfg_path.exists():
        return {}
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

def session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json, */*;q=0.1"})
    return s

def write_csv(path: pathlib.Path, rows: List[Dict[str, Any]], fields: List[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})

# -------------------------- WordPress --------------------------------

def run_wp(base: str, mode: str = "auto", per_page: int = 100, save_every: int = 200, delay: float = 1.0, wp_max: int = 100000):
    out_csv = OUTDIR / "wordpress_posts.csv"
    fields = ["id","date","link","title","content","tags","collected_at"]
    sess = session()

    def rest_available() -> bool:
        try:
            r = sess.get(f"{base.rstrip('/')}/wp-json", timeout=15)
            return r.status_code == 200 and "routes" in r.json()
        except Exception:
            return False

    def fetch_rest():
        rows = []
        page = 1
        while True:
            url = f"{base.rstrip('/')}/wp-json/wp/v2/posts"
            params = {"per_page": per_page, "page": page, "_fields": "id,date,link,title,content,tags"}
            r = sess.get(url, params=params, timeout=30)
            if r.status_code == 400 and "rest_post_invalid_page_number" in r.text:
                break
            if r.status_code != 200:
                break
            arr = r.json() or []
            if not arr: break
            for it in arr:
                rows.append({
                    "id": g(it, "id", default=""),
                    "date": g(it, "date", default=""),
                    "link": g(it, "link", default=""),
                    "title": g(it, "title", "rendered", default=""),
                    "content": re.sub(r"<[^>]+>", " ", g(it, "content", "rendered", default="")).strip(),
                    "tags": "|".join([str(t) for t in (it.get("tags") or [])]),
                    "collected_at": utc_now(),
                })
            if len(rows) >= wp_max: break
            page += 1
            time.sleep(delay)
        return rows

    def fetch_sitemap_light():
        try:
            sm_index = f"{base.rstrip('/')}/sitemap.xml"
            r = sess.get(sm_index, timeout=30)
            if r.status_code != 200:
                return []
            locs = re.findall(r"<loc>(.*?)</loc>", r.text, flags=re.I)
            postmaps = [u for u in locs if "post" in u or "blog" in u]
            rows = []
            for sm in postmaps:
                rr = sess.get(sm, timeout=30)
                if rr.status_code != 200: 
                    continue
                for link, dt in re.findall(r"<loc>(.*?)</loc>.*?<lastmod>(.*?)</lastmod>", rr.text, flags=re.I|re.S):
                    rows.append({
                        "id": "",
                        "date": dt,
                        "link": link,
                        "title": "",
                        "content": "",
                        "tags": "",
                        "collected_at": utc_now(),
                    })
                    if len(rows) >= wp_max: break
                if len(rows) >= wp_max: break
                time.sleep(delay)
            return rows
        except Exception:
            return []

    use_rest = (mode == "rest") or (mode == "auto" and rest_available())
    rows = fetch_rest() if use_rest else fetch_sitemap_light()
    write_csv(out_csv, rows, fields)

    with open(OUTDIR / "wp_state.json", "w", encoding="utf-8") as f:
        json.dump({
            "completed": len(rows),
            "queue": [],
            "mode": "rest" if use_rest else "sitemap",
            "generated_at": utc_now(),
        }, f, ensure_ascii=False, indent=2)

    print(f"[wp] wrote {len(rows)} rows via {'rest' if use_rest else 'sitemap'} → {out_csv.name}")

# ---------------------------- PubMed ---------------------------------

def run_pubmed(author_query: str):
    out = OUTDIR / "pubmed_eppley.csv"
    fields = ["pmid","title","year","journal","authors","doi","link","source","collected_at"]
    sess = session()

    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    p = {"db":"pubmed","term":author_query,"retmax":5000,"retmode":"json"}
    r = sess.get(url, params=p, timeout=30)
    ids = (r.json() or {}).get("esearchresult",{}).get("idlist",[])
    rows: List[Dict[str,Any]] = []
    if not ids:
        write_csv(out, rows, fields)
        print("[pubmed] wrote 0 rows")
        return

    sum_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    for i in range(0, len(ids), 200):
        chunk = ids[i:i+200]
        rr = sess.get(sum_url, params={"db":"pubmed","id":",".join(chunk),"retmode":"json"}, timeout=30)
        js = rr.json() or {}
        result = js.get("result", {})
        for pmid in chunk:
            it = result.get(pmid) or {}
            auths = ", ".join([g(a,"name",default="") for a in it.get("authors") or [] if g(a,"name",default="")])
            eloc = it.get("elocationid","") or ""
            doi = eloc.replace("doi:","").strip() if "doi" in eloc.lower() else ""
            rows.append({
                "pmid": pmid,
                "title": it.get("title",""),
                "year": it.get("pubdate","")[:4],
                "journal": it.get("fulljournalname",""),
                "authors": auths,
                "doi": doi,
                "link": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                "source": "pubmed",
                "collected_at": utc_now(),
            })
        time.sleep(0.34)

    write_csv(out, rows, fields)
    print(f"[pubmed] wrote {len(rows)} rows → {out.name}")

# --------------------------- Crossref --------------------------------

def run_crossref(names: List[str]):
    out = OUTDIR / "crossref_works.csv"
    fields = ["title","year","journal","authors","doi","url","source","collected_at"]
    sess = session()
    rows: List[Dict[str,Any]] = []

    for n in names:
        cursor = "*"
        for _ in range(10):
            params = {"query.author": n, "rows": 200, "cursor": cursor, "mailto": "none@example.com"}
            r = sess.get("https://api.crossref.org/works", params=params, timeout=45)
            if r.status_code != 200: break
            js = r.json() or {}
            items = g(js, "message", "items", default=[])
            for it in items:
                title = " ".join(it.get("title") or []) if it.get("title") else ""
                authors = []
                for a in it.get("author") or []:
                    nm = " ".join(filter(None, [a.get("given",""), a.get("family","")])).strip()
                    if nm: authors.append(nm)
                rows.append({
                    "title": title,
                    "year": g(it,"issued","date-parts",0,0,default=""),
                    "journal": g(it,"container-title",0,default=""),
                    "authors": ", ".join(authors),
                    "doi": it.get("DOI",""),
                    "url": g(it,"URL",default=""),
                    "source": "crossref",
                    "collected_at": utc_now(),
                })
            cursor = g(js, "message", "next-cursor", default="")
            if not cursor: break
            time.sleep(0.2)

    ded = {}
    for r in rows:
        k = r.get("doi") or r.get("title")
        if k and k not in ded: ded[k] = r
    rows = list(ded.values())
    write_csv(out, rows, fields)
    print(f"[crossref] wrote {len(rows)} rows → {out.name}")

# --------------------------- OpenAlex --------------------------------

def run_openalex(names: List[str]):
    CSV = OUTDIR / "openalex_works.csv"
    JSONL = OUTDIR / "openalex_works.jsonl"
    fields = ["title","year","journal","venue","authors","doi","pmid","url",
              "openalex_id","cited_by_count","source","collected_at"]
    sess = session()

    def get_json(params, retries=4, backoff=0.7):
        for i in range(retries):
            try:
                r = sess.get("https://api.openalex.org/works", params=params, timeout=45)
                if r.status_code == 200: return r.json()
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(backoff*(i+1)); continue
                return None
            except requests.RequestException:
                time.sleep(backoff*(i+1))
        return None

    rows: List[Dict[str,Any]] = []
    for n in names:
        page = 1
        while True:
            params = {
                "search": f'author.display_name.search:"{n}"',
                "per_page": 200,
                "sort": "publication_year:desc",
                "page": page
            }
            js = get_json(params)
            if not js or not js.get("results"): break
            for it in js["results"]:
                ids = g(it, "ids", default={}) or {}
                doi = (ids.get("doi") or "").replace("https://doi.org/","").strip()
                auths = []
                for a in g(it,"authorships",default=[]):
                    nm = g(a,"author","display_name",default="")
                    if nm: auths.append(nm)
                journal = g(it,"host_venue","display_name",default="") or g(it,"primary_location","source","display_name",default="")
                url = g(it,"primary_location","source","url",default="") or g(it,"primary_location","landing_page_url",default="")
                rows.append({
                    "title": g(it,"title",default=""),
                    "year": g(it,"publication_year",default=""),
                    "journal": journal,
                    "venue": g(it,"host_venue","display_name",default=""),
                    "authors": ", ".join(auths),
                    "doi": doi,
                    "pmid": g(it,"ids","pmid",default="").replace("https://pubmed.ncbi.nlm.nih.gov/",""),
                    "url": url,
                    "openalex_id": g(it,"id",default=""),
                    "cited_by_count": g(it,"cited_by_count",default=""),
                    "source": "openalex",
                    "collected_at": utc_now(),
                })
            last = int(g(js,"meta","last_page",default=page))
            if page >= last: break
            page += 1
            time.sleep(0.25)

    ded = {}
    for r in rows:
        k = r.get("doi") or r.get("title")
        if k and k not in ded: ded[k] = r
    rows = list(ded.values())

    write_csv(CSV, rows, fields)
    with open(JSONL, "w", encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r, ensure_ascii=False)+"\n")
    print(f"[openalex] wrote {len(rows)} rows → {CSV.name}")

# ----------------------- ClinicalTrials.gov --------------------------

def run_ct(terms: List[str]):
    out = OUTDIR / "clinical_trials.csv"
    fields = ["nct_id","title","status","conditions","study_type","start_date","completion_date","url","source","collected_at"]
    sess = session()
    rows: List[Dict[str,Any]] = []
    for t in terms:
        p = {
            "expr": t,
            "fmt": "json",
            "fields": ",".join(["NCTId","BriefTitle","OverallStatus","Condition","StudyType","StartDate","PrimaryCompletionDate","NCTId"]),
            "max_rnk": 1000
        }
        r = sess.get("https://clinicaltrials.gov/api/query/study_fields", params=p, timeout=45)
        js = (r.json() or {}).get("StudyFieldsResponse",{}).get("StudyFields",[])
        for it in js:
            nct = (it.get("NCTId") or [""])[0]
            rows.append({
                "nct_id": nct,
                "title": (it.get("BriefTitle") or [""])[0],
                "status": (it.get("OverallStatus") or [""])[0],
                "conditions": "|".join(it.get("Condition") or []),
                "study_type": (it.get("StudyType") or [""])[0],
                "start_date": (it.get("StartDate") or [""])[0],
                "completion_date": (it.get("PrimaryCompletionDate") or [""])[0],
                "url": f"https://clinicaltrials.gov/study/{nct}" if nct else "",
                "source": "clinicaltrials",
                "collected_at": utc_now(),
            })
        time.sleep(0.3)

    write_csv(out, rows, fields)
    print(f"[clinicaltrials] wrote {len(rows)} rows → {out.name}")

# ------------------------------- ORCID -------------------------------

def run_orcid(names: List[str]):
    prof_csv = OUTDIR / "orcid_profiles.csv"
    works_csv = OUTDIR / "orcid_works.csv"
    prof_fields = ["orcid","name","affiliation","country","url","source","collected_at"]
    work_fields = ["orcid","work_title","work_type","year","doi","url","source","collected_at"]
    sess = session()
    rows_p, rows_w = [], []

    for n in names:
        r = sess.get(
            "https://pub.orcid.org/v3.0/expanded-search/",
            params={"q": n, "rows": 50},
            headers={"Accept":"application/json","User-Agent": UA},
            timeout=45
        )
        js = r.json() if r.status_code==200 else {}
        for it in js.get("expanded-result", []) or []:
            orcid = it.get("orcid-id","")
            url = f"https://orcid.org/{orcid}" if orcid else ""
            rows_p.append({
                "orcid": orcid,
                "name": f'{it.get("given-names","")} {it.get("family-names","")}'.strip(),
                "affiliation": g(it,"institution-name",0,default=""),
                "country": g(it,"country",0,default=""),
                "url": url,
                "source":"orcid",
                "collected_at": utc_now(),
            })
            if orcid:
                rr = sess.get(f"https://pub.orcid.org/v3.0/{orcid}/works", headers={"Accept":"application/json"}, timeout=45)
                jj = rr.json() if rr.status_code==200 else {}
                for wk in jj.get("group", []) or []:
                    title = g(wk,"work-summary",0,"title","title","value",default="")
                    typ   = g(wk,"work-summary",0,"type",default="")
                    yr    = g(wk,"work-summary",0,"publication-date","year","value",default="")
                    doi = ""
                    ext = g(wk,"work-summary",0,"external-ids","external-id",default=[]) or []
                    for e in ext:
                        if g(e,"external-id-type",default="").lower()=="doi":
                            doi = g(e,"external-id-value",default="")
                            break
                    rows_w.append({
                        "orcid": orcid,
                        "work_title": title,
                        "work_type": typ,
                        "year": yr,
                        "doi": doi,
                        "url": url,
                        "source":"orcid",
                        "collected_at": utc_now(),
                    })
        time.sleep(0.4)

    write_csv(prof_csv, rows_p, prof_fields)
    write_csv(works_csv, rows_w, work_fields)
    print(f"[orcid] wrote {len(rows_p)} profiles, {len(rows_w)} works")

# ------------------------------ YouTube ------------------------------

def run_youtube_shim():
    script = ROOT / "youtube_collect.py"
    if not script.exists():
        print("[yt] youtube_collect.py not present; skipping")
        return
    import subprocess
    proc = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    print("[yt] completed")

# ------------------------------- CLI ---------------------------------

def main():
    cfg = load_config()
    names = cfg.get("names") or ["Barry L. Eppley","Barry Eppley","Eppley BL"]
    wp_base = cfg.get("wordpress_base","https://exploreplasticsurgery.com")
    ct_terms = cfg.get("clinicaltrials_terms") or ['"Barry Eppley"','Eppley Barry','Eppley']
    pubmed_author = cfg.get("pubmed_author_query","Eppley BL[Author]")

    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="all",
                    choices=["all","wp","pubmed","crossref","openalex","ct","orcid","yt"])
    ap.add_argument("--wp-mode", default="auto", choices=["auto","rest","sitemap"])
    ap.add_argument("--wp-max", type=int, default=100000)
    ap.add_argument("--save-every", type=int, default=200)
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("--per-page", type=int, default=100)
    args = ap.parse_args()

    if args.only in ("wp","all"):
        run_wp(wp_base, mode=args.wp_mode, per_page=args.per_page,
               save_every=args.save_every, delay=args.delay, wp_max=args.wp_max)

    if args.only in ("pubmed","all"):
        run_pubmed(pubmed_author)

    if args.only in ("crossref","all"):
        run_crossref(names)

    if args.only in ("openalex","all"):
        run_openalex(names)

    if args.only in ("ct","all"):
        run_ct(ct_terms)

    if args.only in ("orcid","all"):
        run_orcid(names)

    if args.only in ("yt","all"):
        run_youtube_shim()

    print("[done] all requested collectors completed.")

if __name__ == "__main__":
    main()