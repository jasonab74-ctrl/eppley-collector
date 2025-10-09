#!/usr/bin/env python3
"""
Eppley Collector — API-first, resumable, and stable.

Sources covered:
  - WordPress (REST first, then sitemap, then HTML fallback)
  - PubMed (E-utilities)
  - YouTube metadata (yt-dlp --flat-playlist)
  - Crossref (works metadata)
  - OpenAlex (works + metrics)
  - ClinicalTrials.gov v2 (studies)
  - ORCID (profiles search + public works)

Outputs to ./output/ as CSV + JSONL per source.
Designed to run safely on GitHub Actions with no secrets.

Usage (CLI):
  python main.py --only all
  python main.py --only wp --wp-mode auto --wp-max 1200
"""

import argparse, csv, json, os, re, time, pathlib, subprocess, requests
from datetime import datetime, timezone
from typing import Dict, List, Iterable, Optional
from urllib.parse import urljoin, urlencode
from bs4 import BeautifulSoup

# ------------------------------ paths & constants ------------------------------
OUTPUT = pathlib.Path("output"); OUTPUT.mkdir(parents=True, exist_ok=True)
CONFIG = pathlib.Path("config.yaml")  # optional
UA = "eppley-collector/3.0 (+github actions)"

# CSVs
WP_CSV   = OUTPUT / "wordpress_posts.csv"
WP_JSONL = OUTPUT / "wordpress_posts.jsonl"
WP_STATE = OUTPUT / "wp_state.json"

PM_CSV   = OUTPUT / "pubmed_eppley.csv"
PM_JSONL = OUTPUT / "pubmed_eppley.jsonl"

YT_CSV   = OUTPUT / "youtube_metadata.csv"
YT_JSONL = OUTPUT / "youtube_metadata.jsonl"

CR_CSV   = OUTPUT / "crossref_works.csv"
CR_JSONL = OUTPUT / "crossref_works.jsonl"

OA_CSV   = OUTPUT / "openalex_works.csv"
OA_JSONL = OUTPUT / "openalex_works.jsonl"

CT_CSV   = OUTPUT / "clinical_trials.csv"
CT_JSONL = OUTPUT / "clinical_trials.jsonl"

ORCID_PROF_CSV   = OUTPUT / "orcid_profiles.csv"
ORCID_PROF_JSONL = OUTPUT / "orcid_profiles.jsonl"
ORCID_WORKS_CSV  = OUTPUT / "orcid_works.csv"
ORCID_WORKS_JSONL= OUTPUT / "orcid_works.jsonl"

# ------------------------------ helpers ------------------------------
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def load_yaml_config(path: pathlib.Path) -> Dict:
    try:
        import yaml
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

def write_csv(path: pathlib.Path, rows: List[Dict], fields: List[str], mode="w"):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, mode, encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if mode == "w":
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})

def append_jsonl(path: pathlib.Path, rows: List[Dict]):
    if not rows: return
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def safe_get_json(url: str, params=None, headers=None, timeout=30, retries=3, backoff=2.0):
    headers = {"User-Agent": UA, **(headers or {})}
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff * (i + 1))
                continue
        except requests.RequestException:
            time.sleep(backoff * (i + 1))
    return None

def safe_get_text(url: str, headers=None, timeout=30, retries=3, backoff=2.0):
    headers = {"User-Agent": UA, **(headers or {})}
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff * (i + 1))
                continue
        except requests.RequestException:
            time.sleep(backoff * (i + 1))
    return None

def clean_html_to_text(html: str) -> str:
    if not html: return ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(" ", strip=True)

# ------------------------------ WordPress (REST → sitemap → HTML) ------------------------------
WP_FIELDS = ["url","title","date","modified","tags","body","collected_at","source_mode"]

def load_state() -> Dict:
    if WP_STATE.exists():
        try: return json.loads(WP_STATE.read_text(encoding="utf-8"))
        except Exception: pass
    return {"mode":"auto","visited":[], "queue":[], "rest_page":0, "completed":0, "last_save_at":None, "last_modified":{}}

def save_state(s: Dict):
    WP_STATE.write_text(json.dumps(s, ensure_ascii=False, indent=2), encoding="utf-8")

def wp_rest_available(base: str) -> bool:
    j = safe_get_json(urljoin(base, "/wp-json/"))
    return bool(j)

def run_wp_rest(base: str, per_page: int, max_posts: int, save_every: int, state: Dict):
    page = state.get("rest_page", 0) + 1
    collected = 0
    batch = []
    if not WP_CSV.exists(): write_csv(WP_CSV, [], WP_FIELDS, "w")

    while collected < max_posts:
        url = urljoin(base, f"/wp-json/wp/v2/posts?per_page={per_page}&page={page}&_fields=link,date,modified,title,content,tags,categories")
        j = safe_get_json(url, timeout=30)
        if not j or not isinstance(j, list) or not j: break
        for it in j:
            row = {
                "url": it.get("link",""),
                "title": clean_html_to_text((it.get("title") or {}).get("rendered","")),
                "date": it.get("date",""),
                "modified": it.get("modified",""),
                "tags": "",
                "body": clean_html_to_text((it.get("content") or {}).get("rendered","")),
                "collected_at": utc_now(),
                "source_mode": "rest"
            }
            batch.append(row)
            collected += 1
            state["completed"] = state.get("completed",0) + 1
            if len(batch) >= save_every:
                write_csv(WP_CSV, batch, WP_FIELDS, "a"); append_jsonl(WP_JSONL, batch)
                batch.clear(); state["last_save_at"] = utc_now(); state["rest_page"] = page; save_state(state)
            if collected >= max_posts: break
        page += 1
        if len(j) < per_page: break

    if batch:
        write_csv(WP_CSV, batch, WP_FIELDS, "a"); append_jsonl(WP_JSONL, batch)
        state["last_save_at"] = utc_now(); state["rest_page"] = page-1; save_state(state)

def iter_sitemap_post_urls(base: str) -> Iterable[str]:
    for sm in ("/sitemap.xml", "/sitemap_index.xml"):
        txt = safe_get_text(urljoin(base, sm))
        if not txt: continue
        try: soup = BeautifulSoup(txt, "xml")
        except Exception: soup = BeautifulSoup(txt, "html.parser")
        links = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
        post_sitemaps = [u for u in links if re.search(r"post-sitemap|posts-sitemap|sitemap-\d+\.xml", u, re.I)] or links
        seen = set()
        for p in post_sitemaps:
            t2 = safe_get_text(p)
            if not t2: continue
            try: s2 = BeautifulSoup(t2, "xml")
            except Exception: s2 = BeautifulSoup(t2, "html.parser")
            for loc in s2.find_all("loc"):
                u = loc.get_text(strip=True)
                if re.search(r"/20\d{2}/\d{2}/", u) or "your-questions" in u:
                    if u not in seen: seen.add(u); yield u
        break

def extract_post_html(url: str, state: Dict) -> Optional[Dict]:
    txt = safe_get_text(url)
    if not txt: return None
    soup = BeautifulSoup(txt, "html.parser")
    title_el = soup.find(["h1","h2"])
    title = title_el.get_text(strip=True) if title_el else ""
    t = soup.find("time")
    date = t["datetime"] if t and t.get("datetime") else (t.get_text(strip=True) if t else "")
    body = "\n".join([p.get_text(" ", strip=True) for p in soup.find_all("p")]).strip()
    tags = ", ".join(sorted({a.get_text(strip=True) for a in soup.select("a[rel=tag]")}))
    return {"url": url, "title": title, "date": date, "modified": "", "tags": tags,
            "body": body, "collected_at": utc_now(), "source_mode": "html"}

def run_wp_sitemap(base: str, delay: float, max_pages: int, save_every: int, state: Dict):
    visited = set(state.get("visited", []))
    queue = list(state.get("queue") or [])
    if not queue:
        queue = list(iter_sitemap_post_urls(base)); state["queue"] = queue; save_state(state)
    if not WP_CSV.exists(): write_csv(WP_CSV, [], WP_FIELDS, "w")

    batch = []; processed = 0
    while queue and processed < max_pages:
        url = queue.pop(0)
        if url in visited: continue
        visited.add(url)
        row = extract_post_html(url, state)
        if row: batch.append(row)
        processed += 1; state["completed"] = state.get("completed",0) + 1
        if len(batch) >= save_every:
            write_csv(WP_CSV, batch, WP_FIELDS, "a"); append_jsonl(WP_JSONL, batch)
            batch.clear(); state["last_save_at"] = utc_now(); state["visited"] = list(visited); state["queue"] = queue; save_state(state)
        time.sleep(delay)

    if batch: write_csv(WP_CSV, batch, WP_FIELDS, "a"); append_jsonl(WP_JSONL, batch)
    state["visited"] = list(visited); state["queue"] = queue; state["last_save_at"] = utc_now(); save_state(state)

def discover_html_archive(base: str) -> List[str]:
    seeds = [base.rstrip("/") + "/your-questions", base.rstrip("/") + "/", base.rstrip("/") + "/category/", base.rstrip("/") + "/tag/"]
    out = set()
    for s in seeds:
        txt = safe_get_text(s); 
        if not txt: continue
        soup = BeautifulSoup(txt, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"/20\d{2}/\d{2}/", href) or "your-questions" in href:
                out.add(href)
    return list(out)

def run_wp_html(base: str, delay: float, max_pages: int, save_every: int, state: Dict):
    visited = set(state.get("visited", []))
    queue = list(state.get("queue") or [])
    if not queue:
        queue = discover_html_archive(base); state["queue"] = queue; save_state(state)
    if not WP_CSV.exists(): write_csv(WP_CSV, [], WP_FIELDS, "w")

    batch = []; processed = 0
    while queue and processed < max_pages:
        url = queue.pop(0)
        if url in visited: continue
        visited.add(url)
        if re.search(r"/page/\d+|category|tag|your-questions|exploreplasticsurgery", url, re.I):
            txt = safe_get_text(url)
            if txt:
                soup = BeautifulSoup(txt, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if re.search(r"/20\d{2}/\d{2}/", href) or "your-questions" in href:
                        if href not in visited: queue.append(href)
        else:
            row = extract_post_html(url, state)
            if row: batch.append(row)

        processed += 1; state["completed"] = state.get("completed",0) + 1
        if len(batch) >= save_every:
            write_csv(WP_CSV, batch, WP_FIELDS, "a"); append_jsonl(WP_JSONL, batch)
            batch.clear(); state["last_save_at"] = utc_now(); state["visited"] = list(visited); state["queue"] = queue; save_state(state)
        time.sleep(delay)

    if batch: write_csv(WP_CSV, batch, WP_FIELDS, "a"); append_jsonl(WP_JSONL, batch)
    state["visited"] = list(visited); state["queue"] = queue; state["last_save_at"] = utc_now(); save_state(state)

def run_wp(mode: str, base: str, per_page: int, max_pages: int, save_every: int, delay: float, state: Dict):
    if mode == "auto":
        if wp_rest_available(base):
            state["mode"]="rest"; save_state(state); run_wp_rest(base, per_page, max_pages, save_every, state); return
        urls = list(iter_sitemap_post_urls(base))
        if urls:
            state["mode"]="sitemap"; state["queue"]=urls; save_state(state); run_wp_sitemap(base, delay, max_pages, save_every, state); return
        state["mode"]="html"; save_state(state); run_wp_html(base, delay, max_pages, save_every, state); return
    elif mode == "rest": run_wp_rest(base, per_page, max_pages, save_every, state)
    elif mode == "sitemap": run_wp_sitemap(base, delay, max_pages, save_every, state)
    else: run_wp_html(base, delay, max_pages, save_every, state)

# ------------------------------ PubMed ------------------------------
def run_pubmed(config: Dict):
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    term = config.get("pubmed_author_query") or "Eppley BL[Author]"
    search = safe_get_json(base+"esearch.fcgi", params={"db":"pubmed","term":term,"retmode":"json","retmax":10000})
    ids = (search or {}).get("esearchresult", {}).get("idlist", [])
    rows = []
    if ids:
        summ = safe_get_json(base+"esummary.fcgi", params={"db":"pubmed","id":",".join(ids),"retmode":"json"})
        for pmid, rec in (summ or {}).get("result", {}).items():
            if pmid == "uids": continue
            rows.append({
                "pmid": pmid,
                "title": rec.get("title",""),
                "journal": rec.get("fulljournalname",""),
                "year": rec.get("pubdate",""),
                "authors": ", ".join([a.get("name","") for a in rec.get("authors", [])]),
                "doi": rec.get("elocationid",""),
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                "collected_at": utc_now()
            })
    fields = ["pmid","title","journal","year","authors","doi","url","collected_at"]
    write_csv(PM_CSV, rows, fields, "w"); append_jsonl(PM_JSONL, rows)
    print(f"[pubmed] wrote {len(rows)} rows")

# ------------------------------ YouTube (metadata) ------------------------------
def run_youtube(config: Dict):
    ch_urls = config.get("youtube_channel_urls") or []
    rows = []
    for url in ch_urls:
        try:
            proc = subprocess.run(["yt-dlp","--dump-json","--flat-playlist",url],
                                  capture_output=True, text=True, check=False)
            for line in proc.stdout.splitlines():
                try:
                    j = json.loads(line)
                    rows.append({
                        "title": j.get("title",""),
                        "uploader": j.get("uploader",""),
                        "webpage_url": j.get("webpage_url") or (("https://www.youtube.com/watch?v="+j.get("id")) if j.get("id") else ""),
                        "id": j.get("id",""),
                        "duration": j.get("duration",""),
                        "collected_at": utc_now()
                    })
                except json.JSONDecodeError:
                    continue
        except FileNotFoundError:
            print("[yt] yt-dlp missing; skipping"); break
    write_csv(YT_CSV, rows, ["title","uploader","webpage_url","id","duration","collected_at"], "w")
    append_jsonl(YT_JSONL, rows)
    print(f"[yt] wrote {len(rows)} rows")

# ------------------------------ Crossref ------------------------------
def run_crossref(config: Dict):
    names = config.get("names") or ["Barry L. Eppley","Barry Eppley","Eppley BL"]
    rows = []
    for name in names:
        url = "https://api.crossref.org/works"
        params = {"query.author": name, "rows": 1000}
        j = safe_get_json(url, params=params)
        items = ((j or {}).get("message", {}) or {}).get("items", []) or []
        for it in items:
            authors = ", ".join([" ".join([p for p in [a.get("given"), a.get("family")] if p]) for a in it.get("author",[])]) if it.get("author") else ""
            year = ""
            for k in ("published-print","published-online","issued"):
                if it.get(k) and it[k].get("date-parts"):
                    year = str(it[k]["date-parts"][0][0]); break
            rows.append({
                "doi": it.get("DOI",""),
                "title": (it.get("title") or [""])[0],
                "container": it.get("container-title", [""])[0],
                "type": it.get("type",""),
                "year": year,
                "url": it.get("URL",""),
                "authors": authors,
                "source":"crossref",
                "collected_at": utc_now()
            })
    fields = ["doi","title","container","type","year","url","authors","source","collected_at"]
    write_csv(CR_CSV, rows, fields, "w"); append_jsonl(CR_JSONL, rows)
    print(f"[crossref] wrote {len(rows)} rows")

# ------------------------------ OpenAlex ------------------------------
def run_openalex(config: Dict):
    # prefer author_ids if provided; else use search
    author_ids = config.get("openalex_author_ids") or []
    names = config.get("names") or ["Barry L. Eppley","Barry Eppley","Eppley BL"]
    rows = []
    base = "https://api.openalex.org/works"
    def page(url):
        return safe_get_json(url, headers={"Accept":"application/json"})
    if author_ids:
        for aid in author_ids:
            url = f"{base}?filter=author.id:{aid}&per-page=200"
            while url:
                j = page(url); 
                if not j: break
                for it in j.get("results", []):
                    rows.append({
                        "id": it.get("id",""),
                        "doi": it.get("doi",""),
                        "title": it.get("title",""),
                        "publication_year": it.get("publication_year",""),
                        "venue": (it.get("host_venue") or {}).get("display_name",""),
                        "type": it.get("type",""),
                        "cited_by_count": it.get("cited_by_count",""),
                        "open_access": (it.get("open_access") or {}).get("is_oa", False),
                        "url": it.get("primary_location",{}).get("source",{}).get("url","") or it.get("id",""),
                        "source":"openalex",
                        "collected_at": utc_now()
                    })
                url = j.get("meta",{}).get("next_url")
    else:
        for name in names:
            url = f"{base}?search={requests.utils.quote(name)}&per-page=200"
            while url:
                j = page(url); 
                if not j: break
                for it in j.get("results", []):
                    rows.append({
                        "id": it.get("id",""),
                        "doi": it.get("doi",""),
                        "title": it.get("title",""),
                        "publication_year": it.get("publication_year",""),
                        "venue": (it.get("host_venue") or {}).get("display_name",""),
                        "type": it.get("type",""),
                        "cited_by_count": it.get("cited_by_count",""),
                        "open_access": (it.get("open_access") or {}).get("is_oa", False),
                        "url": it.get("primary_location",{}).get("source",{}).get("url","") or it.get("id",""),
                        "source":"openalex",
                        "collected_at": utc_now()
                    })
                url = j.get("meta",{}).get("next_url")
    fields = ["id","doi","title","publication_year","venue","type","cited_by_count","open_access","url","source","collected_at"]
    write_csv(OA_CSV, rows, fields, "w"); append_jsonl(OA_JSONL, rows)
    print(f"[openalex] wrote {len(rows)} rows")

# ------------------------------ ClinicalTrials.gov v2 ------------------------------
def run_clinicaltrials(config: Dict):
    terms = config.get("clinicaltrials_terms") or ["\"Barry Eppley\"", "Eppley Barry", "Eppley"]
    rows = []
    for term in terms:
        url = "https://clinicaltrials.gov/api/v2/studies"
        params = {
            "query.term": term,
            "pageSize": 500,
            "fields": ",".join([
                "NCTId","BriefTitle","OverallStatus","Condition","InterventionName",
                "StartDate","PrimaryCompletionDate","CompletionDate",
                "LocationFacility","LeadSponsorName"
            ])
        }
        j = safe_get_json(url, params=params, headers={"Accept":"application/json"})
        if not j: continue
        for st in j.get("studies", []):
            rec = st.get("protocolSection", {}) or {}
            ident = rec.get("identificationModule", {}) or {}
            status = rec.get("statusModule", {}) or {}
            cond = rec.get("conditionsModule", {}) or {}
            arms = rec.get("armsInterventionsModule", {}) or {}
            contacts = rec.get("contactsLocationsModule", {}) or {}
            nct = ident.get("nctId","")
            rows.append({
                "nct_id": nct,
                "title": ident.get("briefTitle",""),
                "overall_status": status.get("overallStatus",""),
                "conditions": ", ".join(cond.get("conditions",[]) or []),
                "interventions": ", ".join([i.get("name","") for i in arms.get("interventions",[]) or []]),
                "start_date": status.get("startDateStruct",{}).get("date",""),
                "completion_date": status.get("completionDateStruct",{}).get("date",""),
                "location_facilities": ", ".join([l.get("facility","") for l in contacts.get("locations",[]) or []]),
                "lead_sponsor": (rec.get("sponsorsCollaboratorsModule",{}) or {}).get("leadSponsor",{}).get("name",""),
                "url": f"https://clinicaltrials.gov/study/{nct}" if nct else "",
                "collected_at": utc_now()
            })
    fields = ["nct_id","title","overall_status","conditions","interventions","start_date","completion_date","location_facilities","lead_sponsor","url","collected_at"]
    write_csv(CT_CSV, rows, fields, "w"); append_jsonl(CT_JSONL, rows)
    print(f"[clinicaltrials] wrote {len(rows)} rows")

# ------------------------------ ORCID (public) ------------------------------
def run_orcid(config: Dict):
    # Search for likely profiles
    names = config.get("names") or ["Barry L. Eppley","Barry Eppley","Eppley BL"]
    prof_rows, works_rows = [], []
    headers = {"Accept":"application/json"}
    for name in names:
        q = f'given-names:"{name.split()[0]}" AND family-name:"{name.split()[-1]}"'
        url = f"https://pub.orcid.org/v3.0/expanded-search/?q={requests.utils.quote(q)}&rows=50"
        j = safe_get_json(url, headers=headers)
        for item in (j or {}).get("expanded-result", []) or []:
            orcid = item.get("orcid-id","")
            prof_rows.append({
                "orcid": orcid,
                "given_names": item.get("given-names",""),
                "family_name": item.get("family-name",""),
                "institution": (item.get("institution-name") or ""),
                "last_modified": item.get("last-modified-date",""),
                "keywords": ", ".join(item.get("keywords",[]) or []),
                "collected_at": utc_now()
            })
            # Try to fetch public works for each matched ORCID
            if orcid:
                wurl = f"https://pub.orcid.org/v3.0/{orcid}/works"
                jw = safe_get_json(wurl, headers=headers)
                if not jw: continue
                for g in (jw.get("group") or []):
                    for summ in (g.get("work-summary") or []):
                        works_rows.append({
                            "orcid": orcid,
                            "title": (summ.get("title",{}) or {}).get("title",{}).get("value",""),
                            "journal": (summ.get("journal-title",{}) or {}).get("value",""),
                            "pubyear": ((summ.get("publication-date",{}) or {}).get("year",{}) or {}).get("value",""),
                            "put_code": summ.get("put-code",""),
                            "type": summ.get("type",""),
                            "external_ids": ", ".join([ (e.get("external-id-type","")+":"+e.get("external-id-value","")) for e in (summ.get("external-ids",{}) or {}).get("external-id",[]) ]),
                            "url": ((summ.get("url",{}) or {}).get("value","")),
                            "collected_at": utc_now()
                        })
    write_csv(ORCID_PROF_CSV, prof_rows, ["orcid","given_names","family_name","institution","last_modified","keywords","collected_at"], "w")
    append_jsonl(ORCID_PROF_JSONL, prof_rows)
    write_csv(ORCID_WORKS_CSV, works_rows, ["orcid","title","journal","pubyear","put_code","type","external_ids","url","collected_at"], "w")
    append_jsonl(ORCID_WORKS_JSONL, works_rows)
    print(f"[orcid] profiles {len(prof_rows)} • works {len(works_rows)} rows")

# ------------------------------ Orchestrator ------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["wp","pubmed","yt","crossref","openalex","ct","orcid","all"], default="all")
    ap.add_argument("--wp-mode", choices=["auto","rest","sitemap","html"], default="auto")
    ap.add_argument("--per-page", type=int, default=100, help="WordPress REST page size (1..100)")
    ap.add_argument("--wp-max", type=int, default=1000, help="max WP posts/pages this run")
    ap.add_argument("--save-every", type=int, default=200, help="flush every N posts")
    ap.add_argument("--delay", type=float, default=3.0, help="delay for HTML mode (s)")
    args = ap.parse_args()

    cfg = load_yaml_config(CONFIG)
    base = cfg.get("wordpress_base") or "https://exploreplasticsurgery.com"

    # WP resume state
    state = load_state()

    if args.only in ("wp","all"):
        run_wp(args.wp_mode, base, max(1,min(args.per_page,100)), args.wp_max, args.save_every, args.delay, state)

    if args.only in ("pubmed","all"):   run_pubmed(cfg)
    if args.only in ("yt","all"):       run_youtube(cfg)
    if args.only in ("crossref","all"): run_crossref(cfg)
    if args.only in ("openalex","all"): run_openalex(cfg)
    if args.only in ("ct","all"):       run_clinicaltrials(cfg)
    if args.only in ("orcid","all"):    run_orcid(cfg)

    print(json.dumps({
        "outputs": [str(p) for p in OUTPUT.glob("*.csv")],
        "generated_at": utc_now()
    }, indent=2))

if __name__ == "__main__":
    main()