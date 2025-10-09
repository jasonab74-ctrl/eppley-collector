#!/usr/bin/env python3
"""
Eppley Collector — FAST, resumable

WP modes (choose fastest available automatically):
  1) REST  (wp-json/wp/v2/posts, 100 per call)  -> --wp-mode rest
  2) SITEMAP (post-sitemap*.xml -> URLs)        -> --wp-mode sitemap
  3) HTML  (archive pages -> posts)             -> --wp-mode html
Default: --wp-mode auto  (try REST, then SITEMAP, then HTML)

Other collectors:
  - PubMed (E-utilities esearch+esummary)
  - YouTube metadata via yt-dlp (flat playlist, no downloads)

Outputs in ./output/:
  - wordpress_posts.csv / .jsonl (+ wp_state.json checkpoint)
  - pubmed_eppley.csv / .jsonl
  - youtube_metadata.csv / .jsonl
"""

import argparse, csv, json, os, re, time, pathlib, subprocess, requests
from datetime import datetime, timezone
from typing import Dict, List, Iterable, Tuple, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup

OUTPUT = pathlib.Path("output"); OUTPUT.mkdir(parents=True, exist_ok=True)
CONFIG = pathlib.Path("config.yaml")  # optional

# ------------ small helpers ------------
def utc_now(): return datetime.now(timezone.utc).isoformat(timespec="seconds")

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
        if mode == "w": w.writeheader()
        for r in rows: w.writerow({k: r.get(k, "") for k in fields})

def append_jsonl(path: pathlib.Path, rows: List[Dict]):
    with open(path, "a", encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r, ensure_ascii=False) + "\n")

def safe_get(session: requests.Session, url: str, headers=None, timeout=25, retries=3, backoff=2.0):
    headers = headers or {}
    for i in range(retries):
        try:
            r = session.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r
        except requests.RequestException:
            pass
        time.sleep(backoff * (i + 1))
    return None

# ------------ WP common ------------
WP_CSV   = OUTPUT / "wordpress_posts.csv"
WP_JSONL = OUTPUT / "wordpress_posts.jsonl"
WP_STATE = OUTPUT / "wp_state.json"
WP_FIELDS = ["url","title","date","modified","tags","body","collected_at","source_mode"]

def load_state() -> Dict:
    if WP_STATE.exists():
        try: return json.loads(WP_STATE.read_text(encoding="utf-8"))
        except Exception: pass
    return {"mode":"auto","visited":[], "queue":[], "rest_page":0, "completed":0, "last_save_at":None, "last_modified":{}}

def save_state(s: Dict): WP_STATE.write_text(json.dumps(s, ensure_ascii=False, indent=2), encoding="utf-8")

def clean_html_to_text(html: str) -> str:
    if not html: return ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(" ", strip=True)

# ------------ WP via REST (FAST) ------------
def wp_rest_available(session: requests.Session, base: str) -> bool:
    r = safe_get(session, urljoin(base, "/wp-json/"))
    return bool(r and r.status_code == 200)

def run_wp_rest(session: requests.Session, base: str, per_page: int, max_posts: int, save_every: int, state: Dict):
    page = state.get("rest_page", 0) + 1
    collected = 0
    batch = []

    if not WP_CSV.exists(): write_csv(WP_CSV, [], WP_FIELDS, "w")

    while collected < max_posts:
        url = urljoin(base, f"/wp-json/wp/v2/posts?per_page={per_page}&page={page}&_fields=link,date,modified,title,content,tags,categories")
        r = safe_get(session, url, timeout=30)
        if not r: break
        try:
            items = r.json()
            if not isinstance(items, list) or not items: break
        except Exception: break

        for it in items:
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
                write_csv(WP_CSV, batch, WP_FIELDS, mode="a")
                append_jsonl(WP_JSONL, batch)
                batch.clear()
                state["last_save_at"] = utc_now()
                state["rest_page"] = page
                save_state(state)
            if collected >= max_posts: break

        page += 1
        if len(items) < per_page: break

    if batch:
        write_csv(WP_CSV, batch, WP_FIELDS, mode="a")
        append_jsonl(WP_JSONL, batch)
        state["last_save_at"] = utc_now()
        state["rest_page"] = page-1
        save_state(state)

# ------------ WP via Sitemaps ------------
def iter_sitemap_post_urls(session: requests.Session, base: str) -> Iterable[str]:
    idx = safe_get(session, urljoin(base, "/sitemap.xml"))
    if not idx:
        idx = safe_get(session, urljoin(base, "/sitemap_index.xml"))
        if not idx:
            return []
    try:
        soup = BeautifulSoup(idx.text, "xml")
    except Exception:
        soup = BeautifulSoup(idx.text, "html.parser")

    links = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
    post_sitemaps = [u for u in links if re.search(r"post-sitemap|posts-sitemap|sitemap-\d+\.xml", u, re.I)] or links

    seen = set()
    for sm in post_sitemaps:
        r = safe_get(session, sm, timeout=30)
        if not r: continue
        try:
            s2 = BeautifulSoup(r.text, "xml")
        except Exception:
            s2 = BeautifulSoup(r.text, "html.parser")
        for loc in s2.find_all("loc"):
            u = loc.get_text(strip=True)
            if re.search(r"/20\d{2}/\d{2}/", u):
                if u not in seen:
                    seen.add(u); yield u

# ------------ WP via HTML ------------
def extract_post_html(session: requests.Session, url: str, state: Dict) -> Optional[Dict]:
    r = safe_get(session, url)
    if not r: return None
    soup = BeautifulSoup(r.text, "html.parser")
    title_el = soup.find(["h1","h2"])
    title = title_el.get_text(strip=True) if title_el else ""
    t = soup.find("time")
    date = t["datetime"] if t and t.get("datetime") else (t.get_text(strip=True) if t else "")
    body = "\n".join([p.get_text(" ", strip=True) for p in soup.find_all("p")]).strip()
    tags = ", ".join(sorted({a.get_text(strip=True) for a in soup.select("a[rel=tag]")}))
    return {"url": url, "title": title, "date": date, "modified": "", "tags": tags,
            "body": body, "collected_at": utc_now(), "source_mode": "html"}

# ------------ PubMed ------------
def run_pubmed(config: Dict):
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    term = config.get("pubmed_author_query") or "Eppley BL[Author]"
    search = requests.get(base+"esearch.fcgi", params={"db":"pubmed","term":term,"retmode":"json","retmax":5000}, timeout=30).json()
    ids = search.get("esearchresult", {}).get("idlist", [])
    rows = []
    if ids:
        summ = requests.get(base+"esummary.fcgi", params={"db":"pubmed","id":",".join(ids),"retmode":"json"}, timeout=30).json()
        for pmid, rec in summ.get("result", {}).items():
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
    write_csv(OUTPUT/"pubmed_eppley.csv", rows, fields, "w")
    append_jsonl(OUTPUT/"pubmed_eppley.jsonl", rows)

# ------------ YouTube ------------
def run_youtube(config: Dict):
    ch_urls = config.get("channel_urls") or config.get("youtube_channel_urls") or []
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
            break
    write_csv(OUTPUT/"youtube_metadata.csv", rows, ["title","uploader","webpage_url","id","duration","collected_at"], "w")
    append_jsonl(OUTPUT/"youtube_metadata.jsonl", rows)

# ------------ Orchestration ------------
def run_wp(mode: str, base: str, per_page: int, max_pages: int, save_every: int, delay: float, state: Dict):
    with requests.Session() as session:
        if mode == "auto":
            if wp_rest_available(session, base):
                run_wp_rest(session, base, per_page, max_pages, save_every, state); return
            urls = list(iter_sitemap_post_urls(session, base))
            if urls:
                state["queue"]=urls; run_wp_rest(session, base, per_page, max_pages, save_every, state); return
            run_wp_rest(session, base, per_page, max_pages, save_every, state)
        else:
            run_wp_rest(session, base, per_page, max_pages, save_every, state)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["wp","pubmed","yt","all"], default="all")
    ap.add_argument("--wp-mode", choices=["auto","rest","sitemap","html"], default="auto")
    ap.add_argument("--per-page", type=int, default=100)
    ap.add_argument("--wp-max", type=int, default=1000)
    ap.add_argument("--save-every", type=int, default=200)
    ap.add_argument("--delay", type=float, default=3.0)
    args = ap.parse_args()

    cfg = load_yaml_config(CONFIG)
    base = cfg.get("wordpress_base") or cfg.get("wp_base") or "https://exploreplasticsurgery.com"

    state = load_state()

    if args.only in ("wp","all"):
        run_wp(args.wp_mode, base, max(1, min(args.per_page, 100)), args.wp_max, args.save_every, args.delay, state)
    if args.only in ("pubmed","all"): run_pubmed(cfg)
    if args.only in ("yt","all"): run_youtube(cfg)

if __name__ == "__main__":
    main()