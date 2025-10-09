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
            r = session.get(url, headers=headers, timeout=timeout,
                            headers_update={"User-Agent": "eppley-collector/2.0"})
            # headers_update is available in requests >= 2.32; fallback:
        except TypeError:
            hdr = {"User-Agent": "eppley-collector/2.0"}; hdr.update(headers or {})
            try:
                r = session.get(url, headers=hdr, timeout=timeout)
            except requests.RequestException:
                r = None
        except requests.RequestException:
            r = None
        if r and r.status_code == 200: return r
        if r and r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff * (i+1)); continue
        time.sleep(backoff * (i+1))
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
                "tags": "",  # could resolve via /wp/v2/tags if needed
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
        if len(items) < per_page: break  # no more pages

    if batch:
        write_csv(WP_CSV, batch, WP_FIELDS, mode="a")
        append_jsonl(WP_JSONL, batch)
        state["last_save_at"] = utc_now()
        state["rest_page"] = page-1
        save_state(state)

# ------------ WP via Sitemaps (fast discovery) ------------
def iter_sitemap_post_urls(session: requests.Session, base: str) -> Iterable[str]:
    idx = safe_get(session, urljoin(base, "/sitemap.xml"))
    if not idx: 
        # some sites use /sitemap_index.xml
        idx = safe_get(session, urljoin(base, "/sitemap_index.xml"))
        if not idx:
            return []
    try:
        soup = BeautifulSoup(idx.text, "xml")
    except Exception:
        soup = BeautifulSoup(idx.text, "html.parser")

    # find any sitemap that smells like posts
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
            if re.search(r"/20\d{2}/\d{2}/", u) or "your-questions" in u or True:
                if u not in seen:
                    seen.add(u); yield u

def run_wp_sitemap(session: requests.Session, base: str, delay: float, max_pages: int, save_every: int, state: Dict):
    visited = set(state.get("visited", []))
    queue = list(state.get("queue") or [])
    if not queue:
        queue = list(iter_sitemap_post_urls(session, base))
        state["queue"] = queue; save_state(state)

    if not WP_CSV.exists(): write_csv(WP_CSV, [], WP_FIELDS, "w")

    batch = []; processed = 0
    while queue and processed < max_pages:
        url = queue.pop(0)
        if url in visited: continue
        visited.add(url)

        row = extract_post_html(session, url, state)
        if row:
            batch.append(row)

        processed += 1
        state["completed"] = state.get("completed",0) + 1
        if len(batch) >= save_every:
            write_csv(WP_CSV, batch, WP_FIELDS, "a")
            append_jsonl(WP_JSONL, batch)
            batch.clear()
            state["last_save_at"] = utc_now()
            state["visited"] = list(visited); state["queue"] = queue
            save_state(state)
        time.sleep(delay)

    if batch:
        write_csv(WP_CSV, batch, WP_FIELDS, "a")
        append_jsonl(WP_JSONL, batch)
    state["visited"] = list(visited); state["queue"] = queue; state["last_save_at"] = utc_now()
    save_state(state)

# ------------ WP via HTML (fallback) ------------
def discover_html_archive(session: requests.Session, base: str) -> List[str]:
    seeds = [
        base.rstrip("/") + "/your-questions",
        base.rstrip("/") + "/",
        base.rstrip("/") + "/category/",
        base.rstrip("/") + "/tag/"
    ]
    out = set()
    for s in seeds:
        r = safe_get(session, s)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"/20\d{2}/\d{2}/", href) or "your-questions" in href:
                out.add(href)
    return list(out)

def extract_post_html(session: requests.Session, url: str, state: Dict) -> Optional[Dict]:
    # Conditional fetch if we saw it before
    last_mod = (state.get("last_modified") or {}).get(url)
    headers = {"If-Modified-Since": last_mod} if last_mod else None
    r = safe_get(session, url, headers=headers)
    if not r:
        return None
    if r.status_code == 304:  # unchanged
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    title_el = soup.find(["h1","h2"])
    title = title_el.get_text(strip=True) if title_el else ""
    # date
    date = ""; mod = ""
    t = soup.find("time")
    if t and t.get("datetime"): date = t["datetime"]
    elif t: date = t.get_text(strip=True)
    # body
    body = "\n".join([p.get_text(" ", strip=True) for p in soup.find_all("p")]).strip()
    tags = ", ".join(sorted({a.get_text(strip=True) for a in soup.select("a[rel=tag]")}))
    # track Last-Modified for next runs
    lm = r.headers.get("Last-Modified")
    if lm:
        st = state.get("last_modified") or {}
        st[url] = lm
        state["last_modified"] = st
    return {"url": url, "title": title, "date": date, "modified": mod, "tags": tags,
            "body": body, "collected_at": utc_now(), "source_mode": "html"}

def run_wp_html(session: requests.Session, base: str, delay: float, max_pages: int, save_every: int, state: Dict):
    visited = set(state.get("visited", []))
    queue = list(state.get("queue") or [])
    if not queue:
        queue = discover_html_archive(session, base)
        state["queue"] = queue; save_state(state)

    if not WP_CSV.exists(): write_csv(WP_CSV, [], WP_FIELDS, "w")

    batch = []; processed = 0
    while queue and processed < max_pages:
        url = queue.pop(0)
        if url in visited: continue
        visited.add(url)

        # If listing page, push post links; else extract
        if re.search(r"/page/\d+|category|tag|your-questions|exploreplasticsurgery", url, re.I):
            r = safe_get(session, url)
            if r:
                soup = BeautifulSoup(r.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if re.search(r"/20\d{2}/\d{2}/", href) or "your-questions" in href:
                        if href not in visited: queue.append(href)
        else:
            row = extract_post_html(session, url, state)
            if row: batch.append(row)

        processed += 1
        state["completed"] = state.get("completed",0) + 1
        if len(batch) >= save_every:
            write_csv(WP_CSV, batch, WP_FIELDS, "a")
            append_jsonl(WP_JSONL, batch)
            batch.clear()
            state["last_save_at"] = utc_now()
            state["visited"] = list(visited); state["queue"] = queue
            save_state(state)
        time.sleep(delay)

    if batch:
        write_csv(WP_CSV, batch, WP_FIELDS, "a")
        append_jsonl(WP_JSONL, batch)
    state["visited"] = list(visited); state["queue"] = queue; state["last_save_at"] = utc_now()
    save_state(state)

# ------------ PubMed ------------
PM_CSV   = OUTPUT / "pubmed_eppley.csv"
PM_JSONL = OUTPUT / "pubmed_eppley.jsonl"

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
    write_csv(PM_CSV, rows, fields, "w"); append_jsonl(PM_JSONL, rows)
    print(f"[pubmed] wrote {len(rows)} rows")

# ------------ YouTube (metadata only) ------------
YT_CSV   = OUTPUT / "youtube_metadata.csv"
YT_JSONL = OUTPUT / "youtube_metadata.jsonl"

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
            print("[yt] yt-dlp not found; skipping")
            break
    write_csv(YT_CSV, rows, ["title","uploader","webpage_url","id","duration","collected_at"], "w")
    append_jsonl(YT_JSONL, rows)
    print(f"[yt] wrote {len(rows)} rows")

# ------------ Orchestration ------------
def run_wp(mode: str, base: str, per_page: int, max_pages: int, save_every: int, delay: float, state: Dict):
    with requests.Session() as session:
        if mode == "auto":
            if wp_rest_available(session, base):
                print("[wp] using REST mode"); state["mode"]="rest"; save_state(state)
                run_wp_rest(session, base, per_page, max_pages, save_every, state); return
            # try sitemap
            print("[wp] REST unavailable, trying sitemap")
            urls = list(iter_sitemap_post_urls(session, base))
            if urls:
                print("[wp] using SITEMAP mode"); state["mode"]="sitemap"; state["queue"]=urls; save_state(state)
                run_wp_sitemap(session, base, delay, max_pages, save_every, state); return
            # fallback
            print("[wp] using HTML mode"); state["mode"]="html"; save_state(state)
            run_wp_html(session, base, delay, max_pages, save_every, state); return
        elif mode == "rest":
            if not wp_rest_available(session, base):
                print("[wp] REST not available; exiting (choose --wp-mode auto to fallback)"); return
            run_wp_rest(session, base, per_page, max_pages, save_every, state); return
        elif mode == "sitemap":
            run_wp_sitemap(session, base, delay, max_pages, save_every, state); return
        else:
            run_wp_html(session, base, delay, max_pages, save_every, state); return

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["wp","pubmed","yt","all"], default="all")
    ap.add_argument("--wp-mode", choices=["auto","rest","sitemap","html"], default="auto")
    ap.add_argument("--per-page", type=int, default=100, help="REST page size (max 100)")
    ap.add_argument("--wp-max", type=int, default=1000, help="max posts/pages to process this run")
    ap.add_argument("--save-every", type=int, default=200, help="flush output every N posts")
    ap.add_argument("--delay", type=float, default=3.0, help="delay between HTML requests (polite)")
    args = ap.parse_args()

    cfg = load_yaml_config(CONFIG)
    base = cfg.get("wordpress_base") or cfg.get("wp_base") or "https://exploreplasticsurgery.com"

    state = load_state()

    if args.only in ("wp","all"):
        run_wp(args.wp_mode, base, max(1, min(args.per_page, 100)), args.wp_max, args.save_every, args.delay, state)

    if args.only in ("pubmed","all"):
        run_pubmed(cfg)

    if args.only in ("yt","all"):
        run_youtube(cfg)

    print(json.dumps({
        "wordpress_csv": str(WP_CSV),
        "pubmed_csv": str(PM_CSV),
        "youtube_csv": str(YT_CSV),
        "wp_state": str(WP_STATE)
    }, indent=2))

if __name__ == "__main__":
    main()