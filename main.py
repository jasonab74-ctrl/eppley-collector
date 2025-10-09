#!/usr/bin/env python3
"""
Eppley Collector — resilient, resumable scraper
- WordPress Q&A/blog: incremental crawl with checkpointing + partial saves
- PubMed: E-utilities fetch by author query
- YouTube: metadata only via yt-dlp (no downloads)

Outputs (always written to ./output/):
  - wordpress_posts.csv / .jsonl
  - pubmed_eppley.csv / .jsonl
  - youtube_metadata.csv / .jsonl
State:
  - wp_state.json (resume checkpoint for WordPress crawl)

Usage examples (used by GitHub Actions):
  python main.py --only wp --wp-max 1200 --save-every 200
  python main.py --only pubmed
  python main.py --only yt
  python main.py --only all
"""

import argparse, csv, json, os, re, sys, time, math
from datetime import datetime, timezone
from typing import List, Dict, Iterable, Tuple
import pathlib

import requests
from bs4 import BeautifulSoup

# PubMed utils
from urllib.parse import urlencode
# YouTube metadata
import subprocess

OUTPUT_DIR = pathlib.Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_PATH = pathlib.Path("config.yaml")  # optional; we fall back to defaults if missing

# ---------------------------
# Helpers
# ---------------------------

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def load_yaml_config(path: pathlib.Path) -> Dict:
    try:
        import yaml
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"[config] warning: {e}")
        return {}

def write_csv(path: pathlib.Path, rows: List[Dict], fieldnames: List[str], mode="w"):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, mode, encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def append_jsonl(path: pathlib.Path, rows: List[Dict]):
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def safe_get(url: str, session: requests.Session, max_retries=3, backoff=2.0, timeout=20):
    for attempt in range(1, max_retries+1):
        try:
            r = session.get(url, timeout=timeout, headers={"User-Agent": "eppley-collector/1.0"})
            if r.status_code == 200:
                return r
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff * attempt)
                continue
            print(f"[http] {url} -> {r.status_code}")
            return None
        except requests.RequestException as e:
            print(f"[http] error {e} on {url}")
            time.sleep(backoff * attempt)
    return None

# ---------------------------
# WordPress crawl (resumable)
# ---------------------------

WP_STATE = OUTPUT_DIR / "wp_state.json"
WP_CSV = OUTPUT_DIR / "wordpress_posts.csv"
WP_JSONL = OUTPUT_DIR / "wordpress_posts.jsonl"

def load_wp_state() -> Dict:
    if WP_STATE.exists():
        try:
            return json.loads(WP_STATE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"visited": [], "queue": [], "completed": 0, "last_save_at": 0}

def save_wp_state(state: Dict):
    WP_STATE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def discover_wp_urls(seeds: List[str], session: requests.Session, limit=None) -> Iterable[str]:
    """
    Discover post URLs from listing/archive pages.
    We look for <a> links with /20xx/ or /your-questions/ patterns typical of Eppley blog.
    """
    seen = set()
    for seed in seeds:
        r = safe_get(seed, session)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href in seen: continue
            # Heavily-yield pages: /your-questions/ and /exploreplasticsurgery
            if re.search(r"exploreplasticsurgery|your-questions", href, flags=re.I) or re.search(r"/20\d{2}/\d{2}/", href):
                seen.add(href)
                yield href
                if limit and len(seen) >= limit:
                    return

def extract_post(url: str, session: requests.Session) -> Dict:
    r = safe_get(url, session)
    if not r:
        return {}
    soup = BeautifulSoup(r.text, "html.parser")
    title_el = soup.find(["h1","h2"])
    title = (title_el.get_text(strip=True) if title_el else "").strip()
    date = ""
    time_el = soup.find("time")
    if time_el and time_el.get("datetime"):
        date = time_el["datetime"]
    elif time_el:
        date = time_el.get_text(strip=True)
    # Fallback body text
    # Many posts use article/wp-content selectors; we grab paragraphs
    body_parts = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    body = "\n".join(body_parts).strip()
    tags = ", ".join(sorted({a.get_text(strip=True) for a in soup.select("a[rel=tag]")}))
    return {
        "url": url,
        "title": title,
        "date": date,
        "tags": tags,
        "body": body,
        "collected_at": utc_now(),
    }

def run_wp(max_pages: int, save_every: int, delay: float, session: requests.Session, config: Dict):
    seeds = []
    # seeds from config if present
    for key in ("wordpress_seeds", "wordpress", "wp_seeds"):
        if key in config and isinstance(config[key], list):
            seeds = config[key]
            break
    if not seeds:
        # sane defaults that yield a lot on the Eppley site
        seeds = [
            "https://exploreplasticsurgery.com",
            "https://exploreplasticsurgery.com/your-questions"
        ]

    state = load_wp_state()
    visited = set(state.get("visited", []))
    queue = list(state.get("queue") or [])

    if not queue:
        print(f"[wp] discovering from {len(seeds)} seeds…")
        for u in discover_wp_urls(seeds, session):
            if u not in visited:
                queue.append(u)
        print(f"[wp] discovered {len(queue)} listing/post URLs")

    # CSV header
    fieldnames = ["url", "title", "date", "tags", "body", "collected_at"]
    if not WP_CSV.exists():
        write_csv(WP_CSV, [], fieldnames, mode="w")  # create file with header

    batch_rows = []
    processed_this_run = 0
    started = time.time()

    while queue and processed_this_run < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        # If this is a listing page, collect post links
        if re.search(r"/page/\d+|category|tag|your-questions|exploreplasticsurgery", url, flags=re.I):
            r = safe_get(url, session)
            if r:
                soup = BeautifulSoup(r.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if re.search(r"/20\d{2}/\d{2}/|/your-questions/", href, flags=re.I):
                        if href not in visited:
                            queue.append(href)
        else:
            # Treat as post
            item = extract_post(url, session)
            if item.get("url"):
                batch_rows.append(item)

        processed_this_run += 1
        state["completed"] = state.get("completed", 0) + 1

        # partial save
        if len(batch_rows) >= save_every:
            print(f"[wp] saving {len(batch_rows)} rows (partial)…")
            write_csv(WP_CSV, batch_rows, fieldnames, mode="a")
            append_jsonl(WP_JSONL, batch_rows)
            batch_rows = []
            state["last_save_at"] = utc_now()
            state["visited"] = list(visited)
            state["queue"] = queue
            save_wp_state(state)

        # polite delay
        time.sleep(delay)

        # progress heartbeat
        if processed_this_run % 50 == 0:
            elapsed = time.time() - started
            print(f"[wp] progress {processed_this_run}/{max_pages} (elapsed {int(elapsed)}s) queue={len(queue)}")

    # final flush
    if batch_rows:
        print(f"[wp] final save of {len(batch_rows)} rows")
        write_csv(WP_CSV, batch_rows, fieldnames, mode="a")
        append_jsonl(WP_JSONL, batch_rows)

    # persist state
    state["visited"] = list(visited)
    state["queue"] = queue
    state["last_save_at"] = utc_now()
    save_wp_state(state)

    print(f"[wp] done this run. processed={processed_this_run}, remaining in queue={len(queue)}")

# ---------------------------
# PubMed
# ---------------------------

PM_CSV = OUTPUT_DIR / "pubmed_eppley.csv"
PM_JSONL = OUTPUT_DIR / "pubmed_eppley.jsonl"

def fetch_pubmed(author_query="Eppley BL[Author]", retmax=5000) -> List[Dict]:
    """
    Basic E-utilities: esearch -> efetch (summary via esummary)
    """
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    # search
    q = {
        "db": "pubmed",
        "term": author_query,
        "retmode": "json",
        "retmax": retmax
    }
    search = requests.get(base + "esearch.fcgi", params=q, timeout=30).json()
    ids = search.get("esearchresult", {}).get("idlist", [])
    if not ids:
        return []
    # summary
    params = {"db": "pubmed", "id": ",".join(ids), "retmode": "json"}
    summ = requests.get(base + "esummary.fcgi", params=params, timeout=30).json()
    out = []
    for pmid, rec in summ.get("result", {}).items():
        if pmid == "uids": continue
        art = {
            "pmid": pmid,
            "title": rec.get("title", ""),
            "journal": rec.get("fulljournalname", ""),
            "year": rec.get("pubdate", ""),
            "authors": ", ".join([a.get("name", "") for a in rec.get("authors", [])]),
            "doi": rec.get("elocationid", ""),
            "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            "collected_at": utc_now()
        }
        out.append(art)
    return out

def run_pubmed(config: Dict):
    author_q = config.get("pubmed_author_query") or "Eppley BL[Author]"
    rows = fetch_pubmed(author_q)
    fields = ["pmid","title","journal","year","authors","doi","url","collected_at"]
    write_csv(PM_CSV, rows, fields, mode="w")
    append_jsonl(PM_JSONL, rows)
    print(f"[pubmed] wrote {len(rows)} records")

# ---------------------------
# YouTube metadata (yt-dlp)
# ---------------------------

YT_CSV = OUTPUT_DIR / "youtube_metadata.csv"
YT_JSONL = OUTPUT_DIR / "youtube_metadata.jsonl"

def run_youtube(config: Dict):
    """
    Try channel URLs first (with yt-dlp --dump-json --flat-playlist).
    If channel_ids provided and you have YT API, you could swap to that later.
    """
    ch_urls = config.get("channel_urls") or config.get("youtube_channel_urls") or []
    rows = []
    for url in ch_urls:
        try:
            # flat playlist is fast + metadata only
            cmd = ["yt-dlp", "--dump-json", "--flat-playlist", url]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
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
            print("[yt] yt-dlp not found; skip")
            break

    fields = ["title","uploader","webpage_url","id","duration","collected_at"]
    write_csv(YT_CSV, rows, fields, mode="w")
    append_jsonl(YT_JSONL, rows)
    print(f"[yt] wrote {len(rows)} rows")

# ---------------------------
# CLI
# ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["wp","pubmed","yt","all"], default="all")
    ap.add_argument("--wp-max", type=int, default=1000, help="max pages to process this run (listings+posts)")
    ap.add_argument("--save-every", type=int, default=200, help="flush output every N posts")
    ap.add_argument("--delay", type=float, default=3.0, help="seconds between requests")
    args = ap.parse_args()

    config = load_yaml_config(CONFIG_PATH)

    with requests.Session() as session:
        if args.only in ("wp","all"):
            run_wp(args.wp_max, args.save_every, args.delay, session, config)
        if args.only in ("pubmed","all"):
            run_pubmed(config)
        if args.only in ("yt","all"):
            run_youtube(config)

    # summary
    out = {"wordpress_csv": str(WP_CSV), "pubmed_csv": str(PM_CSV), "youtube_csv": str(YT_CSV)}
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()