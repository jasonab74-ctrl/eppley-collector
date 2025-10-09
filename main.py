#!/usr/bin/env python3
"""
Eppley Collector — FAST + Resumable

Speed-first strategy:
  1) WordPress REST API (100 posts/page)  -> fastest
  2) Sitemaps (post-sitemap*.xml)         -> very fast discovery
  3) HTML crawl (polite)                   -> fallback
All modes checkpoint & save partial results frequently.

Outputs (in ./output/):
  - wordpress_posts.csv / .jsonl
  - pubmed_eppley.csv  / .jsonl
  - youtube_metadata.csv / .jsonl
State:
  - wp_state.json (resume/cursor + last-modified map for HTML)

CLI examples (used by GitHub Actions):
  python main.py --only wp --wp-max 600 --save-every 150 --delay 3.5
  python main.py --only pubmed
  python main.py --only yt
  python main.py --only all
"""

from __future__ import annotations
import argparse, csv, json, os, re, time, pathlib, subprocess, requests, xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Iterable, Optional
from urllib.parse import urljoin, urlencode
from bs4 import BeautifulSoup

# ---------------- Config & paths ----------------

OUTPUT_DIR = pathlib.Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_PATH = pathlib.Path("config.yaml")  # optional
WP_STATE = OUTPUT_DIR / "wp_state.json"

WP_CSV   = OUTPUT_DIR / "wordpress_posts.csv"
WP_JSONL = OUTPUT_DIR / "wordpress_posts.jsonl"

PM_CSV   = OUTPUT_DIR / "pubmed_eppley.csv"
PM_JSONL = OUTPUT_DIR / "pubmed_eppley.jsonl"

YT_CSV   = OUTPUT_DIR / "youtube_metadata.csv"
YT_JSONL = OUTPUT_DIR / "youtube_metadata.jsonl"


# ---------------- Utilities ----------------

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

def safe_get(url: str, session: requests.Session, headers: Optional[Dict]=None, max_retries=3, backoff=2.0, timeout=25):
    headers = headers or {}
    for attempt in range(1, max_retries+1):
        try:
            r = session.get(url, timeout=timeout, headers={"User-Agent":"eppley-collector/2.0", **headers})
            # REST endpoints sometimes error with 400 when page too high — treat non-200 as final except 429/5xx
            if r.status_code == 200 or r.status_code == 304:
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

def ensure_wp_headers():
    return ["url", "title", "date", "tags", "body", "collected_at", "source"]

def flush_wp(rows: List[Dict], mode="a"):
    flds = ensure_wp_headers()
    if not WP_CSV.exists() or mode == "w":
        write_csv(WP_CSV, [], flds, mode="w")
        mode = "a"
    write_csv(WP_CSV, rows, flds, mode=mode)
    append_jsonl(WP_JSONL, rows)

# ---------------- State ----------------

def load_state() -> Dict:
    if WP_STATE.exists():
        try:
            return json.loads(WP_STATE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "mode": "auto",
        "rest": {"next_page": 1, "base": None, "per_page": 100, "done": False},
        "sitemap": {"queue": [], "seen": []},
       