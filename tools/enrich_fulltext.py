"""
Fetches readable page text for URLs we have (WordPress, OA articles) and writes:
  output/expanded/pages.jsonl  (cache of url -> text)
Safe: timeouts, gentle UA, skips binary/PDF.
"""
import os, json, time, hashlib
from pathlib import Path
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

BASE_OUT = Path("output")
EXPANDED = BASE_OUT / "expanded"
EXPANDED.mkdir(parents=True, exist_ok=True)
CACHE = EXPANDED / "pages.jsonl"

UA = {"User-Agent": f"EppleyCollector/1.0 (mailto:{os.getenv('NCBI_EMAIL','unknown@example.com')})"}

def _iter_existing():
    if CACHE.exists():
        with CACHE.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    yield json.loads(line)
                except Exception:
                    continue

def _cache_index():
    idx = {}
    for rec in _iter_existing():
        idx[rec.get("url")] = rec
    return idx

def fetch_readable(url, timeout=20):
    try:
        r = requests.get(url, headers=UA, timeout=timeout)
        ctype = r.headers.get("Content-Type","")
        if "text/html" not in ctype:
            return ""
        soup = BeautifulSoup(r.text, "lxml")

        # Heuristic extraction: look for common article containers then fallback
        candidates = []
        for sel in ["article", "main", ".entry-content", ".post-content", ".post", "#content"]:
            for n in soup.select(sel):
                text = n.get_text(separator=" ", strip=True)
                if len(text) > 400:
                    candidates.append(text)
        if not candidates:
            body = soup.find("body")
            text = body.get_text(separator=" ", strip=True) if body else soup.get_text(" ", strip=True)
            return text[:100000]
        best = max(candidates, key=len)
        return best[:100000]
    except Exception:
        return ""

def gather_urls():
    urls = set()
    for name in ["wordpress_posts.csv","crossref_works.csv","openalex_works.csv"]:
        p = BASE_OUT / name
        if not p.exists(): 
            continue
        import pandas as pd
        df = pd.read_csv(p)
        col_candidates = [c for c in ["link","URL","url"] if c in df.columns]
        if not col_candidates: 
            continue
        ucol = col_candidates[0]
        for u in df[ucol].dropna().astype(str).tolist():
            if u.startswith("http"):
                urls.add(u)
    return sorted(urls)

def run():
    idx = _cache_index()
    urls = gather_urls()
    new = 0
    with CACHE.open("a", encoding="utf-8") as out:
        for u in urls:
            if u in idx:
                continue
            text = fetch_readable(u)
            out.write(json.dumps({"url": u, "text": text}) + "\n")
            new += 1
            time.sleep(0.2)
    print(f"enrich_fulltext: cached {new} new pages; total now ~{len(idx)+new}")

if __name__ == "__main__":
    run()
