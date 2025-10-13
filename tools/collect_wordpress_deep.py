#!/usr/bin/env python3
"""
WordPress Deep Scraper for exploreplasticsurgery.com
- Crawls blog + Q&A archives, sitemaps, category/tag pages
- Extracts main content with boilerplate removal
- Dedupes by canonical URL
- Outputs:
  * output/wordpress_fulltext.csv
  * output/corpus/wordpress_fulltext.jsonl
"""

import re, time, json
from typing import List, Set, Tuple
from urllib.parse import urljoin, urlparse
from pathlib import Path
import requests
from bs4 import BeautifulSoup

BASE = "https://exploreplasticsurgery.com/"
UA   = {"User-Agent": "EppleyCollector/2.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"}

ROOT   = Path(".")
OUTDIR = ROOT / "output"
CORPUS = OUTDIR / "corpus"
OUTDIR.mkdir(parents=True, exist_ok=True)
CORPUS.mkdir(parents=True, exist_ok=True)

CSV_PATH = OUTDIR / "wordpress_fulltext.csv"
JSL_PATH = CORPUS / "wordpress_fulltext.jsonl"

SEEDS = [
    BASE,
    urljoin(BASE, "/blog/"),
    urljoin(BASE, "/blogs/"),
    urljoin(BASE, "/blog/page/1/"),
    urljoin(BASE, "/q-and-a/"),
    urljoin(BASE, "/q-and-a/page/1/"),
    urljoin(BASE, "/sitemap.xml"),
    urljoin(BASE, "/post-sitemap.xml"),
    urljoin(BASE, "/page-sitemap.xml"),
    urljoin(BASE, "/category/"),
]

ALLOW_HOST = urlparse(BASE).netloc

def get(url, timeout=25):
    try:
        r = requests.get(url, headers=UA, timeout=timeout)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        print(f"[get] {url} -> {e}")
    return None

def is_same_host(u: str) -> bool:
    try:
        return urlparse(u).netloc == ALLOW_HOST
    except Exception:
        return False

def absolutize(href: str, base: str) -> str:
    try:
        return urljoin(base, href)
    except Exception:
        return href

def discover_links_from_html(html_text: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html_text, "lxml")
    urls = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href: continue
        u = absolutize(href, base_url)
        if is_same_host(u):
            urls.add(u)
    for loc in soup.find_all("loc"):
        u = loc.get_text(strip=True)
        if u and is_same_host(u):
            urls.add(u)
    return list(urls)

def discover_from_xmlsitemap(url: str) -> List[str]:
    text = get(url)
    if not text: return []
    soup = BeautifulSoup(text, "xml")
    urls = []
    for loc in soup.find_all("loc"):
        u = loc.get_text(strip=True)
        if u and is_same_host(u):
            urls.append(u)
    return urls

POST_PAT = re.compile(r"/(blog|blogs|q-and-a)/", re.I)
def looks_like_post(u: str) -> bool:
    if POST_PAT.search(u): return True
    if re.search(r"/\d{4}/\d{2}/\d{2}/", u): return True
    return False

def clean_text(soup: BeautifulSoup) -> str:
    for sel in ["script","style","noscript","header","footer","nav","form","aside"]:
        for n in soup.select(sel): n.decompose()
    for sel in [".sidebar",".widget",".advert",".ads",".breadcrumbs",".comments",".sharing",
                ".related-posts",".site-footer",".site-header",".menu",".pagination",
                ".post-meta",".post-tags",".wp-block-image",".entry-footer",".entry-meta"]:
        for n in soup.select(sel): n.decompose()
    main = soup.select_one("article") or soup.select_one("div.entry-content") or soup.select_one("main") or soup.body
    text = main.get_text("\n", strip=True) if main else soup.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

def extract_post(url: str) -> Tuple[str,str]:
    html_text = get(url)
    if not html_text: return ("","")
    soup = BeautifulSoup(html_text, "lxml")
    title_el = soup.select_one("h1.entry-title") or soup.select_one("h1") or soup.title
    title = title_el.get_text(strip=True) if title_el else ""
    body  = clean_text(soup)
    if len(body) < 200:
        return (title, "")
    return (title, body)

def crawl() -> List[Tuple[str,str,str]]:
    to_visit: List[str] = []
    seen: Set[str] = set()
    posts: List[Tuple[str,str,str]] = []

    # seeds
    for s in dict.fromkeys(SEEDS):
        print(f"[seed] {s}")
        html_text = get(s)
        if not html_text: continue
        to_visit.extend(discover_links_from_html(html_text, s))
        time.sleep(0.2)

    # sitemaps
    for sm in [u for u in set(to_visit) if "sitemap" in u]:
        try:
            to_visit.extend(discover_from_xmlsitemap(sm))
            time.sleep(0.2)
        except Exception as e:
            print(f"[sitemap] {sm} -> {e}")

    MAX_VISITS = 4000
    i = 0
    while to_visit and i < MAX_VISITS:
        u = to_visit.pop()
        i += 1
        if u in seen: continue
        seen.add(u)
        if not is_same_host(u): continue

        if looks_like_post(u):
            title, body = extract_post(u)
            if body:
                posts.append((u, title, body))
                print(f"[post] {len(posts)} {u} ({len(body)} chars)")
            time.sleep(0.25)
            continue

        if re.search(r"/(category|tag|page|blog|blogs|q-and-a)/", u, re.I):
            html_text = get(u)
            if html_text:
                links = discover_links_from_html(html_text, u)
                for L in links:
                    if L not in seen: to_visit.append(L)
                time.sleep(0.15)

    return posts

def write_outputs(posts: List[Tuple[str,str,str]]):
    import csv
    # dedupe by URL
    dedup = {}
    for u,t,b in posts:
        dedup.setdefault(u, (t,b))
    posts = [(u, *dedup[u]) for u in dedup]

    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["url","title","text","source"])
        for u, t, body in posts:
            w.writerow([u, t, body, "wordpress"])

    with JSL_PATH.open("w", encoding="utf-8") as f:
        for i,(u,t,body) in enumerate(posts, 1):
            rec = {"id": f"wp:{i}", "source":"wordpress", "url": u, "title": t, "text": body}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"[done] wordpress -> {CSV_PATH} / {JSL_PATH} (rows={len(posts)})")

def main():
    posts = crawl()
    write_outputs(posts)

if __name__ == "__main__":
    main()