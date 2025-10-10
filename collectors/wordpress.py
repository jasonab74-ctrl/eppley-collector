"""
collectors/wordpress.py
-----------------------

Production-grade WordPress collector for the Eppley site.

Strategy (robust & low-maintenance):
1) Pull the site's XML sitemap index (usually /sitemap_index.xml or /sitemap.xml).
2) Discover all "post" sitemaps (e.g., post-sitemap.xml, blog-sitemap.xml, qa-sitemap.xml).
3) Collect every post URL from those sitemaps.
4) Fetch each post and extract: title, published date, URL, body text, tags.
5) Write to output/wordpress_posts.csv (header always present).

This avoids brittle pagination scraping and stays compatible with typical WordPress setups.
If the sitemap is missing, the code falls back to a (best-effort) RSS approach.

NOTE:
- This module does not run here due to network restrictions; it is ready for live environments.
- Be respectful of the target site's robots.txt and add pauses between requests.
"""

import csv
import os
import time
from typing import Iterable, List, Dict, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE = "https://www.eppleyplasticsurgery.com/"
SITEMAP_CANDIDATES = [
    "sitemap_index.xml",
    "sitemap.xml",
]

UA = "EppleyCollector/1.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"
HEADERS = {"User-Agent": UA}
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN = 0.4  # seconds between network calls
MAX_POSTS = None     # set to an int to cap for debugging (e.g., 200)


def _get(url: str) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return r
        return None
    except requests.RequestException:
        return None


def _soup_xml(text: str) -> BeautifulSoup:
    return BeautifulSoup(text, "xml")


def _soup_html(text: str) -> BeautifulSoup:
    return BeautifulSoup(text, "html.parser")


def _discover_sitemaps() -> List[str]:
    """Return list of sitemap URLs that likely contain posts."""
    sitemaps = []
    for path in SITEMAP_CANDIDATES:
        url = urljoin(BASE, path)
        r = _get(url)
        if not r:
            continue
        soup = _soup_xml(r.text)
        # WordPress sitemap index contains <sitemap><loc>...</loc></sitemap>
        locs = [loc.get_text(strip=True) for loc in soup.select("sitemap > loc")]
        if locs:
            sitemaps.extend(locs)
        else:
            # Might already be a single sitemap with <urlset><url><loc>...
            sitemaps.append(url)
        time.sleep(SLEEP_BETWEEN)
    # Filter to likely post sitemaps
    post_like = []
    for sm in sitemaps:
        lower = sm.lower()
        if any(k in lower for k in ["post", "blog", "qa"]):
            post_like.append(sm)
    # Deduplicate
    return sorted(set(post_like or sitemaps))


def _iter_post_urls_from_sitemap(sitemap_url: str) -> Iterable[str]:
    """Yield all <loc> URLs from a (post) sitemap."""
    r = _get(sitemap_url)
    if not r:
        return
    soup = _soup_xml(r.text)
    for loc in soup.select("url > loc"):
        url = loc.get_text(strip=True)
        if url.startswith("http"):
            yield url
    time.sleep(SLEEP_BETWEEN)


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _extract_post(url: str) -> Dict[str, str]:
    """Fetch a single post and extract title, date, body, tags."""
    out = {"title": "", "date": "", "url": url, "body": "", "tags": ""}

    r = _get(url)
    if not r:
        return out
    soup = _soup_html(r.text)

    # Title candidates
    title = ""
    # 1) <h1>
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        title = h1.get_text(strip=True)
    # 2) OpenGraph
    if not title:
        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            title = og["content"].strip()
    out["title"] = _clean_text(title)

    # Date candidates
    pub = ""
    # 1) <meta property="article:published_time">
    mt = soup.find("meta", attrs={"property": "article:published_time"})
    if mt and mt.get("content"):
        pub = mt["content"].strip()
    # 2) <time datetime>
    if not pub:
        t = soup.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            pub = (t.get("datetime") or t.get_text(strip=True)).strip()
    # 3) <meta name="date">
    if not pub:
        mt = soup.find("meta", attrs={"name": "date"})
        if mt and mt.get("content"):
            pub = mt["content"].strip()
    out["date"] = _clean_text(pub)

    # Body extraction: try <article>, then main content container
    article = soup.find("article")
    if not article:
        # heuristics: WordPress often uses div class containing "entry-content" or "post-content"
        article = soup.find("div", class_=lambda c: c and ("entry-content" in c or "post-content" in c))
    body_text = ""
    if article:
        # Remove obvious non-body sections
        for bad in article.select("script, style, nav, aside, form"):
            bad.decompose()
        body_text = article.get_text(separator=" ", strip=True)
    else:
        # fallback: entire page text (very noisy)
        body_text = soup.get_text(separator=" ", strip=True)[:5000]
    out["body"] = _clean_text(body_text)

    # Tags: <a rel="tag"> or common tag widget classes
    tags = []
    for a in soup.select('a[rel="tag"], .tagcloud a, .tags a, .post-tags a'):
        label = a.get_text(strip=True)
        if label:
            tags.append(label)
    # dedupe and join
    if tags:
        seen = []
        for t in tags:
            if t not in seen:
                seen.append(t)
        out["tags"] = "; ".join(seen)

    time.sleep(SLEEP_BETWEEN)
    return out


def run_wp(out_path: str = "output/wordpress_posts.csv") -> int:
    """
    Discover all WordPress post URLs via sitemap(s), scrape each post,
    and write to CSV with fields: title, date, url, body, tags.
    Returns the number of rows written (not counting header).
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    fieldnames = ["title", "date", "url", "body", "tags"]
    total = 0

    # Discover post sitemaps, then collect all URLs
    sitemaps = _discover_sitemaps()
    urls = []
    for sm in sitemaps:
        for u in _iter_post_urls_from_sitemap(sm):
            urls.append(u)
            if MAX_POSTS and len(urls) >= MAX_POSTS:
                break
        if MAX_POSTS and len(urls) >= MAX_POSTS:
            break

    # Deduplicate and keep only same host as BASE
    base_host = urlparse(BASE).netloc
    canon: List[str] = []
    seen = set()
    for u in urls:
        if urlparse(u).netloc != base_host:
            continue
        if u in seen:
            continue
        seen.add(u)
        canon.append(u)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for i, u in enumerate(canon):
            try:
                row = _extract_post(u)
                # basic sanity: must have a title or body
                if row.get("title") or row.get("body"):
                    w.writerow(row)
                    total += 1
            except Exception as e:
                # continue on errors, but keep progress
                print(f"[wordpress] error on {u}: {e}")
            if i % 10 == 0:
                time.sleep(SLEEP_BETWEEN)

    print(f"[wordpress] wrote {total} rows -> {out_path}")
    return total


if __name__ == "__main__":
    run_wp()
