
import re, sys, os
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
from .utils import polite_get, text_normalize, guess_date_from_html, write_jsonl, write_csv, hash_id

def find_pagination_links(seed, soup):
    # Collect pagination and article links
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue
        abs_url = urljoin(seed, href)
        # Only follow same-domain links for pagination and posts
        if urlparse(abs_url).netloc != urlparse(seed).netloc:
            continue
        links.add(abs_url)
    return links

def extract_posts_from_listing(seed, soup):
    # WP index pages often have articles with <article> or h2.entry-title
    posts = []
    for sel in ["article", "h2.entry-title", "h1.entry-title"]:
        for block in soup.select(sel):
            a = block.find("a", href=True)
            if a and a["href"]:
                posts.append(a["href"])
    # Fallback: any same-domain link with /20XX/ in it (typical WP permalinks)
    if not posts:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"/20\d{2}/\d{2}/", href):
                posts.append(href)
    # De-dup and normalize
    out = []
    seen = set()
    for p in posts:
        abs_url = urljoin(seed, p)
        if abs_url not in seen:
            seen.add(abs_url)
            out.append(abs_url)
    return out

def extract_article(url, session, headers, delay):
    resp = polite_get(url, session, delay_seconds=delay, headers=headers)
    soup = BeautifulSoup(resp.text, "lxml")
    # Try common WP selectors
    title = soup.select_one("h1.entry-title") or soup.select_one("h1.post-title") or soup.find("h1")
    title = text_normalize(title.get_text(" ", strip=True)) if title else ""
    date_iso = guess_date_from_html(soup)
    # Content extraction: typical WP has div.entry-content
    content = soup.select_one("div.entry-content") or soup.select_one("div.post-content") or soup.select_one("article")
    body = ""
    if content:
        # Remove scripts/styles/nav
        for bad in content.select("script, style, nav, header, footer, form"):
            bad.extract()
        body = text_normalize(content.get_text(" ", strip=True))
    # Tags
    tags = [t.get_text(strip=True) for t in soup.select(".tagcloud a, a[rel='tag'], span.tags-links a")]
    return {
        "id": hash_id(url),
        "url": url,
        "title": title,
        "date": date_iso or "",
        "body": body,
        "tags": tags
    }

def crawl_wordpress(seeds, user_agent, delay, max_pages):
    session = requests.Session()
    headers = {"User-Agent": user_agent}
    collected = []
    visited = set()
    to_visit = []

    # Initialize queue with seeds
    for s in seeds:
        to_visit.append(s)

    pbar = tqdm(total=max_pages * max(1, len(seeds)), desc="Discover/listings", unit="page")
    pages_seen_per_seed = {}

    while to_visit and pbar.n < pbar.total:
        url = to_visit.pop(0)
        # limit per seed
        root = None
        for s in seeds:
            if url.startswith(s.rstrip("/") + "/") or url == s.rstrip("/"):
                root = s
                break
        pages_seen_per_seed[root] = pages_seen_per_seed.get(root, 0) + 1
        if pages_seen_per_seed[root] > max_pages:
            continue

        if url in visited:
            continue
        visited.add(url)
        try:
            resp = polite_get(url, session, delay_seconds=delay, headers=headers)
        except Exception as e:
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        # Extract posts
        posts = extract_posts_from_listing(url, soup)
        # Queue likely pagination links (next/older posts)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            txt = (a.get_text(" ", strip=True) or "").lower()
            if any(k in txt for k in ["next", "older", "previous", "older posts"]) or re.search(r"/page/\d+/?$", href):
                abs_url = urljoin(url, href)
                # Only same domain
                if urlparse(abs_url).netloc == urlparse(url).netloc and abs_url not in visited:
                    to_visit.append(abs_url)

        pbar.update(1)

        # Fetch articles
        for post_url in posts:
            abs_post = urljoin(url, post_url)
            try:
                art = extract_article(abs_post, session, headers, delay)
                collected.append(art)
            except Exception:
                continue
    pbar.close()
    return collected

def filter_posts(rows, include_keywords=None, exclude_keywords=None):
    if not rows:
        return rows
    inc = [k.lower() for k in (include_keywords or []) if k]
    exc = [k.lower() for k in (exclude_keywords or []) if k]
    out = []
    for r in rows:
        full = f"{r.get('title','')} {r.get('body','')}"
        full_low = full.lower()
        if inc and not any(k in full_low for k in inc):
            continue
        if exc and any(k in full_low for k in exc):
            continue
        out.append(r)
    return out

def run_from_config(cfg):
    seeds = cfg["wordpress"]["seeds"]
    delay = cfg["general"]["delay_seconds"]
    max_pages = int(cfg["general"]["max_pages_per_seed"])
    ua = cfg["general"]["user_agent"]
    include_k = cfg["wordpress"].get("include_keywords") or []
    exclude_k = cfg["wordpress"].get("exclude_keywords") or []

    rows = crawl_wordpress(seeds, ua, delay, max_pages)
    rows = filter_posts(rows, include_k, exclude_k)

    outdir = cfg["general"]["output_dir"]
    jsonl_path = os.path.join(outdir, "wordpress_posts.jsonl")
    csv_path = os.path.join(outdir, "wordpress_posts.csv")
    write_jsonl(rows, jsonl_path)
    write_csv(rows, csv_path)
    return {"count": len(rows), "jsonl": jsonl_path, "csv": csv_path}
