"""
WordPress Deep Collector
------------------------
Crawls exploreplasticsurgery.com for posts, Q&As, and paginated archives.
Outputs to output/wordpress_posts.csv
"""

import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path
import time

BASE = "https://exploreplasticsurgery.com/"
OUT = Path("output/wordpress_posts.csv")
FIELDS = ["source", "title", "url", "year", "text"]

def extract_post(url):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        title = (soup.find("h1") or {}).get_text(strip=True)
        paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        text = " ".join(paras)
        return {"source": "wordpress", "title": title, "url": url, "year": None, "text": text}
    except Exception as e:
        print(f"[wp] failed {url}: {e}")
        return None

def crawl_section(start_url, max_pages=30):
    urls, posts = set(), []
    next_url = start_url
    seen = set()
    page = 1
    while next_url and page <= max_pages:
        print(f"[wp] crawling page {page}: {next_url}")
        try:
            r = requests.get(next_url, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select("h2.entry-title a"):
                u = urljoin(BASE, a.get("href"))
                if u not in seen:
                    seen.add(u)
                    urls.add(u)
            nxt = soup.select_one("a.next.page-numbers")
            next_url = urljoin(BASE, nxt["href"]) if nxt else None
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"[wp] pagination error: {e}")
            break
    for u in sorted(urls):
        post = extract_post(u)
        if post:
            posts.append(post)
    return posts

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    all_posts = []
    # base blog pages + Q&A categories
    sections = [
        BASE + "blog/",
        BASE + "questions/",
        BASE + "category/facial/",
        BASE + "category/body/",
        BASE + "category/breast/"
    ]
    for sec in sections:
        all_posts.extend(crawl_section(sec))
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in all_posts:
            w.writerow(row)
    print(f"[wp] wrote {len(all_posts)} posts to {OUT}")

if __name__ == "__main__":
    run()