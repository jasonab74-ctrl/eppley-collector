# tools/scrape_wordpress_fulltext.py
import csv, time, re
from pathlib import Path
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
import pandas as pd

UA = {"User-Agent": "EppleyCollector/1.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"}
ROOT = Path(".")
OUTDIR = ROOT / "output" / "corpus"
OUTDIR.mkdir(parents=True, exist_ok=True)
OUTFILE = OUTDIR / "wordpress_fulltext.jsonl"

def load_urls():
    urls = set()
    wp_csv = ROOT / "output" / "wordpress_posts.csv"
    if wp_csv.exists():
        df = pd.read_csv(wp_csv)
        for col in ("url","link","href"):
            if col in df.columns:
                urls.update(df[col].dropna().astype(str).tolist())
    # fallback: mine master for wordpress links
    master = ROOT / "output" / "eppley_master.csv"
    if master.exists():
        dfm = pd.read_csv(master, low_memory=False)
        if "url" in dfm.columns:
            urls.update([u for u in dfm["url"].dropna().astype(str) if "exploreplasticsurgery.com" in u or "eppley" in u])
    return list(urls)

def clean_html(html):
    soup = BeautifulSoup(html, "lxml")
    # Remove scripts/styles/nav/footer
    for bad in soup(["script","style","noscript","header","footer","nav","form","aside"]):
        bad.decompose()
    # Remove common sidebars/ad blocks
    for sel in [".sidebar",".widget",".advert",".ads",".breadcrumbs",".comments",".comment",".related-posts",".sharing"]:
        for b in soup.select(sel):
            b.decompose()
    # Prefer article or main content
    main = soup.select_one("article") or soup.select_one("main") or soup.select_one("div.entry-content") or soup.body
    text = main.get_text("\n", strip=True) if main else soup.get_text("\n", strip=True)
    # collapse excessive newlines
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

def fetch(url):
    try:
        r = requests.get(url, timeout=30, headers=UA)
        if r.status_code != 200 or not r.content:
            print(f"[wp] skip {url} status={r.status_code}")
            return None
        return clean_html(r.text)
    except Exception as e:
        print(f"[wp] error {url}: {e}")
        return None

def run():
    urls = load_urls()
    seen = set()
    written = 0
    with OUTFILE.open("w", encoding="utf-8") as out:
        for u in urls:
            if not isinstance(u, str) or not u.startswith("http"):
                continue
            host = urlparse(u).netloc.lower()
            if "exploreplasticsurgery" not in host:
                continue
            if u in seen:
                continue
            seen.add(u)
            text = fetch(u)
            time.sleep(0.3)  # polite
            if not text or len(text) < 200:
                continue
            rec = {
                "id": f"wp:{len(seen)}",
                "source": "wordpress",
                "title": "",    # could be extracted from <title>
                "url": u,
                "text": text
            }
            out.write((str(rec).replace("'", '"')) + "\n")
            written += 1
    print(f"[wp] wrote {written} records -> {OUTFILE}")

if __name__ == "__main__":
    run()