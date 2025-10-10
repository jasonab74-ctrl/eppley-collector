import csv, requests
from pathlib import Path
from xml.etree import ElementTree as ET

OUT = Path("output/wordpress_posts.csv")
FEED_URL = "https://www.exploreplasticsurgery.com/feed/"  # Dr. Eppley blog RSS
FIELDS = ["source","title","link","pub_date","author","categories","summary"]

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    try:
        r = requests.get(FEED_URL, timeout=30, headers={"User-Agent":"EppleyCollector/1.0"})
        r.raise_for_status()
        root = ET.fromstring(r.content)
        channel = root.find("channel")
        for it in (channel.findall("item") if channel is not None else []):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            pub_date = (it.findtext("pubDate") or "").strip()
            author = (it.findtext("{http://purl.org/dc/elements/1.1/}creator") or "").strip()
            desc = (it.findtext("description") or "").strip()
            cats = [c.text.strip() for c in it.findall("category") if c.text]
            rows.append({
                "source":"wordpress",
                "title": title, "link": link, "pub_date": pub_date,
                "author": author, "categories": "; ".join(cats), "summary": desc
            })
    except Exception as e:
        print(f"[wordpress] {e}")
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[wordpress] wrote {len(rows)} rows -> {OUT}")
