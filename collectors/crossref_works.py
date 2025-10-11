import csv
import time
from pathlib import Path
import requests

OUT = Path("output/crossref_works.csv")
BASE = "https://api.crossref.org/works"
FIELDS = ["source","title","year","journal","type","DOI","URL","authors"]

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    cursor = "*"
    while True:
        params = {
            "query.author": "Barry Eppley",
            "rows": 200,
            "cursor": cursor,
            "mailto": "site@eppley.example"
        }
        try:
            r = requests.get(
                BASE,
                params=params,
                timeout=60,
                headers={"User-Agent":"EppleyCollector/1.0 (mailto:site@eppley.example)"}
            )
            r.raise_for_status()
            data = r.json()
            items = data.get("message", {}).get("items", [])
            for it in items:
                authors = []
                for a in (it.get("author") or []):
                    nm = " ".join(filter(None, [a.get("given",""), a.get("family","")])).strip()
                    if nm:
                        authors.append(nm)
                rows.append({
                    "source": "crossref",
                    "title": (it.get("title", [""])[0] or "").strip(),
                    "year": (it.get("issued", {}).get("date-parts", [[None]])[0][0]),
                    "journal": (it.get("container-title", [""])[0] or ""),
                    "type": it.get("type",""),
                    "DOI": it.get("DOI",""),
                    "URL": it.get("URL",""),
                    "authors": "; ".join(authors),
                })
            next_cur = data.get("message", {}).get("next-cursor")
            if not next_cur or len(items) < 200:
                break
            cursor = next_cur
            time.sleep(0.3)
        except Exception as e:
            print(f"[crossref_works] error: {e}")
            break

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    print(f"[crossref_works] wrote {len(rows)} rows to {OUT}")