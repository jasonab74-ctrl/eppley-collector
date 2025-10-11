import csv
import time
from pathlib import Path
import requests

OUT = Path("output/openalex_works.csv")
BASE = "https://api.openalex.org"
FIELDS = ["source","openalex_id","title","publication_year","host_venue","type","cited_by_count","doi","url"]

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    try:
        r = requests.get(
            f"{BASE}/authors",
            params={"search":"Barry Eppley","per_page":1},
            timeout=30,
            headers={"User-Agent":"EppleyCollector/1.0 (mailto:site@eppley.example)"}
        )
        r.raise_for_status()
        results = r.json().get("results") or []
        aid = results[0]["id"] if results else None
        if not aid:
            print("[openalex_works] author not found")
        page = 1
        while aid:
            params = {
                "filter": f"authorships.author.id:{aid}",
                "per_page": 200,
                "page": page,
                "sort": "publication_year:desc"
            }
            rr = requests.get(
                f"{BASE}/works",
                params=params,
                timeout=60,
                headers={"User-Agent":"EppleyCollector/1.0 (mailto:site@eppley.example)"}
            )
            rr.raise_for_status()
            data = rr.json()
            items = data.get("results") or []
            for w in items:
                rows.append({
                    "source": "openalex",
                    "openalex_id": w.get("id",""),
                    "title": (w.get("title") or "").strip(),
                    "publication_year": w.get("publication_year",""),
                    "host_venue": (w.get("host_venue",{}) or {}).get("display_name",""),
                    "type": w.get("type",""),
                    "cited_by_count": w.get("cited_by_count",0),
                    "doi": (w.get("doi") or "").replace("https://doi.org/",""),
                    "url": (w.get("primary_location",{}) or {}).get("source",{}).get("url","") or w.get("landing_page_url",""),
                })
            if len(items) < 200:
                break
            page += 1
            time.sleep(0.3)
    except Exception as e:
        print(f"[openalex_works] error: {e}")

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    print(f"[openalex_works] wrote {len(rows)} rows to {OUT}")