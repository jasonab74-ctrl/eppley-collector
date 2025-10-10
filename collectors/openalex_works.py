import time, csv, requests
from pathlib import Path

OUT = Path("output/openalex_works.csv")
BASE = "https://api.openalex.org"
UA = {"User-Agent": "EppleyCollector/1.0 (mailto:pubs@eppley.example)"}
FIELDS = ["source","openalex_id","title","publication_year","host_venue","type","cited_by_count","doi","url"]

def find_author_id(name="Barry Eppley"):
    r = requests.get(f"{BASE}/authors", params={"search":name,"per_page":1}, headers=UA, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j.get("results",[{}])[0].get("id")

def fetch_works(aid):
    page, rows = 1, []
    while True:
        params = {"filter":f"authorships.author.id:{aid}","per_page":200,"page":page,"sort":"publication_year:desc"}
        r = requests.get(f"{BASE}/works", params=params, headers=UA, timeout=60)
        r.raise_for_status()
        data = r.json()
        items = data.get("results", []) or []
        for w in items:
            rows.append({
                "source":"openalex",
                "openalex_id": w.get("id",""),
                "title": (w.get("title") or "").strip(),
                "publication_year": w.get("publication_year",""),
                "host_venue": (w.get("host_venue",{}) or {}).get("display_name",""),
                "type": w.get("type",""),
                "cited_by_count": w.get("cited_by_count",0),
                "doi": (w.get("doi") or "").replace("https://doi.org/",""),
                "url": (w.get("primary_location",{}) or {}).get("source",{}).get("url","") or w.get("landing_page_url",""),
            })
        if len(items) < 200: break
        page += 1; time.sleep(0.2)
    return rows

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    aid = find_author_id()
    rows = fetch_works(aid) if aid else []
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS); w.writeheader()
        for r in rows: w.writerow(r)
    print(f"[openalex] wrote {len(rows)} rows -> {OUT}")
