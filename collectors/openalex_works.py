import os, time, csv, requests
from pathlib import Path

OUT = Path("output/openalex_works.csv")
BASE = "https://api.openalex.org"
EMAIL = os.getenv("NCBI_EMAIL") or "unknown@example.com"  # OK to reuse for contact
UA = {"User-Agent": f"EppleyCollector/1.0 (mailto:{EMAIL})"}

FIELDS = [
    "source","openalex_id","title","publication_year","host_venue",
    "type","cited_by_count","is_oa","doi","url"
]

def find_author_id(name="Barry Eppley"):
    r = requests.get(f"{BASE}/authors", params={"search": name, "per_page": 1}, headers=UA, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("results"):
        return data["results"][0]["id"]
    return None

def fetch_works(author_id, max_pages=20):
    page = 1
    rows = []
    while page <= max_pages:
        params = {
            "filter": f"authorships.author.id:{author_id}",
            "per_page": 200,
            "page": page,
            "sort": "publication_year:desc"
        }
        r = requests.get(f"{BASE}/works", params=params, headers=UA, timeout=60)
        r.raise_for_status()
        data = r.json()
        for w in data.get("results", []):
            rows.append({
                "source": "openalex",
                "openalex_id": w.get("id",""),
                "title": (w.get("title") or "").strip(),
                "publication_year": w.get("publication_year",""),
                "host_venue": (w.get("host_venue",{}) or {}).get("display_name",""),
                "type": w.get("type",""),
                "cited_by_count": w.get("cited_by_count",0),
                "is_oa": (w.get("open_access",{}) or {}).get("is_oa",""),
                "doi": (w.get("doi") or "").replace("https://doi.org/",""),
                "url": (w.get("primary_location",{}) or {}).get("source",{}).get("url","") or w.get("landing_page_url",""),
            })
        if not data.get("results") or len(data["results"]) < 200:
            break
        page += 1
        time.sleep(0.2)
    return rows

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    aid = find_author_id("Barry Eppley")
    if not aid:
        with OUT.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writeheader()
        print("openalex_works: author not found; wrote headers only.")
        return
    rows = fetch_works(aid)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"openalex_works: wrote {len(rows)} rows to {OUT}")

if __name__ == "__main__":
    run()
