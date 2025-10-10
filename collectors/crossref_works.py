import time, csv, requests
from pathlib import Path

OUT = Path("output/crossref_works.csv")
BASE = "https://api.crossref.org/works"
UA = {"User-Agent": "EppleyCollector/1.0 (mailto:pubs@eppleycollector.example)"}

FIELDS = [
    "source","title","year","container_title","type","DOI","URL","author_list"
]

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    cursor = "*"
    per = 200
    params = {
        "query.author": "Barry Eppley",
        "rows": per,
        "cursor": cursor,
        "mailto": "pubs@eppleycollector.example"
    }
    while True:
        r = requests.get(BASE, params=params, headers=UA, timeout=60)
        r.raise_for_status()
        data = r.json()
        items = data.get("message", {}).get("items", [])
        for it in items:
            authors = []
            for a in it.get("author", []) or []:
                nm = " ".join(filter(None, [a.get("given",""), a.get("family","")])).strip()
                if nm:
                    authors.append(nm)
            rows.append({
                "source":"crossref",
                "title": (it.get("title",[ ""])[0] or "").strip(),
                "year": (it.get("issued",{}).get("date-parts",[[None]])[0][0]),
                "container_title": (it.get("container-title",[ ""])[0] or ""),
                "type": it.get("type",""),
                "DOI": it.get("DOI",""),
                "URL": it.get("URL",""),
                "author_list": "; ".join(authors)
            })
        next_cur = data.get("message", {}).get("next-cursor")
        if not next_cur or len(items) < per:
            break
        params["cursor"] = next_cur
        time.sleep(0.25)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"crossref_works: wrote {len(rows)} rows to {OUT}")

if __name__ == "__main__":
    run()
