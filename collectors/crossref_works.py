import time, csv, requests
from pathlib import Path

OUT = Path("output/crossref_works.csv")
BASE = "https://api.crossref.org/works"
UA = {"User-Agent": "EppleyCollector/1.0 (mailto:pubs@eppley.example)"}
FIELDS = ["source","title","year","journal","type","DOI","URL","authors"]

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows, cursor = [], "*"
    params = {"query.author":"Barry Eppley","rows":200,"cursor":cursor,"mailto":"pubs@eppley.example"}
    while True:
        r = requests.get(BASE, params=params, headers=UA, timeout=60)
        r.raise_for_status()
        data = r.json()
        items = data.get("message", {}).get("items", [])
        for it in items:
            authors = []
            for a in (it.get("author") or []):
                nm = " ".join([a.get("given",""), a.get("family","")]).strip()
                if nm: authors.append(nm)
            rows.append({
                "source":"crossref",
                "title": (it.get("title",[ ""])[0] or "").strip(),
                "year": (it.get("issued",{}).get("date-parts",[[None]])[0][0]),
                "journal": (it.get("container-title",[ ""])[0] or ""),
                "type": it.get("type",""),
                "DOI": it.get("DOI",""),
                "URL": it.get("URL",""),
                "authors": "; ".join(authors),
            })
        nxt = data.get("message", {}).get("next-cursor")
        if not nxt or len(items) < 200: break
        params["cursor"] = nxt
        time.sleep(0.25)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS); w.writeheader()
        for r in rows: w.writerow(r)
    print(f"[crossref] wrote {len(rows)} rows -> {OUT}")
