"""
Crossref collector → output/crossref_works.csv
Docs: https://api.crossref.org/swagger-ui/index.html
"""
import csv, time, requests

def _headers(email: str):
    ua = f"eppley-collector/1.0 (mailto:{email})" if email else "eppley-collector/1.0"
    return {"User-Agent": ua, "Accept": "application/json"}

def run(out_dir, email=""):
    out_path = out_dir / "crossref_works.csv"
    params = {
        "query.author": "Eppley",
        "rows": 1000,
        "select": "DOI,title,issued,author,container-title,type,URL",
        "mailto": email or None,
    }
    r = requests.get("https://api.crossref.org/works", headers=_headers(email), params=params, timeout=30)
    r.raise_for_status()
    items = r.json().get("message", {}).get("items", [])

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["title","abstract","journal","year","authors","doi","url","type"])
        w.writeheader()
        for it in items:
            title_list = it.get("title") or []
            title = " ".join(title_list)[:500]
            date_parts = (it.get("issued", {}) or {}).get("date-parts", [])
            year = str(date_parts[0][0]) if date_parts and date_parts[0] else ""
            authors = "; ".join([", ".join(filter(None, [a.get("family",""), a.get("given","")])).strip(", ")
                                 for a in (it.get("author") or [])])
            w.writerow({
                "title": title,
                "abstract": "",
                "journal": " ".join(it.get("container-title") or []),
                "year": year,
                "authors": authors,
                "doi": it.get("DOI",""),
                "url": it.get("URL",""),
                "type": it.get("type",""),
            })
    time.sleep(0.2)
