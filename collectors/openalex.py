"""
OpenAlex collector: writes output/openalex_works.csv
Docs: https://docs.openalex.org/
"""
import csv, time, requests

def run(out_dir, email=""):
    out_path = out_dir / "openalex_works.csv"
    base = "https://api.openalex.org/works"
    params = {
        "search": "Barry Eppley",   # broad text search
        "per_page": 200,
        "mailto": email or None,
    }
    headers = {
        "User-Agent": f"eppley-collector/1.0 (mailto:{email})" if email else "eppley-collector/1.0",
        "Accept": "application/json",
    }

    rows = []
    url = base
    while True:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for w in data.get("results", []):
            title = (w.get("title") or "")[:500]
            year = str(w.get("publication_year") or "")
            hv = w.get("host_venue") or {}
            journal = hv.get("display_name") or ""
            authors = "; ".join([a.get("author", {}).get("display_name","") for a in (w.get("authorships") or [])])
            doi = (w.get("doi") or "").replace("https://doi.org/","")
            url_out = w.get("id") or w.get("primary_location",{}).get("landing_page_url") or ""
            rows.append({
                "title": title,
                "abstract": "",          # you can enrich later if needed
                "journal": journal,
                "year": year,
                "authors": authors,
                "doi": doi,
                "url": url_out,
                "type": w.get("type",""),
            })

        next_url = data.get("meta", {}).get("next_cursor")
        if not next_url:
            break
        # cursor paging
        params["cursor"] = next_url
        time.sleep(0.2)

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["title","abstract","journal","year","authors","doi","url","type"])
        w.writeheader(); w.writerows(rows)
