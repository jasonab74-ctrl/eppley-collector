"""
ORCID collectors:
- run_profiles → output/orcid_profiles.csv
- run_works    → output/orcid_works.csv  (best-effort public works)
Docs: https://info.orcid.org/documentation/
"""
import csv, time, requests

def _headers(email: str):
    ua = f"eppley-collector/1.0 (mailto:{email})" if email else "eppley-collector/1.0"
    return {"User-Agent": ua, "Accept": "application/vnd.orcid+json"}

def run_profiles(out_dir, email=""):
    out_path = out_dir / "orcid_profiles.csv"
    url = "https://pub.orcid.org/v3.0/search/"
    params = {"q": 'family-name:Eppley AND given-names:Barry'}
    r = requests.get(url, headers=_headers(email), params=params, timeout=30)
    r.raise_for_status()
    items = r.json().get("result", [])

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["orcid","name","url"])
        w.writeheader()
        for it in items:
            oid = (it.get("orcid-identifier") or {}).get("path","")
            w.writerow({"orcid": oid, "name": "Barry Eppley", "url": f"https://orcid.org/{oid}" if oid else ""})
    time.sleep(0.25)

def run_works(out_dir, email=""):
    out_path = out_dir / "orcid_works.csv"
    ids = []
    profs = out_dir / "orcid_profiles.csv"
    if profs.exists():
        with profs.open(encoding="utf-8") as f:
            next(f, None)
            for line in f:
                oid = (line.strip().split(",") or [""])[0]
                if oid:
                    ids.append(oid)

    rows = []
    for oid in ids[:3]:
        url = f"https://pub.orcid.org/v3.0/{oid}/works"
        r = requests.get(url, headers=_headers(email), timeout=30)
        if r.status_code != 200:
            continue
        data = r.json()
        for g in data.get("group", []):
            ws = (g.get("work-summary") or [{}])[0]
            title = ((ws.get("title") or {}).get("title") or {}).get("value","")
            year = ((ws.get("publication-date") or {}).get("year") or {}).get("value","")
            putcode = ws.get("put-code","")
            link = f"https://orcid.org/{oid}/work/{putcode}" if putcode else ""
            rows.append({
                "title": title[:500], "abstract": "", "journal": "",
                "year": str(year or ""), "authors": "Barry Eppley",
                "doi": "", "url": link, "type": "orcid-work", "keywords": ""
            })
        time.sleep(0.25)

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["title","abstract","journal","year","authors","doi","url","type","keywords"])
        w.writeheader(); w.writerows(rows)
