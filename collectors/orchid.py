"""
ORCID collectors:
- run_profiles -> output/orcid_profiles.csv
- run_works    -> output/orcid_works.csv  (best-effort; public works summary)
Public API docs: https://info.orcid.org/documentation/
"""
import csv, time, requests

HEADERS = lambda email: {
    "User-Agent": f"eppley-collector/1.0 (mailto:{email})" if email else "eppley-collector/1.0",
    "Accept": "application/vnd.orcid+json",
}

def run_profiles(out_dir, email=""):
    out_path = out_dir / "orcid_profiles.csv"
    # Narrow search to likely matches
    url = "https://pub.orcid.org/v3.0/search/"
    params = {"q": 'family-name:Eppley AND given-names:Barry'}
    r = requests.get(url, headers=HEADERS(email), params=params, timeout=30)
    r.raise_for_status()
    items = r.json().get("result", [])

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["orcid","name","url"])
        w.writeheader()
        for it in items:
            orcid = it.get("orcid-identifier", {}).get("path","")
            name = it.get("orcid-identifier", {}).get("host","")
            url_prof = f"https://orcid.org/{orcid}" if orcid else ""
            w.writerow({"orcid": orcid, "name": "Barry Eppley", "url": url_prof})
    time.sleep(0.3)

def run_works(out_dir, email=""):
    out_path = out_dir / "orcid_works.csv"
    profs = out_dir / "orcid_profiles.csv"
    ids = []
    if profs.exists():
        with profs.open(encoding="utf-8") as f:
            next(f, None)
            for line in f:
                parts = line.strip().split(",")
                if parts and parts[0]:
                    ids.append(parts[0])

    rows = []
    for oid in ids[:3]:  # be gentle
        url = f"https://pub.orcid.org/v3.0/{oid}/works"
        r = requests.get(url, headers=HEADERS(email), timeout=30)
        if r.status_code != 200:
            continue
        data = r.json()
        for g in data.get("group", []):
            title = (g.get("work-summary",[{}])[0].get("title",{}).get("title",{}).get("value",""))[:500]
            year = g.get("work-summary",[{}])[0].get("publication-date",{}).get("year",{}).get("value","")
            putcode = g.get("work-summary",[{}])[0].get("put-code","")
            link = f"https://orcid.org/{oid}/work/{putcode}" if putcode else ""
            rows.append({
                "title": title, "abstract":"", "journal":"", "year": str(year),
                "authors":"Barry Eppley", "doi":"", "url": link, "type":"orcid-work", "keywords":""
            })
        time.sleep(0.3)

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["title","abstract","journal","year","authors","doi","url","type","keywords"])
        w.writeheader(); w.writerows(rows)
