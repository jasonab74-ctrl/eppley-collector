"""
ClinicalTrials.gov collector -> output/clinical_trials.csv
Uses v1 Study Fields API for simplicity.
Docs: https://clinicaltrials.gov/api/gui/ref/api_urls
"""
import csv, requests

FIELDS = "NCTId,BriefTitle,Condition,StudyType,StartDate,OverallStatus,LocationCountry,LeadSponsorName"

def run(out_dir, email=""):
    out_path = out_dir / "clinical_trials.csv"
    # Search for 'Eppley' anywhere in record text
    url = "https://clinicaltrials.gov/api/query/study_fields"
    params = {
        "expr": "Eppley",
        "fields": FIELDS,
        "min_rnk": 1,
        "max_rnk": 1000,
        "fmt": "json",
    }
    r = requests.get(url, params=params, timeout=30, headers={"User-Agent": f"eppley-collector/1.0 (mailto:{email})" if email else "eppley-collector/1.0"})
    r.raise_for_status()
    studies = r.json().get("StudyFieldsResponse", {}).get("StudyFields", [])

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["title","abstract","journal","year","authors","doi","url","type","keywords"])
        w.writeheader()
        for s in studies:
            nct = (s.get("NCTId") or [""])[0]
            title = (s.get("BriefTitle") or [""])[0]
            conds = ", ".join(s.get("Condition") or [])
            stype = (s.get("StudyType") or [""])[0]
            start = (s.get("StartDate") or [""])[0]
            year = start[:4] if start else ""
            sponsor = (s.get("LeadSponsorName") or [""])[0]
            status = (s.get("OverallStatus") or [""])[0]
            link = f"https://clinicaltrials.gov/study/{nct}" if nct else ""
            w.writerow({
                "title": title,
                "abstract": "",
                "journal": "",
                "year": year,
                "authors": sponsor,
                "doi": "",
                "url": link,
                "type": f"{stype} ({status})" if status else stype,
                "keywords": conds,
            })
