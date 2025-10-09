# collectors/clinical_trials.py
# ClinicalTrials.gov StudyFields collector with multi-query OR semantics and deduplication.
# Output: output/clinical_trials.csv
import csv, time, requests
from typing import List, Dict, Any

UA = "eppley-collector/1.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"
BASE = "https://clinicaltrials.gov/api/query/study_fields"

# Terms are tried separately and merged client-side (OR)
TERMS: List[str] = [
    '("Barry Eppley")',
    '("Barry L Eppley")',
    '("Barry L. Eppley")',
    '("Eppley BL")',
    '("craniofacial" AND "Eppley")',
    '("plastic surgery" AND "Eppley")',
]

FIELDS: List[str] = [
    "NCTId","BriefTitle","Condition","InterventionName","LeadSponsorName",
    "OverallStatus","StartDate","CompletionDate","StudyType","Phase",
    "LastUpdateSubmitDate","PrimaryOutcomeMeasure","StudyFirstPostDate",
    "LocationCountry","LocationCity","ResponsiblePartyType"
]

def _page(expr: str, min_rnk: int, max_rnk: int, retries: int = 5, backoff: float = 0.5) -> Dict[str, Any]:
    params = {
        "expr": expr,
        "fields": ",".join(FIELDS),
        "min_rnk": str(min_rnk),
        "max_rnk": str(max_rnk),
        "fmt": "json",
    }
    for i in range(retries):
        r = requests.get(BASE, params=params, headers={"User-Agent": UA}, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff * (2 ** i))
            continue
        r.raise_for_status()
    r.raise_for_status()

def run_ct(out_path: str = "output/clinical_trials.csv", page_size: int = 500) -> int:
    """
    Run several queries, merge results, and write a single CSV (deduped by NCTId).
    Returns: number of rows written (excluding header).
    """
    fieldnames = ["nct_id","title","condition","intervention","sponsor","status",
                  "start_date","completion_date","study_type","phase","last_update",
                  "primary_outcome","first_post_date","country","city","responsible_party"]

    seen = set()
    total = 0

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for term in TERMS:
            min_rnk, max_rnk = 1, page_size
            while True:
                js = _page(term, min_rnk, max_rnk)
                resp = (js or {}).get("StudyFieldsResponse", {})
                n_found = int(resp.get("NStudiesFound", 0))
                items = resp.get("StudyFields", []) or []

                for s in items:
                    nct = (s.get("NCTId") or [""])[0]
                    if not nct or nct in seen:
                        continue
                    seen.add(nct)

                    row = {
                        "nct_id": nct,
                        "title": (s.get("BriefTitle") or [""])[0],
                        "condition": "; ".join(s.get("Condition") or []),
                        "intervention": "; ".join(s.get("InterventionName") or []),
                        "sponsor": (s.get("LeadSponsorName") or [""])[0],
                        "status": (s.get("OverallStatus") or [""])[0],
                        "start_date": (s.get("StartDate") or [""])[0],
                        "completion_date": (s.get("CompletionDate") or [""])[0],
                        "study_type": (s.get("StudyType") or [""])[0],
                        "phase": (s.get("Phase") or [""])[0],
                        "last_update": (s.get("LastUpdateSubmitDate") or [""])[0],
                        "primary_outcome": "; ".join(s.get("PrimaryOutcomeMeasure") or []),
                        "first_post_date": (s.get("StudyFirstPostDate") or [""])[0],
                        "country": "; ".join(s.get("LocationCountry") or []),
                        "city": "; ".join(s.get("LocationCity") or []),
                        "responsible_party": (s.get("ResponsiblePartyType") or [""])[0],
                    }
                    w.writerow(row)
                    total += 1

                if max_rnk >= n_found or not items:
                    break
                min_rnk += page_size
                max_rnk += page_size
                time.sleep(0.25)

    print(f"[clinicaltrials] wrote {total} rows -> {out_path}")
    return total

if __name__ == "__main__":
    run_ct()