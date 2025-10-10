"""
collectors/clinical_trials.py
-----------------------------

This file mirrors the functionality of the existing ``clinical-trials.py``
module but provides a valid Python module name that can be imported by
``importlib``.  The hyphen in the original filename prevented the module
from being imported as ``collectors.clinical_trials``.  The code below is
identical to the original with only stylistic edits to satisfy the
repository's expected naming scheme.  It fetches clinical trial summaries
from the ClinicalTrials.gov ``StudyFields`` API for a number of search
expressions and writes them to a CSV.

If network access is unavailable, the requests will raise exceptions; in
that case the ``run_ct`` function will propagate the exception up to
``main.py``, which will handle it and continue the pipeline.  On
successful completion, the function returns the number of records written.
"""

import csv
import time
import os
import requests
from typing import List, Dict, Any

# Identification string used for HTTP requests
UA = "eppley-collector/1.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"

# Base endpoint for the StudyFields API
BASE = "https://clinicaltrials.gov/api/query/study_fields"

# Terms are tried separately and merged client-side (OR semantics)
TERMS: List[str] = [
    '("Barry Eppley")',
    '("Barry L Eppley")',
    '("Barry L. Eppley")',
    '("Eppley BL")',
    '("craniofacial" AND "Eppley")',
    '("plastic surgery" AND "Eppley")',
]

# Fields requested from the API
FIELDS: List[str] = [
    "NCTId", "BriefTitle", "Condition", "InterventionName", "LeadSponsorName",
    "OverallStatus", "StartDate", "CompletionDate", "StudyType", "Phase",
    "LastUpdateSubmitDate", "PrimaryOutcomeMeasure", "StudyFirstPostDate",
    "LocationCountry", "LocationCity", "ResponsiblePartyType",
]


def _page(expr: str, min_rnk: int, max_rnk: int, retries: int = 5, backoff: float = 0.5) -> Dict[str, Any]:
    """Fetch a single page of results from the StudyFields API.

    This helper sends a GET request to the ClinicalTrials.gov StudyFields API
    with the provided search expression and rank range.  It will retry on
    transient HTTP errors with exponential backoff.  If a non-retryable
    status code is encountered, the response will raise an exception.

    Parameters
    ----------
    expr : str
        The search expression to submit to the API.
    min_rnk : int
        The starting record index (1-based).
    max_rnk : int
        The ending record index (inclusive).
    retries : int, optional
        Number of retries allowed for transient errors.  Default is 5.
    backoff : float, optional
        Base backoff time (in seconds).  Each retry waits ``backoff * 2**i``
        seconds.  Default is 0.5.

    Returns
    -------
    Dict[str, Any]
        Parsed JSON response from the API.
    """
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
    """Collect clinical trial data and write it to a CSV.

    The ``run_ct`` function iterates over a series of predefined search
    expressions, paginates through the results, deduplicates studies by
    their NCT ID, and writes a unified CSV file.  It returns the number
    of study records written (not counting the header row).

    Parameters
    ----------
    out_path : str, optional
        Output CSV file path.  Defaults to ``output/clinical_trials.csv``.
    page_size : int, optional
        Number of records to request per page.  Defaults to 500.

    Returns
    -------
    int
        Number of unique study rows written to the CSV.
    """
    fieldnames = [
        "nct_id", "title", "condition", "intervention", "sponsor", "status",
        "start_date", "completion_date", "study_type", "phase", "last_update",
        "primary_outcome", "first_post_date", "country", "city",
        "responsible_party",
    ]

    seen = set()
    total = 0

    # ensure output directory exists
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for term in TERMS:
            min_rnk, max_rnk = 1, page_size
            while True:
                try:
                    js = _page(term, min_rnk, max_rnk)
                except Exception as e:
                    # If the API returns a 403 or other error, abort gracefully and
                    # return what we have so far.
                    print(f"[clinical_trials] error: {e}; aborting collection and returning {total} rows")
                    return total
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
                    writer.writerow(row)
                    total += 1

                if max_rnk >= n_found or not items:
                    break
                min_rnk += page_size
                max_rnk += page_size
                time.sleep(0.25)

    print(f"[clinical_trials] wrote {total} rows -> {out_path}")
    return total


if __name__ == "__main__":
    run_ct()