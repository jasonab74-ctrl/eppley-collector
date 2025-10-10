# collectors/openalex.py
# Robust OpenAlex "works" collector with ORCID + name-variant matching.
# Output: output/openalex_works.csv
import csv, time, requests
from typing import Iterable, List, Dict, Any
from urllib.parse import quote_plus

UA = "eppley-collector/1.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"
BASE = "https://api.openalex.org/works"

# If you have a CONFIRMED ORCID for Dr. Barry Eppley, put it here to get precise results.
SEED_ORCIDS: List[str] = []  # e.g., ["0000-0002-1825-0097"]

NAME_VARIANTS: List[str] = [
    "Barry Eppley",
    "Barry L Eppley",
    "Barry L. Eppley",
    "B L Eppley",
    "Eppley BL",
    "Eppley Barry",
]

def _get_json(url: str, params: Dict[str, Any], retries: int = 5, backoff: float = 0.6):
    for i in range(retries):
        r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff * (2 ** i))
            continue
        r.raise_for_status()
    r.raise_for_status()

def _filters() -> str:
    """Build a comma-OR filter for OpenAlex works."""
    fs = []
    # Exact ORCID matches (highest precision)
    for oid in SEED_ORCIDS:
        fs.append(f"author.orcid:{oid}")
    # Name search variants (display_name.search is flexible)
    for nm in NAME_VARIANTS:
        fs.append(f"author.display_name.search:{quote_plus(nm)}")
    # Join as OR list (comma in OpenAlex filter = OR)
    return ",".join(fs) if fs else ""

def run_openalex(out_path: str = "output/openalex_works.csv", per_page: int = 200) -> int:
    """
    Collect OpenAlex works where an author matches any SEED_ORCIDS or NAME_VARIANTS.
    Uses cursor pagination, polite delays, and writes normalized CSV.
    Returns: number of rows written (excluding header).
    """
    fieldnames = [
        "openalex_id", "doi", "title", "publication_date", "type", "cited_by_count",
        "host_venue_name", "host_venue_publisher",
        "authorships", "concepts",
        "openalex_url",
    ]

    total = 0
    flt = _filters()
    params = {
        "per_page": per_page,
        "cursor": "*",
        "sort": "publication_date:desc",
    }
    if flt:
        params["filter"] = flt

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        while True:
            try:
                js = _get_json(BASE, params=params)
            except Exception as e:
                # Any HTTP error or network failure should halt the collector gracefully.
                # We simply break the loop and return whatever rows have been written so far.
                print(f"[openalex] error: {e}; aborting collection and returning {total} rows")
                return total
            results = js.get("results", []) or []
            for it in results:
                hv = it.get("host_venue") or {}
                auths = it.get("authorships") or []
                conc = it.get("concepts") or []

                w.writerow({
                    "openalex_id": it.get("id", ""),
                    "doi": (it.get("doi") or "").replace("https://doi.org/", ""),
                    "title": it.get("display_name", ""),
                    "publication_date": it.get("publication_date", ""),
                    "type": it.get("type", ""),
                    "cited_by_count": it.get("cited_by_count", 0),
                    "host_venue_name": hv.get("display_name", ""),
                    "host_venue_publisher": hv.get("publisher", ""),
                    "authorships": "; ".join([
                        f'{(a.get("author") or {}).get("display_name","")}'
                        + (f'({(a.get("author") or {}).get("orcid","")})' if (a.get("author") or {}).get("orcid") else "")
                        for a in auths
                    ]),
                    "concepts": "; ".join([c.get("display_name","") for c in conc]),
                    "openalex_url": it.get("id", ""),
                })
                total += 1

            cursor = (js.get("meta") or {}).get("next_cursor")
            if not cursor:
                break
            params["cursor"] = cursor
            time.sleep(0.2)  # polite pacing

    print(f"[openalex] wrote {total} rows -> {out_path}")
    return total

if __name__ == "__main__":
    run_openalex()