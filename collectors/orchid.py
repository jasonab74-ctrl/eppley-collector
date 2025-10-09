# collectors/orcid.py
# ORCID profile discovery + works harvesting.
# Outputs:
#   output/orcid_profiles.csv
#   output/orcid_works.csv
import csv, time, requests
from typing import List, Dict, Any

UA = "eppley-collector/1.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"
SEARCH = "https://pub.orcid.org/v3.0/expanded-search/"
WORKS  = "https://pub.orcid.org/v3.0/{orcid}/works"

NAME_VARIANTS: List[str] = [
    "Barry Eppley",
    "Barry L Eppley",
    "Barry L. Eppley",
    "Eppley Barry",
    "B L Eppley",
    "Eppley BL",
]

# Optionally seed confirmed ORCID iDs if you have them:
SEED_ORCIDS: List[str] = []  # e.g., ["0000-0002-1825-0097"]

def _search(term: str, start: int, rows: int, retries: int = 5, backoff: float = 0.5) -> Dict[str, Any]:
    params = {"q": term, "start": start, "rows": rows}
    headers = {"User-Agent": UA, "Accept": "application/json"}
    for i in range(retries):
        r = requests.get(SEARCH, params=params, headers=headers, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff * (2 ** i))
            continue
        r.raise_for_status()
    r.raise_for_status()

def _works(orcid: str, retries: int = 5, backoff: float = 0.5) -> Dict[str, Any]:
    headers = {"User-Agent": UA, "Accept": "application/json"}
    for i in range(retries):
        r = requests.get(WORKS.format(orcid=orcid), headers=headers, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            return {}
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff * (2 ** i))
            continue
        r.raise_for_status()
    r.raise_for_status()

def run_orcid_profiles(out_path: str = "output/orcid_profiles.csv", page: int = 200) -> int:
    """
    Search ORCID expanded-search for NAME_VARIANTS and write a profile CSV.
    Also seeds any SEED_ORCIDS so downstream works fetching has something to follow.
    """
    fieldnames = ["orcid","given_names","family_name","credit_name",
                  "institutions","countries","keywords","num_works","last_modified"]
    seen = set()
    rows = 0

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        # Seed any known ORCIDs even before search
        for oid in SEED_ORCIDS:
            if oid in seen: continue
            seen.add(oid)
            w.writerow({"orcid": oid})
            rows += 1

        for nm in NAME_VARIANTS:
            start = 0
            while True:
                js = _search(nm, start=start, rows=page)
                results = js.get("expanded-result", []) or []
                if not results:
                    break
                for it in results:
                    oid = it.get("orcid-id") or ""
                    if not oid or oid in seen:
                        continue
                    seen.add(oid)
                    insts = [a.get("organization-name","") for a in it.get("institutions",[]) if a.get("organization-name")]
                    cntrs = [a.get("country","") for a in it.get("institutions",[]) if a.get("country")]
                    w.writerow({
                        "orcid": oid,
                        "given_names": it.get("given-names",""),
                        "family_name": it.get("family-names",""),
                        "credit_name": it.get("credit-name",""),
                        "institutions": ", ".join(insts),
                        "countries": ", ".join(cntrs),
                        "keywords": ", ".join(it.get("keywords",[]) or []),
                        "num_works": it.get("num-works",""),
                        "last_modified": it.get("last-modified-date",""),
                    })
                    rows += 1
                start += page
                time.sleep(0.3)

    print(f"[orcid] profiles: {rows} -> {out_path}")
    return rows

def run_orcid_works(profiles_csv: str = "output/orcid_profiles.csv",
                    out_path: str = "output/orcid_works.csv") -> int:
    """Fetch works summaries for all ORCIDs found in profiles (plus seeds)."""
    ids: List[str] = list(SEED_ORCIDS)
    try:
        with open(profiles_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                oid = (row.get("orcid") or "").strip()
                if oid and oid not in ids:
                    ids.append(oid)
    except FileNotFoundError:
        pass

    fieldnames = ["orcid","put_code","title","type","journal","year","external_ids","source"]
    total = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for oid in ids:
            js = _works(oid) or {}
            groups = js.get("group", []) or []
            for g in groups:
                for s in g.get("work-summary", []) or []:
                    w.writerow({
                        "orcid": oid,
                        "put_code": s.get("put-code",""),
                        "title": ((s.get("title") or {}).get("title") or {}).get("value",""),
                        "type": s.get("type",""),
                        "journal": (s.get("journal-title") or {}).get("value",""),
                        "year": ((s.get("publication-date") or {}).get("year") or {}).get("value",""),
                        "external_ids": "; ".join([
                            f'{(e.get("type") or "").lower()}:{e.get("value","")}'
                            for e in ((s.get("external-ids") or {}).get("external-id") or [])
                        ]),
                        "source": ((s.get("source") or {}).get("source-name") or {}).get("value",""),
                    })
                    total += 1
            time.sleep(0.25)

    print(f"[orcid] works: {total} -> {out_path}")
    return total

if __name__ == "__main__":
    p = run_orcid_profiles()
    w = run_orcid_works()
    print(f"[orcid] profiles={p}, works={w}")