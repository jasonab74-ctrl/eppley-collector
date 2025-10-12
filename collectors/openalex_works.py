# collectors/openalex.py
import csv
import os
import time
import requests
from typing import Any, Dict, List, Optional

OPENALEX_BASE = "https://api.openalex.org/works"
OUT_PATH = "output/openalex_works.csv"

def safe_get(d: Optional[Dict[str, Any]], path: List[str], default: Any = "") -> Any:
    cur = d
    for key in path:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return default
    return default if cur is None else cur

def join_authors(authorships: Optional[List[Dict[str, Any]]]) -> str:
    if not isinstance(authorships, list):
        return ""
    names = []
    for a in authorships:
        if isinstance(a, dict):
            n = safe_get(a, ["author", "display_name"], "")
            if n:
                names.append(n)
    return "; ".join(names)

def join_concepts(concepts: Optional[List[Dict[str, Any]]], top: int = 10) -> str:
    if not isinstance(concepts, list):
        return ""
    # sort by score desc when present
    sorted_concepts = sorted(
        [c for c in concepts if isinstance(c, dict)],
        key=lambda c: c.get("score", 0),
        reverse=True
    )
    names = [c.get("display_name", "") for c in sorted_concepts[:top] if c.get("display_name")]
    return "; ".join(names)

def get_headers() -> Dict[str, str]:
    # Use OPENALEX_EMAIL if present; fall back to NCBI_EMAIL (since you already set that).
    email = os.getenv("OPENALEX_EMAIL") or os.getenv("NCBI_EMAIL") or "unknown@example.com"
    return {
        "User-Agent": f"eppley-collector/1.0 (mailto:{email})",
        "Accept": "application/json",
    }

def fetch_openalex(query: str, per_page: int = 200, max_pages: int = 200) -> List[Dict[str, Any]]:
    """
    Cursor-based pagination; returns a list of valid dict results.
    Defensive against None/empty rows. Retries on transient failures.
    """
    results: List[Dict[str, Any]] = []
    cursor = "*"
    page = 0
    headers = get_headers()

    while page < max_pages:
        params = {
            "search": query,
            "per_page": per_page,
            "cursor": cursor,
        }
        try:
            r = requests.get(OPENALEX_BASE, params=params, headers=headers, timeout=30)
            if r.status_code >= 500:
                # backoff and retry this page
                time.sleep(2.0)
                continue
            r.raise_for_status()
            payload = r.json() or {}
        except Exception as e:
            print(f"[openalex_works] warn: request/page error: {e}")
            break

        page_results = payload.get("results") or []
        # Defensive filter: keep only dict rows
        valid = [row for row in page_results if isinstance(row, dict)]
        results.extend(valid)

        meta = payload.get("meta") or {}
        cursor = meta.get("next_cursor")
        page += 1

        if not cursor or not page_results:
            break

        # Be nice to API
        time.sleep(0.2)

    return results

def write_csv(rows: List[Dict[str, Any]], out_path: str = OUT_PATH) -> int:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    fields = [
        "id",
        "doi",
        "title",
        "publication_year",
        "type",
        "host_venue",
        "primary_location",
        "openalex_id",
        "cited_by_count",
        "authorships",
        "concepts",
        "language",
        "is_paratext",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for item in rows:
            try:
                row = {
                    "id": item.get("id", ""),
                    "doi": (item.get("doi") or item.get("ids", {}).get("doi") or ""),
                    "title": item.get("title", ""),
                    "publication_year": item.get("publication_year", ""),
                    "type": item.get("type", ""),
                    "host_venue": safe_get(item, ["host_venue", "display_name"], ""),
                    "primary_location": safe_get(item, ["primary_location", "source", "display_name"], ""),
                    "openalex_id": safe_get(item, ["ids", "openalex"], ""),
                    "cited_by_count": item.get("cited_by_count", 0),
                    "authorships": join_authors(item.get("authorships")),
                    "concepts": join_concepts(item.get("concepts")),
                    "language": item.get("language", ""),
                    "is_paratext": item.get("is_paratext", False),
                }
                w.writerow(row)
            except Exception as e:
                # Never crash the whole export because of one bad row
                print(f"[openalex_works] skip bad row: {e}")
                continue

    return len(rows)

def run(query: Optional[str] = None) -> int:
    query = query or os.getenv("OPENALEX_QUERY") or "Eppley"
    print(f"==> Running collectors.openalex_works (query='{query}')")
    rows = fetch_openalex(query=query)
    count = write_csv(rows, OUT_PATH)
    print(f"[openalex_works] wrote {count} rows to {OUT_PATH}")
    return count

if __name__ == "__main__":
    run()