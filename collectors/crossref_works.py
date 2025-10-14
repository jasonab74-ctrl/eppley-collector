import csv
import time
import re
from pathlib import Path
import requests
import yaml

OUT = Path("output/crossref_works.csv")
BASE = "https://api.crossref.org/works"
FIELDS = ["source", "title", "year", "journal", "type", "DOI", "URL", "authors"]

# Load name variants from config.yaml if present
def load_name_variants():
    cfg = Path("config.yaml")
    if cfg.exists():
        try:
            data = yaml.safe_load(cfg.read_text(encoding="utf-8")) or {}
            names = data.get("names") or []
            if isinstance(names, list) and names:
                return [n.strip() for n in names if n and isinstance(n, str)]
        except Exception:
            pass
    # sensible defaults if config missing
    return ["Barry L. Eppley", "Barry Eppley", "Eppley BL"]

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def author_matches(item_authors, variants_norm):
    # Keep only if the Crossref "author" list contains a variant
    for a in (item_authors or []):
        given = (a.get("given") or "").strip()
        family = (a.get("family") or "").strip()
        full = norm(f"{given} {family}".strip())
        if full in variants_norm:
            return True
        # allow lightweight partial match on family + initial (e.g., "Eppley BL")
        initials = "".join(part[0] for part in given.split() if part)
        if initials:
            if norm(f"{family} {initials}") in variants_norm:
                return True
    return False

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    cursor = "*"

    variants = load_name_variants()
    variants_norm = set(norm(v) for v in variants)

    kept = 0
    no_match_pages = 0
    MAX_KEEP = 1500        # hard stop safeguard (way above expected)
    MAX_NO_MATCH_PAGES = 5 # stop if paging without hits

    while True:
        params = {
            "query.author": variants[0],  # seed the query
            "rows": 200,
            "cursor": cursor,
            "mailto": "site@eppley.example"
        }
        try:
            r = requests.get(
                BASE,
                params=params,
                timeout=60,
                headers={"User-Agent": "EppleyCollector/1.0 (mailto:site@eppley.example)"}
            )
            r.raise_for_status()
            data = r.json()
            items = data.get("message", {}).get("items", []) or []

            matched_this_page = 0
            for it in items:
                auths = it.get("author") or []
                if not author_matches(auths, variants_norm):
                    continue  # discard non-Eppley items

                authors_fmt = []
                for a in auths:
                    nm = " ".join(filter(None, [a.get("given", ""), a.get("family", "")])).strip()
                    if nm:
                        authors_fmt.append(nm)

                rows.append({
                    "source": "crossref",
                    "title": (it.get("title", [""])[0] or "").strip(),
                    "year": (it.get("issued", {}).get("date-parts", [[None]])[0][0]),
                    "journal": (it.get("container-title", [""])[0] or ""),
                    "type": it.get("type", ""),
                    "DOI": it.get("DOI", ""),
                    "URL": it.get("URL", ""),
                    "authors": "; ".join(authors_fmt),
                })
                kept += 1
                matched_this_page += 1
                if kept >= MAX_KEEP:
                    break

            if matched_this_page == 0:
                no_match_pages += 1
            else:
                no_match_pages = 0

            if kept >= MAX_KEEP or no_match_pages >= MAX_NO_MATCH_PAGES:
                break

            next_cur = data.get("message", {}).get("next-cursor")
            if not next_cur or len(items) < 200:
                break
            cursor = next_cur
            time.sleep(0.25)
        except Exception as e:
            print(f"[crossref_works] error: {e}")
            break

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    print(f"[crossref_works] wrote {len(rows)} rows to {OUT}")

if __name__ == "__main__":
    run()