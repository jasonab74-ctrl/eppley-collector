def run_openalex(names):
    """
    Robust OpenAlex collector (null-safe). Writes:
      output/openalex_works.csv
      output/openalex_works.jsonl
    """
    import csv, json, time, pathlib, requests
    from datetime import datetime, timezone

    OUTDIR = pathlib.Path("output")
    OUTDIR.mkdir(parents=True, exist_ok=True)
    CSV = OUTDIR / "openalex_works.csv"
    JSONL = OUTDIR / "openalex_works.jsonl"

    API = "https://api.openalex.org/works"
    UA  = "eppley-collector/openalex-1.1"

    def utc_now(): 
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def g(d, *path, default=""):
        """Null-safe getter for nested dict/list fields."""
        cur = d
        for p in path:
            if isinstance(cur, dict):
                cur = cur.get(p, None)
            elif isinstance(cur, list) and isinstance(p, int):
                cur = cur[p] if 0 <= p < len(cur) else None
            else:
                return default
            if cur is None:
                return default
        return cur

    def get_json(params, retries=4, backoff=0.7):
        headers = {"User-Agent": UA, "Accept": "application/json"}
        for i in range(retries):
            try:
                r = requests.get(API, params=params, headers=headers, timeout=30)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(backoff * (i + 1)); 
                    continue
                return None
            except requests.RequestException:
                time.sleep(backoff * (i + 1))
        return None

    rows = []
    for n in names:
        page = 1
        while True:
            params = {
                "search": f'author.display_name.search:"{n}"',
                "per_page": 200,
                "sort": "publication_year:desc",
                "page": page
            }
            j = get_json(params)
            if not j or not j.get("results"):
                break

            for it in j["results"]:
                # external IDs
                ids = g(it, "ids", default={}) or {}
                doi = (ids.get("doi") or "").replace("https://doi.org/", "").strip()

                # authors
                authors = []
                for a in g(it, "authorships", default=[]):
                    nm = g(a, "author", "display_name", default="")
                    if nm: authors.append(nm)

                # prefer host_venue; fall back to primary_location.source.*
                journal = g(it, "host_venue", "display_name", default="") or \
                          g(it, "primary_location", "source", "display_name", default="")
                url = g(it, "primary_location", "source", "url", default="") or \
                      g(it, "primary_location", "landing_page_url", default="")

                rows.append({
                    "title": g(it, "title", default=""),
                    "year": g(it, "publication_year", default=""),
                    "journal": journal,
                    "venue": g(it, "host_venue", "display_name", default=""),
                    "authors": ", ".join(authors),
                    "doi": doi,
                    "pmid": g(it, "ids", "pmid", default="").replace("https://pubmed.ncbi.nlm.nih.gov/",""),
                    "url": url,
                    "openalex_id": g(it, "id", default=""),
                    "cited_by_count": g(it, "cited_by_count", default=""),
                    "source": "openalex",
                    "collected_at": utc_now(),
                })

            meta = j.get("meta", {})
            last = int(meta.get("last_page", page))
            if page >= last:
                break
            page += 1
            time.sleep(0.25)

    # de-dupe by DOI (then title)
    seen = {}
    for r in rows:
        k = r.get("doi") or r.get("title")
        if k and k not in seen:
            seen[k] = r
    rows = list(seen.values())

    fields = ["title","year","journal","venue","authors","doi","pmid","url",
              "openalex_id","cited_by_count","source","collected_at"]

    with open(CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for r in rows: w.writerow({k: r.get(k, "") for k in fields})

    with open(JSONL, "w", encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[openalex] wrote {len(rows)} rows")