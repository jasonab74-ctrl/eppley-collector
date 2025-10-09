#!/usr/bin/env python3
"""
Merge publications from PubMed, Crossref, OpenAlex, ORCID into one canonical file.
Dedup order: DOI > PMID/put_code > fuzzy Title (RapidFuzz).
Outputs: output/publications_all.csv + .jsonl
"""

import csv, json, pathlib, re
from collections import OrderedDict
from rapidfuzz import fuzz, process

OUTDIR = pathlib.Path("output")
FILES = {
    "pubmed": OUTDIR/"pubmed_eppley.csv",
    "crossref": OUTDIR/"crossref_works.csv",
    "openalex": OUTDIR/"openalex_works.csv",
    "orcid": OUTDIR/"orcid_works.csv",
}

def norm(s): 
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def load_rows():
    data = {k: [] for k in FILES}
    for src, path in FILES.items():
        if not path.exists(): continue
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            r = csv.DictReader(f)
            data[src] = list(r)
    return data

def best(a, b, prefer=("pubmed","openalex","crossref","orcid")):
    # prefer non-empty, otherwise keep a; prefer source priority
    return a if a else b

def merge():
    data = load_rows()
    canon = OrderedDict()  # key -> record
    titles = []            # for fuzzy matching

    def add(rec, src):
        doi = (rec.get("doi") or rec.get("DOI") or "").lower().strip()
        pmid = (rec.get("pmid") or "").strip()
        title = rec.get("title") or rec.get("Title") or ""
        title_n = norm(title)

        key = None
        if doi: key = f"doi:{doi}"
        elif pmid: key = f"pmid:{pmid}"
        else:
            # fuzzy against existing titles
            if titles:
                match, score, idx = process.extractOne(title_n, [t[0] for t in titles], scorer=fuzz.token_sort_ratio)
                if score >= 92:
                    key = titles[idx][1]
        if not key:
            key = f"t:{len(canon)+1}"
            titles.append((title_n, key))

        base = canon.get(key, {
            "key": key, "title": title, "year": "",
            "journal": "", "venue": "", "authors": "",
            "doi": doi, "pmid": pmid, "url": rec.get("url") or rec.get("URL",""),
            "sources": set()
        })

        # fill fields if missing
        base["title"]   = best(base["title"], title)
        base["doi"]     = best(base["doi"], doi)
        base["pmid"]    = best(base["pmid"], pmid)
        base["url"]     = best(base["url"], rec.get("url") or rec.get("URL",""))
        base["journal"] = best(base["journal"], rec.get("journal") or rec.get("container") or rec.get("venue") or "")
        base["venue"]   = best(base["venue"], rec.get("venue") or rec.get("container") or "")
        base["authors"] = best(base["authors"], rec.get("authors") or "")
        base["year"]    = best(base["year"], rec.get("year") or rec.get("publication_year") or "")
        base["sources"].add(src)

        canon[key] = base

    # harvest in preferred order (higher authority first)
    for src in ("pubmed","openalex","crossref","orcid"):
        for rec in data.get(src, []):
            add(rec, src)

    rows = []
    for k,v in canon.items():
        v["sources"] = ",".join(sorted(v["sources"]))
        rows.append(v)

    fields = ["title","year","journal","venue","authors","doi","pmid","url","sources"]
    OUTDIR.mkdir(parents=True, exist_ok=True)
    with open(OUTDIR/"publications_all.csv","w",encoding="utf-8",newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for r in rows: w.writerow({k:r.get(k,"") for k in fields})

    with open(OUTDIR/"publications_all.jsonl","w",encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r,ensure_ascii=False)+"\n")

    print(f"[merge] publications_all.csv rows={len(rows)}")

if __name__ == "__main__":
    merge()