#!/usr/bin/env python3
"""
Merge publications from PubMed, Crossref, OpenAlex, ORCID, Semantic Scholar.
Dedup order: DOI > PMID/put_code > fuzzy Title (RapidFuzz).
Outputs: output/publications_all.csv + .jsonl
"""

import csv, json, pathlib, re
from collections import OrderedDict
from rapidfuzz import fuzz, process

OUTDIR = pathlib.Path("output")
FILES = {
    "pubmed": OUTDIR/"pubmed_eppley.csv",
    "openalex": OUTDIR/"openalex_works.csv",
    "semanticscholar": OUTDIR/"semanticscholar_works.csv",
    "crossref": OUTDIR/"crossref_works.csv",
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

def best(a, b):  # prefer a if set, else b
    return a if (a and str(a).strip()) else b

def merge():
    data = load_rows()
    canon = OrderedDict()  # key -> record
    titles = []            # (title_norm, key)

    def add(rec, src):
        doi = (rec.get("doi") or rec.get("DOI") or "").lower().strip()
        pmid = (rec.get("pmid") or "").strip()
        put_code = (rec.get("put_code") or "").strip()  # ORCID
        title = rec.get("title") or rec.get("Title") or ""
        title_n = norm(title)

        key = None
        if doi:
            key = f"doi:{doi}"
        elif pmid:
            key = f"pmid:{pmid}"
        elif put_code and src == "orcid":
            key = f"orcid:{put_code}"
        else:
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
            "sources": set(),
            "provenance": {}
        })

        # choose best values (simple precedence by source quality where applicable)
        # precedence: pubmed > openalex > semanticscholar > crossref > orcid
        weight = {"pubmed":5,"openalex":4,"semanticscholar":3,"crossref":2,"orcid":1}.get(src,0)
        def set_field(field, value):
            if not value: return
            cur = base.get(field, "")
            if not cur:
                base[field] = value
                base["provenance"][field] = src
            else:
                # keep existing (higher-weight likely set earlier), unless new is better length
                if field in ("title","journal","venue","authors") and len(value) > len(cur) and weight >= 3:
                    base[field] = value
                    base["provenance"][field] = src

        set_field("title", title)
        set_field("doi", doi)
        set_field("pmid", pmid)
        set_field("url", rec.get("url") or rec.get("URL",""))
        set_field("journal", rec.get("journal") or rec.get("container") or rec.get("venue") or "")
        set_field("venue", rec.get("venue") or rec.get("container") or "")
        set_field("authors", rec.get("authors") or "")
        set_field("year", rec.get("year") or rec.get("publication_year") or "")

        base["sources"].add(src)
        canon[key] = base

    # harvest in preferred order
    for src in ("pubmed","openalex","semanticscholar","crossref","orcid"):
        for rec in data.get(src, []):
            add(rec, src)

    rows = []
    for k,v in canon.items():
        v["sources"] = ",".join(sorted(v["sources"]))
        # stringify provenance dict
        v["provenance"] = json.dumps(v["provenance"], ensure_ascii=False)
        rows.append(v)

    fields = ["title","year","journal","venue","authors","doi","pmid","url","sources","provenance"]
    OUTDIR.mkdir(parents=True, exist_ok=True)
    with open(OUTDIR/"publications_all.csv","w",encoding="utf-8",newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for r in rows: w.writerow({k:r.get(k,"") for k in fields})

    with open(OUTDIR/"publications_all.jsonl","w",encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r,ensure_ascii=False)+"\n")

    print(f"[merge] publications_all.csv rows={len(rows)}")

if __name__ == "__main__":
    merge()