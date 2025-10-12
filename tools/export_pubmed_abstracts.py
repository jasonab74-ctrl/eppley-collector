# tools/export_pubmed_abstracts.py
from pathlib import Path
import pandas as pd
import json

ROOT = Path(".")
OUTDIR = ROOT / "output" / "corpus"
OUTDIR.mkdir(parents=True, exist_ok=True)
OUT = OUTDIR / "pubmed_abstracts.jsonl"

CANDIDATES = [
    ROOT / "output" / "pubmed_eppley.csv",
    ROOT / "output" / "eppley_master.csv",
]

PMID_COLS = ["pmid", "PMID", "pubmed_id", "pubmedId"]
DOI_COLS = ["doi", "DOI"]
TITLE_COLS = ["title", "Title"]
ABSTRACT_COLS = ["abstract", "Abstract", "abstract_text", "AbstractText"]
YEAR_COLS = ["year", "Year", "pub_year", "publication_year"]
JOURNAL_COLS = ["journal", "Journal", "journal_title"]
URL_COLS = ["url", "URL", "link"]

def first_existing(df, cols):
    for c in cols:
        if c in df.columns: 
            return c
    return None

def load_pubmed_df():
    for p in CANDIDATES:
        if p.exists():
            try:
                return pd.read_csv(p, low_memory=False)
            except Exception:
                continue
    return pd.DataFrame()

def run():
    df = load_pubmed_df()
    if df.empty:
        print("[pubmed] no CSV found; nothing to export")
        return

    # pick best columns we can find
    c_pmid  = first_existing(df, PMID_COLS)
    c_doi   = first_existing(df, DOI_COLS)
    c_title = first_existing(df, TITLE_COLS)
    c_abs   = first_existing(df, ABSTRACT_COLS)
    c_year  = first_existing(df, YEAR_COLS)
    c_j     = first_existing(df, JOURNAL_COLS)
    c_url   = first_existing(df, URL_COLS)

    # keep only with abstracts
    if c_abs is None:
        print("[pubmed] no abstract column found")
        return
    dfe = df[df[c_abs].notna() & (df[c_abs].astype(str).str.strip() != "")]
    if dfe.empty:
        print("[pubmed] 0 rows with abstracts")
        return

    # dedupe by PMID or DOI or title
    keys = []
    for _, r in dfe.iterrows():
        k = None
        if c_pmid and pd.notna(r.get(c_pmid)): k = f"pmid:{str(r[c_pmid]).strip()}"
        elif c_doi and pd.notna(r.get(c_doi)):  k = f"doi:{str(r[c_doi]).strip()}"
        elif c_title and pd.notna(r.get(c_title)): k = f"title:{str(r[c_title]).strip().lower()}"
        keys.append(k if k else None)
    dfe = dfe.assign(_k=keys).drop_duplicates(subset=["_k"])

    wrote = 0
    with OUT.open("w", encoding="utf-8") as f:
        for _, r in dfe.iterrows():
            rec = {
                "id": (str(r.get(c_pmid)) if c_pmid and pd.notna(r.get(c_pmid)) else None) or
                      (str(r.get(c_doi)) if c_doi and pd.notna(r.get(c_doi)) else None),
                "source": "pubmed",
                "title": str(r.get(c_title) or "").strip(),
                "journal": str(r.get(c_j) or "").strip(),
                "year": int(r.get(c_year)) if c_year and pd.notna(r.get(c_year)) else None,
                "pmid": str(r.get(c_pmid)).strip() if c_pmid and pd.notna(r.get(c_pmid)) else None,
                "doi": str(r.get(c_doi)).strip() if c_doi and pd.notna(r.get(c_doi)) else None,
                "url": str(r.get(c_url)).strip() if c_url and pd.notna(r.get(c_url)) else (f"https://pubmed.ncbi.nlm.nih.gov/{str(r.get(c_pmid)).strip()}/" if c_pmid and pd.notna(r.get(c_pmid)) else None),
                "text": str(r.get(c_abs)).strip(),
            }
            if not rec["id"]:
                rec["id"] = f"pmabs:{wrote+1}"
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            wrote += 1
    print(f"[pubmed] wrote {wrote} abstracts -> {OUT}")

if __name__ == "__main__":
    run()