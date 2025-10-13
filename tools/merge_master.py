#!/usr/bin/env python3
"""
Merge all available source CSVs into output/eppley_master.csv
- Robust to missing files
- Normalizes columns: source, title, url, year, journal, text, __file
- Dedupes (url, title, source)
"""

from pathlib import Path
import pandas as pd
import numpy as np
import re
from datetime import datetime, timezone

ROOT   = Path(".")
OUTDIR = ROOT / "output"
OUT    = OUTDIR / "eppley_master.csv"
OUTDIR.mkdir(parents=True, exist_ok=True)

def load_csv(path: Path, usecols=None):
    if not path.exists(): return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False, usecols=usecols)
    except Exception:
        return pd.read_csv(path, low_memory=False)

def pick_first(row, cols):
    for c in cols:
        if c in row and pd.notna(row[c]) and str(row[c]).strip():
            return str(row[c]).strip()
    return ""

def normalize(df, mapping, source_name, file_tag):
    out = pd.DataFrame()
    out["source"] = source_name
    out["__file"] = file_tag
    out["title"]  = df[mapping.get("title")]  if mapping.get("title")  in df.columns else ""
    out["url"]    = df[mapping.get("url")]    if mapping.get("url")    in df.columns else ""
    out["year"]   = df[mapping.get("year")]   if mapping.get("year")   in df.columns else np.nan
    out["journal"]= df[mapping.get("journal")]if mapping.get("journal")in df.columns else ""
    # text can come from several
    text_cols = mapping.get("text_cols", [])
    out["text"]  = df.apply(lambda r: pick_first(r, text_cols), axis=1)
    # coerce types
    out["title"]   = out["title"].astype(str)
    out["url"]     = out["url"].astype(str)
    out["journal"] = out["journal"].astype(str)
    # clean year
    def norm_year(v):
        try:
            s = str(v).strip()
            if not s or s.lower() == "nan": return np.nan
            m = re.search(r"\b(19|20)\d{2}\b", s)
            return int(m.group(0)) if m else np.nan
        except Exception:
            return np.nan
    out["year"] = out["year"].apply(norm_year)
    # drop empty text rows to keep quality
    return out[out["text"].astype(str).str.len() > 0]

def main():
    frames = []

    # PubMed
    pm = load_csv(OUTDIR / "pubmed_eppley.csv")
    if not pm.empty:
        frames.append(normalize(pm, {
            "title":"title", "url":"url", "year":"year", "journal":"journal",
            "text_cols":["abstract","Abstract","abstract_text","AbstractText","summary","description"]
        }, "pubmed", "pubmed_eppley.csv"))

    # WordPress deep
    wp = load_csv(OUTDIR / "wordpress_fulltext.csv")
    if not wp.empty:
        frames.append(normalize(wp, {
            "title":"title", "url":"url", "year":None, "journal":None,
            "text_cols":["text","body","content","answer","description"]
        }, "wordpress", "wordpress_fulltext.csv"))

    # YouTube transcripts
    yt = load_csv(OUTDIR / "youtube_transcripts.csv")
    if not yt.empty:
        frames.append(normalize(yt, {
            "title":None, "url":"url", "year":None, "journal":None,
            "text_cols":["transcript","text","description"]
        }, "youtube", "youtube_transcripts.csv"))

    # Crossref (optional if present)
    cr = load_csv(OUTDIR / "crossref_works.csv")
    if not cr.empty:
        frames.append(normalize(cr, {
            "title":"title", "url":"url", "year":"year", "journal":"journal",
            "text_cols":["abstract","summary","description"]
        }, "crossref", "crossref_works.csv"))

    # OpenAlex (optional)
    oa = load_csv(OUTDIR / "openalex_works.csv")
    if not oa.empty:
        frames.append(normalize(oa, {
            "title":"title", "url":"url", "year":"year", "journal":"journal",
            "text_cols":["abstract","summary","description"]
        }, "openalex", "openalex_works.csv"))

    if not frames:
        print("[merge] no sources found; writing empty master")
        pd.DataFrame(columns=["source","__file","title","url","year","journal","text"]).to_csv(OUT, index=False)
        return

    all_df = pd.concat(frames, ignore_index=True)

    # Deduplicate
    all_df["dedupe_key"] = (
        all_df["source"].fillna("") + "||" +
        all_df["title"].fillna("").str.lower() + "||" +
        all_df["url"].fillna("").str.lower()
    )
    all_df = all_df.drop_duplicates(subset=["dedupe_key"]).drop(columns=["dedupe_key"])

    # Write
    all_df.to_csv(OUT, index=False)
    print(f"[merge] wrote {OUT} rows={len(all_df)}")

if __name__ == "__main__":
    main()