"""
Curated Master Merger (NaN-safe, ID-aware, OpenAlex attach)
-----------------------------------------------------------
- Reads all output/*.csv (except eppley_master.csv and eppley_openalex.csv)
  as strings to avoid NaN/float issues.
- Normalizes into a common schema.
- Dedupes by DOI first, then by (title+year+journal).
- LEFT-joins OpenAlex enrichment when available.

Final columns:
  source, __file, title, url, year, journal, text,
  doi, pmid, type, openalex_id, cited_by_count, concepts, authorships, host_venue, oa_url
"""

from __future__ import annotations
import re
from pathlib import Path
from typing import List
import pandas as pd

OUT = Path("output/eppley_master.csv")
OPENALEX = Path("output/eppley_openalex.csv")
SRC_DIR = Path("output")

BASE_COLS = ["source", "__file", "title", "url", "year", "journal", "text"]
ID_COLS   = ["doi", "pmid", "type"]
OA_COLS   = ["openalex_id", "cited_by_count", "concepts", "authorships", "host_venue", "oa_url"]

def s(x) -> str:
    # safe string
    return "" if x is None else str(x)

def norm_title(val: str) -> str:
    t = s(val).strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t

def norm_doi(val: str) -> str:
    d = s(val).strip()
    d = re.sub(r"^https?://(dx\.)?doi\.org/", "", d, flags=re.I)
    return d.lower()

def read_csv(path: Path) -> pd.DataFrame:
    # Read all columns as strings; keep_default_na=False prevents NaN
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df["__file"] = path.name
        return df
    except Exception:
        return pd.DataFrame()

def map_to_common(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # force string on all columns to be safe
    for c in df.columns:
        df[c] = df[c].astype(str)

    # column lookup in a case-insensitive way
    cols = {c.lower(): c for c in df.columns}

    def get(name: str, default: str = "") -> pd.Series:
        if name in cols:
            return df[cols[name]].astype(str)
        return pd.Series([default] * len(df), index=df.index, dtype=str)

    out = pd.DataFrame(index=df.index)
    out["source"]  = get("source")
    out["title"]   = get("title")
    out["url"]     = get("url")
    out["year"]    = get("year")
    out["journal"] = get("journal")
    out["text"]    = get("text")
    out["doi"]     = get("doi")
    out["pmid"]    = get("pmid")
    out["type"]    = get("type")
    out["__file"]  = df["__file"]
    return out

def attach_openalex(master: pd.DataFrame) -> pd.DataFrame:
    if not OPENALEX.exists():
        # ensure OA columns exist even when file absent
        for c in OA_COLS:
            if c not in master.columns:
                master[c] = ""
        return master
    try:
        oa = pd.read_csv(OPENALEX, dtype=str, keep_default_na=False)
        oa["_doi_norm"] = oa.get("doi", "").map(norm_doi) if "doi" in oa.columns else ""
        # prepare master DOI norm
        master["_doi_norm"] = master.get("doi", "").map(norm_doi)

        merged = master.merge(
            oa[["_doi_norm"] + [c for c in OA_COLS if c in oa.columns]],
            on="_doi_norm",
            how="left",
            suffixes=("", "_oa"),
        )
        merged.drop(columns=["_doi_norm"], inplace=True)
        # ensure any missing OA columns exist
        for c in OA_COLS:
            if c not in merged.columns:
                merged[c] = ""
        return merged
    except Exception:
        for c in OA_COLS:
            if c not in master.columns:
                master[c] = ""
        return master

def main():
    frames: List[pd.DataFrame] = []
    for p in SRC_DIR.glob("*.csv"):
        if p.name in {OUT.name, OPENALEX.name}:
            continue
        df = read_csv(p)
        if df.empty:
            continue
        frames.append(map_to_common(df))

    if not frames:
        OUT.write_text("", encoding="utf-8")
        print("[merge] no inputs found")
        return

    all_df = pd.concat(frames, ignore_index=True)

    # Normalize strings
    for c in ["source", "title", "url", "year", "journal", "text", "doi", "pmid", "type"]:
        if c not in all_df.columns:
            all_df[c] = ""
        else:
            all_df[c] = all_df[c].astype(str)

    # Helpers for dedupe
    all_df["doi_norm"]   = all_df["doi"].map(norm_doi)
    all_df["title_norm"] = all_df["title"].map(norm_title)
    all_df["year_str"]   = all_df["year"].astype(str)

    # Primary dedupe: DOI
    has_doi = all_df["doi_norm"] != ""
    df_doi = (all_df[has_doi]
              .sort_values(["doi_norm"])
              .drop_duplicates(subset=["doi_norm"], keep="first"))

    # Secondary dedupe: title+year+journal for items without DOI
    no_doi = all_df[~has_doi].copy()
    no_doi["tyj"] = (
        no_doi["title_norm"] + "||" +
        no_doi["year_str"] + "||" +
        no_doi["journal"].str.lower().str.strip()
    )
    df_no_doi = (no_doi
                 .sort_values(["tyj"])
                 .drop_duplicates(subset=["tyj"], keep="first")
                 .drop(columns=["tyj"]))

    master = pd.concat([df_doi, df_no_doi], ignore_index=True)

    # Order core columns
    ordered = ["source", "__file", "title", "url", "year", "journal", "text", "doi", "pmid", "type"]
    for c in ordered:
        if c not in master.columns:
            master[c] = ""
    master = master[ordered]

    # Attach OpenAlex enrichment if available
    master = attach_openalex(master)

    master.to_csv(OUT, index=False)
    print(f"[merge] wrote {len(master)} rows to {OUT}")

if __name__ == "__main__":
    main()