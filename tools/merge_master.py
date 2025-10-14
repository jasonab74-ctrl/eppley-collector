"""
Curated Master Merger (ID-aware, Deduping, OpenAlex Attach)
----------------------------------------------------------
- Normalizes all known output/*.csv into a single schema
- Preserves useful identifiers when present (doi, pmid, type)
- Deduplicates primarily by DOI, then by (normalized title + year + journal)
- If output/eppley_openalex.csv exists, LEFT-JOINs its columns additively

Final columns:
  source, __file, title, url, year, journal, text,
  doi, pmid, type, openalex_id, cited_by_count, concepts, authorships, host_venue, oa_url
"""

from __future__ import annotations
import csv
import re
from pathlib import Path
from typing import Dict, List
import pandas as pd

OUT = Path("output/eppley_master.csv")
OPENALEX = Path("output/eppley_openalex.csv")
SRC_DIR = Path("output")

BASE_COLS = ["source", "__file", "title", "url", "year", "journal", "text"]
ID_COLS   = ["doi", "pmid", "type"]
OA_COLS   = ["openalex_id", "cited_by_count", "concepts", "authorships", "host_venue", "oa_url"]

def norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_doi(s: str) -> str:
    if not s: return ""
    s = s.strip()
    s = re.sub(r"^https?://(dx\.)?doi\.org/", "", s, flags=re.I)
    return s.lower()

def read_csv(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
        df["__file"] = path.name
        return df
    except Exception:
        return pd.DataFrame()

def map_to_common(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df

    cols = {c.lower(): c for c in df.columns}
    get = lambda name: df[cols[name]] if name in cols else None

    out = pd.DataFrame()
    out["source"]  = get("source") if "source" in cols else ""
    out["title"]   = get("title")  if "title"  in cols else ""
    out["url"]     = get("url")    if "url"    in cols else ""
    out["year"]    = get("year")   if "year"   in cols else ""
    out["journal"] = get("journal") if "journal" in cols else ""
    # prefer text/fulltext if present; else empty
    if "text" in cols:
        out["text"] = get("text")
    else:
        out["text"] = ""

    # identifiers if present
    out["doi"]  = get("doi")  if "doi"  in cols else ""
    out["pmid"] = get("pmid") if "pmid" in cols else ""
    out["type"] = get("type") if "type" in cols else ""

    out["__file"] = df["__file"]
    return out

def attach_openalex(master: pd.DataFrame) -> pd.DataFrame:
    if not OPENALEX.exists():
        return master
    try:
        oa = pd.read_csv(OPENALEX)
        # Normalize DOI for join
        master["_doi_norm"] = master["doi"].map(lambda x: norm_doi(str(x)) if pd.notna(x) else "")
        oa["_doi_norm"] = oa["doi"].map(lambda x: norm_doi(str(x)) if isinstance(x, str) else "")
        # 1) join on DOI
        merged = master.merge(
            oa[[ "_doi_norm"] + OA_COLS],
            on="_doi_norm",
            how="left"
        )
        # cleanup
        merged.drop(columns=["_doi_norm"], inplace=True)
        return merged
    except Exception:
        return master

def main():
    frames: List[pd.DataFrame] = []
    for p in SRC_DIR.glob("*.csv"):
        if p.name == OUT.name or p.name == OPENALEX.name:
            continue
        df = read_csv(p)
        if df.empty: continue
        frames.append(map_to_common(df))

    if not frames:
        OUT.write_text("", encoding="utf-8")
        print("[merge] no inputs found")
        return

    all_df = pd.concat(frames, ignore_index=True)

    # Normalize identifiers for dedupe
    all_df["doi_norm"] = all_df["doi"].map(lambda x: norm_doi(str(x)) if pd.notna(x) else "")
    all_df["title_norm"] = all_df["title"].map(norm_title)
    all_df["year_str"] = all_df["year"].astype(str).fillna("")

    # Primary dedupe: DOI
    has_doi = all_df["doi_norm"] != ""
    df_doi = all_df[has_doi].sort_values(["doi_norm"]).drop_duplicates(subset=["doi_norm"], keep="first")

    # Secondary dedupe: title+year+journal for non-DOI items
    no_doi = all_df[~has_doi].copy()
    no_doi["tyj"] = no_doi["title_norm"] + "||" + no_doi["year_str"] + "||" + no_doi["journal"].fillna("").str.lower().str.strip()
    df_no_doi = no_doi.sort_values(["tyj"]).drop_duplicates(subset=["tyj"], keep="first").drop(columns=["tyj"])

    master = pd.concat([df_doi, df_no_doi], ignore_index=True)

    # Order columns
    cols = BASE_COLS + ID_COLS
    for c in cols:
        if c not in master.columns:
            master[c] = ""
    master = master[["source", "__file", "title", "url", "year", "journal", "text", "doi", "pmid", "type"]]

    # Attach OpenAlex (if available)
    master = attach_openalex(master)

    # Ensure OA columns exist even if no join
    for c in OA_COLS:
        if c not in master.columns:
            master[c] = ""

    # Final write
    master.to_csv(OUT, index=False)
    print(f"[merge] wrote {len(master)} rows to {OUT}")

if __name__ == "__main__":
    main()