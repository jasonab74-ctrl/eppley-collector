"""
Builds NotebookLM-ready corpus: output/eppley_corpus.jsonl
Fields:
  id, source, title, date, authors, url, doi, pmid, abstract, summary, body_text
Merges from collectors and joins any expanded page text.
"""
import json, hashlib
from pathlib import Path
import pandas as pd

OUTDIR = Path("output")
EXPANDED = OUTDIR / "expanded" / "pages.jsonl"
CORPUS = OUTDIR / "eppley_corpus.jsonl"
CSV    = OUTDIR / "eppley_corpus.csv"

def _load_expanded():
    m = {}
    if EXPANDED.exists():
        with EXPANDED.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    m[rec.get("url")] = rec.get("text","")
                except Exception:
                    continue
    return m

def _hash_id(*vals):
    h = hashlib.sha1("||".join([str(v or "") for v in vals]).encode("utf-8")).hexdigest()[:16]
    return h

def normalize():
    frames = []
    # WordPress
    wp = OUTDIR / "wordpress_posts.csv"
    if wp.exists():
        df = pd.read_csv(wp)
        df = df.rename(columns={"link":"url","pub_date":"date"})
        df["source"] = "wordpress"
        df["title"] = df.get("title","")
        df["summary"] = df.get("summary","")
        df["abstract"] = ""
        df["authors"] = df.get("creator","")
        frames.append(df[["source","title","date","authors","url","summary","abstract"]])

    # PubMed
    pm = OUTDIR / "pubmed_eppley.csv"
    if pm.exists():
        df = pd.read_csv(pm)
        df["source"] = "pubmed"
        df["date"] = df.get("year","")
        df["summary"] = ""
        df = df.rename(columns={"pmid":"pmid","doi":"doi"})
        frames.append(df[["source","title","date","authors","url","doi","pmid","abstract","summary"]])

    # Crossref
    cr = OUTDIR / "crossref_works.csv"
    if cr.exists():
        df = pd.read_csv(cr)
        df["source"] = "crossref"
        df["date"] = df.get("year","")
        df["authors"] = df.get("author_list","")
        df["summary"] = ""
        df["abstract"] = ""
        df = df.rename(columns={"DOI":"doi","URL":"url","container_title":"journal"})
        frames.append(df[["source","title","date","authors","url","doi","abstract","summary"]])

    # OpenAlex
    oa = OUTDIR / "openalex_works.csv"
    if oa.exists():
        df = pd.read_csv(oa)
        df["source"] = "openalex"
        df["date"] = df.get("publication_year","")
        df["authors"] = ""
        df["summary"] = ""
        df["abstract"] = ""
        frames.append(df[["source","title","date","authors","url","doi","abstract","summary"]])

    # YouTube (no transcript yet)
    yt = OUTDIR / "youtube_all.csv"
    if yt.exists():
        df = pd.read_csv(yt)
        df["source"] = "youtube"
        df = df.rename(columns={"publishedAt":"date"})
        df["authors"] = df.get("channelTitle","")
        df["abstract"] = ""
        df["summary"] = ""
        df["doi"] = ""
        frames.append(df[["source","title","date","authors","url","doi","abstract","summary"]])

    if not frames:
        return pd.DataFrame(columns=["source","title","date","authors","url","doi","pmid","abstract","summary"])

    return pd.concat(frames, ignore_index=True).fillna("")

def run():
    exp = _load_expanded()
    df = normalize()
    # attach body_text from expanded cache when available
    df["body_text"] = df["url"].map(lambda u: exp.get(u, ""))
    # stable id
    df["id"] = [
        _hash_id(r.source, r.title, r.date, r.url or r.doi)
        for r in df.itertuples(index=False)
    ]
    cols = ["id","source","title","date","authors","url","doi","pmid","abstract","summary","body_text"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]

    # write JSONL and CSV
    with CORPUS.open("w", encoding="utf-8") as f:
        for rec in df.to_dict(orient="records"):
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    df.to_csv(CSV, index=False)
    print(f"make_corpus: wrote {len(df)} records to {CORPUS} and {CSV}")

if __name__ == "__main__":
    run()
