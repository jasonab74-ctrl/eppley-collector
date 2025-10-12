# tools/scrape_crossref_abstracts.py
import json, time, re
from pathlib import Path
import requests
import pandas as pd

UA = {"User-Agent": "EppleyCollector/1.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"}
ROOT = Path(".")
OUTDIR = ROOT / "output" / "corpus"
OUTDIR.mkdir(parents=True, exist_ok=True)
OUT = OUTDIR / "crossref_abstracts.jsonl"

def normalize_doi(doi: str) -> str:
    if not isinstance(doi, str): return ""
    doi = doi.strip()
    doi = doi.replace("https://doi.org/","").replace("http://doi.org/","").strip()
    return doi

def crossref_abstract(doi: str) -> str | None:
    try:
        r = requests.get(f"https://api.crossref.org/works/{doi}", headers=UA, timeout=25)
        if r.status_code != 200: return None
        msg = r.json().get("message", {})
        abs_html = msg.get("abstract")  # often like "<jats:p>...</jats:p>"
        if not abs_html: return None
        txt = re.sub("<[^<]+?>", " ", abs_html)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt or None
    except Exception:
        return None

def openalex_abstract(doi: str) -> str | None:
    try:
        r = requests.get("https://api.openalex.org/works/https://doi.org/" + doi, headers=UA, timeout=25)
        if r.status_code != 200: return None
        data = r.json()
        idx = data.get("abstract_inverted_index")
        if not idx: return None
        # reconstruct text from inverted index
        words = sorted([(pos, w) for w, poss in idx.items() for pos in poss])
        seq = [w for _, w in words]
        return " ".join(seq)
    except Exception:
        return None

def load_candidates():
    # Prefer the raw crossref CSV if present; fallback to master
    paths = [ROOT/"output"/"crossref_works.csv", ROOT/"output"/"eppley_master.csv"]
    dois = set()
    for p in paths:
        if not p.exists(): continue
        df = pd.read_csv(p, low_memory=False)
        for col in ("DOI","doi"):
            if col in df.columns:
                found = df[col].dropna().astype(str).map(normalize_doi)
                dois.update([d for d in found if d])
    return list(dois)

def run():
    dois = load_candidates()
    wrote = 0
    seen = set()
    with OUT.open("w", encoding="utf-8") as f:
        for i, doi in enumerate(dois):
            if doi in seen: continue
            seen.add(doi)
            text = crossref_abstract(doi) or openalex_abstract(doi)
            time.sleep(0.25)
            if not text or len(text) < 40:
                continue
            rec = {"id": f"doi:{doi}", "source": "crossref/openalex", "title": "", "doi": doi, "url": f"https://doi.org/{doi}", "text": text}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            wrote += 1
            if (i+1) % 200 == 0:
                print(f"[doi] processed {i+1} / {len(dois)}")
    print(f"[crossref/openalex] wrote {wrote} abstracts -> {OUT}")

if __name__ == "__main__":
    run()