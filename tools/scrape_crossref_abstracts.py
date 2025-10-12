import json, time, re
from pathlib import Path
import requests, pandas as pd

UA={"User-Agent":"EppleyCollector/1.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"}
ROOT=Path("."); OUTDIR=ROOT/"output"/"corpus"; OUTDIR.mkdir(parents=True,exist_ok=True)
OUT=OUTDIR/"crossref_abstracts.jsonl"

def normalize_doi(d):
    if not isinstance(d,str): return ""
    return d.replace("https://doi.org/","").replace("http://doi.org/","").strip()

def crossref_abstract(doi):
    try:
        r=requests.get(f"https://api.crossref.org/works/{doi}",headers=UA,timeout=25)
        if r.status_code!=200: return None
        msg=r.json().get("message",{})
        abs_html=msg.get("abstract"); 
        if not abs_html: return None
        return re.sub(r"<[^>]+>"," ",abs_html).strip()
    except: return None

def openalex_abstract(doi):
    try:
        r=requests.get("https://api.openalex.org/works/https://doi.org/"+doi,headers=UA,timeout=25)
        if r.status_code!=200: return None
        idx=r.json().get("abstract_inverted_index")
        if not idx: return None
        words=[(pos,w) for w,poss in idx.items() for pos in poss]
        return " ".join(w for _,w in sorted(words))
    except: return None

def load_dois():
    dois=set()
    for p in ["output/crossref_works.csv","output/eppley_master.csv"]:
        f=Path(p)
        if not f.exists(): continue
        df=pd.read_csv(f,low_memory=False)
        for col in ("DOI","doi"):
            if col in df.columns:
                dois.update(df[col].dropna().astype(str).map(normalize_doi))
    return list(dois)

def run():
    dois=load_dois(); seen=set(); wrote=0
    with OUT.open("w",encoding="utf-8") as f:
        for doi in dois:
            if doi in seen: continue
            seen.add(doi)
            txt=crossref_abstract(doi) or openalex_abstract(doi)
            time.sleep(0.25)
            if not txt or len(txt)<40: continue
            f.write(json.dumps({"id":f"doi:{doi}","source":"crossref/openalex","doi":doi,"url":f"https://doi.org/{doi}","text":txt},ensure_ascii=False)+"\n")
            wrote+=1
    print(f"[cr] wrote {wrote} abstracts → {OUT}")

if __name__=="__main__": run()