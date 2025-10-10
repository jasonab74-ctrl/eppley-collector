#!/usr/bin/env python3
"""
Augment PubMed rows with real abstracts using NCBI E-utilities.
Writes: output/pubmed_eppley_with_abstracts.csv
"""
import csv, json, os, re, time, pathlib, requests
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

SRC_JSONL = OUT / "pubmed_eppley.jsonl"
SRC_CSV   = OUT / "pubmed_eppley.csv"
DST_CSV   = OUT / "pubmed_eppley_with_abstracts.csv"

EMAIL = os.getenv("EMAIL", "").strip() or "unknown@example.com"
TOOL  = os.getenv("NCBI_TOOL", "eppley-collector")

PMID_RE = re.compile(r"/(\d{5,})/?$")

def _pmid(row):
    if row.get("pmid"):
        return str(row["pmid"]).strip()
    url = row.get("url","")
    m = PMID_RE.search(url)
    return m.group(1) if m else None

def _efetch(pmid):
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {"db":"pubmed","id":pmid,"retmode":"xml","tool":TOOL,"email":EMAIL}
    for _ in range(3):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return r.text
        time.sleep(1.0)
    return ""

def _abstract(xml_text):
    try:
        root = ET.fromstring(xml_text)
        parts = []
        for at in root.findall(".//Abstract/AbstractText"):
            label = at.attrib.get("Label")
            text = (at.text or "").strip()
            parts.append(f"{label}: {text}" if label else text)
        return "\n\n".join([p for p in parts if p]).strip()
    except ET.ParseError:
        return ""

def _read_rows():
    rows=[]
    if SRC_JSONL.exists():
        for line in SRC_JSONL.read_text(encoding="utf-8").splitlines():
            if not line.strip(): continue
            j = json.loads(line)
            rows.append({
                "pmid": j.get("pmid",""), "title": j.get("title",""),
                "abstract": j.get("abstract",""), "journal": j.get("journal",""),
                "year": j.get("year",""), "authors": j.get("authors",""),
                "doi": j.get("doi",""), "url": j.get("url","")
            })
    elif SRC_CSV.exists():
        with SRC_CSV.open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                row.setdefault("abstract","")
                rows.append(row)
    return rows

def main():
    rows = _read_rows()
    if not rows:
        print("No PubMed source found; nothing to enrich.")
        return 0

    hits = 0
    for i,row in enumerate(rows, 1):
        if row.get("abstract"): continue
        pmid = _pmid(row)
        if not pmid: continue
        xml = _efetch(pmid)
        time.sleep(0.35)
        abs_text = _abstract(xml) if xml else ""
        if abs_text:
            row["abstract"] = abs_text
            hits += 1
        if i % 25 == 0:
            print(f"[ENRICH] {i}/{len(rows)} rows, abstracts found: {hits}")

    with DST_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["pmid","title","abstract","journal","year","authors","doi","url"])
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k,"") for k in ["pmid","title","abstract","journal","year","authors","doi","url"]})

    print(f"Wrote {DST_CSV} • {len(rows)} rows • {hits} abstracts added")
    return 0

if __name__ == "__main__":
    import json
    main()
