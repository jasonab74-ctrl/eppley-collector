#!/usr/bin/env python3
"""
Augment output/pubmed_eppley.(jsonl|csv) with PubMed abstracts.

- Reads pubmed_eppley.jsonl (preferred) or pubmed_eppley.csv
- For each PMID, calls NCBI E-utilities (efetch, XML) to extract <AbstractText>
- Writes output/pubmed_eppley_with_abstracts.csv (same columns + 'abstract')
- Idempotent: skips rows that already have an abstract, retries transient HTTP errors
- Respects NCBI rate guidance (<= 3 req/sec). Set EMAIL env if you can.
"""

import csv, json, os, re, time, pathlib, typing
import xml.etree.ElementTree as ET
import urllib.parse
import requests

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "output"

PUBMED_JSONL = OUT / "pubmed_eppley.jsonl"
PUBMED_CSV   = OUT / "pubmed_eppley.csv"
ENRICHED_CSV = OUT / "pubmed_eppley_with_abstracts.csv"

NCBI_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
EMAIL = os.environ.get("EMAIL", "you@example.com")  # please set via Actions secrets/vars
TOOL  = os.environ.get("NCBI_TOOL", "eppley-collector")

PMID_RE = re.compile(r"/(\d{5,})/?$")  # last path segment in https://pubmed.ncbi.nlm.nih.gov/<pmid>/

def _pmid_from_row(row: dict) -> typing.Optional[str]:
    if "pmid" in row and row["pmid"]:
        return str(row["pmid"]).strip()
    url = row.get("url","")
    m = PMID_RE.search(url)
    return m.group(1) if m else None

def _efetch_xml(pmid: str) -> typing.Optional[str]:
    """Return raw XML string for a PMID, or None."""
    params = {
        "db": "pubmed",
        "id": pmid,
        "retmode": "xml",
        "tool": TOOL,
        "email": EMAIL,
    }
    for attempt in range(3):
        try:
            r = requests.get(NCBI_BASE, params=params, timeout=20)
            if r.status_code == 200:
                return r.text
            time.sleep(1.5)
        except requests.RequestException:
            time.sleep(1.5)
    return None

def _abstract_from_xml(xml_text: str) -> str:
    """Extract concatenated AbstractText from efetch XML."""
    try:
        root = ET.fromstring(xml_text)
        # PubmedArticleSet/PubmedArticle/MedlineCitation/Article/Abstract/AbstractText
        texts = []
        for at in root.findall(".//Abstract/AbstractText"):
            t = (at.text or "").strip()
            label = at.attrib.get("Label")
            if label:
                t = f"{label}: {t}" if t else label
            if t:
                texts.append(t)
        return "\n\n".join(texts).strip()
    except ET.ParseError:
        return ""

def _read_input_rows():
    if PUBMED_JSONL.exists():
        for line in PUBMED_JSONL.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            j = json.loads(line)
            yield {
                "pmid": j.get("pmid",""),
                "title": j.get("title",""),
                "journal": j.get("journal",""),
                "year": j.get("year",""),
                "authors": j.get("authors",""),
                "doi": j.get("doi",""),
                "url": j.get("url",""),
                "abstract": j.get("abstract",""),
            }
    elif PUBMED_CSV.exists():
        with PUBMED_CSV.open(encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                row.setdefault("abstract", "")
                yield row
    else:
        return

def main():
    rows = list(_read_input_rows())
    if not rows:
        print("No pubmed input found in output/. Nothing to do.")
        return 0

    # Enrich
    total = len(rows); hits = 0
    for i,row in enumerate(rows, 1):
        if row.get("abstract"):
            continue
        pmid = _pmid_from_row(row)
        if not pmid:
            continue
        xml = _efetch_xml(pmid)
        time.sleep(0.4)  # be nice to NCBI
        if not xml:
            continue
        abs_text = _abstract_from_xml(xml)
        if abs_text:
            row["abstract"] = abs_text
            hits += 1
        if i % 25 == 0:
            print(f"[{i}/{total}] enriched so far: {hits}")

    # Write CSV
    ENRICHED_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["pmid","title","abstract","journal","year","authors","doi","url"]
    with ENRICHED_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            safe = {k: (row.get(k,"") or "") for k in fieldnames}
            w.writerow(safe)

    print(f"Wrote {ENRICHED_CSV} ({len(rows)} rows, {hits} with abstracts)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
