import os
import csv
import time
from pathlib import Path
import requests
from xml.etree import ElementTree as ET

OUT = Path("output/pubmed_eppley.csv")
BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
EMAIL = os.getenv("NCBI_EMAIL")
FIELDS = ["source","pmid","title","journal","year","authors","doi","abstract","url"]
HEADERS = {"User-Agent": f"EppleyCollector/1.0 (mailto:{EMAIL or 'unknown@example.com'})"}

def esearch(term: str):
    params = {
        "db": "pubmed",
        "term": term,
        "retmax": "10000",
        "retmode": "json",
        "email": EMAIL or "unknown@example.com"
    }
    r = requests.get(f"{BASE}/esearch.fcgi", params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("esearchresult", {}).get("idlist") or []

def efetch(pmids):
    rows = []
    for i in range(0, len(pmids), 200):
        chunk = pmids[i:i+200]
        params = {
            "db":"pubmed",
            "retmode":"xml",
            "id": ",".join(chunk),
            "email": EMAIL or "unknown@example.com"
        }
        r = requests.get(f"{BASE}/efetch.fcgi", params=params, headers=HEADERS, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for art in root.findall(".//PubmedArticle"):
            pmid = (art.findtext(".//PMID") or "").strip()
            title = (art.findtext(".//ArticleTitle") or "").strip()
            journal = (art.findtext(".//Journal/Title") or "").strip()
            year = (art.findtext(".//PubDate/Year") or art.findtext(".//ArticleDate/Year") or "").strip()
            abstract = " ".join((t.text or "") for t in art.findall(".//Abstract/AbstractText")).strip()
            authors = []
            for au in art.findall(".//Author"):
                last = au.findtext("LastName") or ""
                fore = au.findtext("ForeName") or ""
                coll = au.findtext("CollectiveName") or ""
                if coll:
                    authors.append(coll.strip())
                else:
                    nm = " ".join(filter(None,[fore, last])).strip()
                    if nm:
                        authors.append(nm)
            doi = ""
            for idn in art.findall(".//ArticleId"):
                if idn.attrib.get("IdType") == "doi":
                    doi = (idn.text or "").strip()
                    break
            url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else ""
            rows.append({
                "source":"pubmed",
                "pmid": pmid,
                "title": title,
                "journal": journal,
                "year": year,
                "authors": "; ".join(authors),
                "doi": doi,
                "abstract": abstract,
                "url": url,
            })
        time.sleep(0.35)
    return rows

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    if not EMAIL:
        print("[pubmed_eppley] NCBI_EMAIL environment variable not set; skipping PubMed collection.")
        with OUT.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writeheader()
        return
    try:
        ids = esearch('(Eppley B[Author]) OR ("Barry M Eppley"[Author]) OR ("Eppley"[Author] AND plastic*[Affiliation])')
        rows = efetch(ids)
    except Exception as e:
        print(f"[pubmed_eppley] error: {e}")
        rows = []
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    print(f"[pubmed_eppley] wrote {len(rows)} rows to {OUT}")