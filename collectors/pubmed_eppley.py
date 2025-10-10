import os, time, csv, json, urllib.parse, requests
from xml.etree import ElementTree as ET
from pathlib import Path

OUT = Path("output/pubmed_eppley.csv")
EMAIL = os.getenv("NCBI_EMAIL")  # MUST be set in Actions → Variables
BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

HEADERS = {"User-Agent": f"EppleyCollector/1.0 (mailto:{EMAIL or 'unknown@example.com'})"}

def esearch(term: str, retmax: int = 10000):
    params = {
        "db": "pubmed",
        "term": term,
        "retmax": str(retmax),
        "retmode": "json",
    }
    r = requests.get(f"{BASE}/esearch.fcgi", params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("esearchresult", {}).get("idlist", [])

def efetch(pmids):
    if not pmids:
        return []
    # PubMed likes batches <= 200
    rows = []
    for i in range(0, len(pmids), 200):
        chunk = pmids[i:i+200]
        params = {
            "db": "pubmed",
            "retmode": "xml",
            "id": ",".join(chunk),
        }
        r = requests.get(f"{BASE}/efetch.fcgi", params=params, headers=HEADERS, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for art in root.findall(".//PubmedArticle"):
            try:
                pmid = (art.findtext(".//PMID") or "").strip()
                title = (art.findtext(".//ArticleTitle") or "").strip()
                journal = (art.findtext(".//Journal/Title") or "").strip()
                year = (art.findtext(".//PubDate/Year") or
                        art.findtext(".//ArticleDate/Year") or
                        "").strip()
                abstract = " ".join(t.text or "" for t in art.findall(".//Abstract/AbstractText")).strip()
                authors = []
                for au in art.findall(".//Author"):
                    last = au.findtext("LastName") or ""
                    fore = au.findtext("ForeName") or ""
                    coll = au.findtext("CollectiveName") or ""
                    if coll:
                        authors.append(coll)
                    else:
                        nm = " ".join([fore, last]).strip()
                        if nm:
                            authors.append(nm)
                doi = ""
                for idn in art.findall(".//ArticleId"):
                    if idn.attrib.get("IdType") == "doi":
                        doi = (idn.text or "").strip()
                        break
                url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else ""
                rows.append({
                    "source": "pubmed",
                    "pmid": pmid,
                    "title": title,
                    "journal": journal,
                    "year": year,
                    "authors": "; ".join(authors),
                    "doi": doi,
                    "abstract": abstract,
                    "url": url,
                })
            except Exception:
                continue
        time.sleep(0.34)  # ~3 req/s courtesy delay
    return rows

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    if not EMAIL:
        # Fail gracefully but clearly—no email means no PubMed
        with OUT.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "source","pmid","title","journal","year","authors","doi","abstract","url"
            ])
            w.writeheader()
        print("pubmed_eppley: NCBI_EMAIL missing; wrote headers only.")
        return

    # Author search that’s tolerant to variants
    term = '(Eppley B[Author]) OR ("Barry M Eppley"[Author]) OR ("Eppley"[Author] AND plastic*[Affiliation])'
    pmids = esearch(term)
    rows = efetch(pmids)

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "source","pmid","title","journal","year","authors","doi","abstract","url"
        ])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"pubmed_eppley: wrote {len(rows)} rows to {OUT}")

if __name__ == "__main__":
    run()
