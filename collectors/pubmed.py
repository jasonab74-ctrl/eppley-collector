"""
collectors/pubmed.py
--------------------

Production PubMed collector using NCBI E-utilities.

Approach:
1) Build a robust author/affiliation query for Dr. Barry Eppley.
2) Use ESearch (JSON) to collect all PMIDs (with history params).
3) Fetch records in batches via EFetch (XML) to get abstracts + metadata.
4) Write to output/pubmed_eppley.csv with these columns:
   pmid, title, abstract, journal, year, authors, doi, url

Politeness:
- Include a descriptive User-Agent and email param per NCBI policies.
- Throttle requests with small sleeps; batch PMIDs to reduce calls.
"""

import csv
import os
import time
import re
from typing import Dict, List, Tuple, Optional
import requests
from xml.etree import ElementTree as ET

BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
UA = "EppleyCollector/1.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"
EMAIL = os.getenv("NCBI_EMAIL", "you@example.com")  # set your email for compliance

HEADERS = {"User-Agent": UA}
REQUEST_TIMEOUT = 30
SLEEP = 0.34  # polite delay between calls
BATCH = 200   # PMIDs per fetch

# Author search variants
AUTHOR_TERMS = [
    '"Eppley BL"[Author]',
    '"Barry L Eppley"[Author]',
    '"Barry L. Eppley"[Author]',
    '"Eppley B"[Author]',
]

# Optional affiliation hint
AFFIL_TERMS = [
    '("craniofacial"[All Fields] OR "plastic surgery"[All Fields])'
]

# Final query: (author1 OR author2 OR ...) AND (affil-hint)
QUERY = "(" + " OR ".join(AUTHOR_TERMS) + ")" + " AND " + "(" + " OR ".join(AFFIL_TERMS) + ")"


def _esearch(query: str) -> Tuple[List[str], Dict[str, str]]:
    """Return (pmids, history dict) using ESearch with WebEnv/QueryKey."""
    params = {
        "db": "pubmed",
        "term": query,
        "retmax": "0",
        "usehistory": "y",
        "retmode": "json",
        "email": EMAIL,
        "tool": "eppley_collector",
    }
    r = requests.get(BASE + "esearch.fcgi", params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    js = r.json()
    count = int(js["esearchresult"]["count"])
    webenv = js["esearchresult"]["webenv"]
    query_key = js["esearchresult"]["querykey"]

    pmids: List[str] = []
    # pull PMIDs in chunks of 10k using the history
    for retstart in range(0, count, 10000):
        params = {
            "db": "pubmed",
            "query_key": query_key,
            "WebEnv": webenv,
            "retstart": str(retstart),
            "retmax": "10000",
            "retmode": "json",
            "email": EMAIL,
            "tool": "eppley_collector",
        }
        rr = requests.get(BASE + "esearch.fcgi", params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        rr.raise_for_status()
        j = rr.json()
        pmids.extend(j["esearchresult"].get("idlist", []))
        time.sleep(SLEEP)

    return pmids, {"WebEnv": webenv, "query_key": query_key}


def _efetch_xml(pmids: List[str]) -> ET.Element:
    """Fetch PubMed XML for a batch of PMIDs."""
    ids = ",".join(pmids)
    params = {
        "db": "pubmed",
        "id": ids,
        "retmode": "xml",
        "email": EMAIL,
        "tool": "eppley_collector",
    }
    r = requests.get(BASE + "efetch.fcgi", params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return ET.fromstring(r.text)


def _text_or(el: Optional[ET.Element], default: str = "") -> str:
    return (el.text or "").strip() if el is not None else default


def _extract_article(article: ET.Element) -> Dict[str, str]:
    """Extract fields from a single PubmedArticle XML node."""
    # Title
    title = _text_or(article.find(".//ArticleTitle"))

    # Abstract: join multiple <AbstractText> nodes
    abstract_parts = []
    for ab in article.findall(".//Abstract/AbstractText"):
        txt = "".join(ab.itertext()).strip()
        if txt:
            # If the node has a label (e.g., BACKGROUND), prepend it
            label = ab.attrib.get("Label") or ab.attrib.get("NlmCategory")
            if label:
                abstract_parts.append(f"{label}: {txt}")
            else:
                abstract_parts.append(txt)
    abstract = " ".join(abstract_parts)

    # Journal & year
    journal = _text_or(article.find(".//Journal/Title"))
    year = _text_or(article.find(".//JournalIssue/PubDate/Year"))
    if not year:
        # Sometimes only MedlineDate is present (e.g., "1998 Jan-Feb")
        md = _text_or(article.find(".//JournalIssue/PubDate/MedlineDate"))
        if md:
            m = re.match(r"(\d{4})", md)
            if m:
                year = m.group(1)

    # Authors: LastName Initials; ...
    authors = []
    for au in article.findall(".//AuthorList/Author"):
        last = _text_or(au.find("LastName"))
        init = _text_or(au.find("Initials"))
        collab = _text_or(au.find("CollectiveName"))
        if collab:
            authors.append(collab)
        elif last:
            if init:
                authors.append(f"{last} {init}")
            else:
                authors.append(last)
    authors_csv = "; ".join(authors)

    # DOI
    doi = ""
    for iden in article.findall(".//ArticleIdList/ArticleId"):
        if (iden.attrib.get("IdType") or "").lower() == "doi":
            doi = (iden.text or "").strip()
            break

    # PMID and URL
    pmid = _text_or(article.find(".//PMID"))
    url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else ""

    return {
        "pmid": pmid,
        "title": title,
        "abstract": abstract,
        "journal": journal,
        "year": year,
        "authors": authors_csv,
        "doi": doi,
        "url": url,
    }


def run_pubmed(out_path: str = "output/pubmed_eppley.csv") -> int:
    """
    Execute the PubMed harvest and write to CSV with fields:
    pmid, title, abstract, journal, year, authors, doi, url
    Returns the number of rows (excluding header).
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fieldnames = ["pmid", "title", "abstract", "journal", "year", "authors", "doi", "url"]

    total = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        try:
            pmids, _hist = _esearch(QUERY)
        except Exception as e:
            print(f"[pubmed] esearch failed: {e}")
            return 0

        print(f"[pubmed] found {len(pmids)} PMIDs")
        for i in range(0, len(pmids), BATCH):
            batch = pmids[i:i+BATCH]
            try:
                root = _efetch_xml(batch)
            except Exception as e:
                print(f"[pubmed] efetch failed on batch starting {i}: {e}")
                time.sleep(1.5)
                continue

            for art in root.findall(".//PubmedArticle"):
                row = _extract_article(art)
                if row.get("pmid"):
                    w.writerow(row)
                    total += 1

            time.sleep(SLEEP)

    print(f"[pubmed] wrote {total} rows -> {out_path}")
    return total


if __name__ == "__main__":
    run_pubmed()
