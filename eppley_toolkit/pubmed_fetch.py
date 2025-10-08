
import os, time, xml.etree.ElementTree as ET, requests
from .utils import write_jsonl, write_csv

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

def esearch(author_query, retmax=500, email="", api_key=""):
    params = {
        "db": "pubmed",
        "term": author_query,
        "retmax": retmax,
        "retmode": "xml",
        "email": email or ""
    }
    if api_key:
        params["api_key"] = api_key
    r = requests.get(f"{EUTILS}/esearch.fcgi", params=params, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    ids = [e.text for e in root.findall(".//Id")]
    return ids

def efetch(pmids, email="", api_key=""):
    if not pmids:
        return ""
    params = {"db": "pubmed", "id": ",".join(pmids), "retmode": "xml", "email": email or ""}
    if api_key:
        params["api_key"] = api_key
    r = requests.get(f"{EUTILS}/efetch.fcgi", params=params, timeout=60)
    r.raise_for_status()
    return r.text

def parse_pubmed_xml(xml_text):
    # Very light-weight parse for key fields
    root = ET.fromstring(xml_text)
    ns = {}
    rows = []
    for art in root.findall(".//PubmedArticle"):
        pmid = (art.findtext(".//PMID") or "").strip()
        title = (art.findtext(".//ArticleTitle") or "").strip()
        abstract = " ".join([(abst.text or "").strip() for abst in art.findall(".//Abstract/AbstractText")])
        journal = (art.findtext(".//Journal/Title") or "").strip()
        year = (art.findtext(".//Journal/JournalIssue/PubDate/Year") or "").strip()
        doi = ""
        for idn in art.findall(".//ArticleIdList/ArticleId"):
            if idn.get("IdType","").lower() == "doi":
                doi = (idn.text or "").strip()
                break
        rows.append({
            "pmid": pmid,
            "title": title,
            "abstract": abstract,
            "journal": journal,
            "year": year,
            "doi": doi
        })
    return rows

def run_from_config(cfg):
    outdir = cfg["general"]["output_dir"]
    all_rows = []
    for q in cfg["pubmed"]["author_queries"]:
        ids = esearch(q, retmax=int(cfg["pubmed"]["retmax"]), email=cfg["pubmed"]["email"], api_key=cfg["pubmed"]["api_key"])
        xmltxt = efetch(ids, email=cfg["pubmed"]["email"], api_key=cfg["pubmed"]["api_key"])
        rows = parse_pubmed_xml(xmltxt)
        all_rows.extend(rows)
        time.sleep(0.34)  # be polite
    jsonl_path = os.path.join(outdir, "pubmed_eppley.jsonl")
    csv_path = os.path.join(outdir, "pubmed_eppley.csv")
    write_jsonl(all_rows, jsonl_path)
    write_csv(all_rows, csv_path)
    return {"count": len(all_rows), "jsonl": jsonl_path, "csv": csv_path}
