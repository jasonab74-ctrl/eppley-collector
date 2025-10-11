import csv, time
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
import requests

OUT = Path("output")
OUT.mkdir(parents=True, exist_ok=True)
UA = {"User-Agent": "EppleyCollector/1.0 (mailto:site@eppley.example)"}

def collect_wordpress():
    url = "https://www.exploreplasticsurgery.com/feed/"
    dest = OUT / "wordpress_posts.csv"
    fields = ["source","title","link","pub_date","author","categories","summary"]
    rows = []
    try:
        r = requests.get(url, timeout=30, headers=UA)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        channel = root.find("channel")
        for it in channel.findall("item") if channel is not None else []:
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            pub_date = (it.findtext("pubDate") or "").strip()
            author = (it.findtext("{http://purl.org/dc/elements/1.1/}creator") or "").strip()
            desc = (it.findtext("description") or "").strip()
            cats = [c.text.strip() for c in it.findall("category") if c.text]
            rows.append({
                "source":"wordpress",
                "title":title, "link":link, "pub_date":pub_date,
                "author":author, "categories":"; ".join(cats), "summary":desc
            })
    except Exception as e:
        print(f"[wordpress] ERROR: {e}")
    with dest.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for row in rows: w.writerow(row)
    print(f"[wordpress] wrote {len(rows)} rows -> {dest}")

def collect_crossref():
    base = "https://api.crossref.org/works"
    dest = OUT / "crossref_works.csv"
    fields = ["source","title","year","journal","type","DOI","URL","authors"]
    rows, cursor = [], "*"
    try:
        while True:
            params = {"query.author":"Barry Eppley","rows":200,"cursor":cursor,"mailto":"site@eppley.example"}
            r = requests.get(base, params=params, headers=UA, timeout=60)
            r.raise_for_status()
            data = r.json()
            items = data.get("message", {}).get("items", [])
            for it in items:
                authors = []
                for a in (it.get("author") or []):
                    nm = " ".join([a.get("given",""), a.get("family","")]).strip()
                    if nm: authors.append(nm)
                rows.append({
                    "source":"crossref",
                    "title": (it.get("title",[""])[0] or "").strip(),
                    "year": (it.get("issued",{}).get("date-parts",[[None]])[0][0]),
                    "journal": (it.get("container-title",[""])[0] or ""),
                    "type": it.get("type",""),
                    "DOI": it.get("DOI",""),
                    "URL": it.get("URL",""),
                    "authors": "; ".join(authors),
                })
            nxt = data.get("message", {}).get("next-cursor")
            if not nxt or len(items) < 200: break
            cursor = nxt
            time.sleep(0.25)
    except Exception as e:
        print(f"[crossref] ERROR: {e}")
    with dest.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for row in rows: w.writerow(row)
    print(f"[crossref] wrote {len(rows)} rows -> {dest}")

def collect_openalex():
    base = "https://api.openalex.org"
    dest = OUT / "openalex_works.csv"
    fields = ["source","openalex_id","title","publication_year","host_venue","type","cited_by_count","doi","url"]
    rows = []
    try:
        r = requests.get(f"{base}/authors", params={"search":"Barry Eppley","per_page":1}, headers=UA, timeout=30)
        r.raise_for_status()
        aid = (r.json().get("results") or [{}])[0].get("id")
        if not aid:
            print("[openalex] author not found")
            aid = None
        page = 1
        while aid:
            params = {"filter":f"authorships.author.id:{aid}","per_page":200,"page":page,"sort":"publication_year:desc"}
            r = requests.get(f"{base}/works", params=params, headers=UA, timeout=60)
            r.raise_for_status()
            data = r.json()
            items = data.get("results", []) or []
            for w in items:
                rows.append({
                    "source":"openalex",
                    "openalex_id": w.get("id",""),
                    "title": (w.get("title") or "").strip(),
                    "publication_year": w.get("publication_year",""),
                    "host_venue": (w.get("host_venue",{}) or {}).get("display_name",""),
                    "type": w.get("type",""),
                    "cited_by_count": w.get("cited_by_count",0),
                    "doi": (w.get("doi") or "").replace("https://doi.org/",""),
                    "url": (w.get("primary_location",{}) or {}).get("source",{}).get("url","") or w.get("landing_page_url",""),
                })
            if len(items) < 200: break
            page += 1; time.sleep(0.2)
    except Exception as e:
        print(f"[openalex] ERROR: {e}")
    with dest.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for row in rows: w.writerow(row)
    print(f"[openalex] wrote {len(rows)} rows -> {dest}")

def merge_master():
    dest = OUT / "eppley_master.csv"
    frames = []
    for p in OUT.glob("*.csv"):
        try:
            df = pd.read_csv(p)
            df["__file"] = p.name
            frames.append(df)
        except Exception as e:
            print(f"[merge] skip {p.name}: {e}")
    m = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    m.to_csv(dest, index=False)
    print(f"[merge] wrote {dest} ({len(m)} rows)")

if __name__ == "__main__":
    collect_wordpress()
    collect_crossref()
    collect_openalex()
    merge_master()