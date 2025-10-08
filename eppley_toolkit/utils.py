
import time, re, csv, json, os, sys, hashlib
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as dateparser
import requests

def polite_get(url, session, delay_seconds=1.0, headers=None):
    time.sleep(delay_seconds)
    resp = session.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp

def text_normalize(s):
    return re.sub(r'\s+', ' ', s or '').strip()

def guess_date_from_html(soup):
    # Common WP date selectors
    for sel in [
        "time.entry-date", "time.published", "meta[property='article:published_time']",
        "span.posted-on time", "span.date", "time[datetime]"
    ]:
        el = soup.select_one(sel)
        if el:
            if el.has_attr("datetime"):
                try:
                    return dateparser.parse(el["datetime"]).isoformat()
                except Exception:
                    pass
            try:
                return dateparser.parse(el.get_text(" ", strip=True)).isoformat()
            except Exception:
                pass
    return None

def hash_id(s):
    return hashlib.sha1(s.encode('utf-8')).hexdigest()[:16]

def write_jsonl(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_csv(rows, path, fieldnames=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        open(path, "w").write("")
        return
    if fieldnames is None:
        # union of keys
        keys = set()
        for r in rows:
            keys.update(r.keys())
        fieldnames = sorted(keys)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
