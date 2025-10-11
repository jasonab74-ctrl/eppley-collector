import os
import csv
import time
from pathlib import Path
import requests

OUT = Path("output/youtube_all.csv")
API_KEY = os.getenv("YT_API_KEY")
SEARCH_Q = 'Barry Eppley'
FIELDS = ["source","videoId","title","channelTitle","publishedAt","url"]

def search_all(query):
    if not API_KEY:
        print("[youtube_all] YT_API_KEY not set; skipping YouTube collection.")
        return []
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": API_KEY,
        "q": query,
        "type": "video",
        "part": "snippet",
        "maxResults": 50,
        "order": "date",
    }
    rows = []
    next_page = None
    pages = 0
    while pages < 5:
        if next_page:
            params["pageToken"] = next_page
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            print(f"[youtube_all] API error {r.status_code}: {r.text}")
            break
        data = r.json()
        for item in data.get("items", []):
            vid = item["id"]["videoId"]
            sn = item["snippet"]
            rows.append({
                "source": "youtube",
                "videoId": vid,
                "title": sn.get("title",""),
                "channelTitle": sn.get("channelTitle",""),
                "publishedAt": sn.get("publishedAt",""),
                "url": f"https://www.youtube.com/watch?v={vid}"
            })
        next_page = data.get("nextPageToken")
        pages += 1
        if not next_page:
            break
        time.sleep(0.2)
    return rows

def run():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = search_all(SEARCH_Q)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    print(f"[youtube_all] wrote {len(rows)} rows to {OUT}")