import os, time, csv, requests
from pathlib import Path

OUT = Path("output/youtube_all.csv")
API_KEY = os.getenv("YT_API_KEY")  # In Settings → Secrets → Actions
SEARCH_Q = 'Barry Eppley'
FIELDS = ["source","videoId","title","channelTitle","publishedAt","url"]

def search_all(q):
    if not API_KEY:
        return []
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": API_KEY,
        "q": q,
        "type": "video",
        "part": "snippet",
        "maxResults": 50,
        "order": "date",
    }
    rows = []
    next_page = None
    pages = 0
    while pages < 5:  # up to ~250 videos
        if next_page:
            params["pageToken"] = next_page
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            break
        data = r.json()
        for item in data.get("items", []):
            vid = item["id"]["videoId"]
            sn = item["snippet"]
            rows.append({
                "source":"youtube",
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
        for r in rows:
            w.writerow(r)
    print(f"youtube_all: wrote {len(rows)} rows to {OUT}")

if __name__ == "__main__":
    run()
