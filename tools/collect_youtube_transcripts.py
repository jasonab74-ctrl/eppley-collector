#!/usr/bin/env python3
"""
YouTube Transcript Collector (robust)
- Discovers videos via:
    1) existing output/youtube_metadata.csv (if present)
    2) fallback: YouTube Data API channel search (if YT_API_KEY provided)
- Fetches transcripts via youtube-transcript-api (no API key needed) with English preference
- Outputs:
    * output/youtube_transcripts.csv
    * output/corpus/youtube_transcripts.jsonl
"""

from pathlib import Path
import os, re, csv, json, time
import requests
import pandas as pd

OUTDIR = Path("output")
CORPUS = OUTDIR / "corpus"
OUTDIR.mkdir(parents=True, exist_ok=True)
CORPUS.mkdir(parents=True, exist_ok=True)

CSV_OUT = OUTDIR / "youtube_transcripts.csv"
JSL_OUT = CORPUS / "youtube_transcripts.jsonl"

YT_API_KEY = os.environ.get("YT_API_KEY", "").strip()

def from_existing_metadata() -> set:
    vids = set()
    meta_paths = [OUTDIR / "youtube_metadata.csv", OUTDIR / "eppley_master.csv"]
    for p in meta_paths:
        if not p.exists(): continue
        try:
            df = pd.read_csv(p, low_memory=False)
        except Exception:
            continue
        if "videoId" in df.columns:
            vids.update(df["videoId"].dropna().astype(str))
        if "url" in df.columns:
            ids = df["url"].dropna().astype(str).str.extract(r"v=([A-Za-z0-9_-]{6,})", expand=False)
            vids.update(ids.dropna().astype(str))
    return {v for v in vids if len(v) >= 8}

def from_youtube_api(channel_id: str, max_pages: int = 5) -> set:
    vids = set()
    if not YT_API_KEY: return vids
    base = "https://www.googleapis.com/youtube/v3/search"
    page = None
    for _ in range(max_pages):
        params = {
            "part": "id",
            "channelId": channel_id,
            "order": "date",
            "maxResults": 50,
            "type": "video",
            "key": YT_API_KEY
        }
        if page: params["pageToken"] = page
        r = requests.get(base, params=params, timeout=25)
        if r.status_code != 200:
            break
        data = r.json()
        for item in data.get("items", []):
            vid = item.get("id", {}).get("videoId")
            if vid: vids.add(vid)
        page = data.get("nextPageToken")
        if not page: break
        time.sleep(0.25)
    return vids

def guess_eppley_channels() -> list:
    # Known official channel (can be extended if you’ve got more)
    return ["UCwGQ0k1N3uu6fY0yY1wF9yQ"]  # replace/extend if needed

def fetch_transcript(video_id: str) -> str | None:
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        t = YouTubeTranscriptApi.get_transcript(video_id, languages=["en"])
        return " ".join(seg["text"] for seg in t if seg.get("text"))
    except Exception as e:
        print(f"[yt] {video_id} transcript not available: {e}")
        return None

def main():
    # 1) Collect video IDs from metadata
    vids = from_existing_metadata()

    # 2) Fallback: discover from channel API if no/low coverage
    if len(vids) < 10:
        for ch in guess_eppley_channels():
            vids.update(from_youtube_api(ch))

    vids = sorted(vids)
    print(f"[yt] candidate videos: {len(vids)}")

    # 3) Fetch transcripts
    rows = []
    jsl = []
    for i, vid in enumerate(vids, 1):
        url = f"https://www.youtube.com/watch?v={vid}"
        txt = fetch_transcript(vid)
        time.sleep(0.25)
        if not txt or len(txt.strip().split()) < 40:
            continue
        rows.append([vid, url, txt])
        jsl.append({"id": f"yt:{vid}", "source": "youtube", "url": url, "text": txt})
        print(f"[yt] ok {i}/{len(vids)} {vid} ({len(txt)} chars)")

    # 4) Write outputs
    with CSV_OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["videoId","url","transcript"])
        w.writerows(rows)

    with JSL_OUT.open("w", encoding="utf-8") as f:
        for rec in jsl:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"[done] youtube transcripts -> {CSV_OUT} / {JSL_OUT} (kept {len(rows)} transcripts)")

if __name__ == "__main__":
    main()