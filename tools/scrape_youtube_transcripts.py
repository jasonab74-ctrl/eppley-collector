# tools/scrape_youtube_transcripts.py
from pathlib import Path
import re, json, time
import pandas as pd

OUTDIR = Path("output/corpus")
OUTDIR.mkdir(parents=True, exist_ok=True)
OUT = OUTDIR / "youtube_transcripts.jsonl"

def collect_video_ids():
    vids = set()
    ycsv = Path("output/youtube_metadata.csv")
    if ycsv.exists():
        df = pd.read_csv(ycsv)
        if "videoId" in df.columns:
            vids.update(df["videoId"].dropna().astype(str))
        if "url" in df.columns:
            vids.update(df["url"].dropna().astype(str).str.extract(r"v=([A-Za-z0-9_-]{6,})", expand=False).dropna())
    master = Path("output/eppley_master.csv")
    if master.exists():
        dfm = pd.read_csv(master, low_memory=False)
        if "videoId" in dfm.columns:
            vids.update(dfm["videoId"].dropna().astype(str))
        if "url" in dfm.columns:
            vids.update(dfm["url"].dropna().astype(str).str.extract(r"v=([A-Za-z0-9_-]{6,})", expand=False).dropna())
    return [v for v in vids if isinstance(v, str) and len(v) >= 8]

def fetch_transcript(video_id):
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=["en"])
        text = " ".join([seg["text"] for seg in transcript if seg.get("text")])
        return text.strip()
    except Exception as e:
        print(f"[yt] no transcript for {video_id}: {e}")
        return None

def run():
    vids = collect_video_ids()
    wrote = 0
    with OUT.open("w", encoding="utf-8") as f:
        for vid in vids:
            txt = fetch_transcript(vid)
            time.sleep(0.25)
            if not txt or len(txt) < 40:
                continue
            url = f"https://www.youtube.com/watch?v={vid}"
            rec = {"id": f"yt:{vid}", "source": "youtube", "title": "", "url": url, "text": txt}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            wrote += 1
    print(f"[yt] wrote {wrote} transcripts -> {OUT}")

if __name__ == "__main__":
    run()