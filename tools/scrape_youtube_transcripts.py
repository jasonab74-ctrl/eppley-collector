from pathlib import Path
import re, json, time
import pandas as pd

OUTDIR = Path("output/corpus"); OUTDIR.mkdir(parents=True, exist_ok=True)
OUT = OUTDIR / "youtube_transcripts.jsonl"

def collect_video_ids():
    vids=set()
    for p in ["output/youtube_metadata.csv","output/eppley_master.csv"]:
        f=Path(p)
        if not f.exists(): continue
        df=pd.read_csv(f,low_memory=False)
        if "videoId" in df.columns: vids.update(df["videoId"].dropna().astype(str))
        if "url" in df.columns:
            vids.update(df["url"].dropna().astype(str).str.extract(r"v=([A-Za-z0-9_-]{6,})",expand=False).dropna())
    return [v for v in vids if len(v)>=8]

def fetch_transcript(video_id):
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        t=YouTubeTranscriptApi.get_transcript(video_id,languages=["en"])
        return " ".join(seg["text"] for seg in t if seg.get("text"))
    except Exception as e:
        print(f"[yt] skip {video_id}: {e}"); return None

def run():
    vids=collect_video_ids(); wrote=0
    with OUT.open("w",encoding="utf-8") as f:
        for vid in vids:
            txt=fetch_transcript(vid); time.sleep(0.25)
            if not txt or len(txt)<40: continue
            f.write(json.dumps({"id":f"yt:{vid}","source":"youtube","url":f"https://www.youtube.com/watch?v={vid}","text":txt},ensure_ascii=False)+"\n")
            wrote+=1
    print(f"[yt] wrote {wrote} transcripts → {OUT}")

if __name__=="__main__": run()