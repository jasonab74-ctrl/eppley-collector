
import os, json, subprocess, tempfile, requests
from .utils import write_jsonl, write_csv

YOUTUBE_API = "https://www.googleapis.com/youtube/v3"

def fetch_via_api(channel_id, api_key, max_results=200):
    # Get uploads playlist id
    ch = requests.get(f"{YOUTUBE_API}/channels", params={"id": channel_id, "part": "contentDetails", "key": api_key}, timeout=30)
    ch.raise_for_status()
    items = ch.json().get("items", [])
    if not items:
        return []
    uploads_pl = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    # Walk playlistItems
    results = []
    page_token = None
    while True:
        params = {
            "playlistId": uploads_pl,
            "part": "snippet,contentDetails",
            "maxResults": 50,
            "key": api_key
        }
        if page_token:
            params["pageToken"] = page_token
        r = requests.get(f"{YOUTUBE_API}/playlistItems", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for it in data.get("items", []):
            sn = it["snippet"]
            results.append({
                "videoId": it["contentDetails"]["videoId"],
                "publishedAt": sn.get("publishedAt",""),
                "title": sn.get("title",""),
                "description": sn.get("description",""),
                "channelId": sn.get("channelId",""),
                "channelTitle": sn.get("channelTitle","")
            })
        page_token = data.get("nextPageToken")
        if not page_token or len(results) >= max_results:
            break
    return results

def fetch_via_ytdlp(channel_url):
    # Use yt-dlp to dump metadata as JSON, no downloads.
    # Requires yt-dlp installed (in requirements).
    cmd = ["yt-dlp", "--dump-json", "--flat-playlist", channel_url]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr)
    rows = []
    for line in proc.stdout.splitlines():
        try:
            obj = json.loads(line)
            rows.append({
                "videoId": obj.get("id",""),
                "title": obj.get("title",""),
                "url": obj.get("url") or f"https://www.youtube.com/watch?v={obj.get('id','')}",
                "uploader": obj.get("uploader",""),
                "channel": obj.get("channel",""),
                "webpage_url": obj.get("webpage_url",""),
            })
        except json.JSONDecodeError:
            continue
    return rows

def run_from_config(cfg):
    outdir = cfg["general"]["output_dir"]
    rows = []
    if cfg["youtube"].get("use_yt_dlp") and cfg["youtube"].get("channel_urls"):
        for url in cfg["youtube"]["channel_urls"]:
            try:
                rows.extend(fetch_via_ytdlp(url))
            except Exception as e:
                pass
    if cfg["youtube"].get("use_youtube_api") and cfg["youtube"].get("channel_ids") and os.environ.get("YT_API_KEY"):
        for cid in cfg["youtube"]["channel_ids"]:
            rows.extend(fetch_via_api(cid, os.environ["YT_API_KEY"]))
    jsonl_path = os.path.join(outdir, "youtube_metadata.jsonl")
    csv_path = os.path.join(outdir, "youtube_metadata.csv")
    write_jsonl(rows, jsonl_path)
    write_csv(rows, csv_path)
    return {"count": len(rows), "jsonl": jsonl_path, "csv": csv_path}
