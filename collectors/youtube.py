# collectors/youtube.py
# YouTube Data API collector for videos mentioning Barry Eppley or uploaded by related channels.
# Output: output/youtube_all.csv
import os, csv, time, requests
from typing import List, Dict, Any

# Retrieve the YouTube Data API key from the environment.  If unset or empty
# the collector will operate in a no-op mode and simply write an empty CSV.
API_KEY = os.getenv("YT_API_KEY", "").strip()
BASE_SEARCH = "https://www.googleapis.com/youtube/v3/search"
BASE_VIDEOS = "https://www.googleapis.com/youtube/v3/videos"
BASE_CHANNELS = "https://www.googleapis.com/youtube/v3/channels"

UA = "eppley-collector/1.0 (+https://jasonab74-ctrl.github.io/eppley-collector/)"

SEARCH_TERMS = [
    "Barry Eppley",
    '"Barry L Eppley"',
    '"Dr Barry Eppley"',
    '"Dr. Barry Eppley"',
    "Eppley Plastic Surgery",
    "Eppley Craniofacial",
]

# Optional: known channel IDs (if public)
CHANNEL_IDS = [
    # Example placeholder, replace if known:
    # "UCxyz123abcDEF456"  # Barry Eppley official channel
]

FIELDS = [
    "video_id", "title", "description", "channel_title", "channel_id",
    "published_at", "view_count", "like_count", "comment_count",
    "duration", "tags", "definition", "license", "live_broadcast",
    "link"
]


def _get_json(url: str, params: Dict[str, Any], retries: int = 5, backoff: float = 0.6):
    for i in range(retries):
        r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff * (2 ** i))
            continue
        r.raise_for_status()
    r.raise_for_status()


def search_videos(query: str, max_results: int = 50) -> List[str]:
    """Search videos by keyword query."""
    ids = []
    page_token = None
    while True:
        params = {
            "key": API_KEY,
            "q": query,
            "part": "id",
            "type": "video",
            "maxResults": max_results,
            "pageToken": page_token or "",
            "relevanceLanguage": "en",
            "safeSearch": "none",
        }
        js = _get_json(BASE_SEARCH, params)
        for item in js.get("items", []):
            vid = (item.get("id") or {}).get("videoId")
            if vid:
                ids.append(vid)
        page_token = js.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.2)
    return ids


def get_channel_uploads(channel_id: str, max_results: int = 50) -> List[str]:
    """Get all uploads from a specific channel."""
    ids = []
    ch_info = _get_json(BASE_CHANNELS, {
        "key": API_KEY,
        "id": channel_id,
        "part": "contentDetails",
    })
    uploads = None
    for item in ch_info.get("items", []):
        pl = (item.get("contentDetails") or {}).get("relatedPlaylists", {})
        if "uploads" in pl:
            uploads = pl["uploads"]
    if not uploads:
        return ids

    page_token = None
    while True:
        params = {
            "key": API_KEY,
            "playlistId": uploads,
            "part": "contentDetails",
            "maxResults": max_results,
            "pageToken": page_token or "",
        }
        js = _get_json("https://www.googleapis.com/youtube/v3/playlistItems", params)
        for item in js.get("items", []):
            vid = ((item.get("contentDetails") or {}).get("videoId"))
            if vid:
                ids.append(vid)
        page_token = js.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.2)
    return ids


def get_video_metadata(video_ids: List[str]) -> List[Dict[str, Any]]:
    """Fetch video details for given IDs."""
    out = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        params = {
            "key": API_KEY,
            "part": "snippet,contentDetails,statistics,status",
            "id": ",".join(batch),
        }
        js = _get_json(BASE_VIDEOS, params)
        for it in js.get("items", []):
            s = it.get("snippet", {})
            st = it.get("statistics", {})
            cd = it.get("contentDetails", {})
            out.append({
                "video_id": it.get("id", ""),
                "title": s.get("title", ""),
                "description": s.get("description", "").replace("\n", " "),
                "channel_title": s.get("channelTitle", ""),
                "channel_id": s.get("channelId", ""),
                "published_at": s.get("publishedAt", ""),
                "view_count": st.get("viewCount", ""),
                "like_count": st.get("likeCount", ""),
                "comment_count": st.get("commentCount", ""),
                "duration": cd.get("duration", ""),
                "tags": "; ".join(s.get("tags", []) or []),
                "definition": cd.get("definition", ""),
                "license": it.get("status", {}).get("license", ""),
                "live_broadcast": it.get("snippet", {}).get("liveBroadcastContent", ""),
                "link": f"https://www.youtube.com/watch?v={it.get('id','')}"
            })
        time.sleep(0.25)
    return out


def run_youtube(out_path: str = "output/youtube_all.csv") -> int:
    """Collect YouTube video metadata or write an empty CSV if no API key is available.

    This function attempts to search for and download video metadata using the
    YouTube Data API.  If the ``YT_API_KEY`` environment variable is not set,
    it will fall back to writing an empty CSV with the appropriate header
    fields and return zero.  This behaviour prevents the entire pipeline
    from failing when credentials are missing.

    Parameters
    ----------
    out_path : str
        The destination CSV path.  Defaults to ``output/youtube_all.csv``.

    Returns
    -------
    int
        The number of video records written (``0`` if API access is
        unavailable).
    """
    # Ensure the output directory exists
    import os
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # When there is no API key we simply emit an empty CSV and return early.
    if not API_KEY:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
        print(f"[youtube] no API key; wrote 0 rows -> {out_path}")
        return 0

    all_ids = set()

    # Search by keywords
    for term in SEARCH_TERMS:
        vids = search_videos(term)
        print(f"[youtube] term '{term}' -> {len(vids)} videos")
        all_ids.update(vids)
        time.sleep(0.3)

    # Scan known channels
    for cid in CHANNEL_IDS:
        vids = get_channel_uploads(cid)
        print(f"[youtube] channel {cid} -> {len(vids)} videos")
        all_ids.update(vids)
        time.sleep(0.3)

    ids_list = list(all_ids)
    print(f"[youtube] unique video IDs: {len(ids_list)}")

    # Fetch metadata in batches
    details = get_video_metadata(ids_list)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(details)

    print(f"[youtube] wrote {len(details)} rows -> {out_path}")
    return len(details)


if __name__ == "__main__":
    run_youtube()