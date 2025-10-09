#!/usr/bin/env python3
"""
YouTube collector for all Eppley mentions/tags (no API key needed).

Outputs:
- output/youtube_all.csv
- output/youtube_all.jsonl
"""

import csv, json, os, subprocess, sys, time, pathlib, re
from datetime import datetime, timezone
from typing import Dict, List, Iterable

OUTDIR = pathlib.Path("output")
CSV_PATH = OUTDIR / "youtube_all.csv"
JSONL_PATH = OUTDIR / "youtube_all.jsonl"

DEFAULT_TERMS = [
    "Barry Eppley",
    "\"Dr. Barry Eppley\"",
    "Eppley Plastic Surgery",
    "exploreplasticsurgery",
    "barryeppley",
]

def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def load_config() -> Dict:
    cfg = {}
    try:
        import yaml
        p = pathlib.Path("config.yaml")
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
    except Exception:
        pass
    return cfg

def run_ytdlp_lines(args: List[str]):
    proc = subprocess.run(args, capture_output=True, text=True, check=False)
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue

def build_search_queries(terms: List[str], per_term_max: int) -> List[str]:
    q = []
    n = max(20, min(per_term_max, 400))
    for t in terms:
        q.append(f"ytsearch{n}:{t}")
        q.append(f"ytsearchdate{n}:{t}")
    return q

def normalize_row(j: Dict, source: str) -> Dict:
    tags = j.get("tags") or []
    if isinstance(tags, list):
        tags = [str(x) for x in tags]
    else:
        tags = []
    def as_str(x): return "" if x is None else str(x)
    return {
        "id": j.get("id",""),
        "title": as_str(j.get("title","")),
        "channel": as_str(j.get("channel") or j.get("uploader","")),
        "channel_id": as_str(j.get("channel_id","")),
        "uploader_id": as_str(j.get("uploader_id","")),
        "upload_date": as_str(j.get("upload_date","")),
        "duration": j.get("duration") if j.get("duration") is not None else "",
        "view_count": j.get("view_count") if j.get("view_count") is not None else "",
        "like_count": j.get("like_count") if j.get("like_count") is not None else "",
        "comment_count": j.get("comment_count") if j.get("comment_count") is not None else "",
        "tags": "|".join(tags),
        "webpage_url": as_str(j.get("webpage_url") or (f"https://www.youtube.com/watch?v={j.get('id')}" if j.get("id") else "")),
        "description": as_str(j.get("description","")),
        "source": source,
        "collected_at": utc_now(),
    }

def looks_like_eppley(row, variants):
    text = " ".join([
        row.get("title",""),
        row.get("description",""),
        row.get("tags","").replace("|"," "),
        row.get("channel","")
    ]).lower()
    return any(v.lower() in text for v in variants)

def collect_from_search(terms: List[str], per_term_max: int):
    out = []
    for vurl in build_search_queries(terms, per_term_max):
        args = ["yt-dlp", "--dump-json", "--no-warnings", vurl]
        for j in run_ytdlp_lines(args):
            out.append(normalize_row(j, source="search"))
        time.sleep(0.2)
    return out

def collect_from_channels(urls: List[str]):
    out = []
    for url in urls:
        args = ["yt-dlp", "--dump-json", "--no-warnings", url]
        for j in run_ytdlp_lines(args):
            out.append(normalize_row(j, source="channel"))
        time.sleep(0.2)
    return out

def dedupe(rows):
    seen = {}
    for r in rows:
        vid = r.get("id","")
        if not vid: continue
        if vid not in seen:
            seen[vid] = r
        else:
            a, b = seen[vid], r
            def score(x):
                s = 0
                if x.get("tags"): s += 1
                if x.get("description"): s += 1
                try: s += int(x.get("view_count") or 0) > 0
                except: pass
                return s
            if score(b) > score(a): seen[vid] = b
    return list(seen.values())

def write_outputs(rows):
    OUTDIR.mkdir(parents=True, exist_ok=True)
    fields = [
        "id","title","channel","channel_id","uploader_id","upload_date",
        "duration","view_count","like_count","comment_count","tags",
        "webpage_url","description","source","collected_at"
    ]
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for r in rows: w.writerow({k:r.get(k,"") for k in fields})
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[youtube] wrote {len(rows)} rows → {CSV_PATH}")

def main():
    cfg = load_config()
    terms = cfg.get("youtube_search_terms") or DEFAULT_TERMS
    per_term_max = int(cfg.get("youtube_search_per_term", 200))
    channel_urls = cfg.get("youtube_channel_urls") or []
    playlist_urls = cfg.get("youtube_playlist_urls") or []
    variants = cfg.get("youtube_match_variants") or [
        "Barry Eppley","Dr. Barry Eppley","Eppley Plastic Surgery","exploreplasticsurgery","barryeppley","eppley"
    ]

    rows = []
    rows.extend(collect_from_search(terms, per_term_max))
    rows.extend(collect_from_channels(channel_urls + playlist_urls))
    rows = dedupe(rows)
    rows = [r for r in rows if looks_like_eppley(r, variants)]
    write_outputs(rows)

if __name__ == "__main__":
    main()