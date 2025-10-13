#!/usr/bin/env python3
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
from pathlib import Path
from html import escape

ROOT   = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "output"
STATUS = OUTPUT / "status.json"
INDEX  = ROOT / "index.html"

REPO = os.environ.get("GITHUB_REPOSITORY", "")
SHA  = os.environ.get("GITHUB_SHA", "")[:8]

def load_status():
    if STATUS.exists():
        with STATUS.open("r", encoding="utf-8") as f:
            return json.load(f)
    # fallback if status.json is missing
    return {
        "repo": REPO,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": []
    }

def format_dt(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z","+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f %z")[:-8] + " +00:00"
    except Exception:
        return escape(iso)

def csv_link(raw_url: str) -> str:
    # add sha param so browsers fetch the newest asset automatically
    sep = "&" if "?" in raw_url else "?"
    return f'{raw_url}{sep}sha={SHA}' if SHA else raw_url

def render_row(file_obj: dict) -> str:
    name   = escape(file_obj.get("name",""))
    label  = escape(file_obj.get("label",""))
    rows   = file_obj.get("rows", 0)
    exists = file_obj.get("exists", False)
    raw    = file_obj.get("raw_url") or file_obj.get("download_url") or ""
    status_badge = "ok" if exists and rows > 0 else ("skipped" if exists and rows==0 else "warn")
    link = csv_link(raw) if raw else "#"
    return f"""
      <tr>
        <td class="file"><a href="{link}" rel="noopener">{name}</a></td>
        <td class="desc">{label}</td>
        <td class="rows">{rows:,}</td>
        <td class="status"><span class="{status_badge}">{status_badge}</span></td>
        <td class="dl"><a class="csv" href="{link}" rel="noopener">CSV</a></td>
      </tr>
    """.strip()

def main():
    st = load_status()
    files = st.get("files", [])
    total_records = sum(int(f.get("rows",0) or 0) for f in files)
    generated_at  = format_dt(st.get("generated_at",""))

    # Build table rows
    table_rows = "\n".join(render_row(f) for f in files)

    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dr. Barry Eppley Research Dataset</title>
<style>
  :root {{
    --bg: #0c0f14; --panel:#151a22; --text:#e8eef6; --muted:#a7b0be; --link:#8ab4ff;
    --ok:#16a34a; --warn:#f59e0b; --skip:#64748b; --chip:#1f2937; --row:#0f141b;
  }}
  html,body {{ background:var(--bg); color:var(--text); font:16px/1.5 ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial; margin:0; }}
  .wrap {{ max-width:1080px; margin:60px auto; padding:0 16px; }}
  h1 {{ font-size: clamp(28px, 3.6vw, 44px); margin:0 0 10px; }}
  p.sub {{ color:var(--muted); margin:0 0 26px; }}
  .card {{ background:var(--panel); border-radius:14px; padding:18px; box-shadow:0 0 0 1px #0b0f15 inset; }}
  .meta {{ display:flex; gap:12px; flex-wrap:wrap; align-items:center; margin:0 0 14px; color:var(--muted); }}
  .meta .chip {{ background:var(--chip); padding:6px 10px; border-radius:999px; font-size:14px; }}
  .actions {{ display:flex; gap:12px; margin:12px 0 6px; flex-wrap:wrap; }}
  .btn {{ display:inline-block; background:#22304a; color:#e4edfa; text-decoration:none; padding:10px 14px; border-radius:10px; }}
  a {{ color:var(--link); text-decoration:none; }}
  table {{ width:100%; border-collapse:collapse; margin-top:10px; }}
  th, td {{ text-align:left; padding:12px 10px; border-bottom:1px solid #0e141d; vertical-align:middle; }}
  tr:hover td {{ background:var(--row); }}
  th {{ color:#b9c3d3; font-weight:600; }}
  td.file a {{ font-weight:600; }}
  td.rows {{ white-space:nowrap; }}
  .status span {{ padding:4px 10px; border-radius:999px; font-size:12px; text-transform:uppercase; letter-spacing:.02em; }}
  .status .ok {{ background:#052e1a; color:#7df0a6; }}
  .status .warn {{ background:#2d1b05; color:#ffc772; }}
  .status .skipped {{ background:#17212d; color:#aebad0; }}
  footer {{ color:var(--muted); margin:22px 0 0; font-size:14px; }}
</style>
</head>
<body>
  <div class="wrap">
    <h1>Dr. Barry Eppley Research Dataset</h1>
    <p class="sub">Automatically collected from public sources (WordPress, Crossref, OpenAlex, PubMed, YouTube). Updated nightly.</p>

    <div class="card">
      <div class="meta">
        <span class="chip">Last updated: <strong>{escape(generated_at)}</strong></span>
        <span class="chip">Total records: <strong>{total_records:,}</strong></span>
      </div>
      <div class="actions">
        <a class="btn" href="output/eppley_master.csv{'?sha='+SHA if SHA else ''}">Download master.csv</a>
        <a class="btn" href="output/status.json{'?sha='+SHA if SHA else ''}">View status.json</a>
      </div>

      <table>
        <thead>
          <tr>
            <th>File</th>
            <th>Description</th>
            <th>Rows</th>
            <th>Status</th>
            <th>Download</th>
          </tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>

      <footer>If counts ever look stale, you do nothing—the page is rebuilt automatically by the workflow.</footer>
    </div>
  </div>
</body>
</html>
"""
    INDEX.write_text(html, encoding="utf-8")
    print(f"Wrote {INDEX.relative_to(ROOT)}")

if __name__ == "__main__":
    sys.exit(main())