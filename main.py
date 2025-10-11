import importlib
import sys
from pathlib import Path
import pandas as pd

OUTPUT = Path("output")
OUTPUT.mkdir(parents=True, exist_ok=True)
MASTER = OUTPUT / "eppley_master.csv"

PIPELINE = [
    "collectors.wordpress_posts",
    "collectors.crossref_works",
    "collectors.openalex_works",
    "collectors.pubmed_eppley",
    "collectors.youtube_all",
]

def run_collectors():
    for mod in PIPELINE:
        print(f"==> Running {mod}")
        try:
            m = importlib.import_module(mod)
            if hasattr(m, "run"):
                m.run()
                print(f"[ok] {mod}.run() finished")
            else:
                print(f"[warn] {mod} has no run() function")
        except Exception as e:
            print(f"[error] {mod} failed: {e}")

def merge_csvs():
    print("==> Merging CSVs")
    csvs = list(OUTPUT.glob("*.csv"))
    print(f"found {len(csvs)} CSVs: {[p.name for p in csvs]}")
    frames = []
    for p in csvs:
        try:
            df = pd.read_csv(p)
            df["__file"] = p.name
            frames.append(df)
            print(f"[merge] {p.name}: {len(df)} rows")
        except Exception as e:
            print(f"[merge] skip {p.name}: {e}")
    m = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    m.to_csv(MASTER, index=False)
    print(f"[merge] wrote {MASTER} ({len(m)} rows)")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "merge_only":
        merge_csvs()
    else:
        run_collectors()
        merge_csvs()