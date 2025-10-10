# Orchestrates the collectors and merges to eppley_master.csv

import importlib, pandas as pd
from pathlib import Path

OUTPUT = Path("output")
MASTER = OUTPUT / "eppley_master.csv"

# Keep only WORKING collectors for now
PIPELINE = [
    "collectors.wordpress_posts",
    "collectors.pubmed_eppley",
    "collectors.crossref_works",
    "collectors.openalex_works",
    "collectors.youtube_all",
]

def run_collectors():
    for mod in PIPELINE:
        print(f"==> Running {mod}")
        importlib.import_module(mod).run()

def merge_csvs():
    csvs = list(OUTPUT.glob("*.csv"))
    frames = []
    for p in csvs:
        try:
            df = pd.read_csv(p)
            df["__file"] = p.name
            frames.append(df)
        except Exception:
            continue
    m = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    m.to_csv(MASTER, index=False)
    print(f"Merged {len(csvs)} files into {MASTER} ({len(m)} rows)")

if __name__ == "__main__":
    OUTPUT.mkdir(exist_ok=True, parents=True)
    run_collectors()
    merge_csvs()
