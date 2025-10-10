import importlib, pandas as pd
from pathlib import Path

OUTPUT = Path("output")
OUTPUT.mkdir(parents=True, exist_ok=True)
MASTER = OUTPUT / "eppley_master.csv"

PIPELINE = [
    "collectors.wordpress_posts",
    "collectors.crossref_works",
    "collectors.openalex_works",
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
            if len(df) == 0:  # keep headers if empty, but still merge
                df = df.copy()
            df["__file"] = p.name
            frames.append(df)
        except Exception as e:
            print(f"[merge] skip {p.name}: {e}")
    m = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    m.to_csv(MASTER, index=False)
    print(f"[merge] wrote {MASTER} ({len(m)} rows)")

if __name__ == "__main__":
    run_collectors()
    merge_csvs()
