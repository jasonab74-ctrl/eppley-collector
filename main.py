import importlib
from pathlib import Path
import pandas as pd

OUTPUT = Path("output")
OUTPUT.mkdir(exist_ok=True, parents=True)
MASTER = OUTPUT / "eppley_master.csv"

PIPELINE = [
    "collectors.wordpress_posts",
    "collectors.crossref_works",
    "collectors.openalex_works",
    "collectors.pubmed_eppley",
    "collectors.youtube_all",
]

def run_collectors():
    for mod_name in PIPELINE:
        print(f"==> Running {mod_name}")
        try:
            mod = importlib.import_module(mod_name)
            if hasattr(mod, "run"):
                mod.run()
            else:
                print(f"[warn] {mod_name} has no run() function")
        except Exception as e:
            print(f"[error] {mod_name} failed: {e}")

def merge_csvs():
    frames = []
    for p in OUTPUT.glob("*.csv"):
        try:
            df = pd.read_csv(p)
            df["__file"] = p.name
            frames.append(df)
        except Exception as e:
            print(f"[merge] skipping {p.name}: {e}")
    m = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    m.to_csv(MASTER, index=False)
    print(f"[merge] wrote {MASTER} ({len(m)} rows)")

def main():
    run_collectors()
    merge_csvs()

if __name__ == "__main__":
    main()