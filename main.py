#!/usr/bin/env python3
import os, sys, importlib, traceback, pathlib
from datetime import datetime

ROOT = pathlib.Path(__file__).resolve().parent
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

EMAIL = os.getenv("EMAIL", "").strip()  # set via Actions variable NCBI_EMAIL

# collector spec: (module_path, function_name, csv_expected)
COLLECTORS = [
    ("collectors.pubmed",            "run", "pubmed_eppley.csv"),                 # you already have this
    ("collectors.crossref",          "run", "crossref_works.csv"),                # new real
    ("collectors.openalex",          "run", "openalex_works.csv"),                # updated UA
    ("collectors.clinical_trials",   "run", "clinical_trials.csv"),               # new real
    ("collectors.orcid",             "run_profiles", "orcid_profiles.csv"),       # new real
    ("collectors.orcid",             "run_works", "orcid_works.csv"),             # best-effort
    ("collectors.youtube",           "run", "youtube_all.csv"),                   # your existing script; needs YT_API_KEY if using API path
    ("collectors.wordpress",         "run", "wordpress_posts.csv"),               # your existing script
]

def _import_callable(mod_path, func_name):
    try:
        mod = importlib.import_module(mod_path)
        fn = getattr(mod, func_name)
        return fn
    except Exception:
        print(f"[SKIP] {mod_path}.{func_name} not available:\n{traceback.format_exc()}")
        return None

def main():
    print(f"[START] Eppley collectors @ {datetime.utcnow().isoformat()}Z")
    print(f"[INFO] EMAIL={'set' if EMAIL else 'NOT set'}  • OUT={OUT}")

    for mod_path, func_name, expected in COLLECTORS:
        fn = _import_callable(mod_path, func_name)
        if not fn:
            continue
        print(f"[RUN ] {mod_path}.{func_name} -> output/{expected}")
        try:
            # Every collector accepts (out_dir: pathlib.Path, email: str)
            fn(OUT, EMAIL)
            out = OUT / expected
            if out.exists():
                print(f"[OK  ] wrote {expected} ({out.stat().st_size} bytes)")
            else:
                print(f"[WARN] expected {expected} not found after run")
        except Exception:
            print(f"[FAIL] {mod_path}.{func_name}\n{traceback.format_exc()}")

    print(f"[DONE] {datetime.utcnow().isoformat()}Z")

if __name__ == "__main__":
    sys.exit(main())
