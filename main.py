#!/usr/bin/env python3
import os, importlib, traceback, pathlib
from datetime import datetime

ROOT = pathlib.Path(__file__).resolve().parent
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

EMAIL = os.getenv("EMAIL", "").strip()  # Set via Actions Variable NCBI_EMAIL

# (module, function, expected_filename)
COLLECTORS = [
    ("collectors.pubmed",          "run",        "pubmed_eppley.csv"),          # existing in your repo
    ("collectors.crossref",        "run",        "crossref_works.csv"),         # new real
    ("collectors.openalex",        "run",        "openalex_works.csv"),         # updated UA + paging
    ("collectors.clinical_trials", "run",        "clinical_trials.csv"),        # new real
    ("collectors.orcid",           "run_profiles","orcid_profiles.csv"),        # new real
    ("collectors.orcid",           "run_works",  "orcid_works.csv"),            # best-effort
    ("collectors.youtube",         "run",        "youtube_all.csv"),            # your existing script
    ("collectors.wordpress",       "run",        "wordpress_posts.csv"),        # your existing script
]

def _import_callable(mod, fn):
    try:
        m = importlib.import_module(mod)
        return getattr(m, fn)
    except Exception:
        print(f"[SKIP] {mod}.{fn} unavailable:\n{traceback.format_exc()}")
        return None

def main():
    print(f"[START] {datetime.utcnow().isoformat()}Z • OUT={OUT}")
    print(f"[INFO ] EMAIL={'SET' if EMAIL else 'NOT SET'}")

    for mod, fn_name, expected in COLLECTORS:
        fn = _import_callable(mod, fn_name)
        if not fn:
            continue
        print(f"[RUN  ] {mod}.{fn_name} → output/{expected}")
        try:
            fn(OUT, EMAIL)  # every collector accepts (out_dir, email)
            path = OUT / expected
            if path.exists():
                print(f"[OK   ] wrote {expected} ({path.stat().st_size} bytes)")
            else:
                print(f"[WARN ] {expected} missing after run")
        except Exception:
            print(f"[FAIL ] {mod}.{fn_name}\n{traceback.format_exc()}")

    print(f"[DONE ] {datetime.utcnow().isoformat()}Z")

if __name__ == "__main__":
    main()
