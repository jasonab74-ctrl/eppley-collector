
import os, yaml, argparse, json
from eppley_toolkit.wordpress_scraper import run_from_config as wp_run
from eppley_toolkit.pubmed_fetch import run_from_config as pm_run
from eppley_toolkit.youtube_metadata import run_from_config as yt_run

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser(description="Dr. Barry Eppley content collection toolkit")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--only", choices=["wp","pubmed","youtube","all"], default="all", help="Which collector to run")
    args = parser.parse_args()

    cfg = load_config(args.config)
    os.makedirs(cfg["general"]["output_dir"], exist_ok=True)

    results = {}
    if args.only in ("wp","all"):
        results["wordpress"] = wp_run(cfg)
    if args.only in ("pubmed","all"):
        results["pubmed"] = pm_run(cfg)
    if args.only in ("youtube","all"):
        results["youtube"] = yt_run(cfg)

    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
