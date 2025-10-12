# tools/build_corpus_pack.py
from pathlib import Path
import json

ROOT = Path(".")
OUTDIR = ROOT / "output" / "corpus"
OUTDIR.mkdir(parents=True, exist_ok=True)
PACK = OUTDIR / "notebooklm_full_pack.txt"

SOURCES = [
    ("PUBMED ABSTRACTS", OUTDIR / "pubmed_abstracts.jsonl"),
    ("WORDPRESS FULLTEXT", OUTDIR / "wordpress_fulltext.jsonl"),
    ("YOUTUBE TRANSCRIPTS", OUTDIR / "youtube_transcripts.jsonl"),
    ("CROSSREF/OPENALEX ABSTRACTS", OUTDIR / "crossref_abstracts.jsonl"),
]

def append_section(header: str, path: Path):
    if not path.exists():
        return 0
    count = 0
    with open(path, "r", encoding="utf-8") as f, open(PACK, "a", encoding="utf-8") as out:
        out.write("\n\n" + "#"*80 + "\n")
        out.write(header + "\n")
        out.write("#"*80 + "\n\n")
        for line in f:
            obj = json.loads(line)
            title = (obj.get("title") or "").strip()
            if title:
                out.write(f"TITLE: {title}\n")
            if obj.get("journal"):
                out.write(f"JOURNAL: {obj.get('journal')}\n")
            if obj.get("year"):
                out.write(f"YEAR: {obj.get('year')}\n")
            if obj.get("url"):
                out.write(f"URL: {obj.get('url')}\n")
            if obj.get("doi"):
                out.write(f"DOI: {obj.get('doi')}\n")
            out.write("\n")
            out.write((obj.get("text") or "").strip() + "\n")
            out.write("\n" + "-"*40 + "\n")
            count += 1
    return count

def main():
    if PACK.exists():
        PACK.unlink()
    totals = {}
    for header, path in SOURCES:
        totals[header] = append_section(header, path)
    print("[pack] wrote:", PACK)
    print("[pack] counts:", totals)

if __name__ == "__main__":
    main()