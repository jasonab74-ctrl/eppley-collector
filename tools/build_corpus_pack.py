from pathlib import Path, json

ROOT=Path("."); OUTDIR=ROOT/"output"/"corpus"; OUTDIR.mkdir(parents=True,exist_ok=True)
PACK=OUTDIR/"notebooklm_full_pack.txt"

SOURCES=[
    ("WORDPRESS FULLTEXT", OUTDIR/"wordpress_fulltext.jsonl"),
    ("YOUTUBE TRANSCRIPTS", OUTDIR/"youtube_transcripts.jsonl"),
    ("CROSSREF/OPENALEX ABSTRACTS", OUTDIR/"crossref_abstracts.jsonl"),
]

def append_section(header,path):
    if not path.exists(): return 0
    count=0
    with open(path,"r",encoding="utf-8") as f, open(PACK,"a",encoding="utf-8") as out:
        out.write(f"\n\n{'#'*80}\n{header}\n{'#'*80}\n\n")
        for line in f:
            obj=json.loads(line)
            out.write(f"URL: {obj.get('url','')}\n")
            if obj.get('doi'): out.write(f"DOI: {obj['doi']}\n")
            out.write("\n"+(obj.get('text') or "")+"\n"+"-"*40+"\n")
            count+=1
    return count

if PACK.exists(): PACK.unlink()
totals={h:append_section(h,p) for h,p in SOURCES}
print("[pack] complete:", totals)