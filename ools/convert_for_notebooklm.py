"""
convert_for_notebooklm.py
-------------------------

This utility converts the various CSV outputs produced by the Eppley Collector
into Markdown documents that are friendlier for ingestion by NotebookLM.

NotebookLM currently accepts text‑centric file formats such as plain text,
Markdown and PDF, but it does not ingest native spreadsheet formats like
`.csv` or `.xlsx`.  Converting your CSVs into Markdown makes it easy
to upload them as sources.  Each entry in the CSV is represented as a
Markdown heading followed by a short, structured summary.  Long fields
such as abstracts, descriptions or body text are rendered as paragraphs.

The script discovers all `.csv` files in the `output` directory,
converts each to a `.md` file under `output/notebooklm/` with the
same base name, and returns a summary of files processed.

Usage::

    python3 tools/convert_for_notebooklm.py

You can optionally specify a different output directory using the
`--outdir` argument and filter which CSVs to convert by passing one
or more filenames as positional arguments.

This script is intended to be run manually after `python3 main.py` or
on a schedule to prepare NotebookLM sources.  It has no external
dependencies beyond the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List


def safe_filename(name: str) -> str:
    """Return a filesystem‑safe version of ``name`` with spaces replaced by underscores."""
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in name)


def format_heading(row: Dict[str, str], index: int) -> str:
    """Derive a human‑readable heading for a CSV row.

    The function inspects a number of common fields to pick a suitable
    heading.  If none are present it falls back to ``Entry N``.
    """
    for field in [
        "title",
        "nct_id",
        "orcid",
        "video_id",
        "openalex_id",
        "pmid",
    ]:
        value = row.get(field)
        if value:
            return value.strip()
    return f"Entry {index + 1}"


def prettify_key(key: str) -> str:
    """Convert a CSV key into a more human‑friendly label."""
    # Replace underscores with spaces and capitalise words
    return key.replace("_", " ").title()


def write_markdown(rows: Iterable[Dict[str, str]], dest: Path) -> int:
    """Write rows into a Markdown file at ``dest``.

    Returns the number of rows written.  Each row becomes a heading and
    accompanying bullet points.  Long text fields (body, abstract,
    description, summary) are rendered after the bullet list.
    """
    count = 0
    with dest.open("w", encoding="utf-8") as md:
        for idx, row in enumerate(rows):
            # Skip completely empty rows
            if not any(v for v in row.values() if v):
                continue
            heading = format_heading(row, idx)
            md.write(f"## {heading}\n\n")

            # Compose bullet list for all short fields
            for key, value in row.items():
                if not value:
                    continue
                key_lower = key.lower()
                # Skip heading fields and long fields here
                if key_lower in {"title", "body", "abstract", "description", "summary"}:
                    continue
                label = prettify_key(key)
                # Hyperlink DOI values
                if key_lower == "doi":
                    link = value.strip()
                    # ensure not empty; some rows embed a DOI URL; avoid duplicating schema
                    if link and not link.startswith("http"):
                        link = f"https://doi.org/{link}"
                    md.write(f"- **{label}**: [{value.strip()}]({link})\n")
                else:
                    md.write(f"- **{label}**: {value.strip()}\n")
            md.write("\n")

            # Render long text fields
            for long_key in ["body", "abstract", "description", "summary"]:
                text = row.get(long_key)
                if text:
                    md.write(f"{text.strip()}\n\n")
            md.write("\n")
            count += 1
    return count


def convert_csv_to_md(csv_path: Path, output_dir: Path) -> None:
    """Convert a single CSV file into a Markdown document."""
    with csv_path.open(newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    dest_name = safe_filename(csv_path.stem) + ".md"
    dest = output_dir / dest_name
    os.makedirs(output_dir, exist_ok=True)
    count = write_markdown(rows, dest)
    print(f"[convert] {csv_path.name} -> {dest.name} ({count} entries)")


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Convert Eppley collector CSV files to Markdown for NotebookLM.")
    parser.add_argument(
        "files",
        nargs="*",
        help="Specific CSV files in output/ to convert (default: all .csv)",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="output/notebooklm",
        help="Destination directory for markdown files (default: output/notebooklm)",
    )
    args = parser.parse_args(argv)
    output_dir = Path(args.outdir)
    # Determine which CSVs to convert
    base = Path("output")
    if args.files:
        csv_files = [base / f for f in args.files]
    else:
        csv_files = sorted(base.glob("*.csv"))

    for csv_file in csv_files:
        if not csv_file.exists() or not csv_file.suffix.lower() == ".csv":
            continue
        try:
            convert_csv_to_md(csv_file, output_dir)
        except Exception as e:
            print(f"[error] failed to convert {csv_file}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
