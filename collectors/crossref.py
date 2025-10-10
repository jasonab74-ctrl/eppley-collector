"""
collectors/crossref.py
----------------------

Placeholder Crossref collector.  The original repository referenced a
``collectors.crossref`` module to gather works indexed by Crossref.  Since
no such module was provided and the sandbox environment does not allow
external HTTP requests, this file implements a stub ``run_crossref``
function that simply writes an empty CSV with appropriate headers and
returns zero.

Should you wish to add a real Crossref collection later, update the
``run_crossref`` function to fetch data from the Crossref API and write
records to the CSV.  The unified pipeline in ``main.py`` expects the
function to return the number of records written.
"""

import csv
import os
from typing import List


def run_crossref(out_path: str = "output/crossref_works.csv") -> int:
    """Write an empty Crossref works CSV.

    The CSV will include a header row with the fields ``title``, ``year``,
    ``journal``, ``authors``, ``doi`` and ``url``.  No data rows are
    produced.

    Parameters
    ----------
    out_path : str
        Destination path for the CSV.  Defaults to ``output/crossref_works.csv``.

    Returns
    -------
    int
        Always returns ``0`` because no records are written.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fieldnames: List[str] = [
        "title",
        "year",
        "journal",
        "authors",
        "doi",
        "url",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
    print(f"[crossref] wrote 0 rows -> {out_path}")
    return 0


if __name__ == "__main__":
    run_crossref()