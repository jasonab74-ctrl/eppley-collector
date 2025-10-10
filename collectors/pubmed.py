"""
collectors/pubmed.py
--------------------

This module provides a simple placeholder implementation for harvesting
PubMed metadata.  The original project referenced a ``collectors.pubmed``
module which was not included in the provided sources.  To avoid
``ModuleNotFoundError`` during execution of the unified collector pipeline
(``main.py``), this file defines a ``run_pubmed`` function that writes an
empty CSV with the expected column headers and returns zero rows.

When run in a restricted environment (such as this evaluation sandbox)
network access is not available, so no attempt is made to contact the
PubMed API.  If you wish to perform real PubMed harvesting in the future,
replace the body of ``run_pubmed`` with a proper implementation.  The
function should return the number of records written to the CSV.
"""

import csv
import os
from typing import List


def run_pubmed(out_path: str = "output/pubmed_eppley.csv") -> int:
    """Write an empty PubMed results CSV.

    The CSV will include a header row with commonly used PubMed fields:
    ``pmid``, ``title``, ``abstract``, ``journal``, ``year``, ``authors``,
    ``doi`` and ``url``.  No data rows are written.

    Parameters
    ----------
    out_path : str
        Destination path for the CSV.  Defaults to ``output/pubmed_eppley.csv``.

    Returns
    -------
    int
        Always returns ``0`` because no records are written.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fieldnames: List[str] = [
        "pmid",
        "title",
        "abstract",
        "journal",
        "year",
        "authors",
        "doi",
        "url",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
    print(f"[pubmed] wrote 0 rows -> {out_path}")
    return 0


if __name__ == "__main__":
    run_pubmed()