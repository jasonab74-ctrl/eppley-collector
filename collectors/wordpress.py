"""
collectors/wordpress.py
-----------------------

This is a very small placeholder implementation of a WordPress scraper.  The
original upstream repository referenced a ``collectors.wordpress`` module,
but no such file existed in the provided sources.  Without access to the
live WordPress site and with network access restricted in this environment,
it's impossible to perform a real crawl.  To prevent import failures and
allow the pipeline in ``main.py`` to complete gracefully, this module
implements a ``run_wp`` function that writes a CSV with only a header row.

The output CSV will be created at ``output/wordpress_posts.csv`` and will
contain the columns ``title``, ``date``, ``url``, ``body`` and ``tags``.
The function returns ``0`` to indicate that no records were written.

If you later run this code in an environment with internet access you can
replace this stub with a real scraper implementation.  The rest of the
project expects the ``run_wp`` function to return the number of rows
written.
"""

import csv
import os
from typing import List


def run_wp(out_path: str = "output/wordpress_posts.csv") -> int:
    """Write an empty WordPress posts CSV.

    This stub creates the output directory if necessary, writes a header
    row describing the expected fields, and returns ``0``.

    Parameters
    ----------
    out_path : str
        The path at which to write the CSV.  Defaults to
        ``output/wordpress_posts.csv``.

    Returns
    -------
    int
        Always returns ``0`` because no rows are written.
    """
    # ensure output directory exists
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fieldnames: List[str] = [
        "title",
        "date",
        "url",
        "body",
        "tags",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
    print(f"[wordpress] wrote 0 rows -> {out_path}")
    return 0


if __name__ == "__main__":
    run_wp()