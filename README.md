# Dr. Barry Eppley — Content Collection Toolkit

This toolkit helps you *collect and organize* public information about Dr. Barry Eppley for personal research/archival:

- **WordPress scraper**: pulls titles/dates/body/tags from high-yield blog/Q&A pages.
- **PubMed fetcher**: collects publications and metadata via the PubMed E‑utilities.
- **YouTube metadata**: lists videos from a channel via `yt-dlp` (no downloads) or the YouTube Data API.

## Quick start

1. Install Python 3.9+ and `pip`.
2. Create a virtual environment (recommended):
   ```bash
   python -m venv .venv && source .venv/bin/activate
   ```
3. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```
4. Edit `config.yaml`:
   - Confirm the **WordPress seeds** (e.g., `your-questions`, `exploreplasticsurgery`).
   - Optionally add **include/exclude keywords**.
   - For PubMed, keep `Eppley BL[Author]` or add variations.
   - For YouTube, either:
     - Set `use_yt_dlp: true` and include `channel_urls`, or
     - Set `use_youtube_api: true`, provide `channel_ids`, and export `YT_API_KEY`.
5. Run all collectors:
   ```bash
   python main.py --only all
   ```
   Or run individually:
   ```bash
   python main.py --only wp
   python main.py --only pubmed
   python main.py --only youtube
   ```

Outputs will be written to `./output/` as both **CSV** and **JSONL**.

## Notes on ethics and ToS

- Respect each site’s **Terms of Use** and **robots.txt**.
- The WordPress scraper is *rate-limited*; increase `delay_seconds` if needed.
- Use YouTube metadata for organization; downloading videos may breach ToS—use only for personal use if allowed.
- Publications: PubMed is the canonical metadata source; full texts may be paywalled—store DOIs/citations, not paywalled PDFs.

## Tips

- If you want **historical snapshots**, use the Internet Archive Wayback Machine (separately).
- For very large crawls, consider lowering `max_pages_per_seed` to test, then raise it gradually.
- To keep data fresh, re-run periodically and merge on the `id` or `url` field.

## Troubleshooting

- If YouTube handle URLs fail, replace with the channel’s `/channel/UC...` URL or enable the API path.
- If the WordPress theme is unusual, tweak selectors in `wordpress_scraper.py` (`entry-content`, `entry-title`, etc.).
- If PubMed returns 429/403, add your email and/or API key in `config.yaml` and re-run.
