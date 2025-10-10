# Using the Eppley Collector in NotebookLM

This notebook connects to data exported from the Eppley-Collector GitHub project.  
Each Markdown source represents a cleaned dataset of public information:
- Blog / Q&A posts from Dr. Eppley’s website
- Academic articles from PubMed
- Metadata from YouTube videos
- Optional: ClinicalTrials, ORCID, OpenAlex research data

Together they create a unified, citation-ready knowledge base for summarizing, mapping topics, and generating structured insights.

## How it updates
- The GitHub Action runs daily at 9:30 AM Phoenix (16:30 UTC)
- New CSVs and Markdown exports appear under `/output/`
- NotebookLM does **not** auto-sync — replace old sources when new ones appear.

## How to use this notebook
1. Add the latest Markdown exports as sources.
2. Copy the “Exploration” and “Synthesis” prompt packs below into new notes.
3. Run prompts in layers: overview → clustering → focused follow-ups.
4. Save strong responses as Notes for citation-ready reuse.