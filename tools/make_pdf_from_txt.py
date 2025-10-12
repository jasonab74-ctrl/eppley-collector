# tools/make_pdf_from_txt.py
from pathlib import Path
from textwrap import wrap

ROOT = Path(".")
SRC  = ROOT / "output" / "corpus" / "notebooklm_full_pack.txt"
DST  = ROOT / "output" / "corpus" / "notebooklm_full_pack.pdf"

def make_with_reportlab(text: str):
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import LETTER
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.units import inch

    pdfmetrics.registerFont(TTFont("DejaVuSans", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"))
    c = canvas.Canvas(str(DST), pagesize=LETTER)
    width, height = LETTER
    margin = 0.75 * inch
    y = height - margin
    line_h = 12
    max_chars = 95  # conservative for 10pt

    c.setTitle("Eppley — NotebookLM Pack")
    c.setAuthor("Eppley Collector")
    c.setFont("DejaVuSans", 10)

    for raw in text.splitlines():
        lines = wrap(raw, max_chars) or [""]
        for ln in lines:
            if y < margin:
                c.showPage()
                c.setFont("DejaVuSans", 10)
                y = height - margin
            c.drawString(margin, y, ln)
            y -= line_h
    c.save()

def make_with_fpdf(text: str):
    from fpdf import FPDF
    pdf = FPDF(format="Letter")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_title("Eppley — NotebookLM Pack")
    pdf.set_author("Eppley Collector")
    pdf.set_font("Arial", size=11)
    for line in text.splitlines():
        pdf.multi_cell(0, 5, line)
    pdf.output(str(DST))

def main():
    if not SRC.exists():
        raise SystemExit(f"Source not found: {SRC}")
    txt = SRC.read_text(encoding="utf-8")
    try:
        make_with_reportlab(txt)
    except Exception:
        # fallback
        make_with_fpdf(txt)
    print(f"[pdf] wrote {DST} ({DST.stat().st_size} bytes)")

if __name__ == "__main__":
    main()