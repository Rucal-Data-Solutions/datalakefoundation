#!/usr/bin/env python3
"""Generate a single PDF from all Datalake Foundation wiki documentation."""

import os
import re
from datetime import date

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    KeepTogether,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Preformatted,
    Spacer,
    Table,
    TableStyle,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PDF = os.path.join(BASE_DIR, "DatalakeFoundation_Documentation.pdf")

DOCUMENT_ORDER = [
    ("Home", "README.md"),
    ("Release Notes", "docs/RELEASE_NOTES.md"),
    ("Entity Configuration", "docs/configuration/ENTITY_CONFIGURATION.md"),
    ("Metadata Sources", "docs/configuration/METADATA_SOURCES.md"),
    ("Expression Languages", "docs/configuration/EXPRESSIONS.md"),
    ("Log Level Configuration", "LOG_LEVEL_CONFIG.md"),
    ("Processing Strategies", "docs/processing/PROCESSING_STRATEGIES.md"),
    ("Streaming Processing", "docs/processing/STREAMING.md"),
    ("Watermarks", "docs/processing/WATERMARKS.md"),
    ("Delete Inference", "docs/processing/DELETE_INFERENCE.md"),
    ("IO Output Modes", "docs/outputs/IO_OUTPUT_MODES.md"),
    ("Microsoft Fabric Compatibility", "docs/MICROSOFT_FABRIC_COMPATIBILITY.md"),
]

PAGE_W, PAGE_H = A4
MARGIN = 2.2 * cm

# Brand-ish colours
CLR_PRIMARY = colors.HexColor("#1a3a5c")
CLR_ACCENT = colors.HexColor("#2980b9")
CLR_CODE_BG = colors.HexColor("#f4f4f4")
CLR_CODE_BORDER = colors.HexColor("#cccccc")
CLR_TABLE_HEADER = colors.HexColor("#2c3e50")
CLR_TABLE_ALT = colors.HexColor("#ecf0f1")
CLR_BLOCKQUOTE = colors.HexColor("#7f8c8d")
CLR_BLOCKQUOTE_BG = colors.HexColor("#f9f9f9")
CLR_BLOCKQUOTE_BAR = colors.HexColor("#3498db")

# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------


def _build_styles():
    ss = getSampleStyleSheet()

    ss.add(
        ParagraphStyle(
            "CoverTitle",
            parent=ss["Title"],
            fontSize=32,
            leading=38,
            textColor=CLR_PRIMARY,
            alignment=TA_CENTER,
            spaceAfter=12,
        )
    )
    ss.add(
        ParagraphStyle(
            "CoverSubtitle",
            parent=ss["Normal"],
            fontSize=14,
            leading=18,
            textColor=CLR_ACCENT,
            alignment=TA_CENTER,
            spaceAfter=6,
        )
    )
    ss.add(
        ParagraphStyle(
            "TOCHeading",
            parent=ss["Heading1"],
            fontSize=22,
            textColor=CLR_PRIMARY,
            spaceAfter=14,
        )
    )
    ss.add(
        ParagraphStyle(
            "TOCEntry",
            parent=ss["Normal"],
            fontSize=12,
            leading=20,
            leftIndent=10,
            textColor=colors.HexColor("#2c3e50"),
        )
    )
    ss.add(
        ParagraphStyle(
            "H1",
            parent=ss["Heading1"],
            fontSize=22,
            leading=26,
            textColor=CLR_PRIMARY,
            spaceBefore=18,
            spaceAfter=10,
            borderWidth=0,
            borderPadding=0,
        )
    )
    ss.add(
        ParagraphStyle(
            "H2",
            parent=ss["Heading2"],
            fontSize=17,
            leading=21,
            textColor=CLR_PRIMARY,
            spaceBefore=14,
            spaceAfter=8,
        )
    )
    ss.add(
        ParagraphStyle(
            "H3",
            parent=ss["Heading3"],
            fontSize=14,
            leading=17,
            textColor=CLR_ACCENT,
            spaceBefore=10,
            spaceAfter=6,
        )
    )
    ss.add(
        ParagraphStyle(
            "H4",
            parent=ss["Heading4"],
            fontSize=12,
            leading=15,
            textColor=CLR_ACCENT,
            spaceBefore=8,
            spaceAfter=4,
        )
    )
    ss.add(
        ParagraphStyle(
            "Body",
            parent=ss["Normal"],
            fontSize=10,
            leading=14,
            spaceBefore=2,
            spaceAfter=6,
        )
    )
    ss.add(
        ParagraphStyle(
            "CodeBlock",
            parent=ss["Code"],
            fontName="Courier",
            fontSize=8,
            leading=10,
            leftIndent=6,
            rightIndent=6,
            spaceBefore=4,
            spaceAfter=4,
            backColor=CLR_CODE_BG,
        )
    )
    ss.add(
        ParagraphStyle(
            "BulletItem",
            parent=ss["Normal"],
            fontSize=10,
            leading=14,
            leftIndent=24,
            bulletIndent=12,
            spaceBefore=1,
            spaceAfter=1,
        )
    )
    ss.add(
        ParagraphStyle(
            "BulletItem2",
            parent=ss["Normal"],
            fontSize=10,
            leading=14,
            leftIndent=42,
            bulletIndent=30,
            spaceBefore=1,
            spaceAfter=1,
        )
    )
    ss.add(
        ParagraphStyle(
            "NumberedItem",
            parent=ss["Normal"],
            fontSize=10,
            leading=14,
            leftIndent=24,
            spaceBefore=1,
            spaceAfter=1,
        )
    )
    ss.add(
        ParagraphStyle(
            "Blockquote",
            parent=ss["Normal"],
            fontSize=10,
            leading=14,
            leftIndent=20,
            textColor=CLR_BLOCKQUOTE,
            spaceBefore=4,
            spaceAfter=4,
            borderWidth=0,
        )
    )
    return ss


# ---------------------------------------------------------------------------
# Markdown → flowables
# ---------------------------------------------------------------------------

# Inline formatting
def _inline(text):
    """Convert markdown inline formatting to reportlab XML tags."""
    # Escape XML entities first
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")

    # Inline code (before bold/italic to avoid conflicts)
    text = re.sub(r"`([^`]+)`", r'<font face="Courier" size="9" color="#c0392b">\1</font>', text)

    # Bold + italic
    text = re.sub(r"\*\*\*(.+?)\*\*\*", r"<b><i>\1</i></b>", text)
    # Bold
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    # Italic
    text = re.sub(r"(?<!\*)\*([^*]+?)\*(?!\*)", r"<i>\1</i>", text)

    # Links: [text](url) → just show text
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"<u>\1</u>", text)

    return text


def _parse_table(lines, styles):
    """Parse markdown table lines into a reportlab Table."""
    rows = []
    for line in lines:
        line = line.strip().strip("|")
        cells = [c.strip() for c in line.split("|")]
        rows.append(cells)

    if len(rows) < 2:
        return None

    # Remove separator row (second row with dashes)
    if all(re.match(r"^[-: ]+$", c) for c in rows[1]):
        rows.pop(1)

    if not rows:
        return None

    # Build table data with Paragraphs for wrapping
    header_style = ParagraphStyle(
        "TH",
        parent=styles["Body"],
        fontName="Helvetica-Bold",
        fontSize=9,
        textColor=colors.white,
        leading=12,
    )
    cell_style = ParagraphStyle(
        "TD",
        parent=styles["Body"],
        fontSize=9,
        leading=12,
    )

    table_data = []
    for i, row in enumerate(rows):
        st = header_style if i == 0 else cell_style
        table_data.append([Paragraph(_inline(c), st) for c in row])

    n_cols = max(len(r) for r in table_data)
    # Pad short rows
    for r in table_data:
        while len(r) < n_cols:
            r.append(Paragraph("", cell_style))

    avail = PAGE_W - 2 * MARGIN - 10
    col_w = avail / n_cols

    tbl = Table(table_data, colWidths=[col_w] * n_cols, repeatRows=1)
    style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), CLR_TABLE_HEADER),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.5, CLR_CODE_BORDER),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]
    # Alternate row colours
    for i in range(1, len(table_data)):
        if i % 2 == 0:
            style_cmds.append(("BACKGROUND", (0, i), (-1, i), CLR_TABLE_ALT))

    tbl.setStyle(TableStyle(style_cmds))
    return tbl


def _make_code_block(code_lines, styles):
    """Wrap code lines in a bordered, shaded table cell."""
    code_text = "\n".join(code_lines)
    pre = Preformatted(code_text, styles["CodeBlock"])
    wrapper = Table(
        [[pre]],
        colWidths=[PAGE_W - 2 * MARGIN - 10],
    )
    wrapper.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), CLR_CODE_BG),
                ("BOX", (0, 0), (-1, -1), 0.5, CLR_CODE_BORDER),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    return wrapper


def _make_blockquote(text, styles):
    """Create a blockquote with a left accent bar."""
    para = Paragraph(_inline(text), styles["Blockquote"])
    wrapper = Table(
        [[para]],
        colWidths=[PAGE_W - 2 * MARGIN - 10],
    )
    wrapper.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), CLR_BLOCKQUOTE_BG),
                ("LEFTPADDING", (0, 0), (-1, -1), 14),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("LINEBEFOREDECOR", (0, 0), (0, -1), 3, CLR_BLOCKQUOTE_BAR),
            ]
        )
    )
    return wrapper


def _md_to_flowables(md_text, section_title, styles, toc_entries):
    """Convert markdown text to a list of reportlab flowables."""
    flowables = []
    lines = md_text.split("\n")
    i = 0

    # Track whether first H1 matches section title (skip duplicate)
    first_h1_seen = False

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # --- Code block ---
        if stripped.startswith("```"):
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            i += 1  # skip closing ```
            flowables.append(_make_code_block(code_lines, styles))
            flowables.append(Spacer(1, 4))
            continue

        # --- Table ---
        if "|" in stripped and stripped.startswith("|"):
            table_lines = []
            while i < len(lines) and "|" in lines[i].strip() and lines[i].strip().startswith("|"):
                table_lines.append(lines[i])
                i += 1
            tbl = _parse_table(table_lines, styles)
            if tbl:
                flowables.append(Spacer(1, 4))
                flowables.append(tbl)
                flowables.append(Spacer(1, 6))
            continue

        # --- Headings ---
        m = re.match(r"^(#{1,4})\s+(.*)", stripped)
        if m:
            level = len(m.group(1))
            heading_text = m.group(2).strip()

            # Skip first H1 if it duplicates the section title
            if level == 1 and not first_h1_seen:
                first_h1_seen = True
                # Always skip the first H1 — we already added the section title
                i += 1
                continue

            style_name = f"H{level}"
            flowables.append(Paragraph(_inline(heading_text), styles[style_name]))

            # Add H2/H3 to TOC
            if level == 2:
                toc_entries.append((heading_text, level))

            i += 1
            continue

        # --- Blockquote ---
        if stripped.startswith(">"):
            quote_parts = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                quote_parts.append(lines[i].strip().lstrip(">").strip())
                i += 1
            flowables.append(_make_blockquote(" ".join(quote_parts), styles))
            flowables.append(Spacer(1, 4))
            continue

        # --- Numbered list ---
        m_num = re.match(r"^(\d+)\.\s+(.*)", stripped)
        if m_num:
            num = m_num.group(1)
            text = m_num.group(2)
            flowables.append(
                Paragraph(f"<b>{num}.</b>  {_inline(text)}", styles["NumberedItem"])
            )
            i += 1
            continue

        # --- Bullet list ---
        m_bullet = re.match(r"^([ \t]*)[-*]\s+(.*)", line)
        if m_bullet:
            indent = len(m_bullet.group(1).replace("\t", "    "))
            text = m_bullet.group(2)
            if indent >= 2:
                style = styles["BulletItem2"]
            else:
                style = styles["BulletItem"]
            flowables.append(
                Paragraph(f"\u2022  {_inline(text)}", style)
            )
            i += 1
            continue

        # --- Horizontal rule ---
        if re.match(r"^[-*_]{3,}\s*$", stripped):
            i += 1
            flowables.append(Spacer(1, 8))
            continue

        # --- Empty line ---
        if not stripped:
            i += 1
            continue

        # --- Normal paragraph (accumulate lines) ---
        para_parts = []
        while i < len(lines):
            l = lines[i].strip()
            if not l or l.startswith("#") or l.startswith("```") or l.startswith("|") or l.startswith(">") or re.match(r"^[-*]\s+", l) or re.match(r"^\d+\.\s+", l) or re.match(r"^[-*_]{3,}\s*$", l):
                break
            para_parts.append(l)
            i += 1
        if para_parts:
            flowables.append(Paragraph(_inline(" ".join(para_parts)), styles["Body"]))

    return flowables


# ---------------------------------------------------------------------------
# Document building
# ---------------------------------------------------------------------------


def _cover_page(canvas, doc):
    """Draw the cover page."""
    canvas.saveState()

    # Background rectangle at top
    canvas.setFillColor(CLR_PRIMARY)
    canvas.rect(0, PAGE_H - 220, PAGE_W, 220, fill=True, stroke=False)

    # Title
    canvas.setFillColor(colors.white)
    canvas.setFont("Helvetica-Bold", 36)
    canvas.drawCentredString(PAGE_W / 2, PAGE_H - 120, "Datalake Foundation")

    # Subtitle
    canvas.setFont("Helvetica", 16)
    canvas.drawCentredString(PAGE_W / 2, PAGE_H - 155, "Documentation")

    # Version / date
    canvas.setFillColor(colors.HexColor("#bdc3c7"))
    canvas.setFont("Helvetica", 12)
    canvas.drawCentredString(PAGE_W / 2, PAGE_H - 185, f"Generated on {date.today().strftime('%B %d, %Y')}")

    # Decorative line
    canvas.setStrokeColor(CLR_ACCENT)
    canvas.setLineWidth(3)
    canvas.line(MARGIN, PAGE_H - 240, PAGE_W - MARGIN, PAGE_H - 240)

    # Description in center
    canvas.setFillColor(CLR_PRIMARY)
    canvas.setFont("Helvetica", 12)
    y = PAGE_H / 2 + 20
    for line in [
        "A metadata-driven data ingestion library",
        "for Apache Spark and Delta Lake.",
        "",
        "Bronze to Silver layer processing",
        "with pluggable strategies.",
    ]:
        if line:
            canvas.drawCentredString(PAGE_W / 2, y, line)
        y -= 20

    # Footer
    canvas.setFillColor(CLR_BLOCKQUOTE)
    canvas.setFont("Helvetica", 9)
    canvas.drawCentredString(PAGE_W / 2, MARGIN, "Scala 2.13 \u2022 Spark 4.0 \u2022 Delta Lake 4.0")

    canvas.restoreState()


def _normal_header_footer(canvas, doc):
    """Header and footer for content pages."""
    canvas.saveState()

    # Header line
    canvas.setStrokeColor(CLR_ACCENT)
    canvas.setLineWidth(0.5)
    canvas.line(MARGIN, PAGE_H - MARGIN + 8, PAGE_W - MARGIN, PAGE_H - MARGIN + 8)

    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(CLR_BLOCKQUOTE)
    canvas.drawString(MARGIN, PAGE_H - MARGIN + 12, "Datalake Foundation Documentation")

    # Footer
    canvas.setStrokeColor(CLR_CODE_BORDER)
    canvas.line(MARGIN, MARGIN - 8, PAGE_W - MARGIN, MARGIN - 8)

    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(CLR_BLOCKQUOTE)
    canvas.drawRightString(PAGE_W - MARGIN, MARGIN - 20, f"Page {doc.page}")
    canvas.drawString(MARGIN, MARGIN - 20, date.today().strftime("%Y-%m-%d"))

    canvas.restoreState()


def build_pdf():
    styles = _build_styles()

    # Collect all TOC entries: (section_title, page_placeholder)
    toc_data = []  # list of section titles for TOC

    # --- Build content flowables per section ---
    all_sections = []
    for section_title, rel_path in DOCUMENT_ORDER:
        full_path = os.path.join(BASE_DIR, rel_path)
        if not os.path.exists(full_path):
            print(f"  WARNING: {rel_path} not found, skipping.")
            continue

        with open(full_path, "r", encoding="utf-8") as f:
            md_text = f.read()

        toc_sub = []  # sub-entries for this section
        section_flowables = _md_to_flowables(md_text, section_title, styles, toc_sub)

        all_sections.append((section_title, section_flowables, toc_sub))
        toc_data.append((section_title, toc_sub))

    # --- Assemble document ---
    doc = BaseDocTemplate(
        OUTPUT_PDF,
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=MARGIN + 10,
        bottomMargin=MARGIN + 10,
        title="Datalake Foundation Documentation",
        author="Datalake Foundation",
    )

    frame = Frame(
        MARGIN,
        MARGIN + 10,
        PAGE_W - 2 * MARGIN,
        PAGE_H - 2 * MARGIN - 20,
        id="normal",
    )
    cover_frame = Frame(0, 0, PAGE_W, PAGE_H, id="cover")

    doc.addPageTemplates(
        [
            PageTemplate(id="cover", frames=[cover_frame], onPage=_cover_page),
            PageTemplate(id="content", frames=[frame], onPage=_normal_header_footer),
        ]
    )

    story = []

    # Cover page (empty content — drawn by onPage)
    story.append(NextPageTemplate("content"))
    story.append(PageBreak())

    # Table of Contents
    story.append(Paragraph("Table of Contents", styles["TOCHeading"]))
    story.append(Spacer(1, 10))

    for idx, (section_title, sub_entries) in enumerate(toc_data, 1):
        story.append(
            Paragraph(
                f"<b>{idx}.</b>&nbsp;&nbsp;{section_title}",
                styles["TOCEntry"],
            )
        )

    story.append(PageBreak())

    # Sections
    for section_title, section_flowables, _ in all_sections:
        # Section heading
        story.append(Paragraph(_inline(section_title), styles["H1"]))

        # Thin accent line under section title
        separator = Table(
            [[""]],
            colWidths=[PAGE_W - 2 * MARGIN - 10],
            rowHeights=[2],
        )
        separator.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), CLR_ACCENT),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ]
            )
        )
        story.append(separator)
        story.append(Spacer(1, 8))

        story.extend(section_flowables)
        story.append(PageBreak())

    doc.build(story)
    print(f"PDF generated: {OUTPUT_PDF}")


if __name__ == "__main__":
    build_pdf()
