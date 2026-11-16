# -*- coding: utf-8 -*-
"""Génère la fiche de TP CityFlow au format DOCX.

Le contenu pédagogique est rédigé dans ``Fiche_TP_CityFlow.md``. Ce script
applique une mise en page professionnelle (A4, page de garde, sommaire,
tableaux, encadrés, blocs de code, en-tête et pied de page numéroté).

Usage :
    python L4_Fiche_TP/build_fiche.py
"""

from __future__ import annotations

import re
from pathlib import Path

from docx import Document
from docx.enum.section import WD_SECTION
from docx.enum.table import WD_ALIGN_VERTICAL, WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_BREAK
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

ROOT = Path(__file__).resolve().parents[1]
HERE = Path(__file__).resolve().parent
SOURCE = HERE / "Fiche_TP_CityFlow.md"
OUTPUT = HERE / "Fiche_TP_CityFlow.docx"

NAVY = RGBColor(0x0B, 0x3C, 0x5D)
NAVY_DARK = RGBColor(0x08, 0x28, 0x3E)
TEAL = RGBColor(0x1D, 0x78, 0x74)
ORANGE = RGBColor(0xF2, 0xA5, 0x41)
RED = RGBColor(0xD1, 0x49, 0x5B)
GREY = RGBColor(0x5E, 0x6E, 0x80)
MIDGREY = RGBColor(0x9A, 0xA8, 0xB5)
LIGHT = RGBColor(0xF3, 0xF7, 0xF9)
PALE = RGBColor(0xF5, 0xF8, 0xFA)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
CODE_BG = "F2F5F7"
BORDER = "D8E2E9"

BODY_FONT = "Calibri"
CODE_FONT = "Consolas"
CONTENT_WIDTH_CM = 17.4


# ---------------------------------------------------------------------------
# Helpers XML
# ---------------------------------------------------------------------------
def set_cell_shading(cell, hex_color: str) -> None:
    tcPr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_color)
    tcPr.append(shd)


def set_cell_margins(cell, top=90, left=110, bottom=90, right=110) -> None:
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()
    tcMar = tcPr.find(qn("w:tcMar"))
    if tcMar is None:
        tcMar = OxmlElement("w:tcMar")
        tcPr.append(tcMar)
    for tag, value in (("top", top), ("left", left), ("bottom", bottom), ("right", right)):
        node = tcMar.find(qn(f"w:{tag}"))
        if node is None:
            node = OxmlElement(f"w:{tag}")
            tcMar.append(node)
        node.set(qn("w:w"), str(value))
        node.set(qn("w:type"), "dxa")


def set_table_borders(table, color=BORDER, size=4) -> None:
    tbl = table._tbl
    tblPr = tbl.tblPr
    existing = tblPr.find(qn("w:tblBorders"))
    if existing is not None:
        tblPr.remove(existing)
    borders = OxmlElement("w:tblBorders")
    for edge in ("top", "left", "bottom", "right", "insideH", "insideV"):
        el = OxmlElement(f"w:{edge}")
        el.set(qn("w:val"), "single")
        el.set(qn("w:sz"), str(size))
        el.set(qn("w:space"), "0")
        el.set(qn("w:color"), color)
        borders.append(el)
    tblPr.append(borders)


def set_table_width(table, width_cm: float) -> None:
    tbl = table._tbl
    tblPr = tbl.tblPr
    existing = tblPr.find(qn("w:tblW"))
    if existing is not None:
        tblPr.remove(existing)
    tblW = OxmlElement("w:tblW")
    tblW.set(qn("w:w"), str(int(width_cm * 567)))
    tblW.set(qn("w:type"), "dxa")
    tblPr.append(tblW)


def set_repeat_table_header(row) -> None:
    trPr = row._tr.get_or_add_trPr()
    tblHeader = OxmlElement("w:tblHeader")
    tblHeader.set(qn("w:val"), "true")
    trPr.append(tblHeader)


def set_row_cant_split(row) -> None:
    trPr = row._tr.get_or_add_trPr()
    cantSplit = OxmlElement("w:cantSplit")
    cantSplit.set(qn("w:val"), "true")
    trPr.append(cantSplit)


def set_paragraph_mark_size(paragraph, size_pt: float) -> None:
    """Fixe la taille du marque de paragraphe (hauteur d'un paragraphe vide)."""
    pPr = paragraph._p.get_or_add_pPr()
    rPr = pPr.find(qn("w:rPr"))
    if rPr is None:
        rPr = OxmlElement("w:rPr")
        pPr.append(rPr)
    for tag in ("w:sz", "w:szCs"):
        node = rPr.find(qn(tag))
        if node is None:
            node = OxmlElement(tag)
            rPr.append(node)
        node.set(qn("w:val"), str(max(2, int(size_pt * 2))))


def add_paragraph_border(paragraph, edge="bottom", color="F2A541", size=12, space=4) -> None:
    pPr = paragraph._p.get_or_add_pPr()
    pBdr = pPr.find(qn("w:pBdr"))
    if pBdr is None:
        pBdr = OxmlElement("w:pBdr")
        pPr.append(pBdr)
    el = OxmlElement(f"w:{edge}")
    el.set(qn("w:val"), "single")
    el.set(qn("w:sz"), str(size))
    el.set(qn("w:space"), str(space))
    el.set(qn("w:color"), color)
    pBdr.append(el)


def add_field(paragraph, instruction: str) -> None:
    run = paragraph.add_run()
    fld_begin = OxmlElement("w:fldChar")
    fld_begin.set(qn("w:fldCharType"), "begin")
    run._r.append(fld_begin)
    run2 = paragraph.add_run()
    instr = OxmlElement("w:instrText")
    instr.set(qn("xml:space"), "preserve")
    instr.text = f" {instruction} "
    run2._r.append(instr)
    run3 = paragraph.add_run()
    fld_sep = OxmlElement("w:fldChar")
    fld_sep.set(qn("w:fldCharType"), "separate")
    run3._r.append(fld_sep)
    run4 = paragraph.add_run("1")
    run5 = paragraph.add_run()
    fld_end = OxmlElement("w:fldChar")
    fld_end.set(qn("w:fldCharType"), "end")
    run5._r.append(fld_end)


def set_run_font(run, name=BODY_FONT, size=None, bold=None, color=None, italic=None) -> None:
    run.font.name = name
    rPr = run._element.get_or_add_rPr()
    rFonts = rPr.find(qn("w:rFonts"))
    if rFonts is None:
        rFonts = OxmlElement("w:rFonts")
        rPr.append(rFonts)
    rFonts.set(qn("w:ascii"), name)
    rFonts.set(qn("w:hAnsi"), name)
    rFonts.set(qn("w:cs"), name)
    if size is not None:
        run.font.size = Pt(size)
    if bold is not None:
        run.font.bold = bold
    if italic is not None:
        run.font.italic = italic
    if color is not None:
        run.font.color.rgb = color


# ---------------------------------------------------------------------------
# Rendu inline
# ---------------------------------------------------------------------------
INLINE_RE = re.compile(r"(\*\*.+?\*\*|`[^`]+`)")


def add_inline(paragraph, text: str, size=10.5, color=NAVY, bold=False, font=BODY_FONT):
    """Ajoute du texte en interprétant **gras** et `code`."""
    parts = INLINE_RE.split(text)
    for part in parts:
        if not part:
            continue
        if part.startswith("**") and part.endswith("**") and len(part) > 4:
            run = paragraph.add_run(part[2:-2])
            set_run_font(run, font, size, True, color)
        elif part.startswith("`") and part.endswith("`") and len(part) > 2:
            run = paragraph.add_run(part[1:-1])
            set_run_font(run, CODE_FONT, size - 0.7, bold, RGBColor(0x24, 0x4B, 0x63))
        else:
            run = paragraph.add_run(part)
            set_run_font(run, font, size, bold, color)


# ---------------------------------------------------------------------------
# Composants de mise en page
# ---------------------------------------------------------------------------
def add_heading(doc, text: str, level: int):
    p = doc.add_paragraph()
    pf = p.paragraph_format
    if level == 2:
        pf.space_before = Pt(14)
        pf.space_after = Pt(7)
        pf.keep_with_next = True
        add_inline(p, text, size=15.5, color=NAVY, bold=True, font="Calibri")
        add_paragraph_border(p, "bottom", "F2A541", size=10, space=4)
    elif level == 3:
        pf.space_before = Pt(10)
        pf.space_after = Pt(4)
        pf.keep_with_next = True
        add_inline(p, text, size=12.2, color=TEAL, bold=True, font="Calibri")
    else:
        pf.space_before = Pt(7)
        pf.space_after = Pt(3)
        pf.keep_with_next = True
        add_inline(p, text, size=10.8, color=NAVY, bold=True, font="Calibri")
    return p


def add_body(doc, text: str):
    p = doc.add_paragraph()
    pf = p.paragraph_format
    pf.space_before = Pt(0)
    pf.space_after = Pt(5)
    pf.line_spacing = 1.12
    pf.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    add_inline(p, text, size=10.5, color=NAVY)
    return p


def add_bullet(doc, text: str):
    p = doc.add_paragraph(style="List Bullet")
    pf = p.paragraph_format
    pf.space_before = Pt(0)
    pf.space_after = Pt(2)
    pf.line_spacing = 1.08
    pf.left_indent = Cm(0.65)
    pf.first_line_indent = Cm(-0.35)
    add_inline(p, text, size=10.2, color=NAVY)
    return p


def add_numbered(doc, text: str, number: int):
    p = doc.add_paragraph()
    pf = p.paragraph_format
    pf.space_before = Pt(0)
    pf.space_after = Pt(3)
    pf.line_spacing = 1.08
    pf.left_indent = Cm(0.8)
    pf.first_line_indent = Cm(-0.55)
    run = p.add_run(f"{number}.")
    set_run_font(run, BODY_FONT, 10.2, True, TEAL)
    run = p.add_run("  ")
    set_run_font(run, BODY_FONT, 10.2, True, TEAL)
    add_inline(p, text, size=10.2, color=NAVY)
    return p


def add_checkbox(doc, text: str, checked: bool = False):
    p = doc.add_paragraph()
    pf = p.paragraph_format
    pf.space_before = Pt(0)
    pf.space_after = Pt(2)
    pf.line_spacing = 1.08
    pf.left_indent = Cm(0.75)
    pf.first_line_indent = Cm(-0.55)
    run = p.add_run("☑  " if checked else "☐  ")
    set_run_font(run, "Segoe UI Symbol", 10.8, False, TEAL)
    add_inline(p, text, size=10.1, color=NAVY)
    return p


def add_code_block(doc, lines: list[str]):
    table = doc.add_table(rows=1, cols=1)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False
    set_table_width(table, CONTENT_WIDTH_CM)
    set_table_borders(table, "DDE5EA", 4)
    cell = table.cell(0, 0)
    cell.width = Cm(CONTENT_WIDTH_CM)
    set_cell_shading(cell, CODE_BG)
    set_cell_margins(cell, top=100, left=140, bottom=100, right=140)
    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(0)
    p.paragraph_format.line_spacing = 1.0
    for idx, line in enumerate(lines):
        if idx:
            p.add_run().add_break()
        run = p.add_run(line if line else " ")
        set_run_font(run, CODE_FONT, 8.3, False, RGBColor(0x1E, 0x3A, 0x4C))
    spacer = doc.add_paragraph()
    spacer.paragraph_format.space_before = Pt(0)
    spacer.paragraph_format.space_after = Pt(2)
    spacer.paragraph_format.line_spacing = 0.4


def add_callout(doc, text: str):
    table = doc.add_table(rows=1, cols=1)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False
    set_table_width(table, CONTENT_WIDTH_CM)
    set_table_borders(table, "D8E2E9", 4)
    set_row_cant_split(table.rows[0])
    cell = table.cell(0, 0)
    cell.width = Cm(CONTENT_WIDTH_CM)
    set_cell_shading(cell, "F3F7F9")
    set_cell_margins(cell, top=130, left=180, bottom=130, right=180)
    # Bord gauche orange
    tcPr = cell._tc.get_or_add_tcPr()
    tcBorders = OxmlElement("w:tcBorders")
    left = OxmlElement("w:left")
    left.set(qn("w:val"), "single")
    left.set(qn("w:sz"), "24")
    left.set(qn("w:space"), "0")
    left.set(qn("w:color"), "F2A541")
    tcBorders.append(left)
    tcPr.append(tcBorders)
    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(0)
    p.paragraph_format.line_spacing = 1.08
    add_inline(p, text, size=9.8, color=NAVY)
    spacer = doc.add_paragraph()
    spacer.paragraph_format.space_before = Pt(0)
    spacer.paragraph_format.space_after = Pt(3)
    spacer.paragraph_format.line_spacing = 0.4
    set_paragraph_mark_size(spacer, 1)


def add_markdown_table(doc, rows: list[list[str]]):
    if not rows:
        return
    ncols = max(len(r) for r in rows)
    rows = [r + [""] * (ncols - len(r)) for r in rows]
    table = doc.add_table(rows=len(rows), cols=ncols)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False
    set_table_width(table, CONTENT_WIDTH_CM)
    set_table_borders(table, "D8E2E9", 4)

    # Largeurs proportionnelles au contenu (min 2,1 cm)
    weights = []
    for c in range(ncols):
        lengths = [len(re.sub(r"[*`]", "", row[c])) for row in rows]
        weights.append(max(6, min(42, max(lengths))))
    total = sum(weights)
    widths = [max(2.1, CONTENT_WIDTH_CM * w / total) for w in weights]
    scale = CONTENT_WIDTH_CM / sum(widths)
    widths = [w * scale for w in widths]

    for r_idx, row in enumerate(rows):
        tr = table.rows[r_idx]
        set_row_cant_split(tr)
        if r_idx == 0:
            set_repeat_table_header(tr)
        for c_idx, value in enumerate(row):
            cell = tr.cells[c_idx]
            cell.width = Cm(widths[c_idx])
            set_cell_margins(cell, top=70, left=90, bottom=70, right=90)
            cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
            if r_idx == 0:
                set_cell_shading(cell, "0B3C5D")
            elif r_idx % 2 == 0:
                set_cell_shading(cell, "F6F9FA")
            p = cell.paragraphs[0]
            p.paragraph_format.space_before = Pt(0)
            p.paragraph_format.space_after = Pt(0)
            p.paragraph_format.line_spacing = 1.0
            if r_idx == 0:
                add_inline(p, value, size=8.6, color=WHITE, bold=True)
            else:
                add_inline(p, value, size=8.9, color=NAVY)
    spacer = doc.add_paragraph()
    spacer.paragraph_format.space_before = Pt(0)
    spacer.paragraph_format.space_after = Pt(3)
    spacer.paragraph_format.line_spacing = 0.4


# ---------------------------------------------------------------------------
# En-tête / pied de page
# ---------------------------------------------------------------------------
def configure_section(section):
    section.page_width = Cm(21)
    section.page_height = Cm(29.7)
    section.top_margin = Cm(2.0)
    section.bottom_margin = Cm(1.9)
    section.left_margin = Cm(1.8)
    section.right_margin = Cm(1.8)
    section.header_distance = Cm(0.9)
    section.footer_distance = Cm(0.9)
    section.different_first_page_header_footer = True


def configure_header_footer(section):
    header = section.header
    hp = header.paragraphs[0]
    hp.text = ""
    hp.paragraph_format.space_after = Pt(2)
    hp.paragraph_format.tab_stops.add_tab_stop(Cm(CONTENT_WIDTH_CM), WD_ALIGN_PARAGRAPH.RIGHT)
    run = hp.add_run("CityFlow  ·  Fiche de TP")
    set_run_font(run, BODY_FONT, 8.2, True, NAVY)
    run = hp.add_run("\tÉcosystème Big Data 2  ·  2026–2027")
    set_run_font(run, BODY_FONT, 8.2, False, GREY)
    add_paragraph_border(hp, "bottom", "D8E2E9", size=4, space=3)

    footer = section.footer
    fp = footer.paragraphs[0]
    fp.text = ""
    fp.paragraph_format.tab_stops.add_tab_stop(Cm(CONTENT_WIDTH_CM), WD_ALIGN_PARAGRAPH.RIGHT)
    run = fp.add_run("Projet 3 — CityFlow  ·  Mazine CHATER · Yahya BAID · Wenn Bouka DIEUMERCI")
    set_run_font(run, BODY_FONT, 7.8, False, MIDGREY)
    run = fp.add_run("\tPage ")
    set_run_font(run, BODY_FONT, 7.8, False, MIDGREY)
    add_field(fp, "PAGE")
    run = fp.add_run(" / ")
    set_run_font(run, BODY_FONT, 7.8, False, MIDGREY)
    add_field(fp, "NUMPAGES")
    add_paragraph_border(fp, "top", "D8E2E9", size=4, space=3)


# ---------------------------------------------------------------------------
# Page de garde et sommaire
# ---------------------------------------------------------------------------
def add_cover(doc):
    # Bandeau supérieur
    table = doc.add_table(rows=1, cols=1)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False
    set_table_width(table, CONTENT_WIDTH_CM)
    cell = table.cell(0, 0)
    cell.width = Cm(CONTENT_WIDTH_CM)
    set_cell_shading(cell, "0B3C5D")
    set_cell_margins(cell, top=1000, left=600, bottom=900, right=600)
    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(18)
    run = p.add_run("ÉCOSYSTÈME BIG DATA 2   ·   PROJET 3")
    set_run_font(run, BODY_FONT, 10.0, True, ORANGE)

    p = cell.add_paragraph()
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(6)
    run = p.add_run("CityFlow")
    set_run_font(run, BODY_FONT, 46, True, WHITE)

    p = cell.add_paragraph()
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(16)
    run = p.add_run("Traitement de flux en temps réel\navec Apache Spark Structured Streaming")
    set_run_font(run, BODY_FONT, 17.5, False, RGBColor(0xC9, 0xD9, 0xE2))

    p = cell.add_paragraph()
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(0)
    run = p.add_run("FICHE DE TRAVAUX PRATIQUES")
    set_run_font(run, BODY_FONT, 12.5, True, WHITE)

    # Barre orange
    bar = doc.add_table(rows=1, cols=1)
    bar.alignment = WD_TABLE_ALIGNMENT.CENTER
    bar.autofit = False
    set_table_width(bar, CONTENT_WIDTH_CM)
    bcell = bar.cell(0, 0)
    bcell.width = Cm(CONTENT_WIDTH_CM)
    set_cell_shading(bcell, "F2A541")
    set_cell_margins(bcell, top=0, left=0, bottom=0, right=0)
    bcell.paragraphs[0].paragraph_format.space_after = Pt(0)
    bcell.paragraphs[0].add_run(" ")

    spacer = doc.add_paragraph()
    spacer.paragraph_format.space_before = Pt(0)
    spacer.paragraph_format.space_after = Pt(10)

    add_body(
        doc,
        "Ce TP accompagne le prototype CityFlow : un pipeline Spark Structured Streaming "
        "qui agrège un flux de trafic urbain par zone et par fenêtre de 10 minutes, "
        "avec gestion des événements retardés par watermark.",
    )

    rows = [
        ("Module", "Écosystème Big Data 2"),
        ("Enseignante", "Pr. Soukaina Gouraguine"),
        ("Année universitaire", "2026–2027"),
        ("Public", "5e année — binôme ou trinôme"),
        ("Durée", "3 h (installation comprise)"),
        ("Auteurs du prototype", "Mazine CHATER · Yahya BAID · Wenn Bouka DIEUMERCI"),
    ]
    table = doc.add_table(rows=len(rows), cols=2)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False
    set_table_width(table, CONTENT_WIDTH_CM)
    set_table_borders(table, "D8E2E9", 4)
    for idx, (label, value) in enumerate(rows):
        row = table.rows[idx]
        set_row_cant_split(row)
        c0, c1 = row.cells
        c0.width = Cm(5.0)
        c1.width = Cm(CONTENT_WIDTH_CM - 5.0)
        set_cell_margins(c0, top=85, left=120, bottom=85, right=100)
        set_cell_margins(c1, top=85, left=120, bottom=85, right=100)
        set_cell_shading(c0, "F3F7F9")
        p0 = c0.paragraphs[0]
        p0.paragraph_format.space_after = Pt(0)
        add_inline(p0, label, size=9.2, color=GREY, bold=True)
        p1 = c1.paragraphs[0]
        p1.paragraph_format.space_after = Pt(0)
        add_inline(p1, value, size=9.4, color=NAVY, bold=False)

    spacer = doc.add_paragraph()
    spacer.paragraph_format.space_before = Pt(0)
    spacer.paragraph_format.space_after = Pt(8)

    add_callout(
        doc,
        "**Ce que vous allez faire.** Installer l'environnement, exécuter le pipeline en "
        "batch puis en streaming, analyser les agrégats Gold, provoquer un rejet par "
        "watermark, comparer les deux modes et vérifier la réconciliation des volumes.",
    )

    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(24)
    p.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = p.add_run("2026 – 2027")
    set_run_font(run, BODY_FONT, 10, True, MIDGREY)


def add_toc(doc, headings: list[str]):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(12)
    run = p.add_run("Sommaire")
    set_run_font(run, BODY_FONT, 24, True, NAVY)
    add_paragraph_border(p, "bottom", "F2A541", size=12, space=5)

    for heading in headings:
        p = doc.add_paragraph()
        p.paragraph_format.space_before = Pt(1)
        p.paragraph_format.space_after = Pt(6)
        p.paragraph_format.left_indent = Cm(0.2)
        add_inline(p, heading, size=11.0, color=NAVY)

    spacer = doc.add_paragraph()
    spacer.paragraph_format.space_before = Pt(16)

    add_callout(
        doc,
        "**Organisation conseillée.** Les sections 4 à 10 suivent l'ordre de la démarche : "
        "inspection, référence batch, streaming, analyse, watermark, live et comparaison. "
        "Les sections 11 à 13 servent de contrôle et d'annexe.",
    )


# ---------------------------------------------------------------------------
# Parsing Markdown
# ---------------------------------------------------------------------------
def parse_markdown(text: str):
    lines = text.splitlines()
    # On saute le bloc de couverture du Markdown : il est remplacé par la page de garde.
    start = 0
    for i, line in enumerate(lines):
        if line.startswith("## 1."):
            start = i
            break
    lines = lines[start:]

    blocks = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i].rstrip()
        stripped = line.strip()

        if not stripped:
            i += 1
            continue
        if stripped == "<!-- pagebreak -->":
            blocks.append(("pagebreak", None))
            i += 1
            continue
        if stripped == "---":
            i += 1
            continue
        if stripped.startswith("```"):
            i += 1
            code = []
            while i < n and not lines[i].strip().startswith("```"):
                code.append(lines[i].rstrip("\n"))
                i += 1
            i += 1
            blocks.append(("code", code))
            continue
        if stripped.startswith("|"):
            table_lines = []
            while i < n and lines[i].strip().startswith("|"):
                table_lines.append(lines[i].strip())
                i += 1
            rows = []
            for idx, tline in enumerate(table_lines):
                if idx == 1 and re.match(r"^\|[\s:|-]+\|$", tline):
                    continue
                cells = [c.strip() for c in tline.strip("|").split("|")]
                rows.append(cells)
            blocks.append(("table", rows))
            continue
        if stripped.startswith(">"):
            quote = []
            while i < n and lines[i].strip().startswith(">"):
                quote.append(re.sub(r"^>\s?", "", lines[i].strip()))
                i += 1
            blocks.append(("callout", " ".join(quote)))
            continue
        if re.match(r"^#{2,4}\s+", stripped):
            level = len(stripped) - len(stripped.lstrip("#"))
            blocks.append((f"h{level}", stripped[level:].strip()))
            i += 1
            continue
        if re.match(r"^[-*]\s+", stripped):
            item = re.sub(r"^[-*]\s+", "", stripped)
            i += 1
            while i < n:
                raw = lines[i]
                nxt = raw.strip()
                if (raw.startswith(("  ", "\t")) and nxt
                        and not nxt.startswith(("#", "```", "|", ">"))
                        and nxt != "<!-- pagebreak -->" and nxt != "---"
                        and not re.match(r"^[-*]\s+", nxt)
                        and not re.match(r"^\d+\.\s+", nxt)):
                    item += " " + nxt
                    i += 1
                else:
                    break
            check = re.match(r"^\[([ xX])\]\s+(.*)$", item)
            if check:
                blocks.append(("checkbox", (check.group(1).lower() == "x", check.group(2))))
            else:
                blocks.append(("bullet", item))
            continue
        if re.match(r"^\d+\.\s+", stripped):
            num = int(re.match(r"^(\d+)\.", stripped).group(1))
            item = re.sub(r"^\d+\.\s+", "", stripped)
            i += 1
            while i < n:
                raw = lines[i]
                nxt = raw.strip()
                if (raw.startswith(("  ", "\t")) and nxt
                        and not nxt.startswith(("#", "```", "|", ">"))
                        and nxt != "<!-- pagebreak -->" and nxt != "---"
                        and not re.match(r"^[-*]\s+", nxt)
                        and not re.match(r"^\d+\.\s+", nxt)):
                    item += " " + nxt
                    i += 1
                else:
                    break
            blocks.append(("number", (num, item)))
            continue

        para = [stripped]
        i += 1
        while i < n:
            nxt = lines[i].strip()
            if (not nxt or nxt.startswith("#") or nxt.startswith("```")
                    or nxt.startswith("|") or nxt.startswith(">")
                    or nxt == "<!-- pagebreak -->" or nxt == "---"
                    or re.match(r"^[-*]\s+", nxt) or re.match(r"^\d+\.\s+", nxt)):
                break
            para.append(nxt)
            i += 1
        blocks.append(("p", " ".join(para)))
    return blocks


# ---------------------------------------------------------------------------
# Assemblage
# ---------------------------------------------------------------------------
def main():
    text = SOURCE.read_text(encoding="utf-8")
    blocks = parse_markdown(text)
    headings = [value for kind, value in blocks if kind == "h2"]

    doc = Document()

    # Styles de base
    normal = doc.styles["Normal"]
    normal.font.name = BODY_FONT
    normal.font.size = Pt(10.5)
    normal.font.color.rgb = NAVY
    normal.paragraph_format.space_after = Pt(4)

    for style_name in ("List Bullet",):
        st = doc.styles[style_name]
        st.font.name = BODY_FONT
        st.font.size = Pt(10.2)
        st.font.color.rgb = NAVY

    configure_section(doc.sections[0])
    configure_header_footer(doc.sections[0])

    add_cover(doc)
    doc.add_page_break()
    add_toc(doc, headings)
    doc.add_page_break()

    pending_break = False
    for kind, value in blocks:
        if kind == "pagebreak":
            pending_break = True
            continue
        if kind == "h2":
            p = add_heading(doc, value, 2)
            if pending_break:
                p.paragraph_format.page_break_before = True
                pending_break = False
        elif kind == "h3":
            if pending_break:
                doc.add_page_break()
                pending_break = False
            add_heading(doc, value, 3)
        elif kind == "h4":
            if pending_break:
                doc.add_page_break()
                pending_break = False
            add_heading(doc, value, 4)
        elif kind == "p":
            if pending_break:
                doc.add_page_break()
                pending_break = False
            add_body(doc, value)
        elif kind == "bullet":
            if pending_break:
                doc.add_page_break()
                pending_break = False
            add_bullet(doc, value)
        elif kind == "checkbox":
            if pending_break:
                doc.add_page_break()
                pending_break = False
            add_checkbox(doc, value[1], value[0])
        elif kind == "number":
            if pending_break:
                doc.add_page_break()
                pending_break = False
            add_numbered(doc, value[1], value[0])
        elif kind == "code":
            if pending_break:
                doc.add_page_break()
                pending_break = False
            add_code_block(doc, value)
        elif kind == "callout":
            if pending_break:
                doc.add_page_break()
                pending_break = False
            add_callout(doc, value)
        elif kind == "table":
            if pending_break:
                doc.add_page_break()
                pending_break = False
            add_markdown_table(doc, value)

    # Propriétés
    props = doc.core_properties
    props.title = "CityFlow — Fiche de TP"
    props.subject = "Traitement de flux en temps réel avec Spark Structured Streaming"
    props.author = "Mazine CHATER, Yahya BAID, Wenn Bouka DIEUMERCI"
    props.keywords = "Spark, Structured Streaming, watermark, event time, PySpark, Big Data"
    props.comments = "Fiche de travaux pratiques — Écosystème Big Data 2, 2026–2027"

    doc.save(OUTPUT)
    print(f"Fiche générée : {OUTPUT}")
    print(f"Sections      : {len(headings)}")
    print(f"Blocs         : {len(blocks)}")


if __name__ == "__main__":
    main()
