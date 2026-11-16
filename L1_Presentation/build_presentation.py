# -*- coding: utf-8 -*-
"""
CityFlow - Construction de la présentation de soutenance (L1).

Génère L1_Presentation/CityFlow_Presentation.pptx à partir des figures
produites par L2_Code/scripts/build_figures.py et des indicateurs du projet.

Usage :
    python L1_Presentation/build_presentation.py
"""

from __future__ import annotations

import json
from pathlib import Path

from PIL import Image
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.oxml.ns import qn
from pptx.util import Inches, Pt
from lxml import etree

# ---------------------------------------------------------------------------
# Chemins et constantes
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "L1_Presentation" / "assets"
DECK_ASSETS = ASSETS / "_deck"
OUT = ROOT / "L1_Presentation" / "CityFlow_Presentation.pptx"
KPIS = json.loads((ROOT / "output" / "tables" / "kpis.json").read_text(encoding="utf-8"))

DECK_ASSETS.mkdir(parents=True, exist_ok=True)

# Palette alignée sur les figures
NAVY = RGBColor(0x0B, 0x3C, 0x5D)
NAVY_DARK = RGBColor(0x08, 0x28, 0x3E)
NAVY_SOFT = RGBColor(0x12, 0x45, 0x5F)
TEAL = RGBColor(0x1D, 0x78, 0x74)
TEAL_LIGHT = RGBColor(0x6E, 0xAE, 0xAB)
ORANGE = RGBColor(0xF2, 0xA5, 0x41)
RED = RGBColor(0xD1, 0x49, 0x5B)
GREEN = RGBColor(0x5B, 0x8E, 0x7D)
LIGHT = RGBColor(0xEA, 0xF2, 0xF5)
PALE = RGBColor(0xF5, 0xF8, 0xFA)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
GREY = RGBColor(0x6B, 0x7A, 0x8F)
MIDGREY = RGBColor(0xA9, 0xB5, 0xC1)
BORDER = RGBColor(0xD8, 0xE2, 0xE9)
CODE_BG = RGBColor(0x0B, 0x22, 0x39)
CODE_TEXT = RGBColor(0xDF, 0xEB, 0xF4)
CODE_GREEN = RGBColor(0x9E, 0xD6, 0xC9)

BODY = "Segoe UI"
TITLE = "Segoe UI Semibold"
MONO = "Consolas"

SW, SH = 13.333, 7.5
ML = 0.65
MR = 0.65
CW = SW - ML - MR

SECTION = 1


# ---------------------------------------------------------------------------
# Helpers de forme et de texte
# ---------------------------------------------------------------------------
def _solid(shape, color: RGBColor) -> None:
    shape.fill.solid()
    shape.fill.fore_color.rgb = color


def add_rect(slide, x, y, w, h, fill, line=None, line_w=0.75, rounded=False, radius=0.08):
    shape_type = MSO_SHAPE.ROUNDED_RECTANGLE if rounded else MSO_SHAPE.RECTANGLE
    shape = slide.shapes.add_shape(shape_type, Inches(x), Inches(y), Inches(w), Inches(h))
    _solid(shape, fill)
    if line is None:
        shape.line.fill.background()
    else:
        shape.line.color.rgb = line
        shape.line.width = Pt(line_w)
    if rounded:
        try:
            shape.adjustments[0] = radius
        except Exception:
            pass
    try:
        shape.shadow.inherit = False
    except Exception:
        pass
    return shape


def add_line(slide, x1, y1, x2, y2, color, width=1.0, dash=None):
    line = slide.shapes.add_connector(
        MSO_CONNECTOR.STRAIGHT, Inches(x1), Inches(y1), Inches(x2), Inches(y2)
    )
    line.line.color.rgb = color
    line.line.width = Pt(width)
    if dash is not None:
        line.line.dash_style = dash
    return line


def _clear_tf(tf, margins=0.02):
    tf.clear()
    tf.word_wrap = True
    tf.margin_left = Inches(margins)
    tf.margin_right = Inches(margins)
    tf.margin_top = Inches(0.01)
    tf.margin_bottom = Inches(0.01)
    tf.vertical_anchor = MSO_ANCHOR.TOP


def _runs(p, text, size, color, bold=False, font=BODY):
    """Ajoute du texte en interprétant **gras**."""
    parts = str(text).split("**")
    for i, part in enumerate(parts):
        if not part:
            continue
        run = p.add_run()
        run.text = part
        run.font.name = font
        run.font.size = Pt(size)
        run.font.bold = bold or (i % 2 == 1)
        run.font.color.rgb = color


def _bullet(p, char="•", indent=0.22, color=None):
    pPr = p._p.get_or_add_pPr()
    pPr.set("marL", str(int(Inches(indent))))
    pPr.set("indent", str(-int(Inches(indent))))
    if color is not None:
        buClr = etree.SubElement(pPr, qn("a:buClr"))
        srgb = etree.SubElement(buClr, qn("a:srgbClr"))
        srgb.set("val", str(color))
    buFont = etree.SubElement(pPr, qn("a:buFont"))
    buFont.set("typeface", "Arial")
    buChar = etree.SubElement(pPr, qn("a:buChar"))
    buChar.set("char", char)


def add_text(
    slide,
    x,
    y,
    w,
    h,
    paragraphs,
    size=14,
    color=NAVY,
    bold=False,
    font=BODY,
    align=PP_ALIGN.LEFT,
    valign=MSO_ANCHOR.TOP,
    line_spacing=1.0,
    margins=0.02,
    **extra,
):
    """Ajoute un bloc de texte. paragraphs peut être str, liste de str ou de dict.

    Les paramètres supplémentaires (bullet, bullet_color, indent...) s'appliquent
    à chaque paragraphe.
    """
    box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = box.text_frame
    _clear_tf(tf, margins)
    tf.vertical_anchor = valign

    if isinstance(paragraphs, str):
        paragraphs = [paragraphs]

    for idx, item in enumerate(paragraphs):
        if isinstance(item, str):
            data = {"text": item}
        else:
            data = dict(item)
        for key, value in extra.items():
            data.setdefault(key, value)
        p = tf.paragraphs[0] if idx == 0 else tf.add_paragraph()
        p.alignment = data.get("align", align)
        p.line_spacing = data.get("line_spacing", line_spacing)
        p.space_before = Pt(data.get("space_before", 0))
        p.space_after = Pt(data.get("space_after", 0))
        p.level = data.get("level", 0)
        if data.get("bullet"):
            _bullet(p, data.get("char", "•"), data.get("indent", 0.22), data.get("bullet_color"))
        _runs(
            p,
            data.get("text", ""),
            data.get("size", size),
            data.get("color", color),
            data.get("bold", bold),
            data.get("font", font),
        )
    return box


def add_title(slide, title, eyebrow=None, number=None):
    if eyebrow:
        add_text(slide, ML, 0.34, CW, 0.24, eyebrow.upper(), size=9.2, color=TEAL, bold=True)
    add_text(slide, ML, 0.55, CW - 1.0, 0.62, title, size=26, color=NAVY, bold=True, font=TITLE)
    add_rect(slide, ML + 0.01, 1.20, 0.82, 0.05, ORANGE, rounded=False)
    if number is not None:
        add_text(slide, SW - 1.05, 0.33, 0.42, 0.25, f"{number:02d}", size=10, color=MIDGREY,
                 bold=True, align=PP_ALIGN.RIGHT)


def add_footer(slide, page, dark=False):
    fg = RGBColor(0x8F, 0xB0, 0xC0) if dark else MIDGREY
    add_line(slide, ML, 7.13, SW - MR, 7.13, RGBColor(0x2A, 0x55, 0x6B) if dark else BORDER, 0.6)
    add_text(slide, ML, 7.19, 8.0, 0.2, "CityFlow · Projet 3 · Écosystème Big Data 2",
             size=8.2, color=fg)
    add_text(slide, SW - MR - 1.2, 7.19, 1.2, 0.2, f"{page:02d} / 23", size=8.2, color=fg,
             align=PP_ALIGN.RIGHT)


def add_picture_fit(slide, path, x, y, w, h):
    img = Image.open(path)
    iw, ih = img.size
    ratio = min(w / iw, h / ih)
    nw, nh = iw * ratio, ih * ratio
    left = x + (w - nw) / 2
    top = y + (h - nh) / 2
    pic = slide.shapes.add_picture(str(path), Inches(left), Inches(top), Inches(nw), Inches(nh))
    pic.line.color.rgb = RGBColor(0xE6, 0xEC, 0xF1)
    pic.line.width = Pt(0.5)
    return pic


def add_chip(slide, x, y, w, h, text, fill=LIGHT, fg=NAVY, size=9.5, bold=True, border=None):
    add_rect(slide, x, y, w, h, fill, line=border, rounded=True, radius=0.28)
    add_text(slide, x + 0.08, y, w - 0.16, h, text, size=size, color=fg, bold=bold,
             align=PP_ALIGN.CENTER, valign=MSO_ANCHOR.MIDDLE, margins=0.02)


def add_kpi(slide, x, y, w, h, value, label, accent=TEAL, sub=None):
    add_rect(slide, x, y, w, h, WHITE, line=BORDER, rounded=True, radius=0.06)
    add_rect(slide, x + 0.02, y + 0.02, w - 0.04, 0.06, accent, rounded=False)
    add_text(slide, x + 0.16, y + 0.18, w - 0.32, 0.5, value, size=27, color=NAVY, bold=True,
             font=TITLE, margins=0.0)
    add_text(slide, x + 0.16, y + 0.66, w - 0.32, h - 0.7, label, size=9.2, color=GREY,
             margins=0.0, line_spacing=0.95)
    if sub:
        add_text(slide, x + 0.16, y + h - 0.25, w - 0.32, 0.18, sub, size=7.6, color=MIDGREY,
                 margins=0.0)


def add_table(slide, x, y, w, h, rows, col_widths, header=True, font_size=10.8,
              header_size=9.6, first_col_bold=False, row_h=None):
    nrows, ncols = len(rows), len(rows[0])
    shape = slide.shapes.add_table(nrows, ncols, Inches(x), Inches(y), Inches(w), Inches(h))
    table = shape.table
    table.first_row = header
    table.horz_banding = False
    for i, cw in enumerate(col_widths):
        table.columns[i].width = Inches(cw)
    if row_h:
        for i, rh in enumerate(row_h):
            table.rows[i].height = Inches(rh)
    for r in range(nrows):
        for c in range(ncols):
            cell = table.cell(r, c)
            cell.margin_left = Inches(0.09)
            cell.margin_right = Inches(0.07)
            cell.margin_top = Inches(0.035)
            cell.margin_bottom = Inches(0.025)
            cell.vertical_anchor = MSO_ANCHOR.MIDDLE
            tf = cell.text_frame
            tf.clear()
            tf.word_wrap = True
            p = tf.paragraphs[0]
            if header and r == 0:
                cell.fill.solid()
                cell.fill.fore_color.rgb = NAVY
                _runs(p, str(rows[r][c]), header_size, WHITE, bold=True)
                p.alignment = PP_ALIGN.LEFT
            else:
                cell.fill.solid()
                cell.fill.fore_color.rgb = WHITE if r % 2 else RGBColor(0xF7, 0xFA, 0xFC)
                bold = first_col_bold and c == 0
                _runs(p, str(rows[r][c]), font_size, NAVY, bold=bold)
                p.alignment = PP_ALIGN.LEFT
    return table


def crop_asset(name: str, top: int) -> Path:
    src = ASSETS / name
    dst = DECK_ASSETS / f"{Path(name).stem}_crop.png"
    if not dst.exists() or dst.stat().st_mtime < src.stat().st_mtime:
        img = Image.open(src)
        img.crop((0, top, img.width, img.height)).save(dst)
    return dst


def add_notes(slide, text):
    slide.notes_slide.notes_text_frame.text = text


def new_slide(prs):
    return prs.slides.add_slide(prs.slide_layouts[6])


def add_code_block(slide, x, y, w, h, label, code, caption):
    add_rect(slide, x, y, w, h, CODE_BG, line=RGBColor(0x1E, 0x46, 0x60), rounded=True, radius=0.05)
    add_text(slide, x + 0.18, y + 0.10, w - 0.36, 0.22, label.upper(), size=8.5, color=ORANGE,
             bold=True, margins=0.0)
    add_text(slide, x + 0.18, y + 0.36, w - 0.36, h - 0.68, code, size=9.0, color=CODE_TEXT,
             font=MONO, margins=0.0, line_spacing=0.98)
    add_text(slide, x + 0.18, y + h - 0.28, w - 0.36, 0.2, caption, size=8.7, color=RGBColor(0x9F, 0xB7, 0xC6),
             margins=0.0)


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------
prs = Presentation()
prs.slide_width = Inches(SW)
prs.slide_height = Inches(SH)

# ---------------------------------------------------------------------------
# 1. Couverture
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, NAVY_DARK)

# Monogramme
add_rect(s, 0.80, 0.58, 0.62, 0.62, TEAL, rounded=True, radius=0.16)
add_text(s, 0.80, 0.58, 0.62, 0.62, "CF", size=16, color=WHITE, bold=True, align=PP_ALIGN.CENTER,
         valign=MSO_ANCHOR.MIDDLE)
add_text(s, 1.58, 0.62, 6.4, 0.24, "PROJET 3  ·  ÉCOSYSTÈME BIG DATA 2", size=9.5,
         color=TEAL_LIGHT, bold=True)
add_text(s, 1.58, 0.86, 6.3, 0.24, "Prototype de traitement de flux en temps réel", size=10.5,
         color=RGBColor(0xA9, 0xC4, 0xD2))

add_text(s, 0.80, 1.62, 7.2, 0.92, "CityFlow", size=56, color=WHITE, bold=True, font=TITLE)
add_text(s, 0.82, 2.52, 7.1, 0.48, "Real-Time Traffic Intelligence", size=23,
         color=TEAL_LIGHT, font=TITLE)
add_text(
    s, 0.82, 3.20, 6.85, 1.15,
    "Transformer un flux continu de passages urbains en indicateurs de trafic\n"
    "fiables, par zone et par fenêtre de 10 minutes.",
    size=15.5, color=RGBColor(0xC9, 0xD9, 0xE2), line_spacing=1.12,
)
add_chip(s, 0.82, 4.42, 1.55, 0.34, "SPARK 3.5.3", fill=RGBColor(0x14, 0x45, 0x5F),
         fg=TEAL_LIGHT, size=8.6, border=RGBColor(0x2A, 0x5D, 0x75))
add_chip(s, 2.50, 4.42, 1.55, 0.34, "PYTHON 3.12", fill=RGBColor(0x14, 0x45, 0x5F),
         fg=TEAL_LIGHT, size=8.6, border=RGBColor(0x2A, 0x5D, 0x75))
add_chip(s, 4.18, 4.42, 2.00, 0.34, "STRUCTURED STREAMING", fill=RGBColor(0x14, 0x45, 0x5F),
         fg=TEAL_LIGHT, size=7.4, border=RGBColor(0x2A, 0x5D, 0x75))

add_line(s, 0.82, 5.52, 7.55, 5.52, RGBColor(0x2A, 0x5D, 0x75), 0.8)
add_text(s, 0.82, 5.70, 7.0, 0.26, "Mazine CHATER  ·  Yahya BAID  ·  Wenn Bouka DIEUMERCI",
         size=12.5, color=WHITE, bold=True)
add_text(s, 0.82, 6.08, 7.0, 0.24, "Module Écosystème Big Data 2  ·  Pr. Soukaina Gouraguine",
         size=10.5, color=RGBColor(0xA9, 0xC4, 0xD2))
add_text(s, 0.82, 6.38, 7.0, 0.24, "Année universitaire 2026–2027", size=10.5,
         color=RGBColor(0xA9, 0xC4, 0xD2))

# Panneau visuel droit
add_rect(s, 8.28, 0.52, 4.42, 6.46, NAVY_SOFT, line=RGBColor(0x2B, 0x60, 0x78), line_w=0.8,
         rounded=True, radius=0.045)
# Grille de rues
for gx in [8.65, 9.45, 10.25, 11.05, 11.85, 12.45]:
    add_line(s, gx, 0.85, gx, 5.95, RGBColor(0x1A, 0x4E, 0x67), 0.55)
for gy in [1.05, 1.85, 2.65, 3.45, 4.25, 5.05, 5.85]:
    add_line(s, 8.55, gy, 12.48, gy, RGBColor(0x1A, 0x4E, 0x67), 0.55)
# Axes de circulation
add_line(s, 8.55, 3.05, 12.48, 3.05, RGBColor(0x2C, 0x74, 0x7A), 5.0)
add_line(s, 8.55, 3.05, 12.48, 3.05, RGBColor(0xF2, 0xA5, 0x41), 1.0)
add_line(s, 10.65, 0.85, 10.65, 5.95, RGBColor(0x2C, 0x74, 0x7A), 3.2)
# Points de capteurs
for px, py in [(9.45, 1.85), (10.25, 2.65), (11.05, 3.45), (11.85, 4.25), (9.45, 4.25), (10.25, 5.05)]:
    add_rect(s, px - 0.055, py - 0.055, 0.11, 0.11, ORANGE, rounded=True, radius=0.5)
# Onde temps réel
add_rect(s, 10.55, 2.42, 0.22, 0.22, TEAL_LIGHT, rounded=True, radius=0.5)
add_rect(s, 10.39, 2.26, 0.54, 0.54, NAVY_SOFT, line=TEAL_LIGHT, line_w=1.2, rounded=True, radius=0.5)
add_rect(s, 10.20, 2.07, 0.92, 0.92, NAVY_SOFT, line=RGBColor(0x4F, 0x93, 0x93), line_w=0.9,
         rounded=True, radius=0.5)
add_text(s, 8.68, 3.18, 1.55, 0.22, "8 zones  ·  26 capteurs", size=8.3, color=RGBColor(0xBF, 0xD5, 0xDE),
         bold=True)

# Badge live
add_rect(s, 11.12, 0.78, 1.28, 0.30, ORANGE, rounded=True, radius=0.35)
add_text(s, 11.12, 0.78, 1.28, 0.30, "FLUX CONTINU", size=7.3, color=NAVY, bold=True,
         align=PP_ALIGN.CENTER, valign=MSO_ANCHOR.MIDDLE)

# Petits barres : 12 fichiers
bar_x = 8.67
for i, hgt in enumerate([0.28, 0.42, 0.36, 0.55, 0.48, 0.62, 0.51, 0.72, 0.58, 0.66, 0.54, 0.46]):
    add_rect(s, bar_x + i * 0.305, 6.42 - hgt, 0.18, hgt,
             ORANGE if i == 7 else TEAL_LIGHT, rounded=True, radius=0.18)
add_text(s, 8.67, 6.48, 3.6, 0.22, "12 fichiers  ·  07:00 → 09:00 UTC", size=8.2,
         color=RGBColor(0xBF, 0xD5, 0xDE), bold=True)
add_text(s, 8.67, 1.05, 2.8, 0.25, "TEMPS RÉEL", size=8.2, color=RGBColor(0x7F, 0xA9, 0xBC),
         bold=True)

add_notes(
    s,
    "Bonjour, nous sommes Mazine, Yahya et Wenn Bouka. Nous présentons CityFlow, "
    "un prototype de traitement de flux temps réel construit avec Apache Spark Structured Streaming. "
    "L'objectif : transformer un flux de passages de véhicules à Casablanca en indicateurs de trafic "
    "fiables par zone et par fenêtre de 10 minutes. Le projet couvre les quatre livrables demandés : "
    "présentation, code, dataset et fiche de TP.",
)

# ---------------------------------------------------------------------------
# 2. Sommaire
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Cinq temps pour raconter le projet", "Sommaire", 2)

agenda = [
    ("01", "Contexte & problématique", "Pourquoi le trafic urbain impose un traitement de flux.", TEAL),
    ("02", "Concepts du streaming", "Batch vs streaming, event-time, fenêtres, watermark.", NAVY),
    ("03", "Architecture & implémentation", "Du fichier JSON aux sinks Parquet et SQL.", ORANGE),
    ("04", "Résultats & analyse", "Volumes, congestion, latence, réconciliation.", TEAL),
    ("05", "Limites & perspectives", "Ce que le prototype prouve, et ce qu'il reste à faire.", NAVY),
]
y = 1.50
for num, title, desc, color in agenda:
    add_rect(s, ML, y, CW, 0.88, WHITE, line=BORDER, rounded=True, radius=0.09)
    add_rect(s, ML + 0.02, y + 0.02, 0.055, 0.84, color, rounded=False)
    add_text(s, ML + 0.22, y + 0.20, 0.55, 0.42, num, size=19, color=color, bold=True, font=TITLE)
    add_text(s, ML + 0.95, y + 0.15, 4.25, 0.33, title, size=15.2, color=NAVY, bold=True,
             font=TITLE)
    add_text(s, ML + 5.30, y + 0.20, 6.15, 0.42, desc, size=10.8, color=GREY)
    add_text(s, SW - MR - 0.55, y + 0.28, 0.30, 0.25, "→", size=14, color=color, bold=True,
             align=PP_ALIGN.RIGHT)
    y += 1.00

add_text(s, ML, 6.55, CW, 0.28,
         "Fil rouge : partir d'une question métier simple, puis montrer comment le temps de l'événement "
         "change la conception du pipeline.",
         size=10.5, color=GREY, bold=False)
add_footer(s, 2)
add_notes(
    s,
    "Le plan suit la démarche du cahier des charges : contexte, concepts, architecture, résultats, "
    "puis limites. Le fil rouge est le temps de l'événement : c'est lui qui explique les fenêtres, "
    "le watermark et la différence entre batch et streaming.",
)

# ---------------------------------------------------------------------------
# 3. Contexte
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Un flux continu, des décisions à prendre en minutes", "01 · Contexte", 3)

add_text(
    s, ML, 1.48, 6.25, 1.35,
    "Boucles inductives, caméras, GPS : les événements de trafic arrivent **en continu**, "
    "dans le désordre, et le volume dépasse rapidement un tableur ou un script pandas.",
    size=14.2, color=NAVY, line_spacing=1.12,
)
add_text(
    s, ML, 2.55, 6.05, 0.55,
    "Le vrai défi n'est pas seulement de calculer ; c'est de calculer **au bon moment**, "
    "sur le **bon intervalle de temps**.",
    size=12.4, color=GREY, line_spacing=1.10,
)

challenges = [
    ("VOLUME", "≈ 440 événements par minute", "8 zones, 5 types de véhicules, 2 heures de pointe."),
    ("DÉSORDRE", "4,2 % des événements arrivent en retard", "2 194 événements, de 6 à 45 minutes après leur occurrence."),
    ("FIABILITÉ", "Une fenêtre fermée ne doit plus bouger", "Même si un événement retardé arrive ensuite."),
]
y = 3.30
for tag, headline, detail in challenges:
    add_rect(s, ML, y, 6.20, 0.78, PALE, line=BORDER, rounded=True, radius=0.10)
    add_chip(s, ML + 0.14, y + 0.20, 1.03, 0.30, tag, fill=NAVY, fg=WHITE, size=7.4, border=None)
    add_text(s, ML + 1.32, y + 0.10, 4.75, 0.25, headline, size=11.2, color=NAVY, bold=True)
    add_text(s, ML + 1.32, y + 0.37, 4.75, 0.25, detail, size=8.9, color=GREY)
    y += 0.90

# KPI right
add_text(s, 7.28, 1.48, 5.2, 0.25, "LE TERRAIN DE JEU", size=8.8, color=TEAL, bold=True)
kpis = [
    ("8", "zones urbaines", "Casablanca", TEAL),
    ("2 h", "de flux simulé", "07:00 → 09:00 UTC", NAVY),
    ("52 846", "lignes générées", "dont 216 corrompues", ORANGE),
    ("10 min", "de fenêtre", "tumbling, event-time", TEAL),
]
for i, (value, label, sub, color) in enumerate(kpis):
    x = 7.28 + (i % 2) * 2.67
    yy = 1.82 + (i // 2) * 1.55
    add_rect(s, x, yy, 2.48, 1.30, WHITE, line=BORDER, rounded=True, radius=0.07)
    add_rect(s, x + 0.02, yy + 0.02, 0.055, 1.26, color, rounded=False)
    add_text(s, x + 0.18, yy + 0.14, 2.10, 0.46, value, size=24, color=NAVY, bold=True, font=TITLE)
    add_text(s, x + 0.18, yy + 0.63, 2.10, 0.26, label, size=10.2, color=NAVY, bold=True)
    add_text(s, x + 0.18, yy + 0.91, 2.10, 0.24, sub, size=8.5, color=GREY)

# Question band
add_rect(s, ML, 5.95, CW, 0.82, NAVY, rounded=True, radius=0.08)
add_rect(s, ML + 0.02, 5.97, 0.08, 0.78, ORANGE, rounded=False)
add_text(
    s, ML + 0.28, 6.04, CW - 0.55, 0.62,
    "« Comment calculer, en continu et de façon fiable, des indicateurs par zone et par fenêtre "
    "de 10 minutes malgré les retards ? »",
    size=12.8, color=WHITE, bold=True, valign=MSO_ANCHOR.MIDDLE, line_spacing=1.05,
)
add_footer(s, 3)
add_notes(
    s,
    "Le scénario est celui d'une régie de mobilité à Casablanca. Les capteurs produisent environ "
    "440 événements par minute. Deux difficultés s'ajoutent au volume : les événements arrivent "
    "parfois en retard, et une fenêtre déjà publiée ne doit plus être modifiée. D'où la question "
    "centrale du projet.",
)

# ---------------------------------------------------------------------------
# 4. Objectifs
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Quatre indicateurs, trois questions métier", "01 · Objectifs", 4)

questions = [
    ("Où se concentre le trafic ?", "Comparer le volume de passages par zone et par fenêtre."),
    ("Quand la congestion culmine-t-elle ?", "Croiser vitesse moyenne, vitesse limite et taux de congestion."),
    ("Que faire des événements tardifs ?", "Les accepter dans une limite bornée, puis les rejeter proprement."),
]
y = 1.50
for i, (q, detail) in enumerate(questions, 1):
    add_rect(s, ML, y, 6.15, 1.18, PALE, line=BORDER, rounded=True, radius=0.08)
    add_rect(s, ML + 0.18, y + 0.22, 0.46, 0.46, [TEAL, ORANGE, RED][i - 1], rounded=True, radius=0.5)
    add_text(s, ML + 0.18, y + 0.22, 0.46, 0.46, str(i), size=14, color=WHITE, bold=True,
             align=PP_ALIGN.CENTER, valign=MSO_ANCHOR.MIDDLE)
    add_text(s, ML + 0.82, y + 0.15, 5.10, 0.30, q, size=13.4, color=NAVY, bold=True)
    add_text(s, ML + 0.82, y + 0.50, 5.05, 0.55, detail, size=9.8, color=GREY, line_spacing=1.02)
    y += 1.33

add_text(s, 7.12, 1.48, 5.55, 0.26, "SCHÉMA DE SORTIE (GOLD)", size=8.8, color=TEAL, bold=True)
rows = [
    ["Indicateur", "Calcul", "Usage"],
    ["vehicle_count", "nombre de passages", "charge"],
    ["avg / median / p95 speed", "km/h", "fluidité"],
    ["congestion_rate", "part < 60 % de la limite", "alerte"],
    ["traffic_state", "seuils métier", "lecture"],
]
add_table(s, 7.12, 1.80, 5.55, 2.85, rows, [1.55, 2.45, 1.55], font_size=9.7, header_size=8.7,
          first_col_bold=True, row_h=[0.42, 0.58, 0.62, 0.62, 0.55])
add_rect(s, 7.12, 4.95, 5.55, 1.36, LIGHT, line=None, rounded=True, radius=0.08)
add_text(s, 7.30, 5.08, 5.18, 0.24, "CONTRAINTE DE SORTIE", size=8.6, color=TEAL, bold=True)
add_text(
    s, 7.30, 5.34, 5.18, 0.85,
    "Chaque agrégat est **immuable une fois finalisé**. Les données tardives sont tracées, "
    "pas injectées dans une fenêtre déjà publiée.",
    size=9.8, color=NAVY, line_spacing=1.05,
)

add_text(s, ML, 6.55, CW, 0.28,
         "Périmètre : 8 zones · 12 fichiers de 10 min · 2 heures de trafic · sorties Parquet + table SQL.",
         size=10.0, color=GREY)
add_footer(s, 4)
add_notes(
    s,
    "Le prototype doit répondre à trois questions : où, quand, et que faire des retards. "
    "La sortie Gold contient quatre familles d'indicateurs : volume, vitesses, taux de congestion "
    "et état de trafic. La contrainte forte est l'immuabilité d'une fenêtre finalisée.",
)

# ---------------------------------------------------------------------------
# 5. Batch vs streaming
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Batch ou streaming ? La même donnée, pas le même délai", "02 · Concepts", 5)
pic = crop_asset("fig_batch_vs_streaming.png", 75)
add_picture_fit(s, pic, 0.55, 1.38, 12.23, 4.95)
add_rect(s, ML, 6.48, CW, 0.50, PALE, line=None, rounded=True, radius=0.10)
add_text(
    s, ML + 0.20, 6.54, CW - 0.40, 0.38,
    "Batch : 25,1 s pour calculer les 96 agrégats, une fois la collecte terminée.   "
    "Streaming : premier résultat à ~4 s, puis publication au fil de l'eau.",
    size=10.6, color=NAVY, bold=True, valign=MSO_ANCHOR.MIDDLE,
)
add_footer(s, 5)
add_notes(
    s,
    "Le batch attend que tous les fichiers soient disponibles. Le streaming démarre avec le premier "
    "fichier et publie au fil de l'eau. Sur ce jeu de données, le batch met 25,1 secondes et le "
    "premier micro-batch streamé produit un résultat en environ 4 secondes. Ce n'est pas une "
    "compétition de vitesse : les deux modes répondent à des besoins différents.",
)

# ---------------------------------------------------------------------------
# 6. Source -> transformation -> sink
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Le modèle source → transformation → sink", "02 · Concepts", 6)

cards = [
    ("SOURCE", "D'où viennent les données ?", TEAL, [
        "Fichiers JSON Lines déposés progressivement",
        "Schéma explicite, 1 fichier par micro-batch",
        "8 zones · 5 types de véhicules",
    ], "readStream"),
    ("TRANSFORMATION", "Que fait-on des données ?", NAVY, [
        "Nettoyage, typage, validation métier",
        "Jointure broadcast avec zones.csv",
        "Fenêtre 10 min + watermark",
    ], "DataFrame"),
    ("SINK", "Où vont les résultats ?", ORANGE, [
        "Bronze : événements enrichis",
        "Gold : agrégats par zone et fenêtre",
        "Mémoire : table SQL pour l'analyse",
    ], "writeStream"),
]
x = 0.65
for title, question, color, items, tag in cards:
    add_rect(s, x, 1.52, 3.67, 2.95, WHITE, line=BORDER, rounded=True, radius=0.07)
    add_rect(s, x, 1.52, 3.67, 0.10, color, rounded=False)
    add_text(s, x + 0.20, 1.72, 3.25, 0.28, title, size=10.2, color=color, bold=True)
    add_text(s, x + 0.20, 2.02, 3.25, 0.25, question, size=9.2, color=GREY)
    yy = 2.42
    for item in items:
        add_text(s, x + 0.20, yy, 3.28, 0.48, item, size=9.8, color=NAVY, bullet=True,
                 bullet_color=color, indent=0.18, line_spacing=0.98)
        yy += 0.48
    add_chip(s, x + 0.20, 3.92, 1.35, 0.28, tag, fill=PALE, fg=GREY, size=7.8, border=BORDER)
    x += 3.99

for ax in [4.31, 8.30]:
    add_rect(s, ax, 2.74, 0.34, 0.38, ORANGE, rounded=True, radius=0.20)
    add_text(s, ax - 0.01, 2.74, 0.36, 0.38, "→", size=13, color=WHITE, bold=True,
             align=PP_ALIGN.CENTER, valign=MSO_ANCHOR.MIDDLE)

add_rect(s, ML, 4.80, CW, 1.55, LIGHT, line=None, rounded=True, radius=0.07)
add_text(s, ML + 0.25, 4.95, 3.15, 0.28, "LE POINT À RETENIR", size=8.6, color=TEAL, bold=True)
add_text(
    s, ML + 0.25, 5.23, 5.55, 0.95,
    "Le moteur est **paresseux** : aucune donnée n'est lue tant qu'un sink n'est pas démarré. "
    "Chaque sink matérialise une vue différente du même flux.",
    size=10.6, color=NAVY, line_spacing=1.08,
)
add_text(s, 7.10, 5.10, 5.55, 0.28, "DANS CITYFLOW", size=8.6, color=TEAL, bold=True)
add_text(
    s, 7.10, 5.38, 5.55, 0.85,
    "3 sinks · 2 checkpoints Parquet · 1 source JSON · 1 référentiel statique diffusé.",
    size=11.2, color=NAVY, bold=True, line_spacing=1.05,
)
add_footer(s, 6)
add_notes(
    s,
    "Structured Streaming repose sur trois briques : la source, les transformations et le sink. "
    "La source est un répertoire JSON ; les transformations nettoient, enrichissent et agrègent ; "
    "les sinks écrivent les événements enrichis, les agrégats et une table mémoire pour l'analyse. "
    "Le moteur est paresseux : sans writeStream.start(), rien n'est exécuté.",
)

# ---------------------------------------------------------------------------
# 7. Table infinie et micro-batch
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Une table infinie, découpée en micro-batchs", "02 · Concepts", 7)

add_text(s, ML, 1.43, 4.0, 0.24, "LE FLUX", size=8.8, color=TEAL, bold=True)
add_line(s, 1.70, 2.08, 12.36, 2.08, MIDGREY, 1.2)
add_rect(s, 12.34, 1.99, 0.14, 0.18, ORANGE, rounded=True, radius=0.2)
for i in range(12):
    x = 0.78 + i * 0.965
    color = NAVY if i % 3 == 0 else TEAL
    add_rect(s, x, 1.78, 0.72, 0.58, color, rounded=True, radius=0.10)
    add_text(s, x, 1.78, 0.72, 0.58, f"F{i + 1}", size=9.0, color=WHITE, bold=True,
             align=PP_ALIGN.CENTER, valign=MSO_ANCHOR.MIDDLE)
    add_text(s, x - 0.03, 2.42, 0.78, 0.22, f"{(7 + i // 6):02d}:{(i % 6) * 10:02d}",
             size=7.0, color=GREY, align=PP_ALIGN.CENTER)

add_text(s, ML, 2.88, 5.8, 0.28, "À CHAQUE LOT, TROIS OPÉRATIONS", size=8.8, color=TEAL, bold=True)

steps = [
    ("1", "Lire", "Spark ne lit que les nouvelles lignes depuis le dernier offset.", TEAL),
    ("2", "Mettre à jour l'état", "Le State Store conserve les agrégats des fenêtres encore ouvertes.", NAVY),
    ("3", "Écrire", "Le résultat part vers le sink, puis le checkpoint note la progression.", ORANGE),
]
x = 0.65
for num, title, desc, color in steps:
    add_rect(s, x, 3.20, 3.83, 1.65, PALE, line=BORDER, rounded=True, radius=0.07)
    add_rect(s, x + 0.18, 3.38, 0.44, 0.44, color, rounded=True, radius=0.5)
    add_text(s, x + 0.18, 3.38, 0.44, 0.44, num, size=13, color=WHITE, bold=True,
             align=PP_ALIGN.CENTER, valign=MSO_ANCHOR.MIDDLE)
    add_text(s, x + 0.76, 3.36, 2.85, 0.30, title, size=12.4, color=NAVY, bold=True)
    add_text(s, x + 0.76, 3.72, 2.82, 0.85, desc, size=9.8, color=GREY, line_spacing=1.02)
    x += 3.99

add_rect(s, ML, 5.25, CW, 0.82, NAVY, rounded=True, radius=0.07)
add_text(
    s, ML + 0.25, 5.36, CW - 0.50, 0.58,
    "13 micro-batchs  ·  1 fichier = 1 batch  ·  l'état est conservé entre les lots  ·  "
    "le flux n'est jamais chargé en entier.",
    size=11.6, color=WHITE, bold=True, valign=MSO_ANCHOR.MIDDLE,
)
add_text(s, ML, 6.35, CW, 0.50,
         "Le premier batch (B0) est plus lent : démarrage de la JVM, de Catalyst et du State Store. "
         "Les suivants traitent environ 4 400 événements.",
         size=9.5, color=GREY)
add_footer(s, 7)
add_notes(
    s,
    "On peut voir un flux comme une table infinie. Spark la découpe en micro-batchs. "
    "Chaque lot lit uniquement les nouvelles lignes, met à jour l'état des fenêtres encore ouvertes, "
    "écrit le résultat et sauvegarde sa progression dans un checkpoint. Ici, un fichier correspond "
    "à un micro-batch, soit 13 lots au total, dont un lot final vide.",
)

# ---------------------------------------------------------------------------
# 8. Event time / watermark
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Le temps de l'événement, pas celui de la machine", "02 · Concepts", 8)

blocks = [
    ("TEMPS DE L'ÉVÉNEMENT", "Mesuré par le capteur, au moment du passage.", "event_time", TEAL),
    ("TEMPS DE TRAITEMENT", "Moment où Spark reçoit et traite la donnée.", "processing time", NAVY),
    ("FENÊTRE TUMBLANTE", "Intervalles disjoints de 10 minutes, sans recouvrement.", "window(...)", ORANGE),
]
y = 1.48
for label, desc, code, color in blocks:
    add_rect(s, ML, y, 5.65, 1.02, PALE, line=BORDER, rounded=True, radius=0.08)
    add_rect(s, ML + 0.02, y + 0.02, 0.06, 0.98, color, rounded=False)
    add_text(s, ML + 0.20, y + 0.12, 3.7, 0.24, label, size=8.7, color=color, bold=True)
    add_text(s, ML + 0.20, y + 0.40, 5.20, 0.24, desc, size=9.8, color=NAVY)
    add_chip(s, ML + 4.55, y + 0.54, 0.97, 0.25, code, fill=WHITE, fg=GREY, size=6.8, border=BORDER)
    y += 1.15

add_text(s, ML, 5.05, 5.65, 0.24, "WATERMARK", size=8.7, color=RED, bold=True)
add_text(
    s, ML, 5.30, 5.65, 0.85,
    "Tolérance de **10 minutes** : Spark accepte les retards dans cette limite, puis ferme "
    "définitivement les fenêtres dépassées.",
    size=10.3, color=NAVY, line_spacing=1.05,
)
add_text(
    s, ML, 6.18, 5.65, 0.55,
    "Compromis : plus le watermark est grand, plus on accepte de retards, mais plus l'état "
    "mémoire et la latence de finalisation augmentent.",
    size=9.2, color=GREY, line_spacing=1.02,
)

# Timeline
add_rect(s, 6.58, 1.48, 6.10, 5.28, RGBColor(0xF7, 0xFA, 0xFC), line=BORDER, rounded=True, radius=0.04)
add_text(s, 6.82, 1.66, 5.60, 0.25, "UNE FENÊTRE SE FERME QUAND LE WATERMARK LA DÉPASSE",
         size=8.3, color=TEAL, bold=True)
add_line(s, 6.95, 4.28, 12.38, 4.28, MIDGREY, 1.2)
for tx, label in [(7.00, "07:00"), (8.55, "07:10"), (10.10, "07:20"), (11.65, "07:30")]:
    add_line(s, tx, 4.22, tx, 4.34, MIDGREY, 1.0)
    add_text(s, tx - 0.25, 4.42, 0.58, 0.20, label, size=7.8, color=GREY, align=PP_ALIGN.CENTER)

# Fenêtres
windows = [
    (7.05, "07:00–07:10", "FERMÉE", RGBColor(0xD9, 0xDF, 0xE5)),
    (8.60, "07:10–07:20", "FERMÉE", RGBColor(0xD9, 0xDF, 0xE5)),
    (10.15, "07:20–07:30", "OUVERTE", TEAL_LIGHT),
]
for wx, label, status, color in windows:
    add_rect(s, wx, 3.26, 1.45, 0.66, color, line=None, rounded=True, radius=0.10)
    add_text(s, wx, 3.32, 1.45, 0.22, label, size=8.3, color=NAVY, bold=True, align=PP_ALIGN.CENTER)
    add_text(s, wx, 3.56, 1.45, 0.18, status, size=6.8, color=WHITE if status == "OUVERTE" else GREY,
             bold=True, align=PP_ALIGN.CENTER)

# Events
for ex, ey, color in [(7.40, 4.10, RED), (8.90, 3.98, NAVY), (10.52, 4.12, TEAL)]:
    add_rect(s, ex, ey, 0.12, 0.12, color, rounded=True, radius=0.5)

# Watermark
wm_x = 11.60
add_line(s, wm_x, 1.98, wm_x, 4.28, RED, 1.4, dash=None)
add_chip(s, 10.58, 2.12, 1.72, 0.38, "WATERMARK = MAX − 10 MIN", fill=RED, fg=WHITE, size=7.1)

# Événement tardif : annotation sur deux lignes, sous l'axe et avant le watermark
add_text(
    s, 7.55, 4.60, 3.90, 0.48,
    [
        {"text": "07:08 arrivé à 07:35", "bold": True},
        {"text": "→ fenêtre 07:00–07:10 déjà fermée"},
    ],
    size=8.4, color=RED, line_spacing=1.0,
)
add_line(s, 7.48, 4.55, 7.42, 4.23, RED, 1.1)
add_text(
    s, 6.88, 5.18, 5.55, 0.80,
    "Chaque point est un événement placé par son **event_time**. L'événement rouge est "
    "**comptabilisé comme rejeté**, pas injecté dans l'agrégat.",
    size=9.4, color=NAVY, line_spacing=1.05,
)
add_footer(s, 8)
add_notes(
    s,
    "La distinction fondamentale est entre le temps de l'événement et le temps de traitement. "
    "CityFlow agrège sur event_time, avec des fenêtres tumblantes de 10 minutes. Le watermark "
    "de 10 minutes indique jusqu'où Spark accepte encore des retards. Passé cette frontière, "
    "la fenêtre est fermée : un événement trop tardif est rejeté et comptabilisé, pas intégré.",
)

# ---------------------------------------------------------------------------
# 9. Architecture
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Architecture : un flux, trois sinks", "03 · Architecture", 9)
pic = crop_asset("fig_architecture.png", 140)
add_picture_fit(s, pic, 0.52, 1.36, 12.30, 5.28)
add_text(s, ML, 6.78, CW, 0.24,
         "Les deux sinks Parquet ont chacun leur checkpoint : une requête peut reprendre sans relire tout le flux.",
         size=9.4, color=GREY)
add_footer(s, 9)
add_notes(
    s,
    "L'architecture est volontairement simple : une source JSON, des transformations Spark, et trois "
    "sinks. Bronze conserve les événements enrichis pour l'audit et le rejeu ; Gold contient les "
    "agrégats partitionnés par zone ; le sink mémoire sert au debug et aux requêtes SQL. "
    "Chaque sink Parquet possède son propre checkpoint.",
)

# ---------------------------------------------------------------------------
# 10. Choix technologiques
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Des choix simples, justifiés par le périmètre", "03 · Choix technologiques", 10)

rows = [
    ["Technologie", "Rôle", "Pourquoi ce choix"],
    ["Spark Structured Streaming", "Moteur de flux", "event-time, watermark, checkpoints, API DataFrame"],
    ["PySpark 3.5.3", "Développement", "Même code en batch et en streaming, notebook + CLI"],
    ["JSON Lines + schéma", "Source", "Simule l'arrivée progressive, lisible et tolérant"],
    ["Parquet partitionné", "Sink durable", "Colonnaire, compressé, compatible Spark et SQL"],
    ["Python 3.12 · Jupyter", "Reproductibilité", "Démonstration pas à pas et scripts automatisés"],
]
add_table(s, ML, 1.50, 8.20, 4.65, rows, [2.40, 1.72, 4.08], font_size=9.7, header_size=8.7,
          first_col_bold=True, row_h=[0.44, 0.82, 0.82, 0.86, 0.86, 0.85])

add_rect(s, 9.15, 1.50, 3.52, 4.65, NAVY, rounded=True, radius=0.06)
add_text(s, 9.38, 1.70, 3.05, 0.28, "POURQUOI PAS KAFKA ?", size=9.4, color=ORANGE, bold=True)
kafka_points = [
    "Non imposé par le cahier des charges.",
    "Complexité d'exploitation supplémentaire.",
    "Le répertoire JSON suffit à démontrer les sémantiques de flux.",
    "L'architecture reste source-agnostique : Kafka peut remplacer les fichiers.",
]
y = 2.10
for point in kafka_points:
    add_text(s, 9.38, y, 3.05, 0.62, point, size=9.2, color=WHITE, bullet=True,
             bullet_color=ORANGE, indent=0.18, line_spacing=0.98)
    y += 0.62
add_rect(s, 9.38, 5.15, 3.05, 0.72, RGBColor(0x14, 0x4B, 0x67), line=None, rounded=True, radius=0.08)
add_text(s, 9.52, 5.24, 2.78, 0.54,
         "En production :\nKafka → Spark → Delta / Iceberg", size=8.9, color=TEAL_LIGHT, bold=True,
         line_spacing=1.02)

add_text(s, ML, 6.46, CW, 0.28,
         "Environnement testé : Windows 11 · Java 17 (Temurin) · Python 3.12.10 · PySpark 3.5.3.",
         size=9.4, color=GREY)
add_footer(s, 10)
add_notes(
    s,
    "Les choix techniques sont guidés par le périmètre et la reproductibilité. Spark Structured "
    "Streaming apporte nativement la gestion du temps de l'événement, le watermark et les "
    "checkpoints. Kafka n'était pas imposé ; l'ajouter aurait complexifié l'installation sans "
    "changer les concepts démontrés. La source est remplaçable : seul le readStream change.",
)

# ---------------------------------------------------------------------------
# 11. Dataset
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Un jeu de données synthétique, mais exigeant", "03 · Dataset (L3)", 11)

chips = [
    ("12", "fichiers"),
    ("52 846", "lignes"),
    ("8", "zones"),
    ("2 h", "de trafic"),
    ("4,2 %", "en retard"),
    ("0,4 %", "corrompues"),
]
x = 0.65
for value, label in chips:
    add_rect(s, x, 1.42, 1.93, 0.78, PALE, line=BORDER, rounded=True, radius=0.10)
    add_text(s, x + 0.12, 1.49, 1.69, 0.34, value, size=17, color=NAVY, bold=True, font=TITLE,
             align=PP_ALIGN.CENTER)
    add_text(s, x + 0.08, 1.87, 1.77, 0.22, label, size=8.3, color=GREY, align=PP_ALIGN.CENTER)
    x += 2.00

rows = [
    ["Colonne", "Type", "Description"],
    ["event_id", "string", "identifiant unique du passage"],
    ["event_time", "timestamp", "horodatage UTC (ISO-8601)"],
    ["zone_id / zone_name", "string", "zone géographique"],
    ["sensor_id", "string", "capteur à l'origine de la mesure"],
    ["lane", "int", "voie de circulation"],
    ["vehicle_type", "string", "car, taxi, moto, bus, truck"],
    ["speed_kmh", "double", "vitesse instantanée mesurée"],
]
add_text(s, ML, 2.48, 6.45, 0.24, "SCHÉMA DU FLUX", size=8.6, color=TEAL, bold=True)
add_table(s, ML, 2.76, 6.45, 3.35, rows, [1.70, 1.05, 3.70], font_size=8.9, header_size=8.3,
          first_col_bold=True, row_h=[0.36, 0.40, 0.40, 0.40, 0.40, 0.38, 0.40, 0.40])

add_rect(s, 7.32, 2.48, 5.35, 3.63, LIGHT, line=None, rounded=True, radius=0.06)
add_text(s, 7.52, 2.62, 4.95, 0.24, "ORIGINE & CONCEPTION", size=8.6, color=TEAL, bold=True)
origin = [
    "Générateur Python **déterministe** (graine 20261102).",
    "Scénario Casablanca, 2 novembre 2026, 07:00–09:00 UTC.",
    "Profil de pointe + 8 zones + 5 types de véhicules.",
    "Référentiel zones.csv : limite, voies, arrondissement.",
    "Aucune donnée personnelle : données entièrement simulées.",
]
y = 2.94
for item in origin:
    add_text(s, 7.52, y, 4.88, 0.48, item, size=9.5, color=NAVY, bullet=True,
             bullet_color=ORANGE, indent=0.18, line_spacing=0.98)
    y += 0.54
add_text(s, 7.52, 5.62, 4.88, 0.35,
         "12 fichiers livrés progressivement, un par tranche de 10 minutes.",
         size=8.8, color=GREY, bold=True)

add_text(s, ML, 6.38, CW, 0.42,
         "Le dataset contient volontairement des lignes corrompues et des retards : "
         "le pipeline doit les traiter, pas les ignorer.",
         size=9.6, color=GREY)
add_footer(s, 11)
add_notes(
    s,
    "Le dataset est synthétique mais construit pour être exigeant : 12 fichiers, 52 846 lignes, "
    "8 zones, 4,2 % d'événements retardés (2 194) et 0,4 % de lignes JSON corrompues. Le générateur est "
    "déterministe, donc les résultats sont reproductibles. Le référentiel zones.csv apporte la "
    "limite de vitesse et le nombre de voies, nécessaires au calcul de congestion.",
)

# ---------------------------------------------------------------------------
# 12. Implémentation
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Le pipeline en quatre gestes", "03 · Implémentation", 12)

blocks = [
    (
        0.65, 1.48, "01 · Lire",
        "events = (spark.readStream\n"
        "    .schema(EVENT_SCHEMA)\n"
        "    .json(STREAM_DIR))",
        "Schéma explicite : pas d'inférence à chaque lot.",
    ),
    (
        6.63, 1.48, "02 · Nettoyer & enrichir",
        "clean = (events\n"
        "    .filter(\"event_id IS NOT NULL\")\n"
        "    .join(broadcast(zones), \"zone_id\"))",
        "Validation métier, typage, jointure stream-static.",
    ),
    (
        0.65, 3.52, "03 · Agréger",
        "agg = (clean\n"
        "    .withWatermark(\"event_time\", \"10 minutes\")\n"
        "    .groupBy(window(\"event_time\", \"10 minutes\"),\n"
        "             \"zone_id\")\n"
        "    .agg(count(\"*\").alias(\"vehicle_count\"), ...))",
        "Le temps de l'événement pilote le découpage.",
    ),
    (
        6.63, 3.52, "04 · Écrire",
        "agg.writeStream\n"
        "    .outputMode(\"append\")\n"
        "    .option(\"checkpointLocation\", path)\n"
        "    .foreachBatch(write_gold)\n"
        "    .start()",
        "Parquet Gold, métriques par lot, reprise possible.",
    ),
]
for x, y, label, code, caption in blocks:
    add_code_block(s, x, y, 5.70, 1.82, label, code, caption)

add_rect(s, ML, 5.62, CW, 0.72, NAVY, rounded=True, radius=0.08)
add_text(
    s, ML + 0.22, 5.69, CW - 0.45, 0.56,
    "Le code réutilisable est dans cityflow_lib.py et cityflow_pipeline.py — "
    "modes available, live et batch.",
    size=10.2, color=WHITE, bold=True, valign=MSO_ANCHOR.MIDDLE,
)
add_text(s, ML, 6.52, CW, 0.35,
         "Le notebook CityFlow_Streaming.ipynb déroule la même démarche pas à pas, avec les sorties déjà exécutées.",
         size=9.2, color=GREY)
add_footer(s, 12)
add_notes(
    s,
    "Le pipeline tient en quatre gestes : lire avec un schéma explicite, nettoyer et enrichir, "
    "agréger avec watermark et fenêtre, puis écrire dans un sink checkpointé. Le code est "
    "factorisé dans cityflow_lib.py et pilotable en ligne de commande avec trois modes : "
    "available, live et batch. Le notebook montre la même démarche de façon pédagogique.",
)

# ---------------------------------------------------------------------------
# 13. Fiabilité
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Les garde-fous qui rendent le résultat défendable", "03 · Robustesse", 13)

guards = [
    ("Schéma explicite", "Types imposés à l'ingestion ; une ligne invalide devient NULL puis est filtrée.", TEAL),
    ("Validation métier", "Zone inconnue, vitesse hors plage ou timestamp invalide : rejet contrôlé.", NAVY),
    ("Watermark 10 min", "Les retards sont bornés ; les fenêtres fermées ne sont plus modifiées.", ORANGE),
    ("Checkpoints séparés", "Chaque requête reprend à son propre offset, sans relire tout le flux.", TEAL),
    ("Jointure broadcast", "zones.csv est petite et diffusée : pas de shuffle à chaque micro-batch.", NAVY),
    ("approx_count_distinct", "Compatible streaming, contrairement à countDistinct sur un flux.", ORANGE),
]
for i, (title, desc, color) in enumerate(guards):
    x = 0.65 + (i % 3) * 4.08
    y = 1.50 + (i // 3) * 1.78
    add_rect(s, x, y, 3.80, 1.53, WHITE, line=BORDER, rounded=True, radius=0.07)
    add_rect(s, x + 0.02, y + 0.02, 0.06, 1.49, color, rounded=False)
    add_text(s, x + 0.20, y + 0.15, 3.35, 0.28, title, size=11.4, color=NAVY, bold=True)
    add_text(s, x + 0.20, y + 0.48, 3.35, 0.90, desc, size=9.2, color=GREY, line_spacing=1.02)

add_rect(s, ML, 5.22, CW, 1.23, PALE, line=BORDER, rounded=True, radius=0.06)
add_text(s, ML + 0.22, 5.35, 2.20, 0.24, "PIÈGES CORRIGÉS", size=8.6, color=RED, bold=True)
add_text(
    s, ML + 0.22, 5.62, 11.70, 0.70,
    "countDistinct → approx_count_distinct   ·   input_file_name() avant la jointure statique   ·   "
    "sink mémoire en mode complete   ·   foreachBatch avec count(), pas rdd.isEmpty().",
    size=9.7, color=NAVY, bold=True, line_spacing=1.08,
)
add_footer(s, 13)
add_notes(
    s,
    "Six garde-fous rendent le résultat défendable : schéma explicite, validation métier, watermark, "
    "checkpoints, broadcast et approximation compatible streaming. Nous avons aussi corrigé quatre "
    "pièges classiques : countDistinct interdit en streaming, input_file_name à ajouter avant la "
    "jointure, sink mémoire en mode complete, et foreachBatch qui doit utiliser count() plutôt que "
    "rdd.isEmpty().",
)

# ---------------------------------------------------------------------------
# 14. KPIs
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Deux heures de trafic, 86 secondes de calcul", "04 · Résultats", 14)

# Hero
add_rect(s, ML, 1.50, 3.25, 3.55, NAVY, rounded=True, radius=0.05)
add_text(s, ML + 0.24, 1.68, 2.80, 0.24, "DURÉE DU JOB STREAMING", size=8.4, color=TEAL_LIGHT, bold=True)
add_text(s, ML + 0.24, 2.00, 2.80, 0.70, "86,3 s", size=36, color=WHITE, bold=True, font=TITLE)
add_text(s, ML + 0.24, 2.70, 2.72, 0.50, "pour traiter 12 fichiers\net 13 micro-batchs", size=10.2,
         color=RGBColor(0xC9, 0xD9, 0xE2), line_spacing=1.02)
add_line(s, ML + 0.24, 3.44, ML + 2.98, 3.44, RGBColor(0x2A, 0x5D, 0x75), 0.8)
add_text(s, ML + 0.24, 3.58, 2.75, 0.22, "BATCH COMPLET", size=8.0, color=TEAL_LIGHT, bold=True)
add_text(s, ML + 0.24, 3.84, 2.75, 0.38, "25,1 s", size=22, color=WHITE, bold=True, font=TITLE)
add_text(s, ML + 0.24, 4.30, 2.75, 0.52, "dont 16,3 s de calcul\net 8,8 s d'écriture", size=8.8,
         color=RGBColor(0xC9, 0xD9, 0xE2), line_spacing=1.00)

metrics = [
    ("52 630", "événements valides", "99,6 % des lignes", TEAL),
    ("96", "agrégats zone × fenêtre", "12 fenêtres × 8 zones", NAVY),
    ("80", "agrégats finalisés", "10 fenêtres × 8 zones", ORANGE),
    ("43 710", "événements dans Gold", "83,1 % du flux valide", TEAL),
    ("198", "rejetés par watermark", "0,4 % du flux valide", RED),
    ("8 722", "en fenêtres ouvertes", "16 agrégats non finalisés", ORANGE),
]
for i, (value, label, sub, color) in enumerate(metrics):
    x = 4.14 + (i % 3) * 2.83
    y = 1.50 + (i // 3) * 1.72
    add_kpi(s, x, y, 2.67, 1.55, value, label, accent=color, sub=sub)

add_rect(s, 4.14, 5.02, 8.32, 0.78, PALE, line=BORDER, rounded=True, radius=0.08)
add_text(
    s, 4.34, 5.12, 7.92, 0.56,
    "13 micro-batchs  ·  6,9 s de latence moyenne (5,9 s en régime établi)  ·  "
    "610 événements/s  ·  0,4 % de rejets.",
    size=9.7, color=NAVY, bold=True, valign=MSO_ANCHOR.MIDDLE,
)
add_text(s, ML, 6.35, CW, 0.32,
         "Les 8 722 événements restants appartiennent aux deux dernières fenêtres, encore ouvertes "
         "à la fin du flux fini.", size=9.2, color=GREY)
add_footer(s, 14)
add_notes(
    s,
    "Le job streaming traite les 12 fichiers en 86,3 secondes, avec 13 micro-batchs. Le batch complet "
    "met 25,1 secondes, mais il attend la fin de la collecte. Sur les 52 630 événements valides, "
    "43 710 sont finalisés dans Gold, 198 sont rejetés par le watermark et 8 722 restent dans les "
    "deux dernières fenêtres ouvertes. La latence moyenne par micro-batch est de 6,9 secondes, "
    "ou 5,9 secondes en régime établi.",
)

# ---------------------------------------------------------------------------
# 15. Traffic by zone
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Le trafic se concentre au centre, sans explosion de volume", "04 · Résultats", 15)
pic = crop_asset("fig_trafic_par_zone.png", 64)
add_picture_fit(s, pic, 0.50, 1.40, 9.15, 4.85)

add_rect(s, 9.95, 1.45, 2.78, 1.13, NAVY, rounded=True, radius=0.07)
add_text(s, 10.12, 1.55, 2.45, 0.22, "ZONE LA PLUS CHARGÉE", size=7.4, color=TEAL_LIGHT, bold=True)
add_text(s, 10.12, 1.78, 2.45, 0.42, "8 377", size=24, color=WHITE, bold=True, font=TITLE)
add_text(s, 10.12, 2.18, 2.45, 0.22, "passages · Centre-Ville", size=8.3, color=RGBColor(0xC9, 0xD9, 0xE2))

add_rect(s, 9.95, 2.73, 2.78, 1.08, PALE, line=BORDER, rounded=True, radius=0.07)
add_text(s, 10.12, 2.82, 2.45, 0.22, "DEUXIÈME ZONE", size=7.4, color=TEAL, bold=True)
add_text(s, 10.12, 3.04, 2.45, 0.38, "7 793", size=21, color=NAVY, bold=True, font=TITLE)
add_text(s, 10.12, 3.42, 2.45, 0.22, "passages · Maarif", size=8.3, color=GREY)

add_rect(s, 9.95, 3.96, 2.78, 1.22, PALE, line=BORDER, rounded=True, radius=0.07)
add_text(s, 10.12, 4.05, 2.45, 0.22, "VOLUME PAR FENÊTRE", size=7.4, color=ORANGE, bold=True)
add_text(s, 10.12, 4.28, 2.45, 0.36, "400–760", size=19, color=NAVY, bold=True, font=TITLE)
add_text(s, 10.12, 4.64, 2.45, 0.38, "passages par zone\net par tranche de 10 min", size=8.0, color=GREY,
         line_spacing=0.98)

add_rect(s, 9.95, 5.33, 2.78, 0.94, LIGHT, line=None, rounded=True, radius=0.07)
add_text(s, 10.12, 5.43, 2.45, 0.24, "PIC DE VOLUME (GOLD)", size=7.4, color=NAVY, bold=True)
add_text(s, 10.12, 5.66, 2.45, 0.44, "4 505 passages à 07:20\n(fenêtres finalisées)", size=8.2, color=NAVY,
         line_spacing=0.98)

add_text(s, ML, 6.55, CW, 0.28,
         "Les volumes restent stables sur la période : la congestion viendra surtout de la vitesse, pas d'un pic soudain de demande.",
         size=9.4, color=GREY)
add_footer(s, 15)
add_notes(
    s,
    "Le trafic est concentré sur Centre-Ville et Maarif, avec respectivement 8 377 et 7 793 passages. "
    "Les volumes par zone et par fenêtre restent dans une fourchette de 400 à 760. Dans les fenêtres "
    "finalisées (Gold), le pic de volume toutes zones confondues est à 07:20, avec 4 505 passages ; "
    "le calcul batch complet (96 fenêtres) culmine à 4 555 à 07:00, car il intègre les événements "
    "tardifs et les fenêtres encore ouvertes. Il n'y a donc pas d'explosion soudaine : la congestion "
    "s'explique surtout par la vitesse.",
)

# ---------------------------------------------------------------------------
# 16. Speed and congestion
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "La congestion est d'abord un phénomène de vitesse", "04 · Résultats", 16)
add_picture_fit(s, ASSETS / "fig_vitesse_congestion.png", 0.48, 1.40, 12.38, 4.45)

cards = [
    ("17,7 km/h", "Centre-Ville", "limite 40 km/h · 87 % de passages congestionnés", RED),
    ("19,5 km/h", "Anfa", "limite 40 km/h · 82 % de passages congestionnés", RED),
    ("39,9 km/h", "Sidi Maarouf", "limite 60 km/h · 45 % de passages congestionnés", TEAL),
]
x = 0.65
for value, zone, detail, color in cards:
    add_rect(s, x, 6.02, 3.82, 0.84, PALE, line=BORDER, rounded=True, radius=0.08)
    add_rect(s, x + 0.02, 6.04, 0.06, 0.80, color, rounded=False)
    add_text(s, x + 0.20, 6.10, 1.26, 0.28, value, size=14.2, color=color, bold=True, font=TITLE)
    add_text(s, x + 1.48, 6.09, 2.20, 0.25, zone, size=9.2, color=NAVY, bold=True)
    add_text(s, x + 1.48, 6.35, 2.20, 0.38, detail, size=7.7, color=GREY, line_spacing=0.98)
    x += 3.99
add_footer(s, 16)
add_notes(
    s,
    "Le graphique croise vitesse moyenne et taux de congestion. Centre-Ville et Anfa roulent autour "
    "de 18 à 20 km/h pour une limite de 40, avec plus de 80 % des passages sous 60 % de la limite. "
    "Sidi Maarouf reste à 40 km/h pour une limite de 60, avec 45 % de congestion. La congestion "
    "n'est donc pas seulement une question de volume : elle dépend de la vitesse observée.",
)

# ---------------------------------------------------------------------------
# 17. Heatmap
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "La pointe du matin, zone par zone", "04 · Résultats", 17)
add_picture_fit(s, crop_asset("fig_congestion_heatmap.png", 64), 0.48, 1.38, 12.38, 4.72)

chips2 = [
    ("Saturation maximale", "08:00–08:20 · moyenne 0,96", RED),
    ("Centre-Ville", "critique dès 07:40 · taux 1,00", NAVY),
    ("Retour progressif", "après 08:30 · 0,92 → 0,62", TEAL),
]
x = 0.65
for label, detail, color in chips2:
    add_rect(s, x, 6.25, 3.86, 0.62, LIGHT, line=None, rounded=True, radius=0.10)
    add_text(s, x + 0.16, 6.31, 3.55, 0.20, label, size=7.8, color=color, bold=True)
    add_text(s, x + 0.16, 6.50, 3.55, 0.25, detail, size=8.7, color=NAVY, bold=True)
    x += 4.02
add_footer(s, 17)
add_notes(
    s,
    "La carte de chaleur confirme la montée progressive de la congestion. Le maximum moyen est "
    "observé entre 08:00 et 08:20, avec un taux moyen de 0,96. Centre-Ville est saturé dès 07:40. "
    "La congestion reflue après 08:30. Cette lecture zone par zone est directement exploitable "
    "pour prioriser des actions de régulation.",
)

# ---------------------------------------------------------------------------
# 18. Watermark experiment
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Ce que le watermark protège (et ce qu'il coûte)", "04 · Expérience", 18)
add_picture_fit(s, crop_asset("fig_watermark.png", 62), 0.52, 1.40, 7.70, 4.62)

add_rect(s, 8.46, 1.45, 4.22, 4.62, PALE, line=BORDER, rounded=True, radius=0.05)
add_text(s, 8.66, 1.60, 3.85, 0.24, "MESURES SUR LE FLUX", size=8.6, color=TEAL, bold=True)
wm_metrics = [
    ("2 194", "événements en retard (4,2 %)", TEAL),
    ("198", "rejetés par le watermark (0,4 %)", RED),
    ("30 min", "tolérance observée en pratique", ORANGE),
]
y = 1.95
for value, label, color in wm_metrics:
    add_text(s, 8.66, y, 0.10, 0.42, "▍", size=17, color=color, bold=True)
    add_text(s, 8.80, y - 0.02, 3.60, 0.34, value, size=19, color=NAVY, bold=True, font=TITLE)
    add_text(s, 8.80, y + 0.30, 3.60, 0.24, label, size=8.6, color=GREY)
    y += 0.77

add_text(s, 8.66, 4.30, 3.78, 0.80,
         "Les deux dernières fenêtres (08:40–09:00) restent ouvertes : le flux s'arrête à 09:00, "
         "aucun événement postérieur ne fait avancer le watermark.",
         size=8.8, color=NAVY, line_spacing=1.02)
add_text(s, 8.66, 5.30, 3.78, 0.58,
         "Compromis : exhaustivité ↔ état mémoire et latence de finalisation.",
         size=8.8, color=GREY, bold=True, line_spacing=1.02)

add_text(s, ML, 6.18, 7.70, 0.35,
         "Lecture illustrative : temps de l'événement en ordonnée, temps d'arrivée en abscisse.",
         size=8.3, color=GREY)
add_footer(s, 18)
add_notes(
    s,
    "Le watermark est un compromis. Sur ce flux, 2 194 événements sont arrivés en retard, soit 4,2 %. "
    "Le watermark en a rejeté 198, soit 0,4 % du flux valide. En pratique, la tolérance observée "
    "monte à environ 30 minutes selon la position de l'événement dans sa fenêtre. Enfin, les deux "
    "dernières fenêtres restent ouvertes parce que le flux est fini : sans événement après 09:00, "
    "le watermark ne peut pas les dépasser.",
)

# ---------------------------------------------------------------------------
# 19. Volume reconciliation
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Chaque événement est réconcilié", "04 · Résultats", 19)
add_picture_fit(s, crop_asset("fig_bilan_evenements.png", 60), 0.50, 1.40, 8.55, 4.50)

add_rect(s, 9.35, 1.45, 3.35, 4.50, NAVY, rounded=True, radius=0.05)
add_text(s, 9.58, 1.64, 2.95, 0.24, "ÉQUATION DE COHÉRENCE", size=8.4, color=TEAL_LIGHT, bold=True)
add_text(s, 9.58, 1.98, 2.95, 0.50, "52 630", size=27, color=WHITE, bold=True, font=TITLE)
add_text(s, 9.58, 2.45, 2.95, 0.22, "événements valides", size=8.6, color=RGBColor(0xC9, 0xD9, 0xE2))
add_text(s, 9.58, 2.78, 2.95, 0.30, "=", size=19, color=ORANGE, bold=True)
eqs = [
    ("43 710", "finalisés dans Gold", "83,1 %", TEAL_LIGHT),
    ("198", "rejetés par watermark", "0,4 %", RGBColor(0xF0, 0x8A, 0x98)),
    ("8 722", "fenêtres encore ouvertes", "16,6 %", ORANGE),
]
y = 3.18
for value, label, pct, color in eqs:
    add_text(s, 9.58, y, 1.05, 0.28, value, size=14.5, color=color, bold=True, font=TITLE)
    add_text(s, 10.62, y + 0.01, 1.20, 0.22, pct, size=8.4, color=color, bold=True, align=PP_ALIGN.RIGHT)
    add_text(s, 9.58, y + 0.27, 2.55, 0.20, label, size=8.2, color=RGBColor(0xC9, 0xD9, 0xE2))
    y += 0.78

add_text(s, 9.58, 5.60, 2.88, 0.22,
         "Aucun événement perdu silencieusement.",
         size=8.6, color=WHITE, bold=True)

add_text(s, ML, 6.30, CW, 0.38,
         "La somme des trois sorties reconstitue exactement le flux valide : la réconciliation est "
         "un test de correction, pas un simple graphique.",
         size=9.4, color=GREY)
add_footer(s, 19)
add_notes(
    s,
    "Ce graphique est la preuve de cohérence du pipeline. Les 52 630 événements valides se répartissent "
    "en 43 710 finalisés dans Gold, 198 rejetés par le watermark et 8 722 dans les deux fenêtres encore "
    "ouvertes. La somme reconstitue exactement le flux valide. C'est un test de correction important : "
    "aucun événement n'est perdu silencieusement.",
)

# ---------------------------------------------------------------------------
# 20. Micro-batch performance
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Le coût réel d'un micro-batch", "04 · Performance", 20)
add_picture_fit(s, crop_asset("fig_microbatch.png", 112), 0.48, 1.42, 12.38, 4.55)

perf = [
    ("22,2 s", "B0 : démarrage JVM", NAVY),
    ("2,8–10,3 s", "régime établi", TEAL),
    ("610 evt/s", "débit moyen", ORANGE),
]
x = 0.65
for value, label, color in perf:
    add_rect(s, x, 6.10, 3.86, 0.68, PALE, line=BORDER, rounded=True, radius=0.10)
    add_text(s, x + 0.18, 6.18, 1.38, 0.28, value, size=14, color=color, bold=True, font=TITLE)
    add_text(s, x + 1.55, 6.18, 2.10, 0.42, label, size=9.0, color=NAVY, valign=MSO_ANCHOR.MIDDLE)
    x += 4.02
add_text(s, ML, 6.86, CW, 0.20,
         "Chaque durée inclut lecture, agrégation stateful et écriture Parquet.",
         size=8.2, color=GREY)
add_footer(s, 20)
add_notes(
    s,
    "Le premier micro-batch est beaucoup plus lent : 22,2 secondes, à cause du démarrage de la JVM, "
    "de Catalyst et du State Store. En régime établi, les lots prennent entre 2,8 et 10,3 secondes. "
    "Le débit moyen est d'environ 610 événements par seconde, ce qui est suffisant pour un prototype "
    "local et montre que le coût par lot reste contenu.",
)

# ---------------------------------------------------------------------------
# 21. Batch vs streaming synthesis
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Batch et streaming ne répondent pas à la même question", "04 · Synthèse", 21)

rows = [
    ["Critère", "Batch", "Streaming"],
    ["Temps total", "25,1 s", "86,3 s (dont démarrage)"],
    ["Premier résultat", "après toute la collecte", "≈ 4 s après le premier fichier"],
    ["Périmètre", "96 agrégats, une passe", "80 finalisés + 16 encore ouverts"],
    ["Données tardives", "aucune gestion spécifique", "watermark · 198 rejets"],
    ["Reprise", "relance complète", "checkpoint, sans relire le flux"],
    ["Usage", "rejeu, historique, audit", "supervision, alerte, tableau de bord"],
]
add_table(s, ML, 1.50, 12.03, 3.55, rows, [2.05, 4.20, 5.78], font_size=9.8, header_size=9.0,
          first_col_bold=True, row_h=[0.42, 0.47, 0.52, 0.50, 0.52, 0.47, 0.47])

add_rect(s, ML, 5.28, 5.82, 1.12, PALE, line=BORDER, rounded=True, radius=0.07)
add_text(s, ML + 0.20, 5.40, 5.40, 0.24, "BATCH", size=8.8, color=GREY, bold=True)
add_text(s, ML + 0.20, 5.66, 5.35, 0.58,
         "Vérité exhaustive, différée. Idéal pour le rejeu et l'audit.",
         size=11.2, color=NAVY, bold=True, line_spacing=1.02)

add_rect(s, 6.86, 5.28, 5.82, 1.12, NAVY, rounded=True, radius=0.07)
add_text(s, 7.06, 5.40, 5.40, 0.24, "STREAMING", size=8.8, color=TEAL_LIGHT, bold=True)
add_text(s, 7.06, 5.66, 5.35, 0.58,
         "Vérité incrémentale, disponible. Idéal pour l'exploitation.",
         size=11.2, color=WHITE, bold=True, line_spacing=1.02)

add_text(s, ML, 6.62, CW, 0.24,
         "Le bon choix dépend de la question : historiser ou décider maintenant.",
         size=9.4, color=GREY)
add_footer(s, 21)
add_notes(
    s,
    "La comparaison ne désigne pas un gagnant absolu. Le batch est plus rapide quand toutes les "
    "données sont là, et il est plus simple pour le rejeu. Le streaming produit un premier résultat "
    "en quelques secondes, gère les retards et reprend sur checkpoint. En production, les deux "
    "coexistent souvent : le streaming pour le tableau de bord, le batch pour l'historique.",
)

# ---------------------------------------------------------------------------
# 22. Limits and perspectives
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, WHITE)
add_title(s, "Ce que le prototype ne fait pas encore", "05 · Limites & perspectives", 22)

add_rect(s, ML, 1.50, 5.82, 4.90, PALE, line=BORDER, rounded=True, radius=0.06)
add_text(s, ML + 0.22, 1.66, 5.30, 0.26, "LIMITES ASSUMÉES", size=8.8, color=RED, bold=True)
limits = [
    "Source fichier : simule un flux, ne remplace pas un broker.",
    "12 micro-lots : pas de mesure de débit à grande échelle.",
    "Watermark fixe à 10 min, non adaptatif.",
    "Pas de déduplication sur event_id.",
    "Parquet sans transactions ACID ni évolution de schéma.",
    "Pas de tableau de bord ni d'alerte en production.",
]
y = 2.02
for item in limits:
    add_text(s, ML + 0.22, y, 5.30, 0.50, item, size=9.6, color=NAVY, bullet=True,
             bullet_color=RED, indent=0.18, line_spacing=0.98)
    y += 0.54

add_rect(s, 6.86, 1.50, 5.82, 4.90, NAVY, rounded=True, radius=0.06)
add_text(s, 7.08, 1.66, 5.30, 0.26, "PERSPECTIVES", size=8.8, color=ORANGE, bold=True)
persp = [
    "Remplacer les fichiers par Apache Kafka.",
    "Passer à Delta Lake / Iceberg pour l'ACID.",
    "Dédupliquer avec event_id et un state store.",
    "Rendre le watermark adaptatif à la latence observée.",
    "Brancher un tableau de bord Grafana / Superset.",
    "Ajouter des alertes sur seuils de congestion.",
]
y = 2.02
for item in persp:
    add_text(s, 7.08, y, 5.30, 0.50, item, size=9.6, color=WHITE, bullet=True,
             bullet_color=ORANGE, indent=0.18, line_spacing=0.98)
    y += 0.54

add_rect(s, ML, 6.58, CW, 0.30, LIGHT, line=None, rounded=True, radius=0.20)
add_text(s, ML + 0.16, 6.62, CW - 0.32, 0.22,
         "Priorité : passer de la preuve de concept à un service exploitable, sans changer les concepts.",
         size=8.8, color=NAVY, bold=True)
add_footer(s, 22)
add_notes(
    s,
    "Nous assumons les limites : la source fichier simule un flux, le volume reste petit, le watermark "
    "est fixe, il n'y a pas de déduplication et Parquet n'offre pas l'ACID. Les perspectives sont "
    "claires : Kafka, Delta Lake, déduplication, watermark adaptatif, tableau de bord et alertes. "
    "Le prototype prouve les concepts ; il reste à l'industrialiser.",
)

# ---------------------------------------------------------------------------
# 23. Conclusion
# ---------------------------------------------------------------------------
s = new_slide(prs)
add_rect(s, 0, 0, SW, SH, NAVY_DARK)
# grille discrète
for gx in [7.2, 8.4, 9.6, 10.8, 12.0]:
    add_line(s, gx, 0.0, gx, SH, RGBColor(0x14, 0x3A, 0x52), 0.6)
for gy in [0.9, 2.0, 3.1, 4.2, 5.3, 6.4]:
    add_line(s, 6.9, gy, SW, gy, RGBColor(0x14, 0x3A, 0x52), 0.6)

add_text(s, 0.80, 0.62, 5.6, 0.24, "CONCLUSION", size=9.4, color=TEAL_LIGHT, bold=True)
add_text(s, 0.80, 1.02, 6.55, 1.70,
         "De la donnée brute\nà la décision, en continu.",
         size=38, color=WHITE, bold=True, font=TITLE, line_spacing=0.98)
add_text(s, 0.82, 3.00, 5.95, 0.75,
         "CityFlow démontre qu'un pipeline Spark Structured Streaming peut produire des agrégats "
         "fiables, réconciliés et exploitables sur un flux retardé.",
         size=13.2, color=RGBColor(0xC9, 0xD9, 0xE2), line_spacing=1.08)

checks = [
    "Ingestion avec schéma explicite et source progressive.",
    "Agrégation event-time par zone et fenêtre de 10 min.",
    "Watermark, checkpoints et réconciliation des volumes.",
    "Notebook, scripts, dataset et fiche de TP reproductibles.",
]
y = 4.02
for item in checks:
    add_rect(s, 0.82, y + 0.05, 0.21, 0.21, TEAL, rounded=True, radius=0.5)
    add_text(s, 0.82, y + 0.03, 0.21, 0.25, "✓", size=8, color=WHITE, bold=True,
             align=PP_ALIGN.CENTER)
    add_text(s, 1.18, y, 5.55, 0.30, item, size=10.4, color=WHITE)
    y += 0.48

add_text(s, 0.82, 6.18, 5.4, 0.24, "Mazine CHATER  ·  Yahya BAID  ·  Wenn Bouka DIEUMERCI",
         size=9.4, color=RGBColor(0x9F, 0xC0, 0xCF), bold=True)
add_text(s, 0.82, 6.45, 5.4, 0.22, "Écosystème Big Data 2  ·  2026–2027", size=8.8,
         color=RGBColor(0x7F, 0xA9, 0xBC))

# Panneau droit : chiffres clés + merci
add_rect(s, 7.42, 1.10, 5.12, 4.95, NAVY_SOFT, line=RGBColor(0x2B, 0x60, 0x78), rounded=True,
         radius=0.05)
add_text(s, 7.72, 1.34, 4.50, 0.24, "EN UN COUP D'ŒIL", size=8.6, color=TEAL_LIGHT, bold=True)
stats = [
    ("52 630", "événements valides"),
    ("80", "agrégats finalisés"),
    ("0,4 %", "rejetés par le watermark"),
]
yy = 1.75
for value, label in stats:
    add_text(s, 7.72, yy, 2.05, 0.40, value, size=22, color=WHITE, bold=True, font=TITLE)
    add_text(s, 9.82, yy + 0.08, 2.40, 0.25, label, size=8.8, color=RGBColor(0xC9, 0xD9, 0xE2))
    yy += 0.76

add_line(s, 7.72, 4.22, 12.20, 4.22, RGBColor(0x2A, 0x5D, 0x75), 0.8)
add_text(s, 7.72, 4.48, 4.45, 0.70, "Merci.", size=38, color=WHITE, bold=True, font=TITLE)
add_text(s, 7.72, 5.22, 4.45, 0.32, "Questions & discussion", size=13, color=TEAL_LIGHT, bold=True)
add_text(s, 7.72, 5.65, 4.45, 0.24, "Projet 3 · City Flow — Real-Time Traffic Intelligence",
         size=8.3, color=RGBColor(0x9F, 0xC0, 0xCF))

# Petit accent
add_rect(s, 7.42, 6.05, 5.12, 0.055, ORANGE, rounded=False)
add_notes(
    s,
    "En conclusion, CityFlow couvre la chaîne complète : ingestion, nettoyage, enrichissement, "
    "agrégation event-time, watermark, sinks checkpointés et réconciliation. Les résultats sont "
    "cohérents et reproductibles. Le prototype est prêt pour la démonstration, et les perspectives "
    "d'industrialisation sont identifiées. Merci de votre attention ; nous sommes disponibles "
    "pour vos questions.",
)

# ---------------------------------------------------------------------------
# Propriétés et sauvegarde
# ---------------------------------------------------------------------------
prs.core_properties.title = "CityFlow — Real-Time Traffic Intelligence"
prs.core_properties.subject = "Projet 3 — Écosystème Big Data 2"
prs.core_properties.author = "Mazine CHATER, Yahya BAID, Wenn Bouka DIEUMERCI"
prs.core_properties.comments = "Présentation de soutenance — prototype Spark Structured Streaming"
prs.save(OUT)
print(f"Présentation générée : {OUT}")
print(f"Nombre de slides : {len(prs.slides)}")
