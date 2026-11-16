# -*- coding: utf-8 -*-
"""
CityFlow - Construction des figures et des tableaux pour la présentation (L1)
et la fiche de TP (L4).

Lit les résultats produits par le pipeline (Parquet + JSON) et génère :
  - L1_Presentation/assets/*.png
  - output/tables/*.csv
  - output/tables/kpis.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT = PROJECT_ROOT / "output"
ASSETS = PROJECT_ROOT / "L1_Presentation" / "assets"
TABLES = OUTPUT / "tables"
ASSETS.mkdir(parents=True, exist_ok=True)
TABLES.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Style commun
# ---------------------------------------------------------------------------
NAVY = "#0B3C5D"
TEAL = "#1D7874"
ORANGE = "#F2A541"
RED = "#D1495B"
LIGHT = "#EAF2F5"
GREY = "#6B7A8F"
PALETTE = ["#0B3C5D", "#1D7874", "#F2A541", "#D1495B", "#5B8E7D",
           "#8D5A97", "#C97C5D", "#3E7CB1"]

plt.rcParams.update({
    "figure.dpi": 200,
    "savefig.dpi": 200,
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
    "axes.labelsize": 11,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.edgecolor": "#B8C4CE",
    "axes.grid": True,
    "grid.color": "#DDE5EB",
    "grid.linewidth": 0.8,
    "legend.frameon": False,
    "font.family": "DejaVu Sans",
})

ZONE_LABELS = {
    "Z01": "Centre-Ville", "Z02": "Maarif", "Z03": "Ain Diab", "Z04": "Sidi Maarouf",
    "Z05": "Hay Hassani", "Z06": "Anfa", "Z07": "Bourgogne", "Z08": "Ain Sebaa",
}


def save(fig, name: str) -> None:
    path = ASSETS / name
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  figure : {path.name}")


# ---------------------------------------------------------------------------
# Chargement des résultats
# ---------------------------------------------------------------------------
stream = json.loads((OUTPUT / "results_summary.json").read_text(encoding="utf-8"))
batch = json.loads((OUTPUT / "batch_summary.json").read_text(encoding="utf-8"))

gold = pd.read_parquet(OUTPUT / "gold_windows")
allw = pd.read_parquet(OUTPUT / "batch_windows")
for df in (gold, allw):
    # Les colonnes partitionnées sont relues comme catégories par pandas : on les
    # repasse en types simples pour éviter les produits cartésiens dans les groupby.
    df["zone_id"] = df["zone_id"].astype(str)
    df["zone_name"] = df["zone_name"].astype(str)
    df["speed_limit_kmh"] = df["speed_limit_kmh"].astype(int)
    df["window_start"] = pd.to_datetime(df["window_start"])
    df["window_end"] = pd.to_datetime(df["window_end"])

gold["window_label"] = gold["window_start"].dt.strftime("%H:%M")
allw["window_label"] = allw["window_start"].dt.strftime("%H:%M")

zone_summary = (
    allw.groupby(["zone_id", "zone_name", "speed_limit_kmh"], observed=True)
    .agg(vehicle_count=("vehicle_count", "sum"),
         avg_speed_kmh=("avg_speed_kmh", "mean"),
         congestion_rate=("congestion_rate", "mean"),
         congested_events=("congested_events", "sum"),
         active_sensors=("active_sensors", "max"))
    .reset_index()
)
zone_summary["avg_speed_kmh"] = zone_summary["avg_speed_kmh"].round(1)
zone_summary["congestion_rate"] = zone_summary["congestion_rate"].round(3)
zone_summary["vehicle_share"] = (zone_summary["vehicle_count"] / zone_summary["vehicle_count"].sum()).round(3)
zone_summary = zone_summary.sort_values("vehicle_count", ascending=False)

allw["window_label"] = allw["window_start"].dt.strftime("%H:%M")
allw.sort_values(["window_start", "zone_id"]).to_csv(TABLES / "fenetres_toutes.csv", index=False, encoding="utf-8-sig")
gold.sort_values(["window_start", "zone_id"]).to_csv(TABLES / "fenetres_finalisees.csv", index=False, encoding="utf-8-sig")
zone_summary.to_csv(TABLES / "synthese_zones.csv", index=False, encoding="utf-8-sig")

kpis = {
    "evenements_generes": 52846,
    "evenements_valides": stream["valid_events"],
    "lignes_corrompues": 52846 - stream["valid_events"],
    "taux_validite": round(stream["valid_events"] / 52846, 4),
    "fenetres_total": stream["all_windows_batch"],
    "fenetres_finalisees": stream["finalized_windows"],
    "evenements_finalises": stream["finalized_events"],
    "evenements_rejetes": stream["dropped_late_events"],
    "evenements_fenetres_ouvertes": stream["open_window_events"],
    "micro_batches": stream["micro_batches"],
    "latence_moyenne_batch_ms": stream["avg_batch_duration_ms"],
    "latence_max_batch_ms": stream["max_batch_duration_ms"],
    "duree_streaming_s": stream["elapsed_seconds_streaming"],
    "duree_batch_s": batch["elapsed_seconds"],
    "calcul_batch_s": batch["compute_seconds"],
    "zone_la_plus_chargee": zone_summary.iloc[0]["zone_name"],
    "zone_la_plus_chargee_volume": int(zone_summary.iloc[0]["vehicle_count"]),
    "zone_la_plus_congestionnee": zone_summary.sort_values("congestion_rate", ascending=False).iloc[0]["zone_name"],
    "taux_congestion_max": float(zone_summary["congestion_rate"].max()),
    "taux_congestion_moyen": float(zone_summary["congestion_rate"].mean()),
    "debit_moyen_evt_s": round(stream["valid_events"] / stream["elapsed_seconds_streaming"], 1),
}
# Fenêtre de pointe : fenêtre (toutes zones) avec la plus forte congestion moyenne.
peak = (
    allw.groupby("window_label")
    .agg(congestion=("congestion_rate", "mean"), volume=("vehicle_count", "sum"))
    .sort_values("congestion", ascending=False)
    .iloc[0]
)
kpis["fenetre_pic"] = peak.name
kpis["congestion_pic"] = round(float(peak["congestion"]), 3)
(TABLES / "kpis.json").write_text(json.dumps(kpis, indent=2, ensure_ascii=False), encoding="utf-8")
print("  tableaux : output/tables/*.csv, kpis.json")


# ---------------------------------------------------------------------------
# Figure 1 - Architecture
# ---------------------------------------------------------------------------
def figure_architecture():
    fig, ax = plt.subplots(figsize=(15, 8.2))
    ax.set_xlim(0, 15)
    ax.set_ylim(0, 8.2)
    ax.axis("off")

    def box(x, y, w, h, title, lines, color, title_color="white"):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.04,rounding_size=0.12",
                                    linewidth=0, facecolor=color, zorder=2))
        ax.text(x + w / 2, y + h - 0.34, title, ha="center", va="top",
                fontsize=11.5, weight="bold", color=title_color, zorder=3)
        for i, line in enumerate(lines):
            ax.text(x + w / 2, y + h - 0.78 - i * 0.30, line, ha="center", va="top",
                    fontsize=9.2, color=title_color, zorder=3)

    def arrow(x1, y1, x2, y2, color=GREY, style="-|>", lw=1.8, ls="-"):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle=style,
                                     mutation_scale=16, linewidth=lw, color=color,
                                     linestyle=ls, zorder=1))

    # Titre
    ax.text(0.2, 7.9, "Architecture de la solution CityFlow", fontsize=18, weight="bold", color=NAVY)
    ax.text(0.2, 7.5, "Traitement de flux temps réel avec Apache Spark Structured Streaming",
            fontsize=10.5, color=GREY)

    # Sources
    box(0.2, 4.7, 3.0, 2.0, "SOURCES", [
        "Capteurs de trafic",
        "8 zones · 5 types de véhicules",
        "Fichiers JSON Lines",
        "ajoutés progressivement",
    ], NAVY)
    box(0.2, 1.8, 3.0, 2.2, "RÉFÉRENTIEL", [
        "zones.csv (statique)",
        "limite de vitesse",
        "arrondissement · voies",
        "jointure stream-static",
    ], "#3E7CB1")

    # Ingestion
    box(3.9, 4.7, 3.1, 2.0, "INGESTION", [
        "readStream · format JSON",
        "schéma explicite",
        "maxFilesPerTrigger = 1",
        "checkpoint par requête",
    ], TEAL)

    # Transformation
    box(7.7, 2.9, 3.4, 3.8, "TRANSFORMATION", [
        "Nettoyage & typage",
        "Watermark 10 min",
        "Fenêtre tumblante 10 min",
        "Agrégation par zone",
        "Vitesse · congestion",
        "Jointure broadcast",
    ], "#2A6F97")

    # Sinks
    box(11.9, 5.7, 2.9, 1.35, "SINK BRONZE", ["Parquet append", "événements enrichis"], "#5B8E7D")
    box(11.9, 3.95, 2.9, 1.35, "SINK GOLD", ["Parquet partitionné", "agrégats finalisés"], ORANGE)
    box(11.9, 2.2, 2.9, 1.35, "SINK MÉMOIRE", ["table SQL", "monitoring / debug"], "#8D5A97")

    # Checkpoints / monitoring
    ax.add_patch(FancyBboxPatch((3.9, 1.0), 7.2, 1.0, boxstyle="round,pad=0.04,rounding_size=0.12",
                                linewidth=1.2, edgecolor=GREY, facecolor=LIGHT, zorder=2))
    ax.text(7.5, 1.5, "Checkpoints  ·  reprise sans perte  ·  métriques par micro-batch  ·  exactly-once",
            ha="center", va="center", fontsize=9.0, color=NAVY, zorder=3)

    # Flèches
    arrow(3.2, 5.7, 3.9, 5.7, NAVY)
    arrow(3.2, 2.9, 3.9, 2.9, "#3E7CB1", ls="--")
    arrow(3.9, 2.9, 7.7, 3.9, "#3E7CB1", ls="--")
    arrow(7.0, 5.7, 7.7, 5.7, TEAL)
    arrow(11.1, 6.2, 11.9, 6.4, "#5B8E7D")
    arrow(11.1, 4.9, 11.9, 4.7, ORANGE)
    arrow(11.1, 3.7, 11.9, 2.9, "#8D5A97")

    # Annotations
    ax.text(5.45, 6.95, "micro-batch", fontsize=9.5, color=TEAL, ha="center", style="italic")
    ax.text(9.4, 7.05, "event-time", fontsize=9.5, color="#2A6F97", ha="center", style="italic")

    save(fig, "fig_architecture.png")


# ---------------------------------------------------------------------------
# Figure 2 - Batch vs streaming (timeline)
# ---------------------------------------------------------------------------
def figure_batch_vs_streaming():
    fig, ax = plt.subplots(figsize=(13, 5.2))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 5.2)
    ax.axis("off")
    ax.text(0.2, 4.9, "Batch vs streaming : quand les résultats sont-ils disponibles ?",
            fontsize=15, weight="bold", color=NAVY)

    # Batch
    ax.text(0.2, 4.0, "BATCH", fontsize=12, weight="bold", color=GREY)
    ax.add_patch(FancyBboxPatch((1.3, 3.55), 8.0, 0.55, boxstyle="round,pad=0.02,rounding_size=0.08",
                                facecolor="#B8C4CE", edgecolor="none"))
    ax.text(5.3, 3.82, "lecture complète + calcul + écriture  (25,9 s)", ha="center", va="center",
            fontsize=9.8, color="white", weight="bold")
    ax.add_patch(FancyBboxPatch((9.35, 3.55), 2.0, 0.55, boxstyle="round,pad=0.02,rounding_size=0.08",
                                facecolor=NAVY, edgecolor="none"))
    ax.text(10.35, 3.82, "1 résultat", ha="center", va="center", fontsize=9.8, color="white", weight="bold")
    ax.text(10.35, 3.30, "Aucun résultat avant la fin de la collecte", fontsize=9.2,
            color=GREY, style="italic", ha="center")

    # Streaming
    ax.text(0.2, 2.55, "STREAMING", fontsize=11, weight="bold", color=TEAL)
    latency = batch["compute_seconds"] / 12
    for i in range(12):
        x = 1.9 + i * 0.72
        ax.add_patch(FancyBboxPatch((x, 2.1), 0.62, 0.55, boxstyle="round,pad=0.01,rounding_size=0.06",
                                    facecolor=TEAL, alpha=0.55 + 0.04 * (i % 3), edgecolor="none"))
        ax.text(x + 0.31, 2.37, f"B{i}", ha="center", va="center", fontsize=7.5, color="white", weight="bold")
    ax.annotate("1er résultat\n≈ 4 s", xy=(2.21, 2.67), xytext=(3.75, 3.00),
                fontsize=9, color=TEAL, ha="center",
                arrowprops=dict(arrowstyle="->", color=TEAL, lw=1.4,
                                connectionstyle="arc3,rad=-0.2"))
    ax.text(10.65, 2.37, "… en continu", fontsize=9.5, color=TEAL, style="italic")

    # Bandeau
    ax.add_patch(FancyBboxPatch((0.2, 0.55), 12.5, 1.0, boxstyle="round,pad=0.04,rounding_size=0.12",
                                facecolor=LIGHT, edgecolor="none"))
    ax.text(6.45, 1.32, "Même jeu de données · mêmes agrégats · 96 fenêtres",
            ha="center", va="center", fontsize=10.5, color=NAVY, weight="bold")
    ax.text(6.45, 0.9, "Le batch attend la fin du flux ; le streaming publie au fil de l'eau et gère les retards.",
            ha="center", va="center", fontsize=9.5, color=GREY)

    save(fig, "fig_batch_vs_streaming.png")


# ---------------------------------------------------------------------------
# Figure 3 - Trafic par zone et fenêtre
# ---------------------------------------------------------------------------
def figure_traffic():
    pivot = allw.pivot_table(index="window_start", columns="zone_name",
                             values="vehicle_count", aggfunc="sum").sort_index()
    fig, ax = plt.subplots(figsize=(12.5, 5.8))
    for i, zone in enumerate(pivot.columns):
        ax.plot(pivot.index, pivot[zone], marker="o", markersize=4, linewidth=1.7,
                color=PALETTE[i % len(PALETTE)], label=zone)
    ax.axvspan(pd.Timestamp("2026-11-02 08:40"), pd.Timestamp("2026-11-02 09:00"),
               color=ORANGE, alpha=0.10)
    ax.text(pd.Timestamp("2026-11-02 08:50"), ax.get_ylim()[1] * 0.98,
            "fenêtres\nencore ouvertes", ha="center", va="top", fontsize=8.5, color="#B26A00")

    ax.set_title("Volume de trafic par zone et par fenêtre de 10 minutes", loc="left", color=NAVY)
    ax.set_xlabel("Fenêtre temporelle (temps de l'événement, UTC)")
    ax.set_ylabel("Nombre de passages")
    ax.set_xticks(pivot.index)
    ax.set_xticklabels([t.strftime("%H:%M") for t in pivot.index], rotation=45)
    ax.legend(ncol=4, fontsize=8.5, loc="upper center", bbox_to_anchor=(0.5, -0.30))
    ax.margins(x=0.02)
    fig.subplots_adjust(bottom=0.30)
    save(fig, "fig_trafic_par_zone.png")


# ---------------------------------------------------------------------------
# Figure 4 - Heatmap congestion
# ---------------------------------------------------------------------------
def figure_heatmap():
    pivot = allw.pivot_table(index="zone_name", columns="window_start",
                             values="congestion_rate", aggfunc="mean").sort_index()
    fig, ax = plt.subplots(figsize=(12.5, 5.2))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn_r", vmin=0, vmax=1)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([t.strftime("%H:%M") for t in pivot.columns], rotation=45)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title("Taux de congestion par zone et par fenêtre", loc="left", color=NAVY)
    ax.set_xlabel("Fenêtre de 10 minutes")
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            value = pivot.values[i, j]
            ax.text(j, i, f"{value:.2f}", ha="center", va="center",
                    fontsize=7.2, color="white" if value > 0.65 or value < 0.2 else "#1A1A1A")
    cbar = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02)
    cbar.set_label("Taux de congestion (0 = fluide, 1 = saturé)")
    fig.tight_layout()
    save(fig, "fig_congestion_heatmap.png")


# ---------------------------------------------------------------------------
# Figure 5 - Vitesse et congestion par zone
# ---------------------------------------------------------------------------
def figure_speed_congestion():
    zs = zone_summary.sort_values("congestion_rate")
    fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.2), gridspec_kw={"width_ratios": [1, 1]})

    y = np.arange(len(zs))
    axes[0].barh(y, zs["avg_speed_kmh"], color=TEAL, alpha=0.9, height=0.62)
    axes[0].scatter(zs["speed_limit_kmh"], y, color=RED, zorder=3, s=55, marker="D",
                    label="Limite légale")
    for yi, (_, row) in enumerate(zs.iterrows()):
        axes[0].text(row["avg_speed_kmh"] + 1.0, yi, f"{row['avg_speed_kmh']:.0f}",
                     va="center", fontsize=8.7, color=NAVY)
    axes[0].set_yticks(y)
    axes[0].set_yticklabels(zs["zone_name"])
    axes[0].set_xlabel("km/h")
    axes[0].set_title("Vitesse moyenne observée", loc="left", color=NAVY, fontsize=12)
    axes[0].legend(fontsize=8.5, loc="lower right")
    axes[0].grid(axis="x", alpha=0.25)

    colors = [RED if v >= 0.7 else ORANGE if v >= 0.45 else TEAL for v in zs["congestion_rate"]]
    axes[1].barh(y, zs["congestion_rate"] * 100, color=colors, alpha=0.92, height=0.62)
    for yi, v in enumerate(zs["congestion_rate"] * 100):
        axes[1].text(v + 1.2, yi, f"{v:.0f} %", va="center", fontsize=8.7, color=NAVY)
    axes[1].set_yticks(y)
    axes[1].set_yticklabels([])
    axes[1].set_xlabel("% des passages sous 60 % de la limite")
    axes[1].set_title("Taux de congestion", loc="left", color=NAVY, fontsize=12)
    axes[1].set_xlim(0, 105)
    axes[1].grid(axis="x", alpha=0.25)

    fig.tight_layout()
    save(fig, "fig_vitesse_congestion.png")


# ---------------------------------------------------------------------------
# Figure 6 - Micro-batchs
# ---------------------------------------------------------------------------
def figure_microbatch():
    metrics = pd.DataFrame(stream["batch_metrics"])
    metrics = metrics[metrics["num_input_rows"] > 0].reset_index(drop=True)
    warmup = metrics[metrics["batch_id"] == 0]
    metrics = metrics[metrics["batch_id"] > 0].reset_index(drop=True)
    fig, ax1 = plt.subplots(figsize=(12.5, 4.9))
    ax1.bar(metrics["batch_id"], metrics["trigger_execution_ms"], color=NAVY, alpha=0.9, width=0.62,
            label="Durée du micro-batch")
    ax1.set_xlabel("Micro-batch (1 fichier = 1 batch)")
    ax1.set_ylabel("Durée (ms)", color=NAVY)
    ax1.tick_params(axis="y", colors=NAVY)
    ax1.set_xticks(metrics["batch_id"])
    ax1.set_ylim(0, metrics["trigger_execution_ms"].max() * 1.18)
    ax1.grid(axis="y", alpha=0.25)

    ax2 = ax1.twinx()
    ax2.plot(metrics["batch_id"], metrics["num_input_rows"], color=ORANGE, marker="o",
             linewidth=2.2, markersize=6, label="Lignes lues")
    ax2.set_ylabel("Lignes lues", color="#B26A00")
    ax2.tick_params(axis="y", colors="#B26A00")
    ax2.grid(False)

    ax1.set_title("Coût et volume de chaque micro-batch", loc="left", color=NAVY, pad=24)
    ax1.text(0.0, 1.015, "Barres : durée du micro-batch (ms)   ·   Courbe : lignes lues",
             transform=ax1.transAxes, fontsize=9, color=GREY, ha="left", va="bottom")
    warmup_ms = int(warmup["trigger_execution_ms"].iloc[0]) if len(warmup) else 0
    ax1.text(0.995, 0.97,
             f"B0 exclu de l'échelle : démarrage JVM ({warmup_ms/1000:.1f} s)",
             transform=ax1.transAxes, ha="right", va="top", fontsize=8.5,
             color=GREY, style="italic")
    fig.tight_layout()
    save(fig, "fig_microbatch.png")


# ---------------------------------------------------------------------------
# Figure 7 - Bilan des événements (waterfall)
# ---------------------------------------------------------------------------
def figure_accounting():
    labels = ["Valides\n(Bronze)", "Rejetés\n(watermark)", "Fenêtres\nouvertes", "Finalisés\n(Gold)"]
    valid = stream["valid_events"]
    dropped = stream["dropped_late_events"]
    open_ = stream["open_window_events"]
    final = stream["finalized_events"]

    fig, ax = plt.subplots(figsize=(10.5, 5.4))
    bottoms = [0, final, final + dropped, 0]
    heights = [valid, dropped, open_, final]
    colors = [NAVY, RED, ORANGE, TEAL]
    bars = ax.bar(labels, heights, bottom=bottoms, color=colors, width=0.55, alpha=0.95)
    for bar, bottom, height in zip(bars, bottoms, heights):
        ax.text(bar.get_x() + bar.get_width() / 2, bottom + height + valid * 0.015,
                f"{height:,}".replace(",", " ") + f"\n{height / valid:.1%}",
                ha="center", va="bottom", fontsize=9.5, color=NAVY, weight="bold")
    ax.plot([0, 1, 2, 3], [valid, final, final + dropped, 0], color=GREY, linewidth=1, linestyle=":")
    ax.set_ylim(0, valid * 1.16)
    ax.set_ylabel("Nombre d'événements")
    ax.set_title("Réconciliation des volumes : du flux brut aux fenêtres finalisées",
                 loc="left", color=NAVY)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    save(fig, "fig_bilan_evenements.png")


# ---------------------------------------------------------------------------
# Figure 8 - Temps d'événement vs temps d'arrivée (watermark)
# ---------------------------------------------------------------------------
def figure_watermark():
    bronze = pd.read_parquet(OUTPUT / "bronze_events", columns=["event_time", "source_file"])
    bronze["event_time"] = pd.to_datetime(bronze["event_time"])
    bronze["arrival"] = pd.to_datetime(
        bronze["source_file"].str.extract(r"traffic_(\d{8}_\d{4})")[0],
        format="%Y%m%d_%H%M",
    )
    # Le fichier 07:00 contient des événements de 07:00 à 07:10 : on approxime l'arrivée
    # par la fin de la fenêtre du fichier.
    bronze["arrival"] = bronze["arrival"] + pd.Timedelta(minutes=10)
    bronze["lateness_min"] = (bronze["arrival"] - bronze["event_time"]).dt.total_seconds() / 60
    bronze["window_end"] = bronze["event_time"].dt.ceil("10min")

    # Rejet approximé : fenêtre déjà fermée à l'arrivée (retard > 30 min dans ce jeu).
    sample = bronze.sample(min(1800, len(bronze)), random_state=42)
    ontime = sample[sample["lateness_min"] <= 12]
    accepted = sample[(sample["lateness_min"] > 12) & (sample["lateness_min"] <= 30)]
    dropped = sample[sample["lateness_min"] > 30]

    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    ax.scatter(ontime["arrival"], ontime["event_time"], s=7, color=NAVY, alpha=0.25,
               label="À l'heure (≤ 12 min)")
    ax.scatter(accepted["arrival"], accepted["event_time"], s=12, color=ORANGE, alpha=0.65,
               label="En retard, accepté")
    ax.scatter(dropped["arrival"], dropped["event_time"], s=16, color=RED, alpha=0.85,
               label="Trop en retard, ignoré")

    x = pd.date_range("2026-11-02 07:00", "2026-11-02 09:10", freq="5min")
    ax.plot(x, x, color=GREY, linestyle="--", linewidth=1, label="Arrivée = événement")
    ax.plot(x, x - pd.Timedelta(minutes=10), color=TEAL, linestyle=":", linewidth=1.6,
            label="Watermark (retard max 10 min)")

    ax.set_xlim(pd.Timestamp("2026-11-02 07:00"), pd.Timestamp("2026-11-02 09:10"))
    ax.set_ylim(pd.Timestamp("2026-11-02 06:45"), pd.Timestamp("2026-11-02 09:10"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.yaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))
    ax.yaxis.set_major_locator(mdates.MinuteLocator(interval=15))
    ax.set_xlabel("Temps d'arrivée / de traitement (UTC)")
    ax.set_ylabel("Temps de l'événement (UTC)")
    ax.set_title("Temps de l'événement vs temps d'arrivée : visualisation des retards",
                 loc="left", color=NAVY)
    ax.legend(loc="lower right", fontsize=8.8)
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    save(fig, "fig_watermark.png")


# ---------------------------------------------------------------------------
# Figure 9 - Top zones (pour slide)
# ---------------------------------------------------------------------------
def figure_top_zones():
    zs = zone_summary.sort_values("vehicle_count", ascending=True)
    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    bars = ax.barh(zs["zone_name"], zs["vehicle_count"], color=TEAL, alpha=0.9, height=0.62)
    for bar, value in zip(bars, zs["vehicle_count"]):
        ax.text(value + 80, bar.get_y() + bar.get_height() / 2,
                f"{value:,}".replace(",", " "), va="center", fontsize=9, color=NAVY)
    ax.set_xlabel("Nombre de passages sur la période (07:00–09:00)")
    ax.set_title("Classement des zones par volume de trafic", loc="left", color=NAVY)
    ax.set_xlim(0, zs["vehicle_count"].max() * 1.15)
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    save(fig, "fig_top_zones.png")


# ---------------------------------------------------------------------------
# Exécution
# ---------------------------------------------------------------------------
print("Génération des figures :")
figure_architecture()
figure_batch_vs_streaming()
figure_traffic()
figure_heatmap()
figure_speed_congestion()
figure_microbatch()
figure_accounting()
figure_watermark()
figure_top_zones()

print("\nKPIs :")
for key, value in kpis.items():
    print(f"  {key:35s} : {value}")
