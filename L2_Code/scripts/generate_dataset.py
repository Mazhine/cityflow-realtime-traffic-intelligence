#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CityFlow - Générateur du jeu de données de trafic urbain.

Produit :
  - data/stream/*.json          : 12 micro-lots JSON Lines (1 fichier = 10 minutes de temps
                                  d'événement), simulant un flux de passages de véhicules.
  - data/reference/zones.csv    : table de référence des zones (jointure stream-static).
  - data/metadata.json          : métadonnées du jeu de données (graine, volumétrie, période).

Le générateur est déterministe (graine fixe) : les résultats sont donc reproductibles.
Aucune dépendance externe : uniquement la bibliothèque standard Python.

Usage :
    python generate_dataset.py                # génère le jeu de données complet
    python generate_dataset.py --clean        # supprime puis régénère
    python generate_dataset.py --files 6      # génère seulement 6 micro-lots
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Console Windows : éviter les erreurs d'encodage sur les accents et les chemins Unicode.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Paramètres du scénario
# ---------------------------------------------------------------------------
SEED = 20261102
START = datetime(2026, 11, 2, 7, 0, 0, tzinfo=timezone.utc)  # lundi 2 nov. 2026, 07:00 UTC
FILE_MINUTES = 10          # 1 fichier = 10 minutes de temps d'événement
N_FILES = 12               # 12 fichiers = 2 heures de trafic (07:00 -> 09:00)
LATE_RATIO = 0.05          # 5 % d'événements livrés en retard (dont ~1,5 % au-delà du watermark)
CORRUPT_RATIO = 0.004      # 0,4 % de lignes JSON invalides (robustesse du parsing)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "L3_Dataset" / "data"
STREAM_DIR = DATA_DIR / "stream"
REFERENCE_DIR = DATA_DIR / "reference"

# Zones : (id, nom, arrondissement, limite_vitesse, voies, poids_volumetrique)
ZONES = [
    # (id, nom, arrondissement, limite_vitesse, voies, poids_volumetrique, facteur_fluidite)
    ("Z01", "Centre-Ville",  "Casablanca - Sidi Belyout", 40, 4, 1.30, 0.80),
    ("Z02", "Maarif",        "Casablanca - Maarif",       40, 3, 1.20, 0.85),
    ("Z03", "Ain Diab",      "Casablanca - Ain Diab",     60, 3, 0.90, 1.15),
    ("Z04", "Sidi Maarouf",  "Casablanca - Sidi Maarouf", 60, 4, 1.10, 1.20),
    ("Z05", "Hay Hassani",   "Casablanca - Hay Hassani",  50, 3, 1.00, 0.95),
    ("Z06", "Anfa",          "Casablanca - Anfa",         40, 2, 0.80, 0.88),
    ("Z07", "Bourgogne",     "Casablanca - Anfa",         50, 3, 0.85, 0.92),
    ("Z08", "Ain Sebaa",     "Casablanca - Ain Sebaa",    60, 4, 0.95, 1.10),
]

VEHICLE_TYPES = [("car", 0.62), ("taxi", 0.15), ("moto", 0.10), ("bus", 0.07), ("truck", 0.06)]

# Profil de congestion : facteur multiplicatif de la vitesse libre selon l'heure.
# 07:00 -> 09:00 : montée puis pic à 08:00, début de décrue à 09:00.
HOURLY_PROFILE = {
    7: [0.90, 0.78, 0.62, 0.52, 0.48, 0.46],   # 07:00 -> 07:50
    8: [0.44, 0.42, 0.45, 0.50, 0.56, 0.62],   # 08:00 -> 08:50
    9: [0.70, 0.78, 0.85, 0.90, 0.92, 0.95],   # 09:00 -> 09:50
}


def speed_factor(moment: datetime) -> float:
    """Facteur de congestion selon l'heure (profil déterministe + bruit léger)."""
    return HOURLY_PROFILE[moment.hour][moment.minute // 10]


def sensor_id(zone_id: str, lane: int) -> str:
    return f"{zone_id}-S{lane:02d}"


def generate_event(rng: random.Random, event_time: datetime, zone, offset: int) -> dict:
    """Construit un événement de passage de véhicule."""
    zone_id, zone_name, _, speed_limit, lanes, _, flow_factor = zone

    vehicle = rng.choices(
        [v[0] for v in VEHICLE_TYPES], weights=[v[1] for v in VEHICLE_TYPES], k=1
    )[0]

    # Les véhicules lourds roulent un peu moins vite ; bruit gaussien borné.
    vehicle_penalty = {"car": 1.00, "taxi": 1.03, "moto": 1.08, "bus": 0.86, "truck": 0.80}[vehicle]
    factor = speed_factor(event_time)
    raw_speed = speed_limit * factor * flow_factor * vehicle_penalty * rng.gauss(1.0, 0.14)
    speed = max(5.0, min(speed_limit * 1.25, raw_speed))

    lane = rng.randint(1, lanes)
    return {
        "event_id": f"EVT-{event_time:%Y%m%d%H%M}-{offset:06d}",
        "event_time": event_time.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "zone_id": zone_id,
        "zone_name": zone_name,
        "sensor_id": sensor_id(zone_id, lane),
        "lane": lane,
        "vehicle_type": vehicle,
        "speed_kmh": round(speed, 1),
    }


def write_zones() -> None:
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = REFERENCE_DIR / "zones.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["zone_id", "zone_name", "district", "speed_limit_kmh", "lanes"])
        for zone in ZONES:
            writer.writerow([zone[0], zone[1], zone[2], zone[3], zone[4]])


def main() -> None:
    parser = argparse.ArgumentParser(description="Génère le flux JSON CityFlow.")
    parser.add_argument("--files", type=int, default=N_FILES, help="Nombre de micro-lots (défaut : 12)")
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--clean", action="store_true", help="Supprime les données existantes")
    args = parser.parse_args()

    if args.clean and STREAM_DIR.exists():
        shutil.rmtree(STREAM_DIR)
    STREAM_DIR.mkdir(parents=True, exist_ok=True)
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)
    write_zones()

    total_events = 0
    total_late = 0
    total_corrupt = 0
    file_stats = []
    counter = 0

    for file_idx in range(args.files):
        batch_start = START + timedelta(minutes=FILE_MINUTES * file_idx)
        lines: list[str] = []
        n_events = 0
        n_late = 0
        n_corrupt = 0

        for minute in range(FILE_MINUTES):
            minute_start = batch_start + timedelta(minutes=minute)
            # Volume : ~55 passages / minute et par zone, modulé par le poids de la zone.
            for zone in ZONES:
                weight = zone[5]
                n = max(8, int(rng.gauss(55 * weight, 8)))
                for _ in range(n):
                    event_time = minute_start + timedelta(seconds=rng.uniform(0, 59.999))
                    counter += 1
                    event = generate_event(rng, event_time, zone, counter)
                    n_events += 1

                    # ---- Retard de livraison -------------------------------------
                    # Le fichier courant représente l'intervalle [batch_start, batch_start+10min[.
                    # 5 % des événements arrivent avec un retard de 6 à 28 minutes : les retards
                    # > 10 min dépassent le watermark et seront ignorés par l'agrégation.
                    if rng.random() < LATE_RATIO:
                        # Certains retards dépassent largement le watermark (10 min) et
                        # seront donc ignorés par l'agrégation fenêtrée.
                        delay_min = rng.choice([6, 8, 9, 11, 13, 15, 19, 24, 28, 35, 40, 45])
                        late_time = event_time - timedelta(minutes=delay_min)
                        if late_time < START:
                            late_time = event_time
                        else:
                            event["event_time"] = late_time.isoformat(timespec="milliseconds").replace("+00:00", "Z")
                            n_late += 1

                    # ---- Ligne corrompue ----------------------------------------
                    if rng.random() < CORRUPT_RATIO:
                        lines.append('{"event_id": "EVT-BROKEN", "event_time": "2026-11-02T07:0')  # JSON tronqué
                        n_corrupt += 1
                    else:
                        lines.append(json.dumps(event, ensure_ascii=False))

        batch_end = batch_start + timedelta(minutes=FILE_MINUTES)
        filename = f"traffic_{batch_start:%Y%m%d_%H%M}_{batch_end:%H%M}.json"
        with (STREAM_DIR / filename).open("w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")

        total_events += n_events
        total_late += n_late
        total_corrupt += n_corrupt
        file_stats.append(
            {
                "file": filename,
                "events": n_events,
                "late_events": n_late,
                "corrupt_lines": n_corrupt,
                "window_start": batch_start.isoformat().replace("+00:00", "Z"),
                "window_end": batch_end.isoformat().replace("+00:00", "Z"),
            }
        )
        print(f"  {filename} : {n_events:5d} événements, {n_late:3d} en retard, {n_corrupt:2d} corrompus")

    metadata = {
        "dataset": "CityFlow - Real-Time Traffic Intelligence",
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seed": args.seed,
        "timezone": "UTC",
        "period": {
            "start": START.isoformat().replace("+00:00", "Z"),
            "end": (START + timedelta(minutes=FILE_MINUTES * args.files)).isoformat().replace("+00:00", "Z"),
        },
        "files": args.files,
        "file_duration_minutes": FILE_MINUTES,
        "total_events": total_events,
        "late_events": total_late,
        "late_ratio": round(total_late / total_events, 4),
        "corrupt_lines": total_corrupt,
        "zones": len(ZONES),
        "format": "JSON Lines (UTF-8, un objet par ligne)",
        "file_stats": file_stats,
    }
    (DATA_DIR / "metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print("\nRésumé :")
    print(f"  Fichiers      : {args.files}")
    print(f"  Événements    : {total_events}")
    print(f"  En retard     : {total_late} ({total_late / total_events:.1%})")
    print(f"  Lignes KO     : {total_corrupt}")
    print(f"  Zones         : {len(ZONES)}")
    print(f"  Sortie        : {STREAM_DIR}")
    print(f"  Référentiel   : {REFERENCE_DIR / 'zones.csv'}")


if __name__ == "__main__":
    main()
