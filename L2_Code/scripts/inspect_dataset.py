#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Affiche un contrôle rapide du dataset CityFlow.

Usage :
    python L2_Code/scripts/inspect_dataset.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "L3_Dataset" / "data"
STREAM_DIR = DATA_DIR / "stream"
METADATA = DATA_DIR / "metadata.json"


def main() -> None:
    if not METADATA.exists():
        raise SystemExit(f"Métadonnées introuvables : {METADATA}")

    meta = json.loads(METADATA.read_text(encoding="utf-8"))
    files = sorted(STREAM_DIR.glob("*.json"))
    lines = 0
    valid_json = 0

    for path in files:
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                lines += 1
                try:
                    json.loads(line)
                    valid_json += 1
                except json.JSONDecodeError:
                    pass

    print("Contrôle du dataset CityFlow")
    print("=" * 40)
    print(f"Graine                : {meta['seed']}")
    print(f"Période               : {meta['period']['start']} -> {meta['period']['end']}")
    print(f"Fichiers              : {len(files)}")
    print(f"Lignes comptées       : {lines}")
    print(f"Lignes JSON valides   : {valid_json}")
    print(f"Lignes JSON invalides : {lines - valid_json}")
    print(f"Événements attendus   : {meta['total_events']}")
    print(f"Événements retardés   : {meta['late_events']} ({meta['late_ratio']:.1%})")
    print(f"Zones                 : {meta['zones']}")

    ok = (
        len(files) == meta["files"]
        and lines == meta["total_events"]
        and lines - valid_json == meta["corrupt_lines"]
    )
    print("=" * 40)
    print("Statut                :", "OK" if ok else "ÉCART DÉTECTÉ")


if __name__ == "__main__":
    main()
