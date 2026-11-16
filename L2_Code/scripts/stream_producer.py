#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CityFlow - Simulateur d'arrivée progressive des fichiers.

Copie les micro-lots du jeu de données historique vers un répertoire « atterrissage »
(``data/incoming``) à intervalle régulier. Le pipeline lancé en mode ``live`` surveille
ce répertoire et traite chaque fichier comme un micro-batch.

Deux terminaux :
    Terminal 1 : python stream_producer.py --interval 5
    Terminal 2 : python cityflow_pipeline.py --mode live --source-dir ../L3_Dataset/data/incoming --duration 120

La copie est atomique (fichier temporaire puis renommage) pour éviter qu'un fichier
partiellement copié soit lu par Spark.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "L3_Dataset" / "data" / "stream"
DEFAULT_DEST = PROJECT_ROOT / "L3_Dataset" / "data" / "incoming"


def main() -> None:
    parser = argparse.ArgumentParser(description="Simule l'arrivée progressive des fichiers JSON.")
    parser.add_argument("--source", default=str(DEFAULT_SOURCE))
    parser.add_argument("--dest", default=str(DEFAULT_DEST))
    parser.add_argument("--interval", type=float, default=5.0, help="Secondes entre deux fichiers")
    parser.add_argument("--clean", action="store_true", help="Vide le répertoire de destination")
    parser.add_argument("--loop", action="store_true", help="Recommence après le dernier fichier")
    args = parser.parse_args()

    source = Path(args.source)
    dest = Path(args.dest)
    if args.clean and dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)

    files = sorted(source.glob("*.json"))
    if not files:
        raise SystemExit(f"Aucun fichier dans {source}")

    print(f"Producteur CityFlow : {len(files)} fichiers, un toutes les {args.interval}s")
    print(f"Destination : {dest}\n")

    sent = 0
    while True:
        for path in files:
            target = dest / path.name
            tmp = dest / (path.name + ".tmp")
            shutil.copy2(path, tmp)
            tmp.replace(target)  # renommage atomique
            sent += 1
            print(f"[{time.strftime('%H:%M:%S')}] {sent:02d} -> {path.name} "
                  f"({path.stat().st_size / 1024:.0f} Ko)")
            time.sleep(args.interval)

        if not args.loop:
            break
        files = sorted(source.glob("*.json"))

    print(f"\nTerminé : {sent} fichiers déposés.")


if __name__ == "__main__":
    main()
