#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CityFlow - Pipeline de traitement de flux avec Spark Structured Streaming.

Trois modes :
  * ``available`` (défaut) : traite tous les fichiers présents un par un
    (``maxFilesPerTrigger=1`` + ``trigger(availableNow=True)``) et s'arrête.
    C'est le mode utilisé pour la démonstration reproductible et les mesures.
  * ``live`` : traitement continu (``trigger(processingTime=...)``) d'un répertoire
    alimenté progressivement par ``stream_producer.py``.
  * ``batch`` : traitement statique du même jeu de données (référence de comparaison).

Exemples :
    python cityflow_pipeline.py --mode available
    python cityflow_pipeline.py --mode batch
    python cityflow_pipeline.py --mode live --source-dir ../L3_Dataset/data/incoming --duration 90

Sorties (par défaut dans ``output/``) :
    bronze_events/            événements valides typés et enrichis (Parquet)
    gold_windows/             agrégats par zone et fenêtre, finalisés (Parquet)
    metrics/stream_batches.jsonl  métriques de chaque micro-batch
    results_summary.json      synthèse exploitable pour la présentation
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cityflow_lib import (
    EVENT_SCHEMA,
    WINDOW_DURATION,
    clean_events,
    enrich_with_zones,
    read_event_stream,
    read_events_batch,
    read_zones,
    windowed_global_stats,
    windowed_zone_stats,
)
from spark_session import build_spark

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "L3_Dataset" / "data" / "stream"
DEFAULT_ZONES = PROJECT_ROOT / "L3_Dataset" / "data" / "reference" / "zones.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "output"


# ---------------------------------------------------------------------------
# Utilitaires
# ---------------------------------------------------------------------------
def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=json_default), encoding="utf-8")


def parquet_row_count(spark: SparkSession, path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return spark.read.parquet(str(path)).count()
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Construction du pipeline
# ---------------------------------------------------------------------------
def build_pipeline(spark: SparkSession, source: Path, zones_path: Path) -> tuple[DataFrame, DataFrame]:
    """Retourne (événements enrichis, agrégats fenêtrés par zone)."""
    raw = read_event_stream(spark, str(source), max_files_per_trigger=1)
    # input_file_name() doit être évalué sur la source de streaming seule (pas après
    # la jointure avec la table statique des zones, qui constitue une seconde source).
    raw = raw.withColumn("source_file", F.input_file_name())
    zones = read_zones(spark, str(zones_path))
    events = enrich_with_zones(clean_events(raw, with_watermark=True), zones)
    aggregates = windowed_zone_stats(events, WINDOW_DURATION, WINDOW_DURATION)
    return events, aggregates


# ---------------------------------------------------------------------------
# Mode available : démonstration reproductible
# ---------------------------------------------------------------------------
def run_available(spark: SparkSession, args) -> dict:
    source = Path(args.source_dir)
    output = Path(args.output_dir)
    bronze_dir = output / "bronze_events"
    gold_dir = output / "gold_windows"
    metrics_dir = output / "metrics"
    checkpoint_dir = output / "checkpoints"

    for path in (bronze_dir, gold_dir, metrics_dir, checkpoint_dir):
        reset_dir(path)
    metrics_file = metrics_dir / "stream_batches.jsonl"
    metrics_file.write_text("", encoding="utf-8")

    if not source.exists() or not any(source.glob("*.json")):
        raise SystemExit(f"Aucun fichier JSON dans {source}. Lancez generate_dataset.py.")

    files = sorted(source.glob("*.json"))
    print(f"Source       : {source}")
    print(f"Fichiers     : {len(files)}")
    print("Mode         : availableNow, maxFilesPerTrigger=1\n")

    events, aggregates = build_pipeline(spark, source, Path(args.zones))

    # --- Sink 1 : couche brute typée et enrichie (Parquet, append) ----------
    bronze_query = (
        events.writeStream.format("parquet")
        .option("path", str(bronze_dir))
        .option("checkpointLocation", str(checkpoint_dir / "bronze"))
        .outputMode("append")
        .queryName("cityflow_bronze")
        .trigger(availableNow=True)
        .start()
    )

    # --- Sink 2 : agrégats fenêtrés finalisés (Parquet, append) -------------
    # foreachBatch permet d'ajouter le numéro de micro-batch et de journaliser
    # des métriques par lot, tout en conservant l'append sur le sink Parquet.
    def write_gold(batch_df: DataFrame, batch_id: int) -> None:
        batch_started = time.perf_counter()
        rows = batch_df.count()
        if rows:
            enriched = batch_df.withColumn("batch_id", F.lit(batch_id))
            enriched.write.mode("append").partitionBy("zone_id").parquet(str(gold_dir))

        progress = gold_query.lastProgress or {}
        state_ops = progress.get("stateOperators", []) or []
        dropped = sum(int(op.get("numRowsDroppedByWatermark", 0) or 0) for op in state_ops)
        record = {
            "batch_id": batch_id,
            "rows_written": rows,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "batch_duration_ms": round((time.perf_counter() - batch_started) * 1000),
            "num_input_rows": progress.get("numInputRows"),
            "input_rows_per_second": progress.get("inputRowsPerSecond"),
            "processed_rows_per_second": progress.get("processedRowsPerSecond"),
            "num_rows_dropped_by_watermark": dropped,
            "watermark": progress.get("eventTime", {}).get("watermark"),
        }
        with metrics_file.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    global gold_query
    gold_query = (
        aggregates.writeStream.foreachBatch(write_gold)
        .option("checkpointLocation", str(checkpoint_dir / "gold"))
        .outputMode("append")
        .queryName("cityflow_gold")
        .trigger(availableNow=True)
        .start()
    )

    # --- Sink 3 : vue "temps réel" pour le suivi (mémoire, complete) --------
    # Le mode complete réécrit la table à chaque micro-batch : la vue SQL ne contient
    # donc jamais de doublons, contrairement au mode update du sink mémoire.
    live_view = (
        aggregates.writeStream.format("memory")
        .outputMode("complete")
        .queryName("cityflow_live")
        .trigger(availableNow=True)
        .start()
    )

    # --- Sink 4 : vue globale par fenêtre (console) -------------------------
    global_agg = windowed_global_stats(events, WINDOW_DURATION)
    console_query = (
        global_agg.writeStream.format("console")
        .outputMode("complete")
        .option("truncate", "false")
        .option("numRows", "30")
        .queryName("cityflow_console")
        .trigger(availableNow=True)
        .start()
    )

    started = time.perf_counter()
    for query in (bronze_query, gold_query, live_view, console_query):
        query.awaitTermination()
    elapsed = time.perf_counter() - started

    # --- Mesures ------------------------------------------------------------
    valid_events = parquet_row_count(spark, bronze_dir)
    finalized_windows = spark.read.parquet(str(gold_dir)) if gold_dir.exists() else None
    finalized_rows = finalized_windows.count() if finalized_windows is not None else 0

    windowed_events = 0
    if finalized_windows is not None:
        windowed_events = finalized_windows.agg(F.sum("vehicle_count")).collect()[0][0] or 0

    progress = gold_query.recentProgress
    batch_metrics = []
    for p in progress:
        state_ops = p.get("stateOperators") or []
        batch_metrics.append(
            {
                "batch_id": p.get("batchId"),
                "num_input_rows": p.get("numInputRows", 0),
                "trigger_execution_ms": (p.get("durationMs") or {}).get("triggerExecution"),
                "watermark": (p.get("eventTime") or {}).get("watermark"),
                "max_event_time": (p.get("eventTime") or {}).get("max"),
                "dropped_by_watermark": sum(
                    int(op.get("numRowsDroppedByWatermark", 0) or 0) for op in state_ops
                ),
            }
        )
    dropped_late = sum(b["dropped_by_watermark"] for b in batch_metrics)
    input_batches = len(progress)
    batch_durations = [
        b["trigger_execution_ms"] for b in batch_metrics if b["trigger_execution_ms"]
    ]

    # Agrégats batch (sans watermark) pour comparaison et pour la dernière fenêtre ouverte.
    static = read_events_batch(spark, str(source))
    static_clean = enrich_with_zones(clean_events(static, with_watermark=False), read_zones(spark, str(args.zones)))
    batch_agg = windowed_zone_stats(static_clean, WINDOW_DURATION, WINDOW_DURATION)
    all_windows_count = batch_agg.count()

    # Top zones du batch complet (résultats complets, y compris dernière fenêtre).
    top_zones = [
        row.asDict()
        for row in (
            batch_agg.groupBy("zone_id", "zone_name")
            .agg(
                F.sum("vehicle_count").alias("vehicle_count"),
                F.round(F.avg("avg_speed_kmh"), 1).alias("avg_speed_kmh"),
                F.round(F.avg("congestion_rate"), 3).alias("congestion_rate"),
            )
            .orderBy(F.desc("vehicle_count"))
            .collect()
        )
    ]

    worst_windows = [
        row.asDict()
        for row in (
            batch_agg.filter(F.col("vehicle_count") > 100)
            .orderBy(F.desc("congestion_rate"), F.desc("vehicle_count"))
            .limit(5)
            .select("window_start", "window_end", "zone_id", "zone_name",
                    "vehicle_count", "avg_speed_kmh", "congestion_rate", "traffic_state")
            .collect()
        )
    ]

    # Échantillon lisible des agrégats finalisés.
    sample_windows = []
    if finalized_windows is not None and finalized_rows:
        sample_windows = [
            row.asDict()
            for row in finalized_windows.orderBy("window_start", "zone_id").limit(8).collect()
        ]

    summary = {
        "mode": "availableNow",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_files": len(files),
        "source_dir": str(source),
        "elapsed_seconds_streaming": round(elapsed, 2),
        "micro_batches": input_batches,
        "valid_events": valid_events,
        "finalized_windows": finalized_rows,
        "finalized_events": int(windowed_events),
        "dropped_late_events": int(dropped_late),
        "open_window_events": int(valid_events - windowed_events - dropped_late),
        "all_windows_batch": all_windows_count,
        "avg_batch_duration_ms": round(sum(batch_durations) / len(batch_durations), 1) if batch_durations else None,
        "max_batch_duration_ms": max(batch_durations) if batch_durations else None,
        "batch_metrics": batch_metrics,
        "top_zones": top_zones,
        "worst_windows": worst_windows,
        "sample_windows": sample_windows,
        "outputs": {
            "bronze": str(bronze_dir),
            "gold": str(gold_dir),
            "metrics": str(metrics_file),
        },
    }

    save_json(output / "results_summary.json", summary)

    print("\n" + "=" * 68)
    print("SYNTHÈSE DU TRAITEMENT STREAMING")
    print("=" * 68)
    print(f"Fichiers sources             : {len(files)}")
    print(f"Micro-batchs exécutés        : {input_batches}")
    print(f"Événements valides (Bronze)  : {valid_events:,}")
    print(f"Fenêtres finalisées (Gold)   : {finalized_rows:,}")
    print(f"Événements dans les fenêtres : {int(windowed_events):,}".replace(",", " "))
    print(f"Événements tardifs ignorés   : {int(dropped_late):,}".replace(",", " "))
    print(f"Événements en fenêtre ouverte: {int(valid_events - windowed_events - dropped_late):,}".replace(",", " "))
    print(f"Fenêtres (calcul batch)      : {all_windows_count:,}".replace(",", " "))
    print(f"Durée totale streaming       : {elapsed:.2f} s")
    print(f"Durée moyenne d'un lot       : {summary['avg_batch_duration_ms']} ms")
    print(f"Synthèse JSON                : {output / 'results_summary.json'}")

    # Affichage final lisible (la table mémoire contient la vue à jour).
    print("\nDernières fenêtres agrégées (vue mémoire) :")
    spark.sql(
        """
        SELECT window_start, window_end, zone_id, zone_name, vehicle_count,
               avg_speed_kmh, congestion_rate, traffic_state
        FROM cityflow_live
        ORDER BY window_start DESC, vehicle_count DESC
        LIMIT 10
        """
    ).show(truncate=False)

    return summary


# ---------------------------------------------------------------------------
# Mode live : flux continu alimenté par un producteur
# ---------------------------------------------------------------------------
def run_live(spark: SparkSession, args) -> None:
    source = Path(args.source_dir)
    output = Path(args.output_dir)
    bronze_dir = output / "live_bronze"
    gold_dir = output / "live_gold"
    checkpoint_dir = output / "live_checkpoints"
    for path in (bronze_dir, gold_dir, checkpoint_dir):
        ensure_dirs(path)

    print(f"Mode live : surveillance de {source} toutes les {args.interval} secondes")
    print("Ajoutez des fichiers avec stream_producer.py, puis Ctrl+C pour arrêter.\n")

    events, aggregates = build_pipeline(spark, source, Path(args.zones))

    bronze = (
        events.writeStream.format("parquet")
        .option("path", str(bronze_dir))
        .option("checkpointLocation", str(checkpoint_dir / "bronze"))
        .outputMode("append")
        .queryName("cityflow_live_bronze")
        .trigger(processingTime=f"{args.interval} seconds")
        .start()
    )

    console = (
        aggregates.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "50")
        .queryName("cityflow_live_windows")
        .trigger(processingTime=f"{args.interval} seconds")
        .start()
    )

    try:
        deadline = time.time() + args.duration if args.duration else None
        while all(q.isActive for q in (bronze, console)):
            if deadline and time.time() > deadline:
                break
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")
    finally:
        for q in (bronze, console):
            if q.isActive:
                q.stop()

    print("\nRequêtes arrêtées.")
    print(f"Bronze : {bronze_dir}")
    print(f"Consultez les résultats avec : spark.read.parquet(...)")


# ---------------------------------------------------------------------------
# Mode batch : référence de comparaison
# ---------------------------------------------------------------------------
def run_batch(spark: SparkSession, args) -> dict:
    source = Path(args.source_dir)
    output = Path(args.output_dir)
    batch_dir = output / "batch_windows"
    reset_dir(batch_dir)

    started = time.perf_counter()
    static = read_events_batch(spark, str(source))
    events = enrich_with_zones(clean_events(static, with_watermark=False), read_zones(spark, str(args.zones)))
    aggregates = windowed_zone_stats(events, WINDOW_DURATION, WINDOW_DURATION)
    total = aggregates.count()
    compute_elapsed = time.perf_counter() - started

    write_started = time.perf_counter()
    aggregates.write.mode("overwrite").partitionBy("zone_id").parquet(str(batch_dir))
    write_elapsed = time.perf_counter() - write_started
    elapsed = time.perf_counter() - started

    summary = {
        "mode": "batch",
        "elapsed_seconds": round(elapsed, 2),
        "compute_seconds": round(compute_elapsed, 2),
        "write_seconds": round(write_elapsed, 2),
        "valid_events": events.count(),
        "windows": total,
        "output": str(batch_dir),
    }
    save_json(output / "batch_summary.json", summary)

    print(f"Événements traités : {summary['valid_events']:,}".replace(",", " "))
    print(f"Fenêtres calculées : {total:,}".replace(",", " "))
    print(f"Durée batch        : {elapsed:.2f} s")
    print(f"Résultats          : {batch_dir}")
    return summary


# ---------------------------------------------------------------------------
# Entrée principale
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline CityFlow (Spark Structured Streaming)")
    parser.add_argument("--mode", choices=["available", "live", "batch"], default="available")
    parser.add_argument("--source-dir", default=str(DEFAULT_SOURCE))
    parser.add_argument("--zones", default=str(DEFAULT_ZONES))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--interval", type=int, default=10, help="Période de déclenchement en mode live (s)")
    parser.add_argument("--duration", type=int, default=0, help="Durée maximale en mode live (0 = illimitée)")
    args = parser.parse_args()

    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)

    spark = build_spark(f"CityFlow-{args.mode}")
    spark.sparkContext.setLogLevel("ERROR")

    try:
        if args.mode == "available":
            run_available(spark, args)
        elif args.mode == "live":
            run_live(spark, args)
        else:
            run_batch(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
