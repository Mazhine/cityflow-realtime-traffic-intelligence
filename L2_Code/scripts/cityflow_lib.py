# -*- coding: utf-8 -*-
"""
CityFlow - Bibliothèque de transformations PySpark.

Ce module centralise :
  * le schéma explicite des événements de trafic ;
  * le nettoyage / typage du flux ;
  * l'enrichissement par la table de référence des zones (jointure stream-static) ;
  * les agrégations par fenêtre temporelle et par zone ;
  * les fonctions de sink (console, mémoire, Parquet) et de mesure.

Il est utilisé à la fois par le notebook (L2) et par le script ``cityflow_pipeline.py``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# 1. Schéma explicite
# ---------------------------------------------------------------------------
# Un schéma explicite est fortement recommandé en streaming : il évite l'inférence
# (coûteuse et instable) et permet de typer correctement les colonnes dès la lecture.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),   # chaîne ISO-8601, convertie ensuite
        StructField("zone_id", StringType(), True),
        StructField("zone_name", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("lane", IntegerType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("speed_kmh", DoubleType(), True),
    ]
)

ZONE_SCHEMA = StructType(
    [
        StructField("zone_id", StringType(), True),
        StructField("zone_name", StringType(), True),
        StructField("district", StringType(), True),
        StructField("speed_limit_kmh", IntegerType(), True),
        StructField("lanes", IntegerType(), True),
    ]
)

WATERMARK_DELAY = "10 minutes"
WINDOW_DURATION = "10 minutes"
WINDOW_SLIDE = "10 minutes"   # fenêtre tumblante : 1 événement = 1 fenêtre


# ---------------------------------------------------------------------------
# 2. Lecture
# ---------------------------------------------------------------------------
def read_event_stream(spark: SparkSession, path: str, max_files_per_trigger: int = 1) -> DataFrame:
    """Lit un répertoire comme flux de fichiers JSON (source du streaming).

    ``maxFilesPerTrigger=1`` force un micro-batch par fichier : c'est ce qui permet
    d'observer l'évolution des agrégats au fil de l'arrivée des fichiers.
    """
    return (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("mode", "PERMISSIVE")          # une ligne JSON invalide devient une ligne de nulls
        .option("multiLine", "false")
        .json(path)
    )


def read_events_batch(spark: SparkSession, path: str) -> DataFrame:
    """Lecture batch (statique) du même répertoire : sert de référence de comparaison."""
    return (
        spark.read.schema(EVENT_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(path)
    )


def read_zones(spark: SparkSession, path: str) -> DataFrame:
    """Table de référence statique des zones urbaines."""
    return spark.read.schema(ZONE_SCHEMA).option("header", "true").csv(path)


# ---------------------------------------------------------------------------
# 3. Nettoyage et enrichissement
# ---------------------------------------------------------------------------
def clean_events(df: DataFrame, with_watermark: bool = True) -> DataFrame:
    """Type, filtre et annote les événements.

    Règles de validation appliquées :
      - ``event_id`` et ``event_time`` non nuls (rejette les lignes JSON corrompues) ;
      - ``zone_id`` au format Zxx ;
      - vitesse comprise entre 0 et 200 km/h ;
      - ``event_time`` converti en timestamp (UTC).
    """
    cleaned = (
        df.withColumn("event_time", F.to_timestamp("event_time"))
        .filter(F.col("event_id").isNotNull() & F.col("event_time").isNotNull())
        .filter(F.col("zone_id").rlike(r"^Z\d{2}$"))
        .filter(F.col("speed_kmh").between(0, 200))
        .withColumn("event_hour", F.date_trunc("hour", "event_time"))
        .withColumn("speed_band", F.when(F.col("speed_kmh") < 20, "0-20 km/h")
                    .when(F.col("speed_kmh") < 40, "20-40 km/h")
                    .when(F.col("speed_kmh") < 60, "40-60 km/h")
                    .otherwise("60+ km/h"))
    )

    if with_watermark:
        # Watermark : Spark mémorise l'état des fenêtres jusqu'à 10 minutes après
        # l'événement le plus récent, puis libère l'état des fenêtres plus anciennes.
        cleaned = cleaned.withWatermark("event_time", WATERMARK_DELAY)

    return cleaned


def enrich_with_zones(events: DataFrame, zones: DataFrame) -> DataFrame:
    """Jointure stream-static : enrichit chaque événement avec les attributs de sa zone.

    La table des zones est petite : on la diffuse (broadcast) pour éviter un shuffle
    et conserver un traitement par partition du flux.
    """
    return (
        events.drop("zone_name")
        .join(F.broadcast(zones), on="zone_id", how="left")
        .withColumn("speed_ratio", F.round(F.col("speed_kmh") / F.col("speed_limit_kmh"), 3))
        .withColumn(
            "is_congested",
            F.when(F.col("speed_ratio") < 0.60, F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "congestion_level",
            F.when(F.col("speed_ratio") < 0.40, "Critique")
            .when(F.col("speed_ratio") < 0.55, "Dense")
            .when(F.col("speed_ratio") < 0.75, "Fluide")
            .otherwise("Libre"),
        )
    )


# ---------------------------------------------------------------------------
# 4. Agrégations fenêtrées
# ---------------------------------------------------------------------------
def windowed_zone_stats(
    events: DataFrame,
    window_duration: str = WINDOW_DURATION,
    slide: str = WINDOW_SLIDE,
) -> DataFrame:
    """Nombre de passages et statistiques de vitesse par zone et par fenêtre.

    La fenêtre est temporelle et basée sur ``event_time`` (temps de l'événement),
    et non sur l'heure d'arrivée du fichier : c'est le principe du event-time processing.
    """
    return (
        events.groupBy(
            F.window("event_time", window_duration, slide).alias("window"),
            "zone_id",
            "zone_name",
            "district",
            "speed_limit_kmh",
        )
        .agg(
            F.count("*").alias("vehicle_count"),
            F.round(F.avg("speed_kmh"), 1).alias("avg_speed_kmh"),
            F.round(F.percentile_approx("speed_kmh", 0.50), 1).alias("median_speed_kmh"),
            F.round(F.percentile_approx("speed_kmh", 0.95), 1).alias("p95_speed_kmh"),
            F.round(F.min("speed_kmh"), 1).alias("min_speed_kmh"),
            F.round(F.max("speed_kmh"), 1).alias("max_speed_kmh"),
            F.sum("is_congested").alias("congested_events"),
            F.approx_count_distinct("sensor_id", 0.05).alias("active_sensors"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn(
            "avg_speed_ratio",
            F.round(F.col("avg_speed_kmh") / F.col("speed_limit_kmh"), 3),
        )
        .withColumn(
            "congestion_rate",
            F.round(F.col("congested_events") / F.col("vehicle_count"), 3),
        )
        .withColumn(
            "traffic_state",
            F.when(F.col("avg_speed_ratio") < 0.40, "Critique")
            .when(F.col("avg_speed_ratio") < 0.55, "Dense")
            .when(F.col("avg_speed_ratio") < 0.75, "Fluide")
            .otherwise("Libre"),
        )
        .withColumn("computed_at", F.current_timestamp())
        .drop("window")
    )


def windowed_global_stats(events: DataFrame, window_duration: str = WINDOW_DURATION) -> DataFrame:
    """Vue globale (toutes zones) par fenêtre : sert au monitoring temporel."""
    return (
        events.groupBy(F.window("event_time", window_duration).alias("window"))
        .agg(
            F.count("*").alias("vehicle_count"),
            F.round(F.avg("speed_kmh"), 1).alias("avg_speed_kmh"),
            F.sum("is_congested").alias("congested_events"),
            F.approx_count_distinct("zone_id", 0.05).alias("active_zones"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


# ---------------------------------------------------------------------------
# 5. Sinks
# ---------------------------------------------------------------------------
def write_console(df: DataFrame, mode: str, query_name: str, trigger: dict | None = None):
    """Sink de debug : affiche les résultats dans la console."""
    writer = df.writeStream.format("console").outputMode(mode).queryName(query_name)
    writer = writer.option("truncate", "false").option("numRows", "100")
    if trigger:
        writer = writer.trigger(**trigger)
    return writer.start()


def write_memory(df: DataFrame, mode: str, query_name: str, trigger: dict | None = None):
    """Sink mémoire : rend le résultat interrogeable en SQL pendant l'exécution.

    Remarque : le sink mémoire ne persiste rien et ne supporte pas de checkpoint ;
    il est réservé à la démonstration interactive (pas à la production).
    """
    writer = df.writeStream.format("memory").outputMode(mode).queryName(query_name)
    if trigger:
        writer = writer.trigger(**trigger)
    return writer.start()


def write_parquet(df: DataFrame, path: str, checkpoint: str, mode: str, query_name: str,
                  trigger: dict | None = None):
    """Sink durable Parquet, avec checkpoint pour garantir la reprise sans perte."""
    writer = (
        df.writeStream.format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .outputMode(mode)
        .queryName(query_name)
    )
    if trigger:
        writer = writer.trigger(**trigger)
    return writer.start()


def write_parquet_partitioned(df: DataFrame, path: str, checkpoint: str, mode: str,
                              query_name: str, trigger: dict | None = None):
    """Sink Parquet partitionné par zone (utile pour les requêtes analytiques aval)."""
    writer = (
        df.writeStream.format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .partitionBy("zone_id")
        .outputMode(mode)
        .queryName(query_name)
    )
    if trigger:
        writer = writer.trigger(**trigger)
    return writer.start()
