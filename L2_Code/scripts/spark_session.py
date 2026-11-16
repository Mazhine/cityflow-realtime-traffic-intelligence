# -*- coding: utf-8 -*-
"""Configuration commune de la session Spark pour le projet CityFlow."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _configure_hadoop_windows() -> Path | None:
    """Sur Windows, Hadoop a besoin de winutils.exe et hadoop.dll.

    Le projet embarque ces binaires dans ``tools/hadoop/bin`` (Apache Hadoop 3.3.6).
    La JVM ne sait pas charger une DLL dont le chemin contient des espaces ou des
    caractères non ASCII : on copie donc les binaires dans un répertoire temporaire
    neutre, puis on positionne HADOOP_HOME et java.library.path.
    """
    if os.name != "nt":
        return None
    source_bin = PROJECT_ROOT / "tools" / "hadoop" / "bin"
    if not (source_bin / "winutils.exe").exists():
        return None

    target_bin = Path(tempfile.gettempdir()) / "cityflow_hadoop" / "bin"
    target_bin.mkdir(parents=True, exist_ok=True)
    for name in ("winutils.exe", "hadoop.dll"):
        src, dst = source_bin / name, target_bin / name
        if src.exists() and (not dst.exists() or src.stat().st_mtime > dst.stat().st_mtime):
            shutil.copy2(src, dst)

    hadoop_home = target_bin.parent
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)
    os.environ["PATH"] = str(target_bin) + os.pathsep + os.environ.get("PATH", "")
    java_opts = f"-Djava.library.path={target_bin.as_posix()}"
    existing = os.environ.get("JAVA_TOOL_OPTIONS", "")
    if java_opts not in existing:
        os.environ["JAVA_TOOL_OPTIONS"] = (existing + " " + java_opts).strip()
    return hadoop_home


def build_spark(app_name: str = "CityFlow-Traffic-Intelligence") -> SparkSession:
    """Crée (ou récupère) une session Spark locale adaptée au prototype."""
    hadoop_home = _configure_hadoop_windows()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # 4 partitions suffisent pour un prototype local et rendent les micro-batchs lisibles.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        # Les fichiers JSON sont relus à chaque démarrage : on force le schéma côté code.
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
    )

    if hadoop_home is not None:
        builder = builder.config("spark.hadoop.hadoop.home.dir", str(hadoop_home))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
