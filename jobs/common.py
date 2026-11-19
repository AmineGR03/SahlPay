"""Utilitaires partages par les jobs SahlPay (Parties 1 a 3).

Ce module n'est charge que par le driver : il ne contient aucune fonction
envoyee aux executors (les UDF restent definies dans chaque job).
"""
from __future__ import annotations

import os
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession

# Valeurs par defaut alignees sur docker-compose.yml
DEFAULT_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

LAKE = "s3a://lake"
BRONZE_HISTORY = f"{LAKE}/bronze/history"
SILVER_HISTORY = f"{LAKE}/silver/transactions_history"


def build_session(app_name: str, master: str | None = None) -> SparkSession:
    """Cree une SparkSession connectee au Master standalone, acces MinIO inclus.

    Les memes options sont deja dans ``conf/spark-defaults.conf`` de l'image ;
    on les repete ici pour que les jobs restent executables tels quels depuis
    un notebook ou un poste local.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or DEFAULT_MASTER)
        # --- acces S3A vers MinIO ---
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def banner(title: str) -> None:
    """Separateur lisible dans les logs et les captures d'ecran du rapport."""
    print("\n" + "=" * 78)
    print(f"  {title}")
    print("=" * 78)


@contextmanager
def chrono(label: str, results: dict | None = None):
    """Mesure le temps d'une action Spark (utilise pour comparer RDD / DataFrame)."""
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"[duree] {label} : {elapsed:.2f} s")
    if results is not None:
        results[label] = elapsed


def hold_ui(spark: SparkSession, seconds: int) -> None:
    """Maintient le driver en vie pour capturer le DAG dans la Spark UI."""
    if seconds <= 0:
        return
    url = spark.sparkContext.uiWebUrl
    print(f"\nSpark UI disponible sur {url} pendant {seconds} s "
          f"(onglets Jobs / Stages / DAG Visualization).")
    time.sleep(seconds)
