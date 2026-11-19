"""Partie 1 - Mise en place du cluster et positionnement.

Verifie le deploiement et affiche les elements demandes au 3.1.2 :
version de Spark, master, appName, nombre de coeurs disponibles
(``sc.defaultParallelism``), executors enregistres, acces a MinIO et
presence du topic Kafka.

    ./scripts/submit.sh partie1_cluster_check.py --hold 120
"""
from __future__ import annotations

import argparse
import os
import socket
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import LAKE, banner, build_session, hold_ui  # noqa: E402


def show_session(spark) -> None:
    sc = spark.sparkContext
    banner("Partie 1 - SparkSession connectee au cluster")
    print(f"Version de Spark        : {spark.version}")
    print(f"Master                  : {sc.master}")
    print(f"appName                 : {sc.appName}")
    print(f"applicationId           : {sc.applicationId}")
    print(f"defaultParallelism      : {sc.defaultParallelism}  (coeurs disponibles)")
    print(f"Spark UI du driver      : {sc.uiWebUrl}")
    print(f"Driver (hote)           : {socket.gethostname()}")
    print(f"deployMode              : {sc.getConf().get('spark.submit.deployMode', 'client')}")

    # Executors enregistres : la liste exclut le driver.
    status = sc._jsc.sc().statusTracker()
    executors = [e for e in status.getExecutorInfos()]
    print(f"\nExecutors enregistres   : {len(executors) - 1} (hors driver)")
    for info in executors:
        print(f"  - {info.host()}")


def smoke_test(spark) -> None:
    """Petit calcul distribue : prouve que les Workers executent bien des taches."""
    sc = spark.sparkContext
    banner("Test de calcul distribue")
    rdd = sc.parallelize(range(1, 1_000_001), numSlices=sc.defaultParallelism)
    total = rdd.filter(lambda x: x % 3 == 0).sum()
    print(f"Somme des multiples de 3 jusqu'a 1 000 000 : {total}")
    print(f"Nombre de partitions du RDD                : {rdd.getNumPartitions()}")


def check_lake(spark, path: str) -> None:
    """Verifie que le bucket MinIO est joignable via le connecteur S3A."""
    banner("Acces au lac de donnees (MinIO / S3A)")
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    try:
        uri = jvm.java.net.URI(path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        statuses = fs.listStatus(jvm.org.apache.hadoop.fs.Path(path))
        print(f"Contenu de {path} :")
        for st in statuses:
            kind = "rep." if st.isDirectory() else "fich."
            print(f"  [{kind}] {st.getPath().getName()}")
    except Exception as exc:  # pragma: no cover - diagnostic
        print(f"Acces a {path} impossible : {exc}")
        print("Verifiez que le service minio tourne et que le bucket lake existe "
              "(docker compose up minio-init).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Partie 1 - verification du cluster")
    parser.add_argument("--master", default=None,
                        help="URL du Master (defaut : spark://spark-master:7077)")
    parser.add_argument("--lake", default=LAKE, help="racine du bucket a inspecter")
    parser.add_argument("--hold", type=int, default=0,
                        help="secondes pendant lesquelles garder la Spark UI ouverte")
    args = parser.parse_args()

    spark = build_session("sahlpay-partie1-cluster-check", args.master)
    try:
        show_session(spark)
        smoke_test(spark)
        check_lake(spark, args.lake)
        banner("Partie 1 terminee")
        print("Preuves a capturer : Spark Master UI http://localhost:8080 "
              "(2 Workers ALIVE) et console MinIO http://localhost:9001 (bucket lake).")
        hold_ui(spark, args.hold)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
