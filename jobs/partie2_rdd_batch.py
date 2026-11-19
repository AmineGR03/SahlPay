"""Partie 2 - Traitement batch avec SparkCore (API RDD).

Repond aux sept points du 3.2.2 sur l'historique genere par producer.py :

  1. chargement en RDD avec ``sc.textFile`` et parsing par ``json.loads`` ;
  2. ``filter`` sur les transactions COMPLETED ;
  3. ``map`` + ``reduceByKey`` : montant total encaisse par ville ;
  4. montant moyen par transaction et par ville (somme et nombre en un seul
     passage, avec ``aggregateByKey``) ;
  5. top 5 des commercants par nombre de paiements valides (``takeOrdered``) ;
  6. lineage via ``toDebugString()`` et distinction transformations / actions ;
  7. elements pour le rapport : nombre de stages et origine du shuffle.

    ./scripts/submit.sh partie2_rdd_batch.py \
        --input s3a://lake/bronze/history/transactions_history.jsonl --hold 180
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import BRONZE_HISTORY, banner, build_session, chrono, hold_ui  # noqa: E402


# --------------------------------------------------------------------------
# Fonctions executees sur les executors : elles doivent rester au niveau du
# module pour etre serialisables (pickle) et envoyees avec la tache.
# --------------------------------------------------------------------------
def parse_line(line: str):
    """Transforme une ligne JSON en dictionnaire ; renvoie None si illisible."""
    try:
        return json.loads(line)
    except (ValueError, TypeError):
        return None


def is_completed(event) -> bool:
    return event is not None and event.get("status") == "COMPLETED"


def to_city_amount(event):
    """(ville, montant) - couple cle/valeur pour les agregations par ville."""
    return event["city"], float(event["amount_mad"])


def to_city_sum_count(event):
    """(ville, (montant, 1)) - permet somme et comptage en un seul passage."""
    return event["city"], (float(event["amount_mad"]), 1)


def to_merchant_one(event):
    """(merchant_id, 1) pour les seuls paiements chez un commercant (P2M)."""
    return event["payee_id"], 1


def is_p2m(event) -> bool:
    return event.get("type") == "P2M"


def dedup_key(event):
    """Cle d'unicite d'un evenement metier : (tx_id, status)."""
    return (event["tx_id"], event["status"]), event


# --------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Partie 2 - batch RDD SparkCore")
    parser.add_argument(
        "--input",
        default=f"{BRONZE_HISTORY}/transactions_history.jsonl",
        help="fichier JSON Lines (s3a://..., file:///... ou chemin local)",
    )
    parser.add_argument("--master", default=None)
    parser.add_argument("--top", type=int, default=5,
                        help="taille du classement des commercants")
    parser.add_argument("--dedup", action="store_true",
                        help="supprimer les doublons stricts (tx_id, status) "
                             "avant les agregations")
    parser.add_argument("--hold", type=int, default=0,
                        help="secondes pendant lesquelles garder la Spark UI ouverte")
    args = parser.parse_args()

    spark = build_session("sahlpay-partie2-rdd-batch", args.master)
    sc = spark.sparkContext
    durees = {}

    try:
        # ------------------------------------------------------------------
        # 1. Chargement et parsing
        # ------------------------------------------------------------------
        banner("1. Chargement du fichier en RDD et parsing JSON")
        raw = sc.textFile(args.input)
        print(f"Source                  : {args.input}")
        print(f"Partitions du RDD source: {raw.getNumPartitions()}")

        events = raw.map(parse_line).filter(lambda e: e is not None)

        # ------------------------------------------------------------------
        # 2. Filtrage des transactions COMPLETED
        # ------------------------------------------------------------------
        banner("2. Filtrage des transactions COMPLETED")
        completed = events.filter(is_completed)

        if args.dedup:
            # Le generateur injecte ~2 % de doublons stricts ; on garde une
            # occurrence par couple (tx_id, status).
            completed = (
                completed.map(dedup_key)
                .reduceByKey(lambda a, _b: a)
                .values()
            )
            print("Deduplication (tx_id, status) activee.")

        # Le RDD filtre est reutilise par trois agregations : on le met en
        # cache pour ne pas relire et reparser le fichier a chaque action.
        completed.cache()

        with chrono("comptage des evenements COMPLETED", durees):
            nb_completed = completed.count()
        with chrono("comptage des evenements bruts", durees):
            nb_total = events.count()
        print(f"Evenements lus          : {nb_total}")
        print(f"Dont COMPLETED          : {nb_completed} "
              f"({nb_completed / max(nb_total, 1):.1%})")

        # Mesure des doublons, utile pour le rapport et pour la Partie 5.
        nb_distinct = completed.map(lambda e: (e["tx_id"], e["status"])).distinct().count()
        print(f"Couples (tx_id, status) distincts : {nb_distinct} "
              f"-> {nb_completed - nb_distinct} doublons stricts")

        # ------------------------------------------------------------------
        # 3. Montant total encaisse par ville (map + reduceByKey)
        # ------------------------------------------------------------------
        banner("3. Montant total encaisse par ville (map + reduceByKey)")
        total_par_ville = completed.map(to_city_amount).reduceByKey(lambda a, b: a + b)
        with chrono("montant total par ville", durees):
            totaux = total_par_ville.collect()
        for city, total in sorted(totaux, key=lambda kv: -kv[1]):
            print(f"  {city:<12} {total:>15,.2f} MAD")

        # ------------------------------------------------------------------
        # 4. Montant moyen par transaction et par ville (un seul passage)
        # ------------------------------------------------------------------
        banner("4. Montant total ET moyen par ville (aggregateByKey, un seul passage)")
        # aggregateByKey agrege localement (seqOp) avant le shuffle (combOp) :
        # une seule lecture des donnees pour la somme et le comptage.
        stats_ville = completed.map(to_city_sum_count).aggregateByKey(
            (0.0, 0),
            lambda acc, v: (acc[0] + v[0], acc[1] + v[1]),          # seqOp
            lambda a, b: (a[0] + b[0], a[1] + b[1]),                # combOp
        )
        with chrono("montant total et moyen par ville", durees):
            stats = stats_ville.collect()

        print(f"{'Ville':<12} {'Nb transactions':>16} {'Montant total':>18} {'Montant moyen':>16}")
        print("-" * 66)
        for city, (somme, nb) in sorted(stats, key=lambda kv: -kv[1][0]):
            print(f"{city:<12} {nb:>16,} {somme:>18,.2f} {somme / nb:>16,.2f}")

        # ------------------------------------------------------------------
        # 5. Top 5 des commercants par nombre de paiements valides
        # ------------------------------------------------------------------
        banner(f"5. Top {args.top} des commercants par nombre de paiements valides")
        par_commercant = (
            completed.filter(is_p2m)
            .map(to_merchant_one)
            .reduceByKey(lambda a, b: a + b)
        )
        with chrono(f"top {args.top} commercants (takeOrdered)", durees):
            # takeOrdered ramene seulement N elements au driver : pas de collect
            # de l'ensemble du RDD, donc pas de risque de saturer le driver.
            top = par_commercant.takeOrdered(args.top, key=lambda kv: -kv[1])
        for rang, (merchant, nb) in enumerate(top, start=1):
            print(f"  {rang}. {merchant} : {nb} paiements valides")

        # ------------------------------------------------------------------
        # 6. Lineage : transformations (lazy) vs actions
        # ------------------------------------------------------------------
        banner("6. Lineage du RDD (toDebugString)")
        print(total_par_ville.toDebugString().decode("utf-8"))
        print(
            "\nLecture du lineage :\n"
            "  - chaque indentation supplementaire = un nouveau stage, donc un shuffle ;\n"
            "  - ShuffledRDD correspond au reduceByKey : les couples sont redistribues\n"
            "    par hash de la cle (la ville) entre les executors.\n"
            "\nTransformations (lazy, rien n'est calcule) : textFile, map, filter,\n"
            "  reduceByKey, aggregateByKey, distinct, cache.\n"
            "Actions (declenchent l'execution) : count, collect, takeOrdered."
        )

        # ------------------------------------------------------------------
        # 7. Elements pour le rapport
        # ------------------------------------------------------------------
        banner("7. Elements pour le rapport")
        nb_stages = total_par_ville.toDebugString().decode("utf-8").count("\n|")
        print(f"Partitions apres shuffle : {total_par_ville.getNumPartitions()}")
        print(f"Dependances shuffle dans le lineage : {nb_stages}")
        print("Durees mesurees :")
        for label, duree in durees.items():
            print(f"  - {label:<40} {duree:>6.2f} s")
        print(
            "\nA capturer pour le rapport : onglet Jobs puis Stages de la Spark UI\n"
            f"({sc.uiWebUrl}), avec la DAG Visualization du job 'montant total par ville'."
        )

        hold_ui(spark, args.hold)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
