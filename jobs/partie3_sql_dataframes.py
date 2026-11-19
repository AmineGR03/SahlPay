"""Partie 3 - Spark SQL et DataFrames.

Reprend l'analyse de la Partie 2 avec l'API structuree (3.3.2) :

  1. lecture de ``transactions_history.jsonl`` et ``merchants.json`` avec un
     schema explicite (``StructType``), sans inference ;
  2. montant par ville en API DataFrame puis en SQL
     (``createOrReplaceTempView`` + ``spark.sql``) ;
  3. jointure des paiements P2M avec le referentiel merchants, puis montant
     encaisse par categorie et par ville ;
  4. UDF ``amount_band`` comparee a une version ``when/otherwise`` ;
  5. ecriture Parquet dans ``lake/silver/transactions_history/``,
     partitionnee par ``city`` ;
  6. ``explain(True)`` sur la jointure : plan logique, plan optimise par
     Catalyst, strategie physique (BroadcastHashJoin ou SortMergeJoin) ;
  7. mesures de duree pour la comparaison RDD / DataFrame / Dataset.

    ./scripts/submit.sh partie3_sql_dataframes.py --hold 180
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from common import BRONZE_HISTORY, SILVER_HISTORY, banner, build_session, chrono, hold_ui  # noqa: E402

# --------------------------------------------------------------------------
# 1. Schemas explicites : pas d'inference, donc pas de lecture prealable du
#    fichier et un typage garanti (important sur un flux Kafka en Partie 4).
# --------------------------------------------------------------------------
TRANSACTION_SCHEMA = StructType([
    StructField("tx_id", StringType(), nullable=False),
    StructField("type", StringType(), nullable=True),          # P2M ou P2P
    StructField("payer_wallet", StringType(), nullable=True),
    StructField("payee_id", StringType(), nullable=True),      # M-... ou W-...
    StructField("device_id", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=True),       # APP, QR, USSD
    StructField("status", StringType(), nullable=True),
    StructField("amount_mad", DoubleType(), nullable=True),
    StructField("event_time", StringType(), nullable=True),    # converti ensuite
])

MERCHANT_SCHEMA = StructType([
    StructField("merchant_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
])

# Seuils de la bande de montant (point 4 du 3.3.2)
SEUIL_MICRO = 50.0
SEUIL_ELEVE = 2000.0


def amount_band(amount_mad):
    """UDF Python : classe un montant en micro / standard / eleve."""
    if amount_mad is None:
        return None
    if amount_mad < SEUIL_MICRO:
        return "micro"
    if amount_mad <= SEUIL_ELEVE:
        return "standard"
    return "eleve"


def main() -> None:
    parser = argparse.ArgumentParser(description="Partie 3 - Spark SQL et DataFrames")
    parser.add_argument("--input", default=f"{BRONZE_HISTORY}/transactions_history.jsonl")
    parser.add_argument("--merchants", default=f"{BRONZE_HISTORY}/merchants.json")
    parser.add_argument("--output", default=SILVER_HISTORY,
                        help="destination Parquet de la couche Silver")
    parser.add_argument("--master", default=None)
    parser.add_argument("--no-write", action="store_true",
                        help="ne pas ecrire la couche Silver (analyse seule)")
    parser.add_argument("--hold", type=int, default=0)
    args = parser.parse_args()

    spark = build_session("sahlpay-partie3-sql-dataframes", args.master)
    durees = {}

    try:
        # ------------------------------------------------------------------
        # 1. Lecture avec schema explicite
        # ------------------------------------------------------------------
        banner("1. Lecture avec schema explicite (sans inference)")
        transactions = (
            spark.read.schema(TRANSACTION_SCHEMA).json(args.input)
            # L'horodatage metier est converti une seule fois, ici.
            .withColumn("event_ts", F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .withColumn("event_date", F.to_date("event_ts"))
        )
        merchants = spark.read.schema(MERCHANT_SCHEMA).json(args.merchants)

        transactions.printSchema()
        merchants.printSchema()
        print(f"Transactions : {transactions.count()} evenements")
        print(f"Merchants    : {merchants.count()} commercants")

        completed = transactions.filter(F.col("status") == "COMPLETED")
        completed.cache()
        print(f"COMPLETED    : {completed.count()} evenements")

        # ------------------------------------------------------------------
        # 2. Montant par ville : API DataFrame puis SQL
        # ------------------------------------------------------------------
        banner("2a. Montant par ville - API DataFrame")
        par_ville_df = (
            completed.groupBy("city")
            .agg(
                F.count("*").alias("nb_transactions"),
                F.round(F.sum("amount_mad"), 2).alias("montant_total"),
                F.round(F.avg("amount_mad"), 2).alias("montant_moyen"),
            )
            .orderBy(F.desc("montant_total"))
        )
        with chrono("montant par ville (API DataFrame)", durees):
            par_ville_df.show(truncate=False)

        banner("2b. Montant par ville - SQL")
        transactions.createOrReplaceTempView("transactions")
        merchants.createOrReplaceTempView("merchants")
        par_ville_sql = spark.sql("""
            SELECT city,
                   COUNT(*)                        AS nb_transactions,
                   ROUND(SUM(amount_mad), 2)       AS montant_total,
                   ROUND(AVG(amount_mad), 2)       AS montant_moyen
            FROM transactions
            WHERE status = 'COMPLETED'
            GROUP BY city
            ORDER BY montant_total DESC
        """)
        with chrono("montant par ville (SQL)", durees):
            par_ville_sql.show(truncate=False)
        print("Les deux ecritures produisent le meme plan physique : "
              "l'API DataFrame et SQL passent par le meme optimiseur Catalyst.")

        # ------------------------------------------------------------------
        # 3. Jointure P2M x merchants : montant par categorie et par ville
        # ------------------------------------------------------------------
        banner("3. Jointure des paiements P2M avec le referentiel merchants")
        paiements = completed.filter(F.col("type") == "P2M")
        joint = paiements.join(
            merchants.withColumnRenamed("city", "merchant_city"),
            paiements["payee_id"] == merchants["merchant_id"],
            how="inner",
        )
        par_cat_ville = (
            joint.groupBy("merchant_city", "category")
            .agg(
                F.count("*").alias("nb_paiements"),
                F.round(F.sum("amount_mad"), 2).alias("montant_total"),
            )
            .orderBy(F.desc("montant_total"))
        )
        with chrono("montant par categorie et par ville", durees):
            par_cat_ville.show(20, truncate=False)

        # ------------------------------------------------------------------
        # 4. UDF amount_band vs when/otherwise
        # ------------------------------------------------------------------
        banner("4. UDF amount_band comparee a when/otherwise")
        band_udf = F.udf(amount_band, StringType())

        avec_udf = completed.withColumn("amount_band", band_udf(F.col("amount_mad")))
        with chrono("repartition des bandes - version UDF Python", durees):
            avec_udf.groupBy("amount_band").count().orderBy("amount_band").show()

        # Version native : expression Catalyst, evaluee dans la JVM.
        band_col = (
            F.when(F.col("amount_mad") < SEUIL_MICRO, F.lit("micro"))
            .when(F.col("amount_mad") <= SEUIL_ELEVE, F.lit("standard"))
            .otherwise(F.lit("eleve"))
        )
        sans_udf = completed.withColumn("amount_band", band_col)
        with chrono("repartition des bandes - version when/otherwise", durees):
            sans_udf.groupBy("amount_band").count().orderBy("amount_band").show()

        print(
            "Pourquoi when/otherwise est plus rapide :\n"
            "  - l'UDF Python est une boite noire pour Catalyst : aucune optimisation,\n"
            "    ni pushdown ni generation de code (Tungsten / whole-stage codegen) ;\n"
            "  - chaque ligne doit etre serialisee de la JVM vers un worker Python\n"
            "    puis deserialisee au retour, ce qui domine le cout du calcul ;\n"
            "  - when/otherwise reste une expression Catalyst evaluee dans la JVM\n"
            "    sur le format binaire Tungsten, sans changement de processus."
        )

        # Les lignes ecrites en Silver portent la bande calculee sans UDF.
        enrichi = sans_udf

        # ------------------------------------------------------------------
        # 5. Ecriture Parquet partitionnee par ville (couche Silver)
        # ------------------------------------------------------------------
        if args.no_write:
            banner("5. Ecriture Silver ignoree (--no-write)")
        else:
            banner(f"5. Ecriture Parquet partitionnee par city -> {args.output}")
            with chrono("ecriture Parquet (Silver)", durees):
                (
                    enrichi.select(
                        "tx_id", "type", "payer_wallet", "payee_id", "device_id",
                        "channel", "status", "amount_mad", "amount_band",
                        "event_ts", "event_date", "city",
                    )
                    .write.mode("overwrite")
                    .partitionBy("city")
                    .parquet(args.output)
                )
            relu = spark.read.parquet(args.output)
            print(f"Relecture : {relu.count()} lignes, "
                  f"{len(relu.columns)} colonnes, partitionnement par city.")
            print("Verification : mc ls --recursive local/lake/silver/transactions_history/")

        # ------------------------------------------------------------------
        # 6. explain(True) sur la jointure
        # ------------------------------------------------------------------
        banner("6. explain(True) sur la jointure P2M x merchants")
        par_cat_ville.explain(True)
        print(
            "\nLecture du plan :\n"
            "  - Parsed / Analyzed Logical Plan : la requete telle qu'ecrite, puis\n"
            "    resolue (colonnes et types verifies) ;\n"
            "  - Optimized Logical Plan : Catalyst a remonte les filtres sous la\n"
            "    jointure (predicate pushdown) et elague les colonnes inutiles ;\n"
            "  - Physical Plan : le referentiel merchants ne pese que 300 lignes,\n"
            "    donc sous le seuil spark.sql.autoBroadcastJoinThreshold (10 Mo) :\n"
            "    Catalyst choisit un BroadcastHashJoin, qui diffuse la petite table\n"
            "    a tous les executors et evite le shuffle du gros cote.\n"
            "    En desactivant le broadcast (seuil a -1), le plan repasse en\n"
            "    SortMergeJoin, avec Exchange hashpartitioning des deux cotes."
        )

        # Demonstration : meme jointure sans broadcast
        banner("6b. Meme jointure avec le broadcast desactive (SortMergeJoin)")
        ancien_seuil = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        try:
            paiements.join(
                merchants, paiements["payee_id"] == merchants["merchant_id"]
            ).groupBy("category").count().explain(False)
        finally:
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", ancien_seuil)

        # ------------------------------------------------------------------
        # 7. Recapitulatif des durees (comparaison avec la Partie 2)
        # ------------------------------------------------------------------
        banner("7. Durees mesurees (a comparer avec la Partie 2)")
        for label, duree in durees.items():
            print(f"  - {label:<50} {duree:>6.2f} s")
        print(
            "\nRDD / DataFrame / Dataset :\n"
            "  - RDD : collection distribuee d'objets Python, typage non connu de\n"
            "    Spark, aucune optimisation automatique, serialisation Python ;\n"
            "  - DataFrame : lignes typees par un schema, optimisees par Catalyst\n"
            "    et compilees par Tungsten ; en PySpark c'est l'API a privilegier ;\n"
            "  - Dataset : typage statique fort (Scala / Java uniquement), memes\n"
            "    optimisations que DataFrame, verifications a la compilation.\n"
            "    En Python, Dataset[T] n'existe pas : DataFrame = Dataset[Row]."
        )

        hold_ui(spark, args.hold)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
