# SahlPay.ma — Plateforme Big Data pour un portefeuille mobile

Projet **Écosystème Big Data 2** — 5ème année EMSI — Année universitaire 2026–2027.

Suivi des paiements en temps réel et détection de structures inhabituelles avec
**Apache Spark, Kafka, GraphX et GraphFrames**.

> **État du dépôt : Jalon 1 livré** — Parties 1, 2 et 3 (cluster, batch RDD,
> Spark SQL). Les Parties 4, 5 et 6 (Structured Streaming, pipeline de
> production, analyse de graphes) relèvent du Jalon 2 ; l'infrastructure
> nécessaire (Kafka, Delta Lake, GraphFrames) est déjà provisionnée.

---

## 1. Architecture

```
producer.py  ──►  Kafka (topic transactions, 3 partitions)  ──►  Spark Structured Streaming
                                                                        │
                                              ┌─────────────────────────┘
                                              ▼
                              MinIO : Bronze ─► Silver ─► Gold
```

| Composant | Rôle | Version |
|---|---|---|
| `producer.py` | générateur d'événements (fichier ou Kafka) | Python 3.11 |
| Apache Kafka | bus de messages, mode KRaft, 3 partitions | 3.7 |
| Apache Spark | 1 Master + 2 Workers, standalone | 3.5 |
| MinIO | stockage objet S3 (Bronze / Silver / Gold) | RELEASE.2024-09-22 |
| Jupyter | notebooks PySpark | spark-3.5.0 |

Organisation du bucket `lake` :

```
lake/
  bronze/transactions_raw/      (JSON bruts lus dans Kafka — Jalon 2)
  bronze/history/               (historique batch : transactions_history.jsonl, merchants.json)
  silver/transactions_history/  (Parquet nettoyé + enrichi, partitionné par city — Partie 3)
  silver/transactions_clean/    (Parquet du flux — Jalon 2)
  gold/kpi_city_10min/          (Delta Lake, indicateurs agrégés — Jalon 2)
  checkpoints/                  (checkpoints Structured Streaming — Jalon 2)
```

---

## 2. Arborescence du dépôt

```
.
├── docker-compose.yml              # Kafka + Spark (1 master, 2 workers) + MinIO + Jupyter
├── docker/
│   ├── Dockerfile.spark            # image Spark 3.5 + JAR S3A / Kafka / Delta / GraphFrames
│   ├── Dockerfile.jupyter          # notebook PySpark aligné sur la même version
│   ├── requirements-spark.txt
│   └── spark-defaults.conf         # configuration S3A vers MinIO, Delta, event log
├── producer/
│   ├── producer.py                 # générateur d'événements (--mode file | kafka)
│   └── gen_merchants.py            # génère le référentiel des 300 commerçants
├── jobs/
│   ├── common.py                   # SparkSession, accès MinIO, chronomètre
│   ├── partie1_cluster_check.py    # Partie 1 — vérification du cluster
│   ├── partie2_rdd_batch.py        # Partie 2 — batch SparkCore (RDD)
│   └── partie3_sql_dataframes.py   # Partie 3 — Spark SQL et DataFrames
├── scripts/
│   ├── init-kafka.sh               # crée le topic transactions (3 partitions)
│   ├── init-minio.sh               # crée le bucket lake et ses préfixes
│   ├── submit.sh                   # raccourci spark-submit vers le Master
│   └── kafka-consumer-groups.sh    # preuve offsets / consumer groups (Jalon 2)
├── data/
│   ├── merchants.json              # 300 commerçants (JSON Lines)
│   └── transactions_history.jsonl  # historique généré (50 000 événements)
├── notebooks/                      # exploration Jupyter
└── rapport/
    └── rapport-jalon1.md           # réponses théoriques + commentaires (Parties 1–3)
```

---

## 3. Prérequis

* Docker Engine ≥ 24 et Docker Compose v2 (`docker compose version`)
* 8 Go de RAM libres (Spark Master + 2 Workers à 2 Go chacun)
* Ports libres : 7077, 8080, 8081, 8082, 8888, 9000, 9001, 9092, 4040
* Python 3.11 sur le poste **uniquement** si vous lancez `producer.py` hors conteneur
  (`pip install kafka-python==2.0.2`)

---

## 4. Démarrage

```bash
# 1. Construire les images (télécharge les JAR Hadoop-AWS, Kafka, Delta, GraphFrames)
docker compose build

# 2. Démarrer l'ensemble : Kafka, MinIO, Spark Master + 2 Workers, Jupyter
docker compose up -d

# 3. Vérifier l'état des services
docker compose ps
```

Les jobs `kafka-init` et `minio-init` s'exécutent une fois puis s'arrêtent
(statut `exited (0)`) : ils créent le topic `transactions` à 3 partitions et le
bucket `lake`.

| Interface | URL | Identifiants |
|---|---|---|
| Spark Master UI | <http://localhost:8080> | — |
| Spark Worker 1 / 2 | <http://localhost:8081> / <http://localhost:8082> | — |
| Spark UI du driver | <http://localhost:4040> | pendant l'exécution d'un job |
| Console MinIO | <http://localhost:9001> | `minioadmin` / `minioadmin` |
| Jupyter | <http://localhost:8888/?token=sahlpay> | token `sahlpay` |

Arrêt : `docker compose down` (ajouter `-v` pour supprimer aussi les volumes).

---

## 5. Génération des données

```bash
# Référentiel des commerçants (300 lignes) — déjà versionné dans data/
python producer/gen_merchants.py --n 300 --out data/merchants.json

# Historique batch : 50 000 événements (Parties 2 et 3)
python producer/producer.py --mode file --n 50000 \
    --out data/transactions_history.jsonl
```

Dépôt dans la couche Bronze de MinIO :

```bash
docker run --rm --network sahlpay -v "$PWD/data:/data" \
  --entrypoint sh minio/mc -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin && \
   mc cp /data/transactions_history.jsonl local/lake/bronze/history/ && \
   mc cp /data/merchants.json local/lake/bronze/history/ && \
   mc ls --recursive local/lake/"
```

Flux temps réel (Jalon 2) :

```bash
python producer/producer.py --mode kafka --rate 20 --bootstrap localhost:9092
```

Le générateur injecte volontairement : ~5 % d'événements en retard (jusqu'à
15 minutes), ~2 % de doublons stricts `(tx_id, status)`, et trois scénarios
suspects (appareil partagé par 25 comptes, cycles de transferts A→B→C→A,
comptes collecteurs).

---

## 6. Exécution des jobs du Jalon 1

```bash
# Partie 1 — cluster et positionnement
./scripts/submit.sh partie1_cluster_check.py --hold 120

# Partie 2 — batch RDD (SparkCore)
./scripts/submit.sh partie2_rdd_batch.py --hold 180

# Partie 3 — Spark SQL et DataFrames
./scripts/submit.sh partie3_sql_dataframes.py --hold 180
```

L'option `--hold N` garde le driver vivant N secondes après la fin du calcul,
le temps de capturer le DAG dans la Spark UI (<http://localhost:4040>).

Options utiles :

| Option | Job | Effet |
|---|---|---|
| `--input <chemin>` | Parties 2 et 3 | source alternative (`s3a://`, `file:///`) |
| `--dedup` | Partie 2 | supprime les doublons stricts avant agrégation |
| `--top N` | Partie 2 | taille du classement des commerçants |
| `--no-write` | Partie 3 | analyse seule, sans écriture Silver |
| `--master local[4]` | tous | exécution locale, sans cluster |

---

## 7. Captures et preuves attendues (Jalon 1)

| Preuve | Où l'obtenir |
|---|---|
| Spark Master UI avec 2 Workers `ALIVE` | <http://localhost:8080> |
| Bucket `lake` dans MinIO | <http://localhost:9001> ou `mc ls --recursive local/lake/` |
| Version, master, appName, `defaultParallelism` | sortie de `partie1_cluster_check.py` |
| DAG et nombre de stages (Partie 2) | Spark UI → onglet *Stages* → *DAG Visualization* |
| `toDebugString()` (lineage) | sortie de `partie2_rdd_batch.py` |
| Parquet Silver partitionné par ville | `mc ls --recursive local/lake/silver/transactions_history/` |
| Sortie de `explain(True)` | sortie de `partie3_sql_dataframes.py` |

---

## 8. Versions et dépendances

| Dépendance | Version | Usage |
|---|---|---|
| Spark | 3.5 (Scala 2.12) | socle de calcul |
| Hadoop AWS | 3.3.4 | connecteur S3A vers MinIO |
| AWS SDK bundle | 1.12.262 | dépendance de hadoop-aws 3.3.4 |
| spark-sql-kafka-0-10 | 3.5.1 | source Kafka (Jalon 2) |
| Delta Lake | 3.2.0 | couche Gold (Jalon 2) |
| GraphFrames | 0.8.3-spark3.5-s_2.12 | analyse de graphes (Jalon 2) |
| kafka-python | 2.0.2 | producteur Python |

Les JAR sont téléchargés **à la construction de l'image**, pas via
`--packages` : aucune résolution Ivy n'est nécessaire à chaque soumission, et
le cluster fonctionne sans accès réseau une fois construit.

---

## 9. Échéances

| Jalon | Contenu | Date |
|---|---|---|
| Jalon 1 | Parties 1, 2 et 3 (batch) | 22/11/2026 |
| Jalon 2 | Parties 4, 5 et 6 (streaming, GraphX/GraphFrames) + soutenance | 20/12/2026 |
