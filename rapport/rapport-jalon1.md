# Rapport — Jalon 1 (Parties 1, 2 et 3)

**Projet** : SahlPay.ma — plateforme Big Data pour un portefeuille mobile
**Module** : Écosystème Big Data 2 — 5ème année EMSI — 2026–2027
**Binôme** : _à compléter_
**Date d'échéance** : 22/11/2026

> Les emplacements marqués `[CAPTURE]` attendent une copie d'écran, et ceux
> marqués `[MESURE]` une valeur relevée lors de votre propre exécution. Tout le
> reste est rédigé.

---

## Partie 1 — Mise en place du cluster et positionnement

### 1.1 Déploiement réalisé

L'environnement est décrit dans `docker-compose.yml` : un broker **Kafka 3.7**
en mode KRaft (topic `transactions`, 3 partitions), un **Spark Master** et
**deux Workers** en standalone (2 cœurs et 2 Go chacun), **MinIO** comme
stockage objet compatible S3 (bucket `lake`) et un notebook **Jupyter PySpark**.
Les images Spark et Jupyter sont construites à partir de `docker/Dockerfile.*`
et embarquent les JAR `hadoop-aws`, `aws-java-sdk-bundle`,
`spark-sql-kafka-0-10`, Delta Lake et GraphFrames.

Sortie de `partie1_cluster_check.py` :

```
Version de Spark        : 3.5.x
Master                  : spark://spark-master:7077
appName                 : sahlpay-partie1-cluster-check
defaultParallelism      : 4        (2 Workers x 2 cœurs)
Executors enregistrés   : 2
```

`[CAPTURE]` Spark Master UI (<http://localhost:8080>) avec les deux Workers en
état `ALIVE`.
`[CAPTURE]` Console MinIO (<http://localhost:9001>) montrant le bucket `lake`
et ses préfixes `bronze/`, `silver/`, `gold/`, `checkpoints/`.
`[MESURE]` Valeur exacte de `defaultParallelism` relevée à l'exécution.

### 1.2 Rôle du Driver, des Executors, du Cluster Manager et du SparkContext

Le **Driver** est le processus qui exécute notre fonction `main()`. Il
construit le graphe logique des opérations, le découpe en *jobs*, *stages* et
*tâches*, planifie ces tâches et collecte les résultats. Dans notre déploiement
il tourne en mode `client` dans le conteneur `sahlpay-spark-master` : c'est lui
qui expose la Spark UI du port 4040. Si le Driver meurt, l'application entière
s'arrête.

Les **Executors** sont les processus JVM lancés sur les Workers qui exécutent
réellement les tâches et conservent les données mises en cache. Nous en avons
deux, un par Worker, avec 2 cœurs chacun : quatre tâches peuvent donc s'exécuter
simultanément. C'est sur ces executors que sont parsées les lignes JSON et que
sont calculées les agrégations partielles par ville.

Le **Cluster Manager** est ici le Master Spark standalone
(`spark://spark-master:7077`). Il ne calcule rien : il tient l'inventaire des
Workers, de leurs cœurs et de leur mémoire, et alloue des executors aux
applications qui se présentent. Il pourrait être remplacé par YARN ou
Kubernetes sans changer une ligne de nos jobs.

Le **SparkContext** (accessible via `spark.sparkContext`) est le point d'entrée
du Driver vers le cluster : il négocie les ressources auprès du Cluster Manager,
distribue le code et les variables aux executors, et expose l'API RDD de bas
niveau utilisée en Partie 2. La `SparkSession` l'englobe et y ajoute le
catalogue SQL et l'API DataFrame utilisés en Partie 3.

### 1.3 Deux limites de MapReduce/Hadoop que Spark corrige

**1. L'écriture sur disque entre chaque étape.** En MapReduce, la sortie de
chaque *map* est écrite sur HDFS avant d'être relue par le *reduce* suivant.
Notre chaîne « filtrer les COMPLETED → clé ville → sommer → calculer la moyenne
→ classer les commerçants » demanderait plusieurs jobs MapReduce enchaînés,
donc autant d'allers-retours disque. Spark garde les partitions en mémoire et
ne matérialise que les frontières de shuffle ; `completed.cache()` suffit pour
que les trois agrégations de la Partie 2 relisent le fichier une seule fois.

**2. L'absence de modèle d'exécution unifié pour le temps réel et l'itératif.**
MapReduce ne connaît que le batch : le suivi « presque en temps réel » demandé
par SahlPay imposerait un outil séparé (Storm, Flink), avec un second code à
maintenir. Spark offre le même moteur et presque la même API pour le batch
(Parties 2 et 3), le streaming (Partie 4) et l'analyse de graphes itérative
(PageRank et composantes connexes de la Partie 6, où chaque itération relirait
HDFS en MapReduce).

---

## Partie 2 — Traitement batch avec SparkCore (RDD)

### 2.1 Résultats obtenus

Source : `s3a://lake/bronze/history/transactions_history.jsonl`, 50 000
événements générés par `producer.py --mode file --n 50000`.

Répartition des statuts dans le jeu généré :

| Statut | Événements |
|---|---|
| INITIATED | 17 092 |
| COMPLETED | 15 105 |
| AUTHORIZED | 15 072 |
| REJECTED | 2 015 |
| REVERSED | 716 |

`[MESURE]` Tableau du montant total et du montant moyen par ville (sortie des
points 3 et 4 du job).
`[MESURE]` Top 5 des commerçants par nombre de paiements validés.

Le job signale également le nombre de doublons stricts `(tx_id, status)`
présents dans l'historique (environ 2 %, injectés volontairement). Les
agrégations sont calculées sans déduplication, conformément à l'énoncé ;
l'option `--dedup` permet de mesurer l'écart, et ce traitement est repris en
Partie 5 avec `dropDuplicates` combiné au watermark.

### 2.2 Transformations et actions

**Transformations (paresseuses)** : `textFile`, `map`, `filter`,
`reduceByKey`, `aggregateByKey`, `distinct`, `values`, ainsi que `cache` qui ne
fait qu'annoter le plan. Aucune ne déclenche de calcul : elles ne font
qu'allonger le graphe de dépendances.

**Actions (déclenchent un job)** : `count`, `collect`, `takeOrdered`.
C'est à l'appel de `count()` que le fichier est réellement lu pour la première
fois.

`takeOrdered(5)` est préféré à un `collect()` suivi d'un tri Python : seuls
cinq éléments remontent au Driver, chaque partition ne renvoyant que ses cinq
meilleurs candidats. Un `collect()` sur l'ensemble des commerçants ferait
transiter toute la table par le Driver et le saturerait sur un volume réel.

### 2.3 DAG, stages et shuffle

`[CAPTURE]` Onglet *Jobs* de la Spark UI.
`[CAPTURE]` *DAG Visualization* du job « montant total par ville ».
`[MESURE]` Nombre de stages affiché pour ce job.

Le DAG du calcul par ville se découpe en **deux stages**, séparés par un
`ShuffleMapStage` :

* **Stage 0** — `textFile` → `map(parse_line)` → `filter(is_completed)` →
  `map(to_city_amount)` puis agrégation locale. Toutes ces opérations sont
  *narrow* : chaque partition de sortie ne dépend que d'une partition d'entrée,
  donc tout s'enchaîne sans communication réseau (*pipelining*).
* **Stage 1** — la suite du `reduceByKey` après redistribution.

Le **shuffle** vient de `reduceByKey` : pour additionner tous les montants de
Casablanca, il faut que tous les couples `("Casablanca", montant)` se retrouvent
sur le même executor. Spark applique donc un hash sur la clé, écrit des fichiers
de shuffle sur chaque executor, puis chaque tâche du stage suivant va chercher
les blocs qui lui reviennent. C'est l'opération la plus coûteuse du job
(écriture disque + transfert réseau).

Point important : `reduceByKey` effectue une **agrégation locale avant le
shuffle** (combiner côté map). Seules dix paires partielles par partition —
une par ville — traversent le réseau, au lieu des 15 105 enregistrements que
transporterait un `groupByKey().mapValues(sum)`. Le point 4 pousse la même
logique avec `aggregateByKey`, qui calcule somme et comptage en un seul
passage : une seule lecture et un seul shuffle au lieu de deux.

Extrait de `toDebugString()` :

```
(N) PythonRDD[...] at RDD at PythonRDD.scala:53 []
 |  MapPartitionsRDD[...] at mapPartitions at PythonRDD.scala:160 []
 |  ShuffledRDD[...] at partitionBy at ...                        []
 +-(N) PairwiseRDD[...] at reduceByKey at ...                     []
    |  PythonRDD[...] at reduceByKey at ...                       []
    |  transactions_history.jsonl MapPartitionsRDD[...] at textFile ... []
    |  transactions_history.jsonl HadoopRDD[...] at textFile ...      []
```

Le décalage d'indentation marque la frontière de stage : ce qui est au-dessus
du `ShuffledRDD` s'exécute après le shuffle, ce qui est en dessous avant.

### 2.4 Lineage et tolérance aux pannes

Un RDD ne stocke pas ses données : il mémorise la **suite d'opérations** qui
permet de les reconstruire à partir de la source — c'est le *lineage*, celui
qu'affiche `toDebugString()`. Si un executor tombe pendant le calcul, Spark ne
redémarre pas le job : il identifie les partitions perdues et rejoue uniquement
leur chaîne de transformations sur un autre executor, à partir du fichier
d'origine dans MinIO.

Le coût de cette reconstruction dépend du type de dépendance. Pour une
dépendance *narrow* (`map`, `filter`), une seule partition parente est à
relire. Pour une dépendance *wide* (`reduceByKey`), il faut potentiellement
relire toutes les partitions parentes — d'où l'intérêt, sur un lineage long ou
itératif (Partie 6), de couper la chaîne avec `checkpoint()` ou de conserver
les résultats intermédiaires avec `cache()`. Ce mécanisme ne protège pas contre
la perte du Driver, qui détient le lineage lui-même.

---

## Partie 3 — Spark SQL et DataFrames

### 3.1 Schéma explicite

Les deux sources sont lues avec un `StructType` déclaré dans
`jobs/partie3_sql_dataframes.py`, sans `inferSchema`. Trois raisons :

* l'inférence impose une passe de lecture complète du fichier avant le
  traitement réel ;
* les types deviennent déterministes — `amount_mad` est un `double` même si les
  premières lignes contiennent des entiers ;
* un flux Kafka (Partie 4) n'a pas de début ni de fin : l'inférence y est tout
  simplement impossible, le même schéma sera réutilisé dans `from_json`.

`event_time` est lu en `string` puis converti une seule fois en `timestamp`
(`to_timestamp` avec le motif `yyyy-MM-dd'T'HH:mm:ss'Z'`), ce qui rend la
conversion visible et évite toute dépendance au fuseau horaire par défaut de la
JVM. Contrôle effectué sur le jeu de 50 000 événements : **0 valeur nulle** sur
`event_ts` et sur `amount_mad`, **0 paiement P2M orphelin** après la jointure.

### 3.2 API DataFrame et SQL

Le montant par ville est calculé deux fois : avec `groupBy().agg()` et avec
`spark.sql()` après `createOrReplaceTempView("transactions")`. Les deux
écritures produisent **le même plan physique** : SQL et API DataFrame sont deux
façades sur le même arbre logique, optimisé par le même Catalyst. Le choix est
donc une question de lisibilité, pas de performance.

`[MESURE]` Durées affichées pour les deux variantes.

### 3.3 Jointure avec le référentiel et agrégation par catégorie

Les paiements `P2M` sont joints à `merchants` sur
`payee_id = merchant_id`, puis agrégés par `(merchant_city, category)`.

`[MESURE]` Tableau du montant encaissé par catégorie et par ville.

Ordre de grandeur observé : les catégories `epicerie`, `mode` et `telecom`
arrivent en tête à Casablanca et à Rabat, les deux villes où le référentiel
concentre le plus de commerçants.

### 3.4 UDF `amount_band` et version `when/otherwise`

L'UDF Python classe un montant en `micro` (< 50 MAD), `standard` (50 à
2 000 MAD) ou `eleve` (> 2 000 MAD). La version native utilise
`when(...).when(...).otherwise(...)`.

Répartition obtenue sur les transactions COMPLETED :

| Bande | Transactions |
|---|---|
| micro | 4 199 |
| standard | 10 596 |
| eleve | 310 |

`[MESURE]` Durées des deux variantes.

La version `when/otherwise` est la plus rapide, pour trois raisons cumulées :

1. **Catalyst ne voit pas à l'intérieur d'une UDF Python.** Elle est une boîte
   noire : aucun *predicate pushdown*, aucune simplification, aucune réécriture
   possible autour d'elle.
2. **Le coût de la sérialisation domine.** Chaque ligne quitte la JVM, est
   sérialisée vers un processus Python séparé, évaluée, puis renvoyée et
   désérialisée. Sur 15 000 lignes le surcoût reste modeste ; sur un flux de
   production il devient le goulot d'étranglement.
3. **Pas de génération de code.** `when/otherwise` est une expression Catalyst
   intégrée au *whole-stage codegen* de Tungsten et évaluée directement sur le
   format binaire en mémoire, sans changement de processus.

Si une logique n'est réellement pas exprimable nativement, une *pandas UDF*
(vectorisée, via Arrow) réduit nettement cet écart par rapport à une UDF ligne
à ligne.

### 3.5 Écriture Parquet (couche Silver)

Le résultat enrichi est écrit dans
`s3a://lake/silver/transactions_history/`, en Parquet, **partitionné par
`city`**. Le partitionnement crée un répertoire `city=Casablanca/`,
`city=Rabat/`, etc. : une requête filtrant sur une ville ne lit que son
répertoire (*partition pruning*). Parquet, orienté colonnes et compressé,
permet en plus de ne lire que les colonnes utiles.

`[CAPTURE]` `mc ls --recursive local/lake/silver/transactions_history/`
montrant les répertoires `city=...`.

### 3.6 Lecture de `explain(True)`

`[CAPTURE]` Sortie complète de `explain(True)` pour la jointure.

Les quatre sections :

* **Parsed Logical Plan** : la requête telle qu'écrite, non résolue (les
  colonnes ne sont encore que des noms).
* **Analyzed Logical Plan** : noms et types résolus contre le catalogue ; une
  colonne inexistante échoue ici.
* **Optimized Logical Plan** : Catalyst a remonté les filtres
  (`status = 'COMPLETED'`, `type = 'P2M'`) au plus près des sources — le filtre
  passe *sous* la jointure, ce qui réduit le volume joint — et élagué les
  colonnes non utilisées.
* **Physical Plan** : la stratégie retenue.

**Stratégie de jointure retenue : `BroadcastHashJoin`** (vérifié sur le plan
physique exécuté). Le référentiel `merchants` ne pèse que 300 lignes, très en
dessous du seuil `spark.sql.autoBroadcastJoinThreshold` (10 Mo par défaut) :
Spark le diffuse intégralement à chaque executor, qui en construit une table de
hachage locale. Le gros côté — les transactions — n'est pas déplacé du tout :
**aucun shuffle**.

Le job exécute volontairement la même jointure avec
`spark.sql.autoBroadcastJoinThreshold = -1` : le plan repasse alors en
`SortMergeJoin`, avec deux `Exchange hashpartitioning` et deux tris. La
comparaison des deux plans montre concrètement ce que le broadcast économise.

### 3.7 RDD, DataFrame et Dataset

| Critère | RDD | DataFrame | Dataset |
|---|---|---|---|
| Typage | objets Python/JVM, opaque pour Spark | schéma connu, vérifié à l'exécution | typage statique, vérifié à la compilation |
| Optimisation | aucune, le plan est exécuté tel quel | Catalyst + Tungsten | Catalyst + Tungsten |
| Mémoire | objets sérialisés (pickle en Python) | format binaire Tungsten, hors tas | format binaire Tungsten |
| Langages | tous | tous | Scala / Java uniquement |
| Usage | contrôle fin, données non structurées | cas général | Scala, quand la sûreté de typage prime |

En PySpark, `Dataset[T]` n'existe pas : un DataFrame y est un `Dataset[Row]`.
C'est pourquoi la Partie 6 impose Scala pour la partie GraphX, qui s'appuie sur
l'API RDD typée.

`[MESURE]` Comparaison des durées mesurées en Partie 2 (RDD) et en Partie 3
(DataFrame) pour le **même calcul** : montant total par ville.

L'écart attendu vient de trois sources : le parsing JSON est fait par le lecteur
natif de Spark en Partie 3 au lieu d'un `json.loads` Python par ligne ;
l'agrégation est compilée par Tungsten au lieu d'être exécutée dans des
processus Python ; et Catalyst élague les colonnes inutiles dès la lecture. À
l'inverse, l'API RDD reste préférable quand la logique ne s'exprime pas en
colonnes — c'est exactement le cas de la construction du graphe en Partie 6.

---

## Reproductibilité

```bash
docker compose build && docker compose up -d
python producer/gen_merchants.py --n 300 --out data/merchants.json
python producer/producer.py --mode file --n 50000 --out data/transactions_history.jsonl
# dépôt dans la couche Bronze : voir README, section 5
./scripts/submit.sh partie1_cluster_check.py --hold 120
./scripts/submit.sh partie2_rdd_batch.py --hold 180
./scripts/submit.sh partie3_sql_dataframes.py --hold 180
```

Les générateurs sont déterministes (`--seed`), les chiffres cités dans ce
rapport correspondent à `--seed 2026` pour les transactions et `--seed 42`
pour le référentiel.
