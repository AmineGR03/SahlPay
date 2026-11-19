#!/bin/bash
# Soumet un job PySpark du projet au Master Spark standalone.
#
#   ./scripts/submit.sh partie1_cluster_check.py
#   ./scripts/submit.sh partie2_rdd_batch.py --input s3a://lake/bronze/history/
#
set -euo pipefail

JOB="${1:?usage: submit.sh <fichier_job.py> [args...]}"
shift || true

docker exec -i sahlpay-spark-master \
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --total-executor-cores 4 \
    --executor-memory 1500m \
    "/opt/sahlpay/jobs/${JOB}" "$@"
