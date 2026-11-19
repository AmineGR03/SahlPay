#!/bin/sh
# Cree le bucket "lake" et l'arborescence Bronze / Silver / Gold (Partie 1, point 2).
set -eu

ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
USER="${MINIO_ROOT_USER:-minioadmin}"
PASS="${MINIO_ROOT_PASSWORD:-minioadmin}"

echo "[init-minio] connexion a ${ENDPOINT}"
until mc alias set local "${ENDPOINT}" "${USER}" "${PASS}" >/dev/null 2>&1; do
  sleep 2
done

mc mb --ignore-existing local/lake
# MinIO n'a pas de vrais dossiers : on depose un marqueur par prefixe
for prefix in bronze/transactions_raw bronze/history silver/transactions_clean \
              silver/transactions_history gold/kpi_city_10min gold/graph_analysis checkpoints; do
  echo "placeholder" | mc pipe "local/lake/${prefix}/.keep"
done

echo "[init-minio] arborescence du bucket lake :"
mc ls --recursive local/lake/
echo "[init-minio] termine."
