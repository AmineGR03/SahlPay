#!/bin/bash
# Cree le topic "transactions" avec 3 partitions (Partie 1, point 2).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
TOPIC="${KAFKA_TOPIC:-transactions}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"

echo "[init-kafka] attente du broker ${BOOTSTRAP}..."
for _ in $(seq 1 30); do
  if kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[init-kafka] creation du topic ${TOPIC} (${PARTITIONS} partitions)"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor 1

echo "[init-kafka] description du topic :"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
echo "[init-kafka] termine."
