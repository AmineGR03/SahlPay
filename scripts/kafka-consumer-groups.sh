#!/bin/bash
# Raccourci pour la preuve "offsets et consumer groups" (Partie 5, jalon 2).
# Usage : ./scripts/kafka-consumer-groups.sh --describe
docker exec -it sahlpay-kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --all-groups "$@"
