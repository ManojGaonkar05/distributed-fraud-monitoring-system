#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [[ ! -f .env ]] || source .env; set +a
: "${KAFKA_BOOTSTRAP_SERVERS:?Set KAFKA_BOOTSTRAP_SERVERS in .env}"
: "${KAFKA_TOPIC:?Set KAFKA_TOPIC in .env}"
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --create --if-not-exists --topic "$KAFKA_TOPIC" --partitions "${KAFKA_PARTITIONS:-3}" --replication-factor "${KAFKA_REPLICATION_FACTOR:-1}"
