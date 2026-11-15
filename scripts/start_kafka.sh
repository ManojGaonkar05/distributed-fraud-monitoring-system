#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [[ ! -f .env ]] || source .env; set +a
: "${KAFKA_HOME:?Set KAFKA_HOME to the Kafka installation directory}"
KAFKA_CONFIG="${KAFKA_CONFIG:-$KAFKA_HOME/config/server.properties}"
exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONFIG"
