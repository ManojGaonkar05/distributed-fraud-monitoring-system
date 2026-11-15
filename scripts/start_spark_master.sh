#!/usr/bin/env bash
set -euo pipefail
: "${SPARK_HOME:?Set SPARK_HOME to the Spark installation directory}"
: "${SPARK_MASTER_HOST:?Set SPARK_MASTER_HOST to VM2's reachable hostname/IP}"
exec "$SPARK_HOME/sbin/start-master.sh" -h "$SPARK_MASTER_HOST" -p "${SPARK_MASTER_PORT:-7077}" --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"
