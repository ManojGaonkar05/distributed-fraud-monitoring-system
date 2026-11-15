#!/usr/bin/env bash
set -euo pipefail
: "${SPARK_HOME:?Set SPARK_HOME to the Spark installation directory}"
: "${SPARK_MASTER:?Set SPARK_MASTER to spark://VM2:7077}"
exec "$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER" --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" --cores "${SPARK_WORKER_CORES:-2}" --memory "${SPARK_WORKER_MEMORY:-2g}"
