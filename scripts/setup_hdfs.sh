#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [[ ! -f .env ]] || source .env; set +a
: "${HDFS_CHECKPOINT_ROOT:?Set HDFS_CHECKPOINT_ROOT}"
: "${ICEBERG_WAREHOUSE:?Set ICEBERG_WAREHOUSE}"
hdfs dfs -mkdir -p "$HDFS_CHECKPOINT_ROOT" "$ICEBERG_WAREHOUSE"
hdfs dfs -chmod 770 "$HDFS_CHECKPOINT_ROOT" "$ICEBERG_WAREHOUSE"
