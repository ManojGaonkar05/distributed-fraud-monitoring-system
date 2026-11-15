#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [[ ! -f .env ]] || source .env; set +a
: "${KAFKA_BOOTSTRAP_SERVERS:?Set KAFKA_BOOTSTRAP_SERVERS}"
: "${KAFKA_TOPIC:?Set KAFKA_TOPIC}"
: "${SPARK_MASTER:?Set SPARK_MASTER}"
: "${HDFS_CHECKPOINT_ROOT:?Set HDFS_CHECKPOINT_ROOT}"
: "${ICEBERG_WAREHOUSE:?Set ICEBERG_WAREHOUSE}"
RUN_ID="${RUN_ID:-run1}"
spark-submit --master "$SPARK_MASTER" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG:-lab}=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG:-lab}.type=hadoop" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG:-lab}.warehouse=$ICEBERG_WAREHOUSE" \
  --conf spark.sql.shuffle.partitions=6 \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST:-$(hostname -I | awk '{print $1}')}" \
  --conf "spark.driver.port=${SPARK_DRIVER_PORT:-39001}" \
  --conf "spark.blockManager.port=${SPARK_BLOCKMANAGER_PORT:-39002}" \
  streaming/fraud_stream.py
