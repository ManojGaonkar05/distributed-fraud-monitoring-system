#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [[ ! -f .env ]] || source .env; set +a
: "${SPARK_HOME:?Set SPARK_HOME}"
: "${SPARK_MASTER:?Set SPARK_MASTER}"
: "${ICEBERG_WAREHOUSE:?Set ICEBERG_WAREHOUSE}"
CATALOG="${ICEBERG_CATALOG:-lab}"
exec "$SPARK_HOME/sbin/start-thriftserver.sh" --master "$SPARK_MASTER" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf "spark.sql.catalog.${CATALOG}=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.${CATALOG}.type=hadoop" \
  --conf "spark.sql.catalog.${CATALOG}.warehouse=$ICEBERG_WAREHOUSE" \
  --hiveconf "hive.server2.thrift.port=${SPARK_THRIFT_PORT:-10000}"
