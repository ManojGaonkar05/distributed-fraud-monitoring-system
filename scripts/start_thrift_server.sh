#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [[ ! -f .env ]] || source .env; set +a

# Spark daemons on this lab use Java 11.
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"

: "${SPARK_HOME:?Set SPARK_HOME}"
: "${SPARK_MASTER:?Set SPARK_MASTER}"
: "${ICEBERG_WAREHOUSE:?Set ICEBERG_WAREHOUSE}"

CATALOG="${ICEBERG_CATALOG:-lab}"
THRIFT_PORT="${SPARK_THRIFT_PORT:-10000}"

# Allow VM3 and other lab clients to reach the server, not just localhost.
export HIVE_SERVER2_THRIFT_BIND_HOST="${HIVE_SERVER2_THRIFT_BIND_HOST:-0.0.0.0}"
export HIVE_SERVER2_THRIFT_PORT="$THRIFT_PORT"

exec "$SPARK_HOME/sbin/start-thriftserver.sh" --master "$SPARK_MASTER" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf "spark.sql.catalog.$CATALOG=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.$CATALOG.type=hadoop" \
  --conf "spark.sql.catalog.$CATALOG.warehouse=$ICEBERG_WAREHOUSE" \
  --hiveconf "hive.server2.thrift.port=$THRIFT_PORT" \
  --hiveconf "hive.server2.thrift.bind.host=$HIVE_SERVER2_THRIFT_BIND_HOST"
