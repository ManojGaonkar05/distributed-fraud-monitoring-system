#!/usr/bin/env bash
set -euo pipefail
: "${HADOOP_HOME:?Set HADOOP_HOME to the Hadoop installation directory}"
exec "$HADOOP_HOME/sbin/start-dfs.sh"
