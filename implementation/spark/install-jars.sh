#!/usr/bin/env bash
# Shared pinned-jar installer used by BOTH images:
#   - spark/Dockerfile          (bigdata-iot-spark:1.0.0)
#   - airflow/Dockerfile        (bigdata-iot-airflow:1.0.0, spark-app stage)
# Single source of truth so the two images can never drift apart on
# connector/JDBC versions (tester blocker F1 remediation).
#
# Usage: install-jars.sh [target-dir]   (default: /opt/spark/jars)
# Requires: curl + ca-certificates + sha256sum; internet access to
# repo1.maven.org (~13 MB). Every jar is sha256-pinned (all 5 checksums
# re-verified by download + sha256sum on 2026-09-14).
set -euo pipefail

TARGET_DIR="${1:-/opt/spark/jars}"
mkdir -p "$TARGET_DIR"
cd "$TARGET_DIR"

MV=https://repo1.maven.org/maven2

# fetch <path-under-maven-central> <dest-name> <sha256>
fetch() {
  local path="$1" dest="$2" sum="$3"
  curl -fsSL -o "$dest" "$MV/$path"
  echo "$sum  $dest" | sha256sum -c -
}

# Spark 4.2.0 Kafka connector (Scala 2.13) + its token provider
fetch org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.0/spark-sql-kafka-0-10_2.13-4.2.0.jar \
  spark-sql-kafka-0-10_2.13-4.2.0.jar \
  64539c45e5d39f7ddb747312c239f38f5208d08c1a0ddf7d70beb2fbd9373705

fetch org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.0/spark-token-provider-kafka-0-10_2.13-4.2.0.jar \
  spark-token-provider-kafka-0-10_2.13-4.2.0.jar \
  641c7947afc8aa1189e433b7c2d305293993c8691bc1c2062d6d4f513b12518c

# kafka-clients 3.9.2 = version declared by the Spark 4.2.0 connector POM
fetch org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar \
  kafka-clients-3.9.2.jar \
  014b4ef36884478a932641818b080c36a8f8e2e92dc771e800c9d2694f4f83d8

fetch org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar \
  commons-pool2-2.13.1.jar \
  f77a5060d6936a9144023584232f9de3f2248f1abfe156e9d31795f18770674f

# PostgreSQL JDBC driver
fetch org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
  postgresql-42.7.4.jar \
  188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e

echo "[install-jars] OK - pinned jars in $TARGET_DIR:"
ls -la
