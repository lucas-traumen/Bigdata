# Airflow scheduler image with a REAL Spark distribution + the SAME pinned
# connector/JDBC jars + the SAME application artifacts as
# bigdata-iot-spark:1.0.0, so LocalExecutor can run the actual batch program
# (spark-submit local[2]) inside this container.
#
# Self-contained build (tester blocker F1 remediation): the spark-app stage
# below re-creates the Spark application layer from the same sources and the
# same shared installer script (spark/install-jars.sh) as the spark image.
# It does NOT depend on bigdata-iot-spark existing in the local image cache,
# so `docker compose build airflow-scheduler` alone is reproducible.
#
# Build context MUST be the implementation/ root: compose.yaml sets
# context: . + dockerfile: airflow/Dockerfile for both airflow services
# (implementation/.dockerignore keep-lists only spark/ + airflow/).
#
# syntax=docker/dockerfile:1

# ---- stage 1: Spark distribution + pinned jars + application artifacts ----
FROM apache/spark:4.2.0-java21-python3 AS spark-app

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Same 5 sha256-pinned jars as the spark image (shared installer).
COPY spark/install-jars.sh /tmp/install-jars.sh
RUN bash /tmp/install-jars.sh /opt/spark/jars

# Same application artifacts as the spark image: jobs/ (batch.py, warmup.py,
# ...) and run/ (run-batch.sh called by the DAG + run-stream.sh).
COPY spark/jobs/ /opt/spark/jobs/
COPY spark/run/ /opt/spark/run/
RUN chmod +x /opt/spark/run/*.sh

# ---- stage 2: Airflow + JDK 17 + the assembled Spark application layer ----
FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

# Reuse the exact Spark distribution + baked jars + app artifacts from the
# spark-app stage: identical versions for streaming, manual batch and
# Airflow-triggered batch.
COPY --from=spark-app /opt/spark /opt/spark

# Resolve the JDK home regardless of build architecture (amd64/arm64).
RUN ln -sfn "$(ls -d /usr/lib/jvm/java-17-openjdk-* | head -n1)" /opt/java-home

ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/opt/java-home \
    PATH="/opt/spark/bin:/opt/spark/run:${PATH}"

# Build-time assertions (F1): fail the BUILD itself -- before airflow-init
# can ever run -- if the batch program, the application code, the
# connector/JDBC jars or spark-submit are missing.
RUN set -eux; \
    test -x /opt/spark/run/run-batch.sh; \
    test -x /opt/spark/run/run-stream.sh; \
    test -f /opt/spark/jobs/batch.py; \
    test -f /opt/spark/jobs/stream_app.py; \
    test -f /opt/spark/jobs/warmup.py; \
    ls /opt/spark/jars/postgresql-*.jar >/dev/null; \
    ls /opt/spark/jars/spark-sql-kafka-0-10_2.13-*.jar >/dev/null; \
    ls /opt/spark/jars/kafka-clients-*.jar >/dev/null; \
    /opt/spark/bin/spark-submit --version

# Full runtime smoke under the REAL runtime combo (Spark 4.2.0 + Java 17 +
# baked jars): SparkSession starts, Kafka connector + PostgreSQL JDBC classes
# resolve. Same check as the spark image build warmup.
RUN /opt/spark/bin/spark-submit --master 'local[1]' \
      --conf spark.ui.enabled=false \
      /opt/spark/jobs/warmup.py

COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/

USER airflow
