# Airflow 2.10.5 control-plane image built ON TOP of the Spark 4.2.0 image.
#
# Why FROM apache/spark:4.2.0 (the exact image Phase 1 uses):
#   - apache-airflow + pyspark coexist in ONE Python 3.10 env (no dual-venv).
#   - Spark jars (Delta 4.4.0, Kafka connector) are already in /opt/spark/jars
#     of the bigdata/spark:4.2.0-delta-4.4.0 image -> spark-submit from the
#     Airflow scheduler needs no extra packages.
#   - /opt/spark/bin is already on PATH in the base image (needed for the
#     scheduler to run spark-submit client-mode).
#
# NOTE (base image fact): apache/spark:4.2.0 = Ubuntu 22.04 (jammy) with
# system Python 3.10 -> the official Airflow constraints file below uses
# constraints-3.10.txt.
#
# Jobs code is NOT copied into this image: docker-compose.airflow.yml mounts
# ../spark/jobs and ../spark/run read-only into the scheduler container, so
# the DAG always runs the latest job code from the host.
#
# pip runs with --user (default --break-system-packages is NOT needed, the
# base image is Ubuntu 22.04 python3 without PEP 668 enforcement).

FROM bigdata/spark:4.2.0-delta-4.4.0

USER root

ARG AIRFLOW_VERSION=2.10.5
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt
ARG PYTHON_VERSION=3.10

# Runtime deps: curl for webserver healthcheck; git so the scheduler can load
# DAGs from a git repo later if desired.
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl git && \
    rm -rf /var/lib/apt/lists/*

ENV AIRFLOW_HOME=/opt/airflow

RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs && \
    chown -R spark:spark ${AIRFLOW_HOME}

# Install Airflow + extra Python packages the DAGs need.
#   --user installs into /home/spark/.local (image runs as user spark).
RUN pip3 install --no-cache-dir --user \
      "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "${CONSTRAINT_URL}" && \
    pip3 install --no-cache-dir --user \
      "apache-airflow-providers-standard" \
      "psycopg2-binary" \
      "confluent-kafka==2.10.1" \
      --constraint "${CONSTRAINT_URL}"

# PATH: airflow CLI + home bin; spark-submit already available via /opt/spark/bin.
ENV PATH="/home/spark/.local/bin:/opt/spark/bin:${PATH}"

# --- Airflow configuration (compose may override any of these) -------------
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False

COPY dags/ ${AIRFLOW_HOME}/dags/
RUN chown -R spark:spark ${AIRFLOW_HOME}

USER spark
WORKDIR ${AIRFLOW_HOME}

EXPOSE 8081

# Default: webserver. Compose overrides this for init/scheduler services.
CMD ["airflow", "webserver", "-p", "8081"]
