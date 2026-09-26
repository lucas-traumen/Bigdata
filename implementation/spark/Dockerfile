# Spark 4.2.0 local-mode image for both spark-stream and spark-batch.
# Kafka connector + PostgreSQL JDBC are baked from Maven Central at build time
# so runtime never needs network. Build requires internet access to
# repo1.maven.org (~13 MB total). Build smoke: warmup.py validates the jars.
FROM apache/spark:4.2.0-java21-python3

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark/jars

# --- pinned connector/JDBC jars via the pinned installer (all 5 jars
# sha256-verified) ------------------------------------------------------------
COPY install-jars.sh /tmp/install-jars.sh
RUN bash /tmp/install-jars.sh /opt/spark/jars

WORKDIR /opt/spark

COPY jobs/ /opt/spark/jobs/
COPY run/ /opt/spark/run/
RUN chmod +x /opt/spark/run/*.sh

# Build-time smoke: session start + connector/JDBC classes resolve.
RUN ./bin/spark-submit --master 'local[1]' \
      --conf spark.ui.enabled=false \
      ./jobs/warmup.py

# drop back to the image default non-root user (uid 185)
USER 185
