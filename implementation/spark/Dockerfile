# Spark 4.2.0 + Delta Lake 4.4.0 image for the Big Data IoT platform.
#
# Why these jars are baked in (not --packages):
#   - Deterministic: no Maven download needed at runtime (works offline).
#   - Smaller memory footprint than a driver resolving packages on startup.
#
# NOTE on hadoop-client-runtime:
#   delta-kernel-defaults 4.4.0 declares hadoop-client-runtime 3.4.2, but
#   spark-4.2.0-bin-hadoop3 already ships hadoop-client-runtime 3.5.0.
#   We intentionally do NOT add the 3.4.2 jar to avoid two conflicting
#   shaded hadoop runtimes on the same classpath (Spark's 3.5.0 wins).
#
# Base image is the official apache/spark image (includes hadoop-client-api,
# hadoop-client-runtime, parquet 1.17, jackson 2.21, scala-library 2.13.18,
# spark-tags, scala-parallel-collections - all reused).

FROM apache/spark:4.2.0

USER root

ARG MAVEN=https://repo1.maven.org/maven2

# Delta Lake 4.4.0 (Spark 4.2 / Scala 2.13)
RUN mkdir -p /opt/spark/jars-extra && \
    cd /opt/spark/jars && \
    wget -q ${MAVEN}/io/delta/delta-spark_4.2_2.13/4.4.0/delta-spark_4.2_2.13-4.4.0.jar \
              ${MAVEN}/io/delta/delta-storage/4.4.0/delta-storage-4.4.0.jar \
              ${MAVEN}/io/delta/delta-kernel-api/4.4.0/delta-kernel-api-4.4.0.jar \
              ${MAVEN}/io/delta/delta-kernel-defaults/4.4.0/delta-kernel-defaults-4.4.0.jar

# Spark Kafka connector 4.2.0 (not bundled in the base image) + kafka-clients
RUN cd /opt/spark/jars && \
    wget -q ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.0/spark-sql-kafka-0-10_2.13-4.2.0.jar \
              ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.0/spark-token-provider-kafka-0-10_2.13-4.2.0.jar \
              ${MAVEN}/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar \
              ${MAVEN}/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar

# Application code (jobs + wrapper scripts) and metrics config
COPY conf/metrics.properties /opt/spark/conf/metrics.properties
COPY jobs/ /opt/spark/jobs/
COPY run/ /opt/spark/run/
RUN chmod +x /opt/spark/run/*.sh

USER spark

# Default: start the standalone master; override with worker/driver commands.
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
