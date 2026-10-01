"""Build-time dependency smoke test (run by spark/Dockerfile).

Verifies that, inside the image:
  * a SparkSession starts in local mode;
  * the Kafka connector classes (baked jars) are on the classpath;
  * the PostgreSQL JDBC driver is on the classpath;
  * the psycopg2 Python driver (used by pg_sink inside foreachBatch) imports.

No network, no Kafka, no PostgreSQL required — pure classpath + session check.
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[1]")
    .appName("dependency-warmup")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", "false")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
try:
    df = spark.createDataFrame([(1, "a")], ["id", "v"])
    assert df.count() == 1
    kafka_cls = spark._jvm.java.lang.Class.forName(
        "org.apache.spark.sql.kafka010.KafkaSourceProvider")
    pg_cls = spark._jvm.java.lang.Class.forName("org.postgresql.Driver")
    import psycopg2  # noqa: F401 - fail the BUILD if the driver is missing
    print(f"[warmup] OK kafka010={kafka_cls} pgjdbc={pg_cls} psycopg2={psycopg2.__version__}",
          flush=True)
finally:
    spark.stop()
