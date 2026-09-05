"""Silver job: Bronze -> Silver + quarantine (batch, idempotent).

Validation rules (physical bounds + schema checks):
  - event_time must be parseable as a timestamp (not null)
  - value must be a number in [-50, 200]
  - required fields present: event_id, sensor_id, sensor_type, value, location

Invalid rows are written to the silver_quarantine Delta table with a
quarantine_reason column; valid rows go to silver/sensor.

The job is a batch job designed to run periodically (run-silver.sh loops).
It tracks the last processed Bronze version in an HDFS state file and uses
MERGE INTO keyed on event_id, so re-processing the same Bronze data never
creates duplicate rows in Silver or Quarantine.

Only PySpark + Spark SQL is used (DESCRIBE HISTORY / MERGE INTO with
path-based table refs) so no extra Python packages are needed.
"""

import sys

from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import DoubleType

from common import (
    BRONZE_PATH,
    CHECKPOINT_BASE,
    QUARANTINE_PATH,
    SILVER_PATH,
    get_spark,
)

VALUE_MIN = -50.0
VALUE_MAX = 200.0

REQUIRED_COLS = ["event_id", "sensor_id", "sensor_type", "value", "location"]
STATE_PATH = f"{CHECKPOINT_BASE}/silver_state/last_version"


def build_reasons(df):
    """Add a quarantine_reason column; NULL when the row is valid."""
    value_num = col("value_num")
    reason = when(col("event_time_ts").isNull(), "missing_or_invalid_event_time")
    reason = reason.when(value_num.isNull(), "missing_or_non_numeric_value")
    reason = reason.when(
        (value_num < lit(VALUE_MIN)) | (value_num > lit(VALUE_MAX)),
        "value_out_of_physical_bounds",
    )
    for c in REQUIRED_COLS:
        reason = reason.when(
            col(c).isNull() | (col(c).cast("string") == lit("")),
            f"missing_field:{c}",
        )
    return df.withColumn(
        "quarantine_reason", reason.otherwise(lit(None).cast("string"))
    )


def table_exists(spark, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(0)
        return True
    except Exception:
        return False


def bronze_head_version(spark) -> int:
    """Latest committed version of the Bronze table (DESCRIBE HISTORY)."""
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{BRONZE_PATH}`")
    return hist.limit(1).select("version").first()[0]


def read_last_version(spark) -> int:
    try:
        row = spark.read.text(STATE_PATH).first()
        return int(row.value)
    except Exception:
        return -1


def write_last_version(spark, version: int) -> None:
    spark.createDataFrame([(str(version),)], ["value"]).write.mode(
        "overwrite"
    ).text(STATE_PATH)


def merge_insert(spark, df, path: str) -> None:
    """Idempotent insert: MERGE INTO keyed on event_id (insert-only)."""
    df.createOrReplaceTempView("incoming")
    spark.sql(
        f"""
        MERGE INTO delta.`{path}` AS t
        USING incoming AS s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    spark = get_spark("silver-bronze-to-silver")

    current_version = bronze_head_version(spark)
    last_version = read_last_version(spark)

    if current_version <= last_version:
        print(
            f"[silver] no new bronze data (version {current_version} <= {last_version})",
            file=sys.stderr,
        )
        spark.stop()
        return

    bronze = spark.read.format("delta").load(BRONZE_PATH)

    # Cast value/event_time once; keep the rest of Bronze schema intact.
    typed = bronze.withColumn("value_num", col("value").cast(DoubleType()))
    typed = typed.withColumn("event_time_ts", col("event_time").cast("timestamp"))

    checked = build_reasons(typed)

    valid = (
        checked.filter(col("quarantine_reason").isNull())
        .withColumn("event_time", col("event_time_ts"))
        .withColumn("value", col("value_num"))
        .drop("value_num", "event_time_ts")
    )
    invalid = checked.filter(col("quarantine_reason").isNotNull()).drop(
        "value_num", "event_time_ts"
    )

    # Pass 2 dedup inside this batch (late duplicates that share an event_id).
    valid = valid.dropDuplicates(["event_id"])
    invalid = invalid.dropDuplicates(["event_id"])

    # MERGE INTO keyed on event_id: idempotent across repeated passes.
    for df, path in ((valid, SILVER_PATH), (invalid, QUARANTINE_PATH)):
        if table_exists(spark, path):
            merge_insert(spark, df, path)
        else:
            writer = df.write.format("delta").mode("append")
            if path == SILVER_PATH:
                writer = writer.partitionBy("event_date")
            writer.save(path)

    write_last_version(spark, current_version)
    print(
        f"[silver] processed bronze version {current_version} "
        f"(valid={valid.count()} quarantined={invalid.count()}, "
        f"bounds [{VALUE_MIN}, {VALUE_MAX}])",
        file=sys.stderr,
    )
    spark.stop()


if __name__ == "__main__":
    main()
