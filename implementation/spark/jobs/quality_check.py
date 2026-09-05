"""Bronze quality check (batch, read-only).

Counts Bronze rows and the share of records that the Silver job would
reject, using exactly the same validation criteria as silver.py (the
build_reasons function is imported, so the rules can never drift apart):
  - event_time missing or not parseable as a timestamp
  - value missing, not numeric, or outside [-50, 200]
  - required fields missing: event_id, sensor_id, sensor_type, value, location

Prints a single-line JSON report to stdout and always exits 0, even when
the Bronze table does not exist yet:
  {"bronze_rows": N, "bad_rows": M, "bad_ratio": r, "reasons": {...}}
"""

import json
import sys

from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

from common import BRONZE_PATH, get_spark
from silver import build_reasons, table_exists


def main() -> None:
    spark = get_spark("bronze-quality-check")

    if not table_exists(spark, BRONZE_PATH):
        print("[quality-check] Bronze table not found yet", file=sys.stderr)
        report = {"bronze_rows": 0, "bad_rows": 0, "bad_ratio": 0.0, "reasons": {}}
        print(json.dumps(report, separators=(",", ":")))
        spark.stop()
        return

    bronze = spark.read.format("delta").load(BRONZE_PATH)

    # Same casts as silver.py before applying the shared validation rules.
    typed = bronze.withColumn("value_num", col("value").cast(DoubleType()))
    typed = typed.withColumn("event_time_ts", col("event_time").cast("timestamp"))
    checked = build_reasons(typed)

    total = checked.count()
    bad = checked.filter(col("quarantine_reason").isNotNull())
    bad_rows = bad.count()
    reasons = {
        row["quarantine_reason"]: row["count"]
        for row in bad.groupBy("quarantine_reason").count().collect()
    }

    report = {
        "bronze_rows": total,
        "bad_rows": bad_rows,
        "bad_ratio": round(bad_rows / total, 6) if total > 0 else 0.0,
        "reasons": reasons,
    }
    print(json.dumps(report, separators=(",", ":")))
    print(
        f"[quality-check] bronze_rows={total} bad_rows={bad_rows}",
        file=sys.stderr,
    )
    spark.stop()


if __name__ == "__main__":
    main()
