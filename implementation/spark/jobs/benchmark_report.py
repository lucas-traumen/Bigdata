"""Benchmark report generator (batch).

Counts rows in the 5 Delta tables (like count-tables.py), then adds:
  - quarantine ratio (quarantine / (silver + quarantine))
  - rows in the most recent Gold 1m window (max window_start)
  - run timestamp

The report is printed to stdout as single-line JSON AND written to HDFS at
$HDFS_BASE/reports/benchmark_report_<epoch_ms>.json (one file per run, so
hourly Airflow runs accumulate a history).
"""

import json
import sys
import time

from common import (
    BRONZE_PATH,
    GOLD_1M_PATH,
    GOLD_5M_PATH,
    HDFS_BASE,
    QUARANTINE_PATH,
    SILVER_PATH,
    get_spark,
)

REPORT_DIR = f"{HDFS_BASE}/reports"

TABLES = [
    ("bronze", BRONZE_PATH),
    ("silver", SILVER_PATH),
    ("quarantine", QUARANTINE_PATH),
    ("gold_1m", GOLD_1M_PATH),
    ("gold_5m", GOLD_5M_PATH),
]


def count_table(spark, path: str) -> int:
    """Row count, or -1 if the table does not exist yet."""
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return -1


def latest_gold_window_rows(spark) -> int:
    """Rows in the newest Gold 1m window (-1 if table missing, 0 if empty)."""
    try:
        gold = spark.read.format("delta").load(GOLD_1M_PATH)
        if gold.limit(1).count() == 0:
            return 0
        max_start = gold.agg({"window_start": "max"}).first()[0]
        return gold.filter(gold.window_start == max_start).count()
    except Exception:
        return -1


def main() -> None:
    spark = get_spark("benchmark-report")

    counts = {}
    for name, path in TABLES:
        counts[name] = count_table(spark, path)

    silver = counts.get("silver", -1)
    quarantine = counts.get("quarantine", -1)
    if silver >= 0 and quarantine >= 0 and (silver + quarantine) > 0:
        quarantine_ratio = round(quarantine / (silver + quarantine), 6)
    else:
        quarantine_ratio = None

    report = {
        "run_epoch_ms": int(time.time() * 1000),
        "run_time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "table_rows": counts,
        "quarantine_ratio": quarantine_ratio,
        "gold_1m_latest_window_rows": latest_gold_window_rows(spark),
    }

    # Write the JSON document to HDFS: one DataFrame row = one text line.
    # Spark creates REPORT_DIR on demand; the report itself becomes a Spark
    # output directory containing a single part file with the JSON line.
    epoch_ms = report["run_epoch_ms"]
    out_path = f"{REPORT_DIR}/benchmark_report_{epoch_ms}.json"
    spark.createDataFrame(
        [(json.dumps(report, separators=(",", ":")),)], ["value"]
    ).write.mode("overwrite").text(out_path)
    spark.stop()

    print(f"[benchmark-report] wrote {out_path}", file=sys.stderr)
    print(json.dumps(report, separators=(",", ":")))


if __name__ == "__main__":
    main()
