"""Delta table maintenance: OPTIMIZE (compaction) and VACUUM (retention).

Modes (mutually exclusive, see argparse):
  --optimize                    run OPTIMIZE on Silver/Gold tables that exist
  --vacuum --retention-hours N  run VACUUM, removing files older than N hours

Default retention is 168h (7 days), matching the report's operational note
(keep retention >= 7 days while streaming queries may still read old files).
If a shorter retention is requested, the Delta safety check
(spark.databricks.delta.retentionDurationCheck) is disabled, otherwise VACUUM
refuses horizons below 168h.

Tables that do not exist yet are skipped with a log line.

Usage:
  spark-submit --master spark://spark-master:7077 maintenance.py --optimize
  spark-submit --master spark://spark-master:7077 maintenance.py \
      --vacuum --retention-hours 168
"""

import argparse
import sys

from common import GOLD_1M_PATH, GOLD_5M_PATH, SILVER_PATH, get_spark

# Same order as the medallion layers for readable logs.
TABLES = [
    ("silver", SILVER_PATH),
    ("gold_1m", GOLD_1M_PATH),
    ("gold_5m", GOLD_5M_PATH),
]

DEFAULT_RETENTION_HOURS = 168  # 7 days (report recommendation)


def table_exists(spark, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(0)
        return True
    except Exception:
        return False


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--optimize", action="store_true",
                       help="run OPTIMIZE on Silver/Gold tables")
    group.add_argument("--vacuum", action="store_true",
                       help="run VACUUM on Silver/Gold tables")
    p.add_argument("--retention-hours", type=int, default=DEFAULT_RETENTION_HOURS,
                   help="VACUUM retention horizon in hours (default 168)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("delta-maintenance")

    if args.vacuum and args.retention_hours < DEFAULT_RETENTION_HOURS:
        # Delta refuses VACUUM with retention < 168h unless this check is off.
        # Only disable when explicitly requested with a short horizon.
        spark.conf.set("spark.databricks.delta.retentionDurationCheck", "false")

    for name, path in TABLES:
        if not table_exists(spark, path):
            print(f"[maintenance] {name}: table not found at {path}, skipping",
                  file=sys.stderr)
            continue
        if args.optimize:
            print(f"[maintenance] OPTIMIZE delta.`{path}`", file=sys.stderr)
            spark.sql(f"OPTIMIZE delta.`{path}`")
        else:
            print(f"[maintenance] VACUUM delta.`{path}` "
                  f"RETAIN {args.retention_hours} HOURS", file=sys.stderr)
            spark.sql(f"VACUUM delta.`{path}` RETAIN {args.retention_hours} HOURS")

    print("[maintenance] done", file=sys.stderr)
    spark.stop()


if __name__ == "__main__":
    main()
