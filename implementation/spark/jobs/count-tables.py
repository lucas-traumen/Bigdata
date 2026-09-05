"""Count rows in the Bronze/Silver/Gold Delta tables (smoke-test helper).

Usage (inside the spark image):
  spark-submit --master spark://spark-master:7077 count-tables.py
"""

import sys

from common import (
    BRONZE_PATH,
    GOLD_1M_PATH,
    GOLD_5M_PATH,
    QUARANTINE_PATH,
    SILVER_PATH,
    get_spark,
)

TABLES = [
    ("bronze", BRONZE_PATH),
    ("silver", SILVER_PATH),
    ("quarantine", QUARANTINE_PATH),
    ("gold_1m", GOLD_1M_PATH),
    ("gold_5m", GOLD_5M_PATH),
]


def main() -> None:
    spark = get_spark("count-tables")
    total = {}
    for name, path in TABLES:
        try:
            n = spark.read.format("delta").load(path).count()
        except Exception as exc:  # table does not exist yet
            print(f"TABLE:{name}:ERROR:{type(exc).__name__}", file=sys.stderr)
            n = -1
        total[name] = n
        print(f"TABLE:{name}:{n}")
    spark.stop()


if __name__ == "__main__":
    main()
