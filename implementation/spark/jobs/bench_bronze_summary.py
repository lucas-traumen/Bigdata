#!/usr/bin/env python3
"""Summarize Bronze payload bytes for one benchmark prefix.

This helper reads persisted Bronze Parquet after a run has drained. It returns
one scalar JSON object and never collects the event history to the driver.
"""

from __future__ import annotations

import argparse
import json
import os
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", required=True,
                        help="sensor prefix used by the benchmark, e.g. load01_")
    parser.add_argument("--topic-prefix", default="sensors")
    parser.add_argument("--bronze-path",
                        default=os.environ.get("BRONZE_PATH", "/data/bigdata/bronze/sensor"))
    parser.add_argument("--output", help="optional JSON output path")
    args = parser.parse_args(argv)

    from pyspark.sql import SparkSession, functions as F

    spark = (
        SparkSession.builder
        .appName("bench-bronze-summary")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        selected = spark.read.parquet(args.bronze_path).where(
            F.col("mqtt_topic").startswith(args.topic_prefix + "/" + args.prefix)
        )
        row = selected.agg(
            F.count(F.lit(1)).alias("bronze_rows"),
            F.coalesce(F.sum(F.octet_length("raw_payload")), F.lit(0))
            .alias("payload_bytes"),
            F.avg(F.octet_length("raw_payload")).alias("avg_payload_bytes"),
            F.countDistinct("kafka_topic", "kafka_partition", "kafka_offset")
            .alias("distinct_kafka_records"),
        ).first().asDict()
        result = {
            "prefix": args.prefix,
            "bronze_rows": int(row.get("bronze_rows") or 0),
            "payload_bytes": int(row.get("payload_bytes") or 0),
            "avg_payload_bytes": (
                float(row["avg_payload_bytes"])
                if row.get("avg_payload_bytes") is not None else None
            ),
            "distinct_kafka_records": int(row.get("distinct_kafka_records") or 0),
        }
        encoded = json.dumps(result, ensure_ascii=False, sort_keys=True)
        print("BENCH_BRONZE_SUMMARY=" + encoded)
        if args.output:
            try:
                with open(args.output, "w", encoding="utf-8") as fh:
                    fh.write(encoded + "\n")
            except OSError as exc:
                # DATA_ROOT is commonly owned by the host user while Spark runs
                # as an unprivileged container user. The stdout record remains
                # the authoritative fallback so the caller can save it on the
                # host without changing container permissions.
                print(
                    f"WARNING: could not write {args.output}: {exc}; "
                    "use the BENCH_BRONZE_SUMMARY stdout record",
                    file=sys.stderr,
                )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
