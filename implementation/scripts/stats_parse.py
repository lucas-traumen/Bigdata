#!/usr/bin/env python3
"""Record builder for resource-report.sh (docker-stats parsing + JSON line).

resource-report.sh invokes this module as a script:

    python3 scripts/stats_parse.py TS MEM_AVAIL DISC_FREE KAFKA_OFFSETS \
        F_BRONZE F_SILVER F_QUARANTINE PG_COUNTS STATS_TEXT

and appends the printed single JSON line to the report JSONL. The parsing
logic lives in importable functions so implementation/tests can exercise it
with canned `docker stats` samples using only the stdlib (no Docker needed).

Input format note (reviewer M1): docker stats is called with the TAB-delimited
format '{{.Name}}\\t{{.MemUsage}}\\t{{.MemPerc}}\\t{{.CPUPerc}}'. MemUsage
itself contains spaces ("50MiB / 1GiB"), so whitespace splitting misaligns
the mem_pct/cpu_pct columns; the tab is the stable field separator.
"""

import json
import sys


def parse_docker_stats(stats_text):
    """Parse tab-separated `docker stats --no-stream` rows.

    Expected row shape (4 TAB-separated fields, see module docstring):
        bigdata-kafka-1\\t512MiB / 1GiB\\t50.12%\\t1.23%

    Rows that are empty, shorter than 4 fields, or have an empty name/field
    are skipped instead of producing shifted (wrong) columns. Extra trailing
    fields are ignored (tolerates a stray trailing tab).

    Returns {name: {"mem_usage": ..., "mem_pct": ..., "cpu_pct": ...}}.
    """
    containers = {}
    for line in (stats_text or "").splitlines():
        fields = line.split("\t")
        if len(fields) < 4:
            continue  # malformed/short row — skip rather than mis-align
        name, mem_usage, mem_pct, cpu_pct = (f.strip() for f in fields[:4])
        if not name or not mem_usage or not mem_pct or not cpu_pct:
            continue
        containers[name] = {
            "mem_usage": mem_usage,
            "mem_pct": mem_pct,
            "cpu_pct": cpu_pct,
        }
    return containers


def build_record(ts, mem_avail, disk_free, kafka_offsets, f_bronze, f_silver,
                 f_quarantine, pg_counts, stats_text):
    """Assemble one resource-report JSON record (same shape as before M1)."""
    return {
        "ts": ts,
        "mem_available_mb": int(mem_avail) if mem_avail not in ("null", "") else None,
        "disk_free_gb": int(disk_free) if disk_free not in ("null", "") else None,
        "kafka_end_offsets": None if kafka_offsets == "null" else kafka_offsets.rstrip(";"),
        "parquet_files": {"bronze": int(f_bronze or 0), "silver": int(f_silver or 0),
                          "quarantine": int(f_quarantine or 0)},
        "pg_counts": json.loads(pg_counts) if pg_counts and pg_counts != "null" else None,
        "containers": parse_docker_stats(stats_text),
    }


def main(argv):
    if len(argv) < 10:
        print("usage: stats_parse.py TS MEM_AVAIL DISK_FREE KAFKA_OFFSETS "
              "F_BRONZE F_SILVER F_QUARANTINE PG_COUNTS STATS_TEXT", file=sys.stderr)
        return 2
    rec = build_record(*argv[1:10])
    print(json.dumps(rec, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
