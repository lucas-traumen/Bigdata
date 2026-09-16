"""Unit tests for the docker-stats parsing used by resource-report.sh (M1).

The parser lives in implementation/scripts/stats_parse.py so it can be tested
with canned `docker stats --no-stream` samples using only the stdlib (no
Docker daemon needed). Contract pinned here:

  * docker stats is called with the TAB-delimited format
    '{{.Name}}\\t{{.MemUsage}}\\t{{.MemPerc}}\\t{{.CPUPerc}}' — MemUsage
    contains spaces ("50MiB / 1GiB"), so tabs are the only stable separator;
  * malformed/short rows are skipped, never mis-aligned;
  * build_record keeps the JSONL record shape consumed by the reports.
"""

import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from stats_parse import build_record, parse_docker_stats  # noqa: E402

CANNED_STATS = (
    "bigdata-kafka-1\t512MiB / 1GiB\t50.12%\t1.23%\n"
    "bigdata-spark-stream-1\t1.486GiB / 2.5GiB\t59.44%\t145.7%\n"
    "bigdata-postgres-1\t50MiB / 1GiB\t4.90%\t0.15%\n"
)


class ParseDockerStatsTests(unittest.TestCase):
    def test_mem_usage_with_spaces_keeps_alignment(self):
        parsed = parse_docker_stats(CANNED_STATS)
        self.assertEqual(parsed["bigdata-kafka-1"], {
            "mem_usage": "512MiB / 1GiB",
            "mem_pct": "50.12%",
            "cpu_pct": "1.23%",
        })

    def test_all_containers_parsed(self):
        parsed = parse_docker_stats(CANNED_STATS)
        self.assertEqual(sorted(parsed), [
            "bigdata-kafka-1", "bigdata-postgres-1", "bigdata-spark-stream-1"])

    def test_skips_rows_without_tab_separator(self):
        # old space-separated shape: split("\t") yields one field -> skipped
        row = "bigdata-kafka-1 512MiB / 1GiB 50.12% 1.23%"
        self.assertEqual(parse_docker_stats(row), {})

    def test_skips_short_rows(self):
        row = "bigdata-kafka-1\t512MiB / 1GiB\t50.12%"  # missing cpu_pct
        self.assertEqual(parse_docker_stats(row), {})
        # canned rows remain intact when a short row is present in the stream
        parsed = parse_docker_stats(CANNED_STATS + row + "\n")
        self.assertEqual(len(parsed), 3)
        self.assertEqual(parsed["bigdata-postgres-1"]["cpu_pct"], "0.15%")

    def test_skips_rows_with_empty_fields(self):
        row = "bigdata-broken\t\t50.12%\t1.23%"
        parsed = parse_docker_stats(row)
        self.assertEqual(parsed, {})

    def test_skips_empty_lines(self):
        self.assertEqual(parse_docker_stats("\n\n" + CANNED_STATS + "\n \n"),
                         parse_docker_stats(CANNED_STATS))

    def test_tolerates_trailing_tab(self):
        row = "bigdata-kafka-1\t512MiB / 1GiB\t50.12%\t1.23%\t"
        parsed = parse_docker_stats(row)
        self.assertEqual(parsed["bigdata-kafka-1"]["cpu_pct"], "1.23%")

    def test_empty_and_none_input(self):
        self.assertEqual(parse_docker_stats(""), {})
        self.assertEqual(parse_docker_stats(None), {})


class BuildRecordTests(unittest.TestCase):
    def test_full_record_shape(self):
        rec = build_record(
            ts="2026-09-14T10:00:00Z", mem_avail="3962", disk_free="11",
            kafka_offsets="sensor_raw-0:5;sensor_raw-1:3;",
            f_bronze="2", f_silver="1", f_quarantine="0",
            pg_counts='{"sensor_latest": 1, "alerts": 0}',
            stats_text=CANNED_STATS)
        self.assertEqual(rec["ts"], "2026-09-14T10:00:00Z")
        self.assertEqual(rec["mem_available_mb"], 3962)
        self.assertEqual(rec["disk_free_gb"], 11)
        self.assertEqual(rec["kafka_end_offsets"], "sensor_raw-0:5;sensor_raw-1:3")
        self.assertEqual(rec["parquet_files"],
                         {"bronze": 2, "silver": 1, "quarantine": 0})
        self.assertEqual(rec["pg_counts"], {"sensor_latest": 1, "alerts": 0})
        self.assertEqual(rec["containers"]["bigdata-spark-stream-1"]["mem_usage"],
                         "1.486GiB / 2.5GiB")

    def test_null_placeholders_become_none(self):
        rec = build_record("t", "null", "", "null", None, None, None, "null", "")
        self.assertIsNone(rec["mem_available_mb"])
        self.assertIsNone(rec["disk_free_gb"])
        self.assertIsNone(rec["kafka_end_offsets"])
        self.assertIsNone(rec["pg_counts"])
        self.assertEqual(rec["parquet_files"],
                         {"bronze": 0, "silver": 0, "quarantine": 0})
        self.assertEqual(rec["containers"], {})

    def test_record_is_single_json_line(self):
        import json
        rec = build_record("t", "100", "5", "null", "0", "0", "0", "null", "")
        line = json.dumps(rec, separators=(",", ":"))
        self.assertNotIn("\n", line)
        self.assertEqual(json.loads(line), rec)


if __name__ == "__main__":
    unittest.main()
