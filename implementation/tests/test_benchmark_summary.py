"""Tests for bounded-memory benchmark aggregation and percentile evidence."""

from __future__ import annotations

import io
import json
import sys
import tempfile
import tracemalloc
import unittest
import weakref
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import benchmark_summary  # noqa: E402


class BenchmarkSummaryTests(unittest.TestCase):
    def write_jsonl(self, path: Path, rows: list[dict]) -> None:
        path.write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
        )

    def test_percentile_interpolates(self):
        self.assertEqual(benchmark_summary.percentile([1, 2, 3, 4], 0.5), 2.5)
        self.assertEqual(benchmark_summary.percentile([], 0.95), None)

    def test_summary_filters_run_prefix_and_records_payload_rates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            metadata = {
                "run_id": "pilot01",
                "start_utc": "2026-10-01T00:00:00+00:00",
                "end_utc": "2026-10-01T00:00:10+00:00",
            }
            (logs / "bench-pilot01-metadata.json").write_text(
                json.dumps(metadata), encoding="utf-8"
            )
            self.write_jsonl(
                logs / "simulator-pilot01-A.jsonl",
                [
                    {"event_id": "pilot01_A-000001", "ts": "2026-10-01T00:00:01Z",
                     "published": True, "payload_bytes": 100,
                     "published_payload_bytes": 100},
                    {"event_id": "pilot01_A-000002", "ts": "2026-10-01T00:00:03Z",
                     "published": False, "payload_bytes": 120,
                     "published_payload_bytes": 0, "fault": "bad_timestamp"},
                ],
            )
            self.write_jsonl(
                logs / "bridge-deliveries.jsonl",
                [
                    {"event_id": "pilot01_A-000001", "payload_bytes": 100},
                    {"event_id": "other_A-000001", "payload_bytes": 999},
                ],
            )
            self.write_jsonl(
                logs / "stream-progress.jsonl",
                [
                    {"query": "q1_bronze", "batch_id": 0, "num_input_rows": 999,
                     "duration_ms": 999, "ts": "2026-09-30T23:59:59Z"},
                    {"query": "q1_bronze", "batch_id": 1, "num_input_rows": 2,
                     "duration_ms": 100, "ts": "2026-10-01T00:00:00Z"},
                    {"query": "q1_bronze", "batch_id": 2, "num_input_rows": 3,
                     "duration_ms": 200, "ts": "2026-10-01T00:00:10Z"},
                    {"query": "q1_bronze", "batch_id": 99, "num_input_rows": 999,
                     "duration_ms": 999, "ts": "2026-10-02T00:00:00Z"},
                    {"query": "q2_clean", "batch_id": 4, "num_input_rows": 7,
                     "duration_ms": 400, "ts": "invalid"},
                    {"query": "q2_clean", "batch_id": 5, "num_input_rows": 0,
                     "duration_ms": -1, "ts": "2026-10-01T00:00:05Z"},
                ],
            )
            output = logs / "summary.json"
            with patch.object(
                sys, "argv", ["benchmark_summary.py", "--run-id", "pilot01",
                               "--data-root", str(root), "--output", str(output)]
            ):
                self.assertEqual(benchmark_summary.main(), 0)
            result = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(result["source"]["records"], 2)
            self.assertEqual(result["source"]["published_records"], 1)
            self.assertEqual(result["source"]["payload_bytes"], 220)
            self.assertEqual(result["bridge"]["delivery_records"], 1)
            self.assertEqual(result["metadata"], metadata)
            self.assertEqual(result["stream_progress"], {
                "q1_bronze": {
                    "batches_seen": 2, "input_rows_in_run_window": 5,
                    "last_batch_id": 2, "duration_ms_p50": 150,
                    "duration_ms_p95": 195,
                },
                "q2_clean": {
                    "batches_seen": 2, "input_rows_in_run_window": 7,
                    "last_batch_id": 5, "duration_ms_p50": 400,
                    "duration_ms_p95": 400,
                },
            })

    def test_summary_consumes_sources_once_without_retaining_event_rows(self):
        class TrackedRow(dict):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            manifest_paths = [
                logs / "simulator-oneshot-A.jsonl",
                logs / "simulator-oneshot-B.jsonl",
            ]
            delivery_path = logs / "bridge-deliveries.jsonl"
            progress_path = logs / "stream-progress.jsonl"
            for path in manifest_paths:
                path.touch()
            calls = {}

            def rows(path):
                calls[path] = calls.get(path, 0) + 1
                self.assertEqual(calls[path], 1, "Evidence must be read once")
                if path == progress_path:
                    yield {"query": "q1_bronze", "batch_id": 1,
                           "num_input_rows": 64, "duration_ms": 100}
                    return
                live_rows = weakref.WeakValueDictionary()
                count = 64 if path == delivery_path else 32
                for index in range(count):
                    row = TrackedRow(
                        event_id=f"oneshot_A-{index % 16:06d}",
                        ts=f"2026-10-01T00:00:{index % 32:02d}Z",
                        published=index != 31,
                        payload_bytes=10,
                        published_payload_bytes=10 if index != 31 else 0,
                    )
                    live_rows[index] = row
                    self.assertLessEqual(
                        len(live_rows), 2,
                        "Summary retained event rows from a streaming source",
                    )
                    yield row

            output = logs / "summary.json"
            with patch.object(benchmark_summary, "jsonl", side_effect=rows):
                self.assertEqual(benchmark_summary.main([
                    "--run-id", "oneshot", "--data-root", str(root),
                    "--output", str(output),
                ]), 0)
            result = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(result["source"]["records"], 64)
            self.assertEqual(result["source"]["distinct_event_ids"], 16)
            self.assertEqual(result["source"]["published_records"], 62)
            self.assertEqual(result["source"]["puback_failures"], 2)
            self.assertEqual(result["source"]["payload_bytes"], 640)
            self.assertEqual(result["source"]["published_payload_bytes"], 620)
            self.assertEqual(result["source"]["source_seconds_from_manifest"], 31)
            self.assertEqual(result["bridge"]["delivery_records"], 64)
            self.assertEqual(result["bridge"]["distinct_delivered_event_ids"], 16)
            self.assertEqual(result["bridge"]["payload_bytes"], 640)
            self.assertEqual(calls, {
                **{path: 1 for path in manifest_paths},
                delivery_path: 1, progress_path: 1,
            })

    def test_summary_counts_duplicates_across_files_and_failed_publications(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            metadata = {
                "start_utc": "2026-10-01T00:00:00Z",
                "end_utc": "2026-10-01T00:00:10Z",
            }
            (logs / "bench-pilot01-metadata.json").write_text(
                json.dumps(metadata), encoding="utf-8"
            )
            manifest_a = logs / "simulator-pilot01-A.jsonl"
            manifest_b = logs / "simulator-pilot01-B.jsonl"
            self.write_jsonl(manifest_a, [
                {"event_id": "pilot01_A-000001", "ts": "2026-10-01T00:00:08Z",
                 "published": True, "payload_bytes": 100,
                 "published_payload_bytes": 100, "fault": "duplicate"},
                {"event_id": "pilot01_A-000002", "ts": "2026-10-01T00:00:04Z",
                 "published": False, "payload_bytes": 120,
                 "published_payload_bytes": 0, "fault": "bad_timestamp"},
                {},
                {"event_id": "pilot01_B-000001", "ts": "2026-10-01T00:00:02Z",
                 "published": 1, "payload_bytes": 80, "fault": "duplicate"},
            ])
            with manifest_a.open("a", encoding="utf-8") as fh:
                fh.write("malformed JSON\n[]\n")
            self.write_jsonl(manifest_b, [
                {"event_id": "pilot01_A-000001", "ts": "2026-10-01T00:00:06Z",
                 "published": True, "payload_bytes": 100,
                 "published_payload_bytes": 100, "fault": "duplicate"},
                {"event_id": "pilot01_a-000001", "ts": "invalid",
                 "published": True, "payload_bytes": 70,
                 "published_payload_bytes": 70, "fault": ""},
                {"event_id": "pilot01_A-000002", "ts": "2026-10-01T00:00:12Z",
                 "published": False, "payload_bytes": 120,
                 "published_payload_bytes": 0, "fault": "bad_timestamp"},
                {"event_id": None, "ts": "2026-10-01T00:00:03Z",
                 "published": True, "payload_bytes": 50,
                 "published_payload_bytes": 50},
            ])
            self.write_jsonl(logs / "simulator-other-A.jsonl", [
                {"event_id": "other_A-000001", "published": True,
                 "payload_bytes": 9999},
            ])
            self.write_jsonl(logs / "bridge-deliveries.jsonl", [
                {"event_id": "pilot01_A-000001", "payload_bytes": 100},
                {"event_id": "pilot01_A-000001", "payload_bytes": 110},
                {"event_id": "pilot01_a-000001", "payload_bytes": 70},
                {"event_id": "pilot01_extra", "payload_bytes": 75,
                 "ts": "2026-10-01T00:00:20Z"},
                {"event_id": "other_A-000001", "payload_bytes": 999},
                {"event_id": "pilot01extra_A-000001", "payload_bytes": 999},
                {"event_id": "pilot01", "payload_bytes": 999},
                {"event_id": "", "payload_bytes": 999},
                {"event_id": None, "payload_bytes": 999},
            ])
            evidence = {path: path.read_bytes() for path in logs.iterdir()}
            output = logs / "summary.json"
            self.assertEqual(benchmark_summary.main([
                "--run-id", "pilot01", "--data-root", str(root),
                "--output", str(output),
            ]), 0)
            result = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(result["source"], {
                "manifest_files": [str(manifest_a), str(manifest_b)],
                "records": 8, "distinct_event_ids": 4,
                "published_records": 4, "puback_failures": 4,
                "payload_bytes": 640, "published_payload_bytes": 320,
                "source_seconds_from_manifest": 10,
                "events_per_second": 0.8, "payload_bytes_per_second": 64,
                "faults": {"bad_timestamp": 2, "duplicate": 3},
            })
            self.assertEqual(result["bridge"], {
                "delivery_records": 4, "distinct_delivered_event_ids": 3,
                "payload_bytes": 355,
            })
            self.assertEqual(set(logs.iterdir()), set(evidence) | {output})
            for path, content in evidence.items():
                self.assertEqual(path.read_bytes(), content)

    def test_source_time_bounds_use_metadata_only_without_valid_timestamps(self):
        for timestamps, expected_seconds in [
            (["invalid", None], 10),
            (["2026-10-01T00:00:05Z", "invalid"], None),
        ]:
            with self.subTest(timestamps=timestamps), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                logs = root / "logs"
                logs.mkdir()
                (logs / "bench-pilot01-metadata.json").write_text(json.dumps({
                    "start_utc": "2026-10-01T00:00:00Z",
                    "end_utc": "2026-10-01T00:00:10Z",
                }), encoding="utf-8")
                self.write_jsonl(logs / "simulator-pilot01-A.jsonl", [
                    {"event_id": 7, "ts": timestamps[0], "published": True,
                     "payload_bytes": 20, "published_payload_bytes": 20},
                    {"event_id": None, "ts": timestamps[1], "payload_bytes": 30},
                ])
                output = logs / "summary.json"
                self.assertEqual(benchmark_summary.main([
                    "--run-id", "pilot01", "--data-root", str(root),
                    "--output", str(output),
                ]), 0)
                source = json.loads(output.read_text(encoding="utf-8"))["source"]
                self.assertEqual(source["source_seconds_from_manifest"], expected_seconds)
                self.assertEqual(source["distinct_event_ids"], 1)
                self.assertEqual(source["puback_failures"], 1)
                self.assertEqual(source["events_per_second"],
                                 0.2 if expected_seconds else None)
                self.assertEqual(source["payload_bytes_per_second"],
                                 5 if expected_seconds else None)

    def test_missing_evidence_prints_an_empty_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "missing"
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                self.assertEqual(benchmark_summary.main([
                    "--run-id", "empty", "--data-root", str(root),
                ]), 0)
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["source"], {
                "manifest_files": [], "records": 0, "distinct_event_ids": 0,
                "published_records": 0, "puback_failures": 0,
                "payload_bytes": 0, "published_payload_bytes": 0,
                "source_seconds_from_manifest": None,
                "events_per_second": None, "payload_bytes_per_second": None,
                "faults": {},
            })
            self.assertEqual(result["bridge"], {
                "delivery_records": 0, "distinct_delivered_event_ids": 0,
                "payload_bytes": 0,
            })
            self.assertEqual(result["stream_progress"], {})
            self.assertEqual(result["metadata"], {})
            self.assertFalse(root.exists())

    def test_temporary_index_is_removed_on_success_and_evidence_read_failure(self):
        for failure in (None, "source", "bridge"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                logs = root / "logs"
                logs.mkdir()
                source_path = logs / "simulator-pilot01-A.jsonl"
                bridge_path = logs / "bridge-deliveries.jsonl"
                self.write_jsonl(source_path, [{"event_id": "pilot01_A-000001"}])
                self.write_jsonl(bridge_path, [{"event_id": "pilot01_A-000001"}])
                evidence = {path: path.read_bytes() for path in logs.iterdir()}
                scratch_paths = []
                original_jsonl = benchmark_summary.jsonl
                failure_path = {"source": source_path, "bridge": bridge_path}.get(failure)

                def temporary_index(*args, **kwargs):
                    directory = tempfile.TemporaryDirectory(*args, **kwargs)
                    scratch_paths.append(Path(directory.name))
                    self.assertEqual(scratch_paths[-1].parent, logs)
                    return directory

                def evidence_rows(path):
                    for row in original_jsonl(path):
                        yield row
                        if path == failure_path:
                            raise OSError("fixture read failure")

                output = logs / "summary.json"
                argv = ["--run-id", "pilot01", "--data-root", str(root),
                        "--output", str(output)]
                with patch.object(benchmark_summary, "TemporaryDirectory",
                                  side_effect=temporary_index), patch.object(
                    benchmark_summary, "jsonl", side_effect=evidence_rows
                ):
                    if failure:
                        with self.assertRaisesRegex(OSError, "fixture read failure"):
                            benchmark_summary.main(argv)
                    else:
                        self.assertEqual(benchmark_summary.main(argv), 0)
                self.assertEqual(len(scratch_paths), 1)
                self.assertFalse(scratch_paths[0].exists())
                self.assertEqual(output.exists(), failure is None)
                for path, content in evidence.items():
                    self.assertEqual(path.read_bytes(), content)

    def test_python_memory_is_bounded_with_many_distinct_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            source_path = logs / "simulator-memory01-A.jsonl"
            source_path.touch()
            count = 100_000

            def rows(path):
                if path == source_path:
                    for index in range(count):
                        yield {"event_id": f"memory01_{index:020d}", "published": True,
                               "payload_bytes": 1, "published_payload_bytes": 1}
                elif path == logs / "bridge-deliveries.jsonl":
                    for index in range(count):
                        yield {"event_id": f"memory01_{index // 2:020d}",
                               "payload_bytes": 1}

            output = logs / "summary.json"
            with patch.object(benchmark_summary, "jsonl", side_effect=rows):
                tracemalloc.start()
                try:
                    self.assertEqual(benchmark_summary.main([
                        "--run-id", "memory01", "--data-root", str(root),
                        "--output", str(output),
                    ]), 0)
                    _, peak_bytes = tracemalloc.get_traced_memory()
                finally:
                    tracemalloc.stop()
            # Allow interpreter headroom while catching Python collections of every ID.
            self.assertLess(peak_bytes, 8 * 1024 * 1024)
            result = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(result["source"]["records"], count)
            self.assertEqual(result["source"]["distinct_event_ids"], count)
            self.assertEqual(result["source"]["published_records"], count)
            self.assertEqual(result["source"]["payload_bytes"], count)
            self.assertEqual(result["bridge"]["delivery_records"], count)
            self.assertEqual(result["bridge"]["distinct_delivered_event_ids"], count // 2)
            self.assertEqual(result["bridge"]["payload_bytes"], count)


if __name__ == "__main__":
    unittest.main()
