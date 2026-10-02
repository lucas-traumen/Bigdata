#!/usr/bin/env python3
"""Summarize one benchmark run from append-only JSONL evidence.

The source manifests and bridge log are filtered by event-id/run prefix. The
Spark progress log is filtered by the run's recorded wall-clock interval, so
historical runs are not silently mixed into the result.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterable


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.is_file():
        return
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                yield value


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def duration_seconds(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    seconds = (end - start).total_seconds()
    return seconds if seconds > 0 else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--data-root", default="/data/bigdata")
    parser.add_argument("--output")
    args = parser.parse_args(argv)

    root = Path(args.data_root)
    logs = root / "logs"
    metadata_path = logs / f"bench-{args.run_id}-metadata.json"
    metadata: dict[str, Any] = {}
    if metadata_path.is_file():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    run_prefix = f"{args.run_id}_"
    start = parse_ts(metadata.get("start_utc"))
    end = parse_ts(metadata.get("end_utc"))

    manifest_paths = sorted(logs.glob(f"simulator-{args.run_id}-*.jsonl"))
    source_records = 0
    payload_bytes = 0
    published_bytes = 0
    published = 0
    faults: Counter[str] = Counter()
    source_start: datetime | None = None
    source_end: datetime | None = None
    delivery_records = 0
    delivered_bytes = 0

    # Keep the exact index with the evidence, away from potentially RAM-backed /tmp.
    # Missing evidence still produces an empty summary without creating a logs dir.
    with TemporaryDirectory(
        prefix=".benchmark-summary-", dir=logs if logs.is_dir() else None
    ) as scratch:
        with closing(sqlite3.connect(Path(scratch) / "event-ids.sqlite3")) as event_db:
            # This index is disposable: bound the cache and skip journal/fsync costs.
            event_db.execute("PRAGMA cache_size = -8192")
            event_db.execute("PRAGMA mmap_size = 0")
            event_db.execute("PRAGMA temp_store = FILE")
            event_db.execute("PRAGMA journal_mode = OFF")
            event_db.execute("PRAGMA synchronous = OFF")
            event_db.execute(
                "CREATE TABLE event_ids (event_id TEXT PRIMARY KEY) WITHOUT ROWID"
            )
            for path in manifest_paths:
                for row in jsonl(path):
                    source_records += 1
                    payload_bytes += int(row.get("payload_bytes") or 0)
                    published_bytes += int(row.get("published_payload_bytes") or 0)
                    published += row.get("published") is True
                    if row.get("fault"):
                        faults[str(row["fault"])] += 1
                    ts = parse_ts(str(row.get("ts")))
                    if ts is not None:
                        if source_start is None or ts < source_start:
                            source_start = ts
                        if source_end is None or ts > source_end:
                            source_end = ts
                    if row.get("event_id"):
                        event_db.execute(
                            "INSERT OR IGNORE INTO event_ids VALUES (?)",
                            (str(row["event_id"]),),
                        )
            event_db.commit()
            distinct_event_ids = event_db.execute(
                "SELECT COUNT(*) FROM event_ids"
            ).fetchone()[0]

            # Source and bridge distinct counts are independent; reuse the disk pages.
            event_db.execute("DELETE FROM event_ids")
            event_db.commit()
            for row in jsonl(logs / "bridge-deliveries.jsonl"):
                event_id = str(row.get("event_id") or "")
                if event_id.startswith(run_prefix):
                    delivery_records += 1
                    delivered_bytes += int(row.get("payload_bytes") or 0)
                    event_db.execute(
                        "INSERT OR IGNORE INTO event_ids VALUES (?)", (event_id,)
                    )
            event_db.commit()
            distinct_delivered_event_ids = event_db.execute(
                "SELECT COUNT(*) FROM event_ids"
            ).fetchone()[0]

    source_seconds = duration_seconds(
        source_start if source_start is not None else start,
        source_end if source_end is not None else end,
    )

    progress: dict[str, dict[str, Any]] = {}
    progress_durations: defaultdict[str, list[float]] = defaultdict(list)
    progress_rows: Counter[str] = Counter()
    progress_counts: Counter[str] = Counter()
    progress_path = logs / "stream-progress.jsonl"
    for row in jsonl(progress_path):
        ts = parse_ts(str(row.get("ts")))
        if start and ts and ts < start:
            continue
        if end and ts and ts > end:
            continue
        query = str(row.get("query") or "unknown")
        progress[query] = row
        progress_counts[query] += 1
        progress_rows[query] += int(row.get("num_input_rows") or 0)
        duration = row.get("duration_ms")
        if isinstance(duration, (int, float)) and duration >= 0:
            progress_durations[query].append(float(duration))

    progress_summary: dict[str, Any] = {}
    for query in sorted(progress):
        durations = progress_durations[query]
        progress_summary[query] = {
            "batches_seen": progress_counts[query],
            "input_rows_in_run_window": progress_rows[query],
            "last_batch_id": progress[query].get("batch_id"),
            "duration_ms_p50": percentile(durations, 0.50),
            "duration_ms_p95": percentile(durations, 0.95),
        }

    result: dict[str, Any] = {
        "run_id": args.run_id,
        "run_prefix": run_prefix,
        "metadata": metadata,
        "source": {
            "manifest_files": [str(path) for path in manifest_paths],
            "records": source_records,
            "distinct_event_ids": distinct_event_ids,
            "published_records": published,
            "puback_failures": source_records - published,
            "payload_bytes": payload_bytes,
            "published_payload_bytes": published_bytes,
            "source_seconds_from_manifest": source_seconds,
            "events_per_second": (
                source_records / source_seconds if source_seconds else None
            ),
            "payload_bytes_per_second": (
                payload_bytes / source_seconds if source_seconds else None
            ),
            "faults": dict(sorted(faults.items())),
        },
        "bridge": {
            "delivery_records": delivery_records,
            "distinct_delivered_event_ids": distinct_delivered_event_ids,
            "payload_bytes": delivered_bytes,
        },
        "stream_progress": progress_summary,
        "notes": [
            "Source and bridge counts are at-least-once observations; they are not summed across queries.",
            "Bronze payload bytes must be confirmed with bench_bronze_summary.py after q1 has drained.",
        ],
    }
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(encoded + "\n", encoding="utf-8")
    else:
        print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
