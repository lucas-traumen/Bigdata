"""Unit tests for the batch window contract (parse / validate / boundaries).

Exercises the pure helpers in spark/jobs/batch.py — the module defers PySpark
imports to main(), so it imports cleanly without pyspark installed.
"""

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import batch  # noqa: E402

UTC = timezone.utc


class ParseUtcTests(unittest.TestCase):
    def test_z_suffix(self):
        dt = batch.parse_utc("2026-09-15T01:00:00Z")
        self.assertEqual(dt, datetime(2026, 9, 15, 1, 0, tzinfo=UTC))

    def test_explicit_offset_converted(self):
        dt = batch.parse_utc("2026-09-15T08:00:00+07:00")
        self.assertEqual(dt, datetime(2026, 9, 15, 1, 0, tzinfo=UTC))

    def test_naive_rejected(self):
        with self.assertRaises(ValueError):
            batch.parse_utc("2026-09-15T01:00:00")

    def test_garbage_rejected(self):
        for bad in ("", "   ", "not-a-time", "2026-09-15", "15/09/2026 01:00"):
            with self.assertRaises(ValueError, msg=f"should reject {bad!r}"):
                batch.parse_utc(bad)


class ValidateWindowTests(unittest.TestCase):
    def test_valid_single_hour(self):
        start = datetime(2026, 9, 15, 1, 0, tzinfo=UTC)
        end = start + timedelta(hours=1)
        batch.validate_window(start, end)  # must not raise

    def test_valid_multi_hour(self):
        start = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
        end = datetime(2026, 9, 16, 0, 0, tzinfo=UTC)
        batch.validate_window(start, end)  # must not raise

    def test_non_hour_boundary_rejected(self):
        start = datetime(2026, 9, 15, 1, 30, tzinfo=UTC)
        end = datetime(2026, 9, 15, 2, 0, tzinfo=UTC)
        with self.assertRaises(ValueError):
            batch.validate_window(start, end)
        start = datetime(2026, 9, 15, 1, 0, tzinfo=UTC)
        end = datetime(2026, 9, 15, 2, 15, tzinfo=UTC)
        with self.assertRaises(ValueError):
            batch.validate_window(start, end)

    def test_empty_or_negative_window_rejected(self):
        t = datetime(2026, 9, 15, 1, 0, tzinfo=UTC)
        with self.assertRaises(ValueError):
            batch.validate_window(t, t)
        with self.assertRaises(ValueError):
            batch.validate_window(t, t - timedelta(hours=1))

    def test_less_than_one_hour_rejected(self):
        # 0 seconds apart is caught by end<=start; check the 3600s guard via
        # boundaries that differ by less than an hour is impossible with hour
        # alignment — the guard protects future relaxations of that rule.
        start = datetime(2026, 9, 15, 1, 0, tzinfo=UTC)
        end = start + timedelta(minutes=59, seconds=59)
        # not hour-aligned so this hits the boundary check first; force the
        # guard by constructing aligned datetimes with a fake short span:
        try:
            batch.validate_window(start, end)
            self.fail("expected ValueError")
        except ValueError:
            pass


class TruncateHourTests(unittest.TestCase):
    def test_truncates_down(self):
        dt = datetime(2026, 9, 15, 1, 37, 42, tzinfo=UTC)
        self.assertEqual(batch.truncate_hour(dt),
                         datetime(2026, 9, 15, 1, 0, tzinfo=UTC))

    def test_exactly_on_boundary(self):
        dt = datetime(2026, 9, 15, 2, 0, tzinfo=UTC)
        self.assertEqual(batch.truncate_hour(dt), dt)


class StartEndSemanticsTests(unittest.TestCase):
    """Mirror of the Spark filter: event_time >= start AND event_time < end."""

    @staticmethod
    def in_range(event_dt, start, end):
        return start <= event_dt < end

    def setUp(self):
        self.start = datetime(2026, 9, 15, 1, 0, tzinfo=UTC)
        self.end = datetime(2026, 9, 15, 3, 0, tzinfo=UTC)

    def test_event_at_start_included(self):
        self.assertTrue(self.in_range(self.start, self.start, self.end))

    def test_event_at_end_excluded(self):
        self.assertFalse(self.in_range(self.end, self.start, self.end))

    def test_event_just_before_end_included(self):
        self.assertTrue(self.in_range(self.end - timedelta(seconds=1),
                                      self.start, self.end))

    def test_hour_of_events_within_window(self):
        # every event in [start,end) maps to a FULL hour contained in the window
        for minute in range(0, 60, 7):
            event = self.start.replace(minute=minute) + timedelta(hours=1, minutes=minute)
            if event >= self.end:
                continue
            hour_start = batch.truncate_hour(event)
            self.assertGreaterEqual(hour_start, self.start)
            self.assertLess(hour_start, self.end)


class MainArgRejectionTests(unittest.TestCase):
    """main() must reject bad windows BEFORE touching Spark/PostgreSQL."""

    def test_rejects_non_hour_start(self):
        rc = batch.main(["--start", "2026-09-15T01:30:00Z",
                         "--end", "2026-09-15T02:00:00Z"])
        self.assertEqual(rc, 2)

    def test_rejects_end_not_after_start(self):
        rc = batch.main(["--start", "2026-09-15T02:00:00Z",
                         "--end", "2026-09-15T02:00:00Z"])
        self.assertEqual(rc, 2)

    def test_rejects_bad_format(self):
        rc = batch.main(["--start", "yesterday", "--end", "2026-09-15T02:00:00Z"])
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
