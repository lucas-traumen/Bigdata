"""Unit tests for the shared validation rules (pure-Python mirror).

These tests exercise validation.classify_event / parse_iso_utc /
resolve_temperature from implementation/spark/jobs/validation.py WITHOUT
PySpark (the module imports pyspark lazily inside its Spark-side builder).

The PySpark implementation (build_validity_columns) must stay semantically
identical; equivalence on real streams is verified by the e2e/recovery
scripts on the target machine (runtime DEFERRED on this host).
"""

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import validation  # noqa: E402


def valid_event(**overrides):
    """A canonical valid event; tests override individual fields."""
    base = {
        "event_id": "sim01-000001",
        "sensor_id": "TEMP_01",
        "event_time": "2026-09-13T10:00:00+07:00",
        "temperature_c": 36.2,
        "ingest_time": "2026-09-13T03:00:00.120Z",
    }
    base.update(overrides)
    return base


class ParseIsoUtcTests(unittest.TestCase):
    def test_offset_converted_to_utc(self):
        dt = validation.parse_iso_utc("2026-09-13T10:00:00+07:00")
        self.assertEqual(dt, datetime(2026, 9, 13, 3, 0, tzinfo=timezone.utc))

    def test_z_suffix(self):
        dt = validation.parse_iso_utc("2026-09-13T03:00:00.120Z")
        self.assertEqual(dt, datetime(2026, 9, 13, 3, 0, 0, 120000, tzinfo=timezone.utc))

    def test_naive_treated_as_utc(self):
        dt = validation.parse_iso_utc("2026-09-13T03:00:00")
        self.assertEqual(dt.utcoffset(), timedelta(0))
        self.assertEqual(dt.hour, 3)

    def test_garbage_returns_none(self):
        self.assertIsNone(validation.parse_iso_utc("not-a-timestamp"))
        self.assertIsNone(validation.parse_iso_utc(""))
        self.assertIsNone(validation.parse_iso_utc(None))
        self.assertIsNone(validation.parse_iso_utc(12345))
        self.assertIsNone(validation.parse_iso_utc("2026-13-45T99:99:99"))


class ResolveTemperatureTests(unittest.TestCase):
    def test_preferred_field(self):
        self.assertEqual(
            validation.resolve_temperature({"temperature_c": 30.5, "value": 99.0}),
            (30.5, True))

    def test_legacy_alias_used_when_canonical_missing(self):
        self.assertEqual(validation.resolve_temperature({"value": "21.75"}), (21.75, True))

    def test_empty_canonical_falls_through_to_alias(self):
        # mirrors coalesce(nullif(trim(temperature_c),''), nullif(trim(value),''))
        self.assertEqual(
            validation.resolve_temperature({"temperature_c": "  ", "value": "30"}), (30.0, True))

    def test_missing_both(self):
        self.assertEqual(validation.resolve_temperature({}), (None, False))

    def test_non_numeric_string(self):
        self.assertEqual(validation.resolve_temperature({"temperature_c": "hot"}), (None, True))


class ClassifyEventTests(unittest.TestCase):
    def test_valid_event_passes(self):
        ok, reason = validation.classify_event(valid_event())
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_missing_temperature(self):
        ok, reason = validation.classify_event(valid_event(temperature_c=None))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_BAD_TEMPERATURE)

    def test_non_numeric_temperature(self):
        ok, reason = validation.classify_event(valid_event(temperature_c="hot"))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_BAD_TEMPERATURE)

    def test_out_of_physical_bounds(self):
        ok, reason = validation.classify_event(valid_event(temperature_c=9999))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_OUT_OF_BOUNDS)
        ok, reason = validation.classify_event(valid_event(temperature_c=-50.1))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_OUT_OF_BOUNDS)

    def test_bounds_are_inclusive(self):
        ok, _ = validation.classify_event(valid_event(temperature_c=-50))
        self.assertTrue(ok)
        ok, _ = validation.classify_event(valid_event(temperature_c=200))
        self.assertTrue(ok)

    def test_bad_event_time(self):
        ok, reason = validation.classify_event(valid_event(event_time="not-a-timestamp"))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_MISSING_EVENT_TIME)
        ok, reason = validation.classify_event(valid_event(event_time=None))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_MISSING_EVENT_TIME)

    def test_bad_ingest_time(self):
        ok, reason = validation.classify_event(valid_event(ingest_time="yesterday"))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_MISSING_INGEST_TIME)

    def test_missing_sensor_and_event_id(self):
        ok, reason = validation.classify_event(valid_event(sensor_id=None))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_MISSING_SENSOR)
        ok, reason = validation.classify_event(valid_event(event_id=""))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_MISSING_EVENT_ID)

    def test_alert_threshold_value_stays_valid(self):
        # > 35 C is a business rule (alert), NOT a validation failure
        ok, reason = validation.classify_event(valid_event(temperature_c=35.5))
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_late_valid_event_not_quarantined(self):
        late = valid_event(event_time="2026-09-12T10:00:00Z")  # ~1 day old
        ok, reason = validation.classify_event(late)
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_check_order_timestamp_wins_over_temperature(self):
        # both broken: reason must be the timestamp one (documented order)
        ok, reason = validation.classify_event(
            valid_event(event_time="nope", temperature_c="hot"))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_MISSING_EVENT_TIME)


class LateFlagMathTests(unittest.TestCase):
    def test_late_threshold_semantics(self):
        # mirror of the Spark expression: received - event > LATE_THRESHOLD_S
        event_time = datetime(2026, 9, 13, 3, 0, 0, tzinfo=timezone.utc)
        received = event_time + timedelta(seconds=validation.LATE_THRESHOLD_S)
        self.assertFalse((received - event_time).total_seconds() > validation.LATE_THRESHOLD_S)
        received = event_time + timedelta(seconds=validation.LATE_THRESHOLD_S + 1)
        self.assertTrue((received - event_time).total_seconds() > validation.LATE_THRESHOLD_S)


class ConfigDefaultTests(unittest.TestCase):
    def test_defaults_match_env_example(self):
        self.assertEqual(validation.BOUNDS_MIN, float(os.environ.get("BOUNDS_MIN", -50)))
        self.assertEqual(validation.BOUNDS_MAX, 200.0)
        self.assertEqual(validation.ALERT_THRESHOLD_C, 35.0)
        self.assertEqual(validation.LATE_THRESHOLD_S, 60)


if __name__ == "__main__":
    unittest.main()
