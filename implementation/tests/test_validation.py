"""Unit tests for the shared validation rules (pure-Python mirror).

These tests exercise validation.classify_event / parse_iso_utc /
resolve_metric / evaluate_metrics from implementation/spark/jobs/validation.py
WITHOUT PySpark (the module imports pyspark lazily inside its Spark-side
builder; common.py is pyspark-free at module level).

The PySpark implementation (build_validity_columns) must stay semantically
identical; equivalence on real streams is verified by the e2e/recovery
scripts on the target machine (runtime DEFERRED on this host).

Error model under test (plan E3.2/E3.3):
  * identity errors (timestamps, sensor_id, event_id) => row quarantined;
  * per-metric issues (non-numeric / out-of-bounds) => row stays valid, the
    metric is masked to NULL and a reason "non_numeric:<m>" or
    "value_out_of_bounds:<m>" is recorded;
  * absent metrics (station subsets) are normal and produce no reason.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import common  # noqa: E402
import validation  # noqa: E402


def valid_event(**overrides):
    """A canonical valid MULTI-station event; tests override individual fields."""
    base = {
        "event_id": "zoneA-000001",
        "sensor_id": "zoneA_MULTI_01",
        "event_time": "2026-09-18T10:00:00+07:00",
        "temperature_c": 30.5,
        "humidity_pct": 61.0,
        "co2_ppm": 612.0,
        "pressure_hpa": 1008.4,
        "pm25_ugm3": 18.6,
        "light_lux": 31200.0,
        "ingest_time": "2026-09-18T03:00:00.120Z",
    }
    base.update(overrides)
    return base


class ParseIsoUtcTests(unittest.TestCase):
    def test_offset_converted_to_utc(self):
        dt = validation.parse_iso_utc("2026-09-18T10:00:00+07:00")
        self.assertEqual(dt, datetime(2026, 9, 18, 3, 0, tzinfo=timezone.utc))

    def test_z_suffix(self):
        dt = validation.parse_iso_utc("2026-09-18T03:00:00.120Z")
        self.assertEqual(dt, datetime(2026, 9, 18, 3, 0, 0, 120000, tzinfo=timezone.utc))

    def test_naive_treated_as_utc(self):
        dt = validation.parse_iso_utc("2026-09-18T03:00:00")
        self.assertEqual(dt.utcoffset(), timedelta(0))
        self.assertEqual(dt.hour, 3)

    def test_garbage_returns_none(self):
        self.assertIsNone(validation.parse_iso_utc("not-a-timestamp"))
        self.assertIsNone(validation.parse_iso_utc(""))
        self.assertIsNone(validation.parse_iso_utc(None))
        self.assertIsNone(validation.parse_iso_utc(12345))
        self.assertIsNone(validation.parse_iso_utc("2026-13-45T99:99:99"))


class ResolveMetricTests(unittest.TestCase):
    def test_preferred_field(self):
        self.assertEqual(
            validation.resolve_metric({"temperature_c": 30.5, "value": 99.0},
                                      "temperature_c"),
            (30.5, True))

    def test_legacy_alias_used_when_canonical_missing(self):
        self.assertEqual(
            validation.resolve_metric({"value": "21.75"}, "temperature_c"),
            (21.75, True))

    def test_empty_canonical_falls_through_to_alias(self):
        # mirrors coalesce(nullif(trim(temperature_c),''), nullif(trim(value),''))
        self.assertEqual(
            validation.resolve_metric({"temperature_c": "  ", "value": "30"},
                                      "temperature_c"),
            (30.0, True))

    def test_no_alias_for_other_metrics(self):
        # the legacy `value` alias applies ONLY to temperature_c
        self.assertEqual(validation.resolve_metric({"value": "30"}, "co2_ppm"),
                         (None, False))

    def test_missing_both(self):
        self.assertEqual(validation.resolve_metric({}, "co2_ppm"), (None, False))

    def test_non_numeric_string(self):
        self.assertEqual(
            validation.resolve_metric({"co2_ppm": "hot"}, "co2_ppm"), (None, True))


class ClassifyEventTests(unittest.TestCase):
    """Identity-level classification only: metric problems NEVER quarantine."""

    def test_valid_event_passes(self):
        ok, reason = validation.classify_event(valid_event())
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_identity_only_metric_problems_do_not_quarantine(self):
        ok, reason = validation.classify_event(valid_event(co2_ppm=9999, temperature_c="hot"))
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_metric_subset_is_valid(self):
        # weather station payload without co2/pm25: valid, not an error
        ok, reason = validation.classify_event(
            valid_event(co2_ppm=None, pm25_ugm3=None, humidity_pct=None,
                        pressure_hpa=None, light_lux=None))
        self.assertTrue(ok)
        self.assertIsNone(reason)

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

    def test_late_valid_event_not_quarantined(self):
        late = valid_event(event_time="2026-09-12T10:00:00Z")  # ~6 days old
        ok, reason = validation.classify_event(late)
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_check_order_timestamp_wins_over_sensor(self):
        ok, reason = validation.classify_event(
            valid_event(event_time="nope", sensor_id=None))
        self.assertFalse(ok)
        self.assertEqual(reason, validation.R_MISSING_EVENT_TIME)


class EvaluateMetricsTests(unittest.TestCase):
    def test_full_vector_no_issues(self):
        values, issues = validation.evaluate_metrics(valid_event())
        self.assertEqual(issues, [])
        self.assertEqual(values["temperature_c"], 30.5)
        self.assertEqual(values["co2_ppm"], 612.0)

    def test_station_subset_absent_metrics_have_no_issue(self):
        values, issues = validation.evaluate_metrics(
            valid_event(temperature_c=None, humidity_pct=None, pressure_hpa=None,
                        light_lux=None))  # air-station vector
        self.assertEqual(issues, [])
        self.assertIsNone(values["temperature_c"])
        self.assertEqual(values["co2_ppm"], 612.0)
        self.assertEqual(values["pm25_ugm3"], 18.6)

    def test_out_of_bounds_masked_with_reason(self):
        values, issues = validation.evaluate_metrics(valid_event(co2_ppm=5001))
        self.assertEqual(issues, ["value_out_of_bounds:co2_ppm"])
        self.assertIsNone(values["co2_ppm"])
        self.assertEqual(values["temperature_c"], 30.5)  # other metrics intact

    def test_each_metric_has_correct_bounds(self):
        cases = {
            "temperature_c": (199.9, 200.1, 201.0),
            "humidity_pct": (99.9, 100.1, 100.5),
            "co2_ppm": (4999.9, 5000.1, 5001.0),
            "pressure_hpa": (1099.9, 1100.1, 1100.5),
            "pm25_ugm3": (999.9, 1000.1, 1000.5),
            "light_lux": (199999.9, 200000.1, 200001.0),
        }
        for name, (inside, outside, beyond) in cases.items():
            _, issues = validation.evaluate_metrics(valid_event(**{name: inside}))
            self.assertEqual(issues, [], f"{name}={inside} must stay in bounds")
            _, issues = validation.evaluate_metrics(valid_event(**{name: beyond}))
            self.assertEqual(issues, [f"value_out_of_bounds:{name}"],
                             f"{name}={beyond} must be flagged")

    def test_bounds_are_inclusive(self):
        _, issues = validation.evaluate_metrics(valid_event(temperature_c=-50))
        self.assertEqual(issues, [])
        _, issues = validation.evaluate_metrics(valid_event(temperature_c=200))
        self.assertEqual(issues, [])
        _, issues = validation.evaluate_metrics(valid_event(light_lux=0))
        self.assertEqual(issues, [])
        _, issues = validation.evaluate_metrics(valid_event(light_lux=200000))
        self.assertEqual(issues, [])

    def test_non_numeric_masked_with_reason(self):
        values, issues = validation.evaluate_metrics(valid_event(humidity_pct="wet"))
        self.assertEqual(issues, ["non_numeric:humidity_pct"])
        self.assertIsNone(values["humidity_pct"])

    def test_multiple_issues_follow_metric_order(self):
        values, issues = validation.evaluate_metrics(
            valid_event(temperature_c="hot", co2_ppm=99999, pressure_hpa=10))
        self.assertEqual(issues, [
            "non_numeric:temperature_c",
            "value_out_of_bounds:co2_ppm",
            "value_out_of_bounds:pressure_hpa",
        ])
        for name in ("temperature_c", "co2_ppm", "pressure_hpa"):
            self.assertIsNone(values[name])

    def test_alert_threshold_value_stays_valid(self):
        # > 35 C is a business rule (alert), NOT a validation failure
        values, issues = validation.evaluate_metrics(valid_event(temperature_c=42.0))
        self.assertEqual(issues, [])
        self.assertEqual(values["temperature_c"], 42.0)


class MetricConfigTests(unittest.TestCase):
    """The registry in common.py must match the user-approved metric matrix."""

    def test_six_metrics_in_order(self):
        self.assertEqual(
            common.METRIC_NAMES,
            ("temperature_c", "humidity_pct", "co2_ppm", "pressure_hpa",
             "pm25_ugm3", "light_lux"))

    def test_bounds_thresholds_directions(self):
        expected = {
            "temperature_c": (-50.0, 200.0, 35.0, "high"),
            "humidity_pct": (0.0, 100.0, 80.0, "high"),
            "co2_ppm": (0.0, 5000.0, 1000.0, "high"),
            "pressure_hpa": (300.0, 1100.0, 950.0, "low"),
            "pm25_ugm3": (0.0, 1000.0, 35.0, "high"),
            "light_lux": (0.0, 200000.0, None, None),
        }
        self.assertEqual(common.METRIC_CONFIG, expected)

    def test_alert_metrics_exclude_light_lux(self):
        self.assertEqual(
            common.alert_metrics(),
            ("temperature_c", "humidity_pct", "co2_ppm", "pressure_hpa", "pm25_ugm3"))
        with self.assertRaises(ValueError):
            common.alert_rule("light_lux")

    def test_alert_rule_directions(self):
        self.assertEqual(common.alert_rule("temperature_c"), (35.0, "high"))
        self.assertEqual(common.alert_rule("pressure_hpa"), (950.0, "low"))

    def test_legacy_alias_registry(self):
        self.assertEqual(common.METRIC_ALIASES, {"temperature_c": ("value",)})


class LateFlagMathTests(unittest.TestCase):
    def test_late_threshold_semantics(self):
        # mirror of the Spark expression: received - event > LATE_THRESHOLD_S
        event_time = datetime(2026, 9, 18, 3, 0, 0, tzinfo=timezone.utc)
        received = event_time + timedelta(seconds=validation.LATE_THRESHOLD_S)
        self.assertFalse((received - event_time).total_seconds() > validation.LATE_THRESHOLD_S)
        received = event_time + timedelta(seconds=validation.LATE_THRESHOLD_S + 1)
        self.assertTrue((received - event_time).total_seconds() > validation.LATE_THRESHOLD_S)


class ConfigDefaultTests(unittest.TestCase):
    def test_late_threshold_default(self):
        self.assertEqual(validation.LATE_THRESHOLD_S,
                         float(os.environ.get("LATE_THRESHOLD_S", 60)))


if __name__ == "__main__":
    unittest.main()
