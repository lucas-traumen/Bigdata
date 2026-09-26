"""Unit tests for the PostgreSQL idempotency SQL semantics.

Runs the EXACT SQL strings from spark/jobs/pg_sink.py against an in-memory
SQLite database (SQLite supports the same ON CONFLICT / row-value-comparison
/ IS DISTINCT FROM syntax). PostgreSQL behaviour (timestamptz comparisons,
EXCLUDED references) is verified by the e2e/batch scripts on the target
machine — this suite pins down the SEMANTICS of the statements without any
server dependency.

Coverage (plan E3.3): multi-metric sensor_latest upsert, per-metric alert
inserts generated from METRIC_CONFIG (idempotent per (event_id, rule_id)),
metric-vector payload conflict detection, and the (sensor, hour, metric)
Gold upsert.
"""

import sqlite3
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import common  # noqa: E402  (METRIC_NAMES drives the staging column layout)
import pg_sink  # noqa: E402  (psycopg2 import is lazy inside connect_pg)

METRICS = common.METRIC_NAMES
METRIC_COLS = ", ".join(METRICS)
METRIC_PLACEHOLDERS = ", ".join("?" for _ in METRICS)


def make_db():
    """Schema mirroring implementation/sql/schema.sql for SQLite types."""
    conn = sqlite3.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS gold")
    conn.execute(f"""
        CREATE TABLE sensor_latest (
            sensor_id     TEXT PRIMARY KEY,
            event_id      TEXT NOT NULL,
            event_time    TEXT NOT NULL,
            {", ".join(f"{m} REAL" for m in METRICS)},
            received_at   TEXT NOT NULL,
            updated_at    TEXT)""")
    conn.execute("""
        CREATE TABLE alerts (
            event_id      TEXT NOT NULL,
            rule_id       TEXT NOT NULL,
            sensor_id     TEXT NOT NULL,
            event_time    TEXT,
            metric        TEXT NOT NULL,
            threshold     REAL,
            direction     TEXT,
            value         REAL,
            temperature_c REAL,
            created_at    TEXT,
            PRIMARY KEY (event_id, rule_id))""")
    conn.execute(f"""
        CREATE TABLE processed_events (
            event_id           TEXT PRIMARY KEY,
            sensor_id          TEXT,
            event_time         TEXT,
            {", ".join(f"{m} REAL" for m in METRICS)},
            kafka_topic        TEXT,
            kafka_partition    INTEGER,
            kafka_offset       INTEGER,
            first_processed_at TEXT,
            last_duplicate_at  TEXT,
            duplicate_count    INTEGER NOT NULL DEFAULT 0)""")
    conn.execute("""
        CREATE TABLE gold.sensor_hourly (
            sensor_id    TEXT NOT NULL,
            hour_start   TEXT NOT NULL,
            metric       TEXT NOT NULL,
            event_count  INTEGER NOT NULL,
            avg_value    REAL NOT NULL,
            min_value    REAL NOT NULL,
            max_value    REAL NOT NULL,
            computed_at  TEXT,
            PRIMARY KEY (sensor_id, hour_start, metric))""")
    conn.execute(f"""
        CREATE TABLE staging_stream_events (
            event_id        TEXT,
            sensor_id       TEXT,
            event_time      TEXT,
            {", ".join(f"{m} REAL" for m in METRICS)},
            received_at     TEXT,
            kafka_topic     TEXT,
            kafka_partition INTEGER,
            kafka_offset    INTEGER)""")
    conn.execute("""
        CREATE TABLE staging_hourly_agg (
            sensor_id    TEXT,
            hour_start   TEXT,
            metric       TEXT,
            event_count  INTEGER,
            avg_value    REAL,
            min_value    REAL,
            max_value    REAL)""")
    return conn


def stage_event(conn, event_id, sensor_id="zoneA_MULTI_01",
                event_time="2026-09-15T01:05:00Z", metrics=None,
                received="2026-09-15T01:05:01Z",
                topic="sensor_raw", partition=0, offset=1):
    """Stage one event; ``metrics`` maps metric name -> value (absent = NULL)."""
    metrics = metrics or {}
    conn.execute(
        f"INSERT INTO staging_stream_events "
        f"(event_id, sensor_id, event_time, {METRIC_COLS}, received_at, "
        f"kafka_topic, kafka_partition, kafka_offset) "
        f"VALUES (?,?,?,{METRIC_PLACEHOLDERS},?,?,?,?)",
        (event_id, sensor_id, event_time,
         *[metrics.get(m) for m in METRICS],
         received, topic, partition, offset))


def apply_alerts(conn):
    """Execute every per-metric alert statement (as apply_stream_batch does)."""
    for statement in pg_sink.sql_insert_alerts():
        conn.execute(statement)


class SensorLatestTieBreakTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def latest(self, metric="temperature_c", sensor="zoneA_MULTI_01"):
        return self.conn.execute(
            f"SELECT event_id, event_time, received_at, {metric} "
            f"FROM sensor_latest WHERE sensor_id=?", (sensor,)).fetchone()

    def apply(self):
        self.conn.execute(pg_sink.SQL_UPSERT_LATEST)
        self.conn.execute("DELETE FROM staging_stream_events")

    def test_newer_event_overwrites(self):
        stage_event(self.conn, "e1", metrics={"temperature_c": 20.0})
        self.apply()
        stage_event(self.conn, "e2", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 25.0})
        self.apply()
        row = self.latest()
        self.assertEqual(row[0], "e2")
        self.assertEqual(row[3], 25.0)

    def test_older_late_event_does_not_overwrite(self):
        # newer first, then a LATE older event arrives — must NOT win
        stage_event(self.conn, "e_new", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 25.0})
        self.apply()
        stage_event(self.conn, "e_old", event_time="2026-09-15T01:00:00Z",
                    metrics={"temperature_c": 20.0})
        self.apply()
        self.assertEqual(self.latest()[0], "e_new")

    def test_same_timestamp_tie_break_by_received_at(self):
        stage_event(self.conn, "e_a", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 21.0},
                    received="2026-09-15T02:00:05Z")
        self.apply()
        stage_event(self.conn, "e_b", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 22.0},
                    received="2026-09-15T02:00:09Z")
        self.apply()
        row = self.latest()
        self.assertEqual(row[0], "e_b")          # later received_at wins
        self.assertEqual(row[3], 22.0)

    def test_same_timestamp_and_received_tie_break_by_event_id(self):
        stage_event(self.conn, "e_2", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 21.0},
                    received="2026-09-15T02:00:05Z")
        self.apply()
        stage_event(self.conn, "e_10", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 22.0},
                    received="2026-09-15T02:00:05Z")
        self.apply()
        # lexicographic row comparison: "e_2" > "e_10"
        self.assertEqual(self.latest()[0], "e_2")

    def test_insert_order_independent(self):
        # apply in the opposite order and compare with the ordered run
        stage_event(self.conn, "e_a", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 21.0},
                    received="2026-09-15T02:00:05Z")
        stage_event(self.conn, "e_b", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 22.0},
                    received="2026-09-15T02:00:09Z")
        self.apply()
        self.assertEqual(self.latest()[0], "e_b")

    def test_station_subset_carries_all_metrics_of_winner(self):
        stage_event(self.conn, "e1", metrics={"co2_ppm": 900.0, "pm25_ugm3": 30.0})
        self.apply()
        row = self.conn.execute(
            "SELECT event_id, co2_ppm, pm25_ugm3, temperature_c "
            "FROM sensor_latest WHERE sensor_id='zoneA_MULTI_01'").fetchone()
        self.assertEqual(row[0], "e1")
        self.assertEqual(row[1], 900.0)
        self.assertEqual(row[2], 30.0)
        self.assertIsNone(row[3])  # absent metric stays NULL


class IdentityOnlyEventTests(unittest.TestCase):
    """Reviewer round B (T1): a staged event whose metric VECTOR is entirely
    NULL (identity fields only — e.g. a sensor that publishes no reading at
    all) must NOT reach sensor_latest: the ``_any_metric_non_null`` guard in
    SQL_UPSERT_LATEST excludes it, so it can never NULL-out a richer stored
    row. It must STILL be marked processed in processed_events — replay
    bookkeeping covers every staged event_id regardless of metric content.
    """

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def test_identity_only_event_not_in_sensor_latest(self):
        stage_event(self.conn, "e_empty", metrics={})  # all six metrics NULL
        self.conn.execute(pg_sink.SQL_UPSERT_LATEST)
        rows = self.conn.execute("SELECT count(*) FROM sensor_latest").fetchone()[0]
        self.assertEqual(rows, 0)

    def test_newer_identity_only_event_does_not_null_out_richer_row(self):
        stage_event(self.conn, "e_rich", metrics={"temperature_c": 25.0})
        self.conn.execute(pg_sink.SQL_UPSERT_LATEST)
        self.conn.execute("DELETE FROM staging_stream_events")
        # strictly newer event_time, but carries NO metric: must not win
        stage_event(self.conn, "e_empty", event_time="2026-09-15T03:00:00Z",
                    metrics={})
        self.conn.execute(pg_sink.SQL_UPSERT_LATEST)
        row = self.conn.execute(
            "SELECT event_id, temperature_c FROM sensor_latest "
            "WHERE sensor_id='zoneA_MULTI_01'").fetchone()
        self.assertEqual(row[0], "e_rich")
        self.assertEqual(row[1], 25.0)

    def test_identity_only_event_still_marked_processed(self):
        stage_event(self.conn, "e_empty", metrics={})
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        rows = self.conn.execute(
            "SELECT event_id, duplicate_count FROM processed_events").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], ("e_empty", 0))


class MultiRowSensorLatestTests(unittest.TestCase):
    """B1: one batch may stage SEVERAL rows for the same sensor_id (fast
    production rate, fault-injection replays). PostgreSQL aborts an
    INSERT ... ON CONFLICT DO UPDATE whose input hits the same conflict row
    twice, so the statement must first reduce its input to one deterministic
    winner per sensor. These tests pin that reduction down.
    """

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def latest(self, sensor="zoneA_MULTI_01"):
        return self.conn.execute(
            "SELECT event_id, event_time, received_at, temperature_c "
            "FROM sensor_latest WHERE sensor_id=?", (sensor,)).fetchone()

    def latest_count(self):
        return self.conn.execute("SELECT count(*) FROM sensor_latest").fetchone()[0]

    def apply(self):
        self.conn.execute(pg_sink.SQL_UPSERT_LATEST)
        self.conn.execute("DELETE FROM staging_stream_events")

    def test_same_sensor_multi_row_batch_picks_winner(self):
        # three events for one sensor in ONE statement; middle event_time wins
        stage_event(self.conn, "e1", event_time="2026-09-15T01:00:00Z",
                    metrics={"temperature_c": 20.0}, offset=1)
        stage_event(self.conn, "e2", event_time="2026-09-15T03:00:00Z",
                    metrics={"temperature_c": 30.0}, offset=2)
        stage_event(self.conn, "e3", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 25.0}, offset=3)
        self.apply()
        self.assertEqual(self.latest_count(), 1)
        self.assertEqual(self.latest()[0], "e2")
        self.assertEqual(self.latest()[3], 30.0)

    def test_winner_deterministic_regardless_of_stage_order(self):
        events = [("e1", "2026-09-15T01:00:00Z", 20.0, 1),
                  ("e2", "2026-09-15T03:00:00Z", 30.0, 2),
                  ("e3", "2026-09-15T02:00:00Z", 25.0, 3)]
        for order in ((0, 1, 2), (2, 0, 1), (1, 2, 0)):
            conn = make_db()
            for i in order:
                event_id, ts, temp, off = events[i]
                stage_event(conn, event_id, event_time=ts,
                            metrics={"temperature_c": temp}, offset=off)
            conn.execute(pg_sink.SQL_UPSERT_LATEST)
            row = conn.execute(
                "SELECT event_id FROM sensor_latest WHERE sensor_id='zoneA_MULTI_01'"
            ).fetchone()
            conn.close()
            self.assertEqual(row[0], "e2", f"stage order {order} changed the winner")

    def test_multi_sensor_multi_row_in_one_batch(self):
        stage_event(self.conn, "a1", sensor_id="zoneA_MULTI_01",
                    event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 21.0}, offset=1)
        stage_event(self.conn, "a0", sensor_id="zoneA_MULTI_01",
                    event_time="2026-09-15T01:00:00Z",
                    metrics={"temperature_c": 20.0}, offset=2)
        stage_event(self.conn, "b1", sensor_id="zoneB_AIR_01",
                    event_time="2026-09-15T01:30:00Z",
                    metrics={"co2_ppm": 800.0}, offset=3)
        stage_event(self.conn, "b0", sensor_id="zoneB_AIR_01",
                    event_time="2026-09-15T00:30:00Z",
                    metrics={"co2_ppm": 700.0}, offset=4)
        self.apply()
        self.assertEqual(self.latest_count(), 2)
        self.assertEqual(self.latest("zoneA_MULTI_01")[0], "a1")
        self.assertEqual(self.latest("zoneB_AIR_01")[0], "b1")

    def test_existing_row_not_overwritten_by_older_multi_row_batch(self):
        stage_event(self.conn, "e_new", event_time="2026-09-15T03:00:00Z",
                    metrics={"temperature_c": 25.0})
        self.apply()
        # a later batch carrying only OLDER events (e.g. delayed replay)
        stage_event(self.conn, "e_old1", event_time="2026-09-15T01:00:00Z",
                    metrics={"temperature_c": 20.0}, offset=2)
        stage_event(self.conn, "e_old2", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 22.0}, offset=3)
        self.apply()
        self.assertEqual(self.latest()[0], "e_new")
        self.assertEqual(self.latest()[3], 25.0)

    def test_batch_with_newer_and_older_overwrites_with_newer(self):
        stage_event(self.conn, "e_mid", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 22.0})
        self.apply()
        stage_event(self.conn, "e_old", event_time="2026-09-15T01:00:00Z",
                    metrics={"temperature_c": 20.0}, offset=2)
        stage_event(self.conn, "e_top", event_time="2026-09-15T03:00:00Z",
                    metrics={"temperature_c": 26.0}, offset=3)
        self.apply()
        self.assertEqual(self.latest()[0], "e_top")
        self.assertEqual(self.latest()[3], 26.0)

    def test_same_timestamp_multi_row_tie_break_by_event_id(self):
        # same event_time AND received_at in one batch -> greater event_id wins
        stage_event(self.conn, "e_10", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 21.0},
                    received="2026-09-15T02:00:05Z", offset=1)
        stage_event(self.conn, "e_2", event_time="2026-09-15T02:00:00Z",
                    metrics={"temperature_c": 22.0},
                    received="2026-09-15T02:00:05Z", offset=2)
        self.apply()
        # lexicographic row comparison: "e_2" > "e_10"
        self.assertEqual(self.latest()[0], "e_2")


class AlertRuleTests(unittest.TestCase):
    """Per-metric alerts computed inside Q3 from METRIC_CONFIG (plan E3.3):
    one INSERT per alertable metric, rule_id = metric name, idempotent per
    (event_id, rule_id), high direction = strictly above, low = strictly below.
    """

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def alerts(self):
        return self.conn.execute(
            "SELECT rule_id, metric, threshold, direction, value, temperature_c "
            "FROM alerts ORDER BY rule_id").fetchall()

    def count(self):
        return self.conn.execute("SELECT count(*) FROM alerts").fetchone()[0]

    def test_statement_per_alertable_metric_only(self):
        rule_ids = {"temperature_c", "humidity_pct", "co2_ppm", "pressure_hpa",
                    "pm25_ugm3"}
        # the statements themselves reference rule ids; verify via content
        statements = pg_sink.sql_insert_alerts()
        self.assertEqual(len(statements), len(rule_ids))
        for name in rule_ids:
            self.assertTrue(any(f"'{name}'" in s for s in statements),
                            f"no INSERT generated for {name}")
        self.assertFalse(any("'light_lux'" in s for s in statements),
                         "light_lux must not produce an alert statement")

    def test_threshold_crossing_event_inserted(self):
        stage_event(self.conn, "e_hot", metrics={"temperature_c": 36.2})
        apply_alerts(self.conn)
        self.assertEqual(self.count(), 1)
        rule_id, metric, threshold, direction, value, temp_c = self.alerts()[0]
        self.assertEqual((rule_id, metric, threshold, direction, value),
                         ("temperature_c", "temperature_c", 35.0, "high", 36.2))
        self.assertEqual(temp_c, 36.2)  # legacy compat column follows value

    def test_replay_does_not_duplicate(self):
        stage_event(self.conn, "e_hot", metrics={"temperature_c": 36.2})
        apply_alerts(self.conn)
        apply_alerts(self.conn)  # replay
        apply_alerts(self.conn)  # and again
        self.assertEqual(self.count(), 1)

    def test_below_threshold_no_alert(self):
        stage_event(self.conn, "e_cool", metrics={"temperature_c": 35.0})
        apply_alerts(self.conn)
        self.assertEqual(self.count(), 0)

    def test_low_direction_pressure(self):
        stage_event(self.conn, "e_low", metrics={"pressure_hpa": 940.0})
        apply_alerts(self.conn)
        self.assertEqual(self.count(), 1)
        rule_id, metric, threshold, direction, value, _ = self.alerts()[0]
        self.assertEqual((rule_id, metric, threshold, direction, value),
                         ("pressure_hpa", "pressure_hpa", 950.0, "low", 940.0))
        # at the threshold or above it: no alert (strictly below only)
        self.conn.execute("DELETE FROM alerts")
        self.conn.execute("DELETE FROM staging_stream_events")
        stage_event(self.conn, "e_edge", metrics={"pressure_hpa": 950.0})
        apply_alerts(self.conn)
        self.assertEqual(self.count(), 0)
        self.conn.execute("DELETE FROM staging_stream_events")
        stage_event(self.conn, "e_above", metrics={"pressure_hpa": 960.0})
        apply_alerts(self.conn)
        self.assertEqual(self.count(), 0)

    def test_each_high_metric_crosses_independently(self):
        cases = {"humidity_pct": 80.5, "co2_ppm": 1200.0, "pm25_ugm3": 35.5}
        for name, value in cases.items():
            stage_event(self.conn, f"e_{name}", metrics={name: value})
            apply_alerts(self.conn)
        rows = self.alerts()
        self.assertEqual([r[0] for r in rows],
                         ["co2_ppm", "humidity_pct", "pm25_ugm3"])
        self.assertEqual([r[4] for r in rows], [1200.0, 80.5, 35.5])

    def test_multi_metric_event_creates_one_row_per_pair(self):
        stage_event(self.conn, "e_multi",
                    metrics={"temperature_c": 36.0, "co2_ppm": 1200.0})
        apply_alerts(self.conn)
        self.assertEqual(self.count(), 2)
        self.assertEqual({r[0] for r in self.alerts()},
                         {"temperature_c", "co2_ppm"})

    def test_no_alert_for_non_alertable_metric(self):
        stage_event(self.conn, "e_bright", metrics={"light_lux": 150000.0})
        apply_alerts(self.conn)
        self.assertEqual(self.count(), 0)

    def test_non_temperature_alert_keeps_legacy_column_null(self):
        stage_event(self.conn, "e_air",
                    metrics={"co2_ppm": 1500.0, "temperature_c": None})
        apply_alerts(self.conn)
        rule_id, metric, threshold, direction, value, temp_c = self.alerts()[0]
        self.assertEqual(rule_id, "co2_ppm")
        self.assertIsNone(temp_c)


class ProcessedEventsReplayTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def test_replay_bumps_counter_without_growing(self):
        stage_event(self.conn, "e1", metrics={"temperature_c": 30.0})
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        rows = self.conn.execute(
            "SELECT event_id, duplicate_count FROM processed_events").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 1)

    def test_payload_conflict_detected_on_temperature(self):
        stage_event(self.conn, "e1", metrics={"temperature_c": 30.0})
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        # replay same event_id with a DIFFERENT temperature
        self.conn.execute("DELETE FROM staging_stream_events")
        stage_event(self.conn, "e1", metrics={"temperature_c": 88.0})
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        conflicts = self.conn.execute(
            pg_sink.SQL_COUNT_PAYLOAD_CONFLICTS).fetchone()[0]
        self.assertEqual(conflicts, 1)

    def test_payload_conflict_detected_on_other_metric(self):
        stage_event(self.conn, "e1", metrics={"co2_ppm": 600.0})
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        self.conn.execute("DELETE FROM staging_stream_events")
        stage_event(self.conn, "e1", metrics={"co2_ppm": 601.0})
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        conflicts = self.conn.execute(
            pg_sink.SQL_COUNT_PAYLOAD_CONFLICTS).fetchone()[0]
        self.assertEqual(conflicts, 1)

    def test_no_conflict_for_identical_replay(self):
        stage_event(self.conn, "e1", metrics={"temperature_c": 30.0})
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        conflicts = self.conn.execute(
            pg_sink.SQL_COUNT_PAYLOAD_CONFLICTS).fetchone()[0]
        self.assertEqual(conflicts, 0)


class MultiRowProcessedEventsTests(unittest.TestCase):
    """B1: duplicate fault injection can stage the SAME event_id several times
    in one batch. The statement must insert exactly one row per event_id
    (first occurrence = lowest Kafka position) with duplicate_count equal to
    the remaining in-batch occurrences, and add the full batch size when the
    event was already processed before.
    """

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def row(self, event_id="e1"):
        return self.conn.execute(
            "SELECT event_id, kafka_partition, kafka_offset, duplicate_count "
            "FROM processed_events WHERE event_id=?", (event_id,)).fetchone()

    def counts(self):
        return self.conn.execute(
            "SELECT event_id, duplicate_count FROM processed_events "
            "ORDER BY event_id").fetchall()

    def mark(self):
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        self.conn.execute("DELETE FROM staging_stream_events")

    def test_fresh_event_multi_row_counts_in_batch_duplicates(self):
        # 3 occurrences of a never-seen event -> 1 row, duplicate_count = 2
        for off in (1, 2, 3):
            stage_event(self.conn, "e1", metrics={"temperature_c": 30.0}, offset=off)
        self.mark()
        rows = self.counts()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 2)

    def test_first_occurrence_keeps_lowest_kafka_position(self):
        stage_event(self.conn, "e1", partition=0, offset=7)
        stage_event(self.conn, "e1", partition=0, offset=3)
        self.mark()
        row = self.row()
        self.assertEqual((row[1], row[2]), (0, 3))

    def test_existing_event_incremented_by_full_batch_size(self):
        stage_event(self.conn, "e1", metrics={"temperature_c": 30.0}, offset=1)
        self.mark()                                   # fresh -> duplicate_count 0
        for off in (10, 11, 12):                      # replayed 3x in one batch
            stage_event(self.conn, "e1", metrics={"temperature_c": 30.0}, offset=off)
        self.mark()
        # every occurrence of the second batch is a duplicate: 0 + 3 = 3
        self.assertEqual(self.row()[3], 3)

    def test_mixed_batch_fresh_and_replayed_events(self):
        stage_event(self.conn, "e_old", metrics={"temperature_c": 30.0}, offset=1)
        self.mark()                                   # e_old processed (count 0)
        # one batch: e_old replayed 2x, e_new seen 3x
        stage_event(self.conn, "e_old", metrics={"temperature_c": 30.0}, offset=5)
        stage_event(self.conn, "e_old", metrics={"temperature_c": 30.0}, offset=6)
        stage_event(self.conn, "e_new", metrics={"temperature_c": 31.0}, offset=7)
        stage_event(self.conn, "e_new", metrics={"temperature_c": 31.0}, offset=8)
        stage_event(self.conn, "e_new", metrics={"temperature_c": 31.0}, offset=9)
        self.mark()
        self.assertEqual(self.counts(), [("e_new", 2), ("e_old", 2)])
        self.assertEqual(self.row("e_old")[3], 2)     # 0 + 2 occurrences
        self.assertEqual(self.row("e_new")[3], 2)     # 3 occurrences - 1 first

    def test_payload_conflict_with_multi_row_staging(self):
        stage_event(self.conn, "e1", metrics={"temperature_c": 30.0}, offset=1)
        self.mark()
        # replay same event_id 3x, two of which carry a DIFFERENT temperature
        stage_event(self.conn, "e1", metrics={"temperature_c": 88.0}, offset=10)
        stage_event(self.conn, "e1", metrics={"temperature_c": 88.0}, offset=11)
        stage_event(self.conn, "e1", metrics={"temperature_c": 30.0}, offset=12)
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        # conflict query runs BEFORE staging is cleared (cf. apply_stream_batch)
        conflicts = self.conn.execute(
            pg_sink.SQL_COUNT_PAYLOAD_CONFLICTS).fetchone()[0]
        self.assertEqual(conflicts, 2)
        self.assertEqual(self.row()[3], 3)            # count grew by batch size

    def test_identical_multi_row_replay_no_conflict(self):
        stage_event(self.conn, "e1", metrics={"temperature_c": 30.0}, offset=1)
        self.mark()
        for off in (2, 3):
            stage_event(self.conn, "e1", metrics={"temperature_c": 30.0}, offset=off)
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        conflicts = self.conn.execute(
            pg_sink.SQL_COUNT_PAYLOAD_CONFLICTS).fetchone()[0]
        self.assertEqual(conflicts, 0)
        self.assertEqual(self.row()[3], 2)
        self.conn.execute("DELETE FROM staging_stream_events")


class HourlyAggregateIdempotencyTests(unittest.TestCase):
    """Gold upsert per (sensor_id, hour_start, metric): full-replace reruns."""

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def stage_agg(self, sensor, hour, metric, count, avg, mn, mx):
        self.conn.execute(
            "INSERT INTO staging_hourly_agg VALUES (?,?,?,?,?,?,?)",
            (sensor, hour, metric, count, avg, mn, mx))

    def hourly(self, sensor="zoneA_MULTI_01", hour="2026-09-15T01:00:00Z",
               metric="temperature_c"):
        return self.conn.execute(
            "SELECT event_count, avg_value, min_value, max_value "
            "FROM gold.sensor_hourly WHERE sensor_id=? AND hour_start=? AND metric=?",
            (sensor, hour, metric)).fetchone()

    def test_same_aggregate_rerun_does_not_accumulate(self):
        self.stage_agg("zoneA_MULTI_01", "2026-09-15T01:00:00Z",
                       "temperature_c", 10, 30.0, 29.0, 31.0)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)  # rerun same window
        row = self.hourly()
        self.assertEqual(row[0], 10)  # NOT 20
        self.assertEqual(row[1], 30.0)

    def test_rerun_with_updated_data_replaces(self):
        # late data fixed by rerunning the hour: count 10 -> 12
        self.stage_agg("zoneA_MULTI_01", "2026-09-15T01:00:00Z",
                       "temperature_c", 10, 30.0, 29.0, 31.0)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)
        self.conn.execute("DELETE FROM staging_hourly_agg")
        self.stage_agg("zoneA_MULTI_01", "2026-09-15T01:00:00Z",
                       "temperature_c", 12, 30.5, 29.0, 33.0)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)
        row = self.hourly()
        self.assertEqual(row[0], 12)
        self.assertEqual(row[1], 30.5)
        self.assertEqual(row[3], 33.0)

    def test_metrics_of_same_sensor_hour_are_independent(self):
        self.stage_agg("zoneA_MULTI_01", "2026-09-15T01:00:00Z",
                       "temperature_c", 10, 30.0, 29.0, 31.0)
        self.stage_agg("zoneA_MULTI_01", "2026-09-15T01:00:00Z",
                       "co2_ppm", 8, 600.0, 550.0, 640.0)
        self.stage_agg("zoneA_MULTI_01", "2026-09-15T01:00:00Z",
                       "humidity_pct", 10, 61.0, 58.0, 63.0)
        self.stage_agg("zoneB_AIR_01", "2026-09-15T01:00:00Z",
                       "co2_ppm", 3, 800.0, 700.0, 900.0)
        self.stage_agg("zoneA_MULTI_01", "2026-09-15T02:00:00Z",
                       "temperature_c", 7, 31.0, 30.0, 32.0)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)
        total_rows = self.conn.execute(
            "SELECT count(*) FROM gold.sensor_hourly").fetchone()[0]
        self.assertEqual(total_rows, 5)
        self.assertEqual(self.hourly(metric="co2_ppm")[0], 8)
        self.assertEqual(self.hourly(metric="humidity_pct")[1], 61.0)
        self.assertEqual(
            self.hourly(sensor="zoneB_AIR_01", metric="co2_ppm")[0], 3)
        self.assertEqual(
            self.hourly(hour="2026-09-15T02:00:00Z")[0], 7)

    def test_rerun_replaces_only_matching_metric(self):
        self.stage_agg("zoneA_MULTI_01", "2026-09-15T01:00:00Z",
                       "temperature_c", 10, 30.0, 29.0, 31.0)
        self.stage_agg("zoneA_MULTI_01", "2026-09-15T01:00:00Z",
                       "co2_ppm", 8, 600.0, 550.0, 640.0)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)
        # rerun with fresh data for temperature only
        self.conn.execute("DELETE FROM staging_hourly_agg")
        self.stage_agg("zoneA_MULTI_01", "2026-09-15T01:00:00Z",
                       "temperature_c", 11, 30.2, 29.0, 32.0)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)
        self.assertEqual(self.hourly(metric="temperature_c")[0], 11)
        self.assertEqual(self.hourly(metric="co2_ppm")[0], 8)  # untouched


if __name__ == "__main__":
    unittest.main()
