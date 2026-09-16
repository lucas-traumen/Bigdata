"""Unit tests for the PostgreSQL idempotency SQL semantics.

Runs the EXACT SQL strings from spark/jobs/pg_sink.py against an in-memory
SQLite database (SQLite supports the same ON CONFLICT / row-value-comparison
syntax). PostgreSQL behaviour (timestamptz comparisons, EXCLUDED references)
is verified by the e2e/batch scripts on the target machine — this suite pins
down the SEMANTICS of the statements without any server dependency.
"""

import sqlite3
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import pg_sink  # noqa: E402  (psycopg2 import is lazy inside connect_pg)


def make_db():
    """Schema mirroring implementation/sql/schema.sql for SQLite types."""
    conn = sqlite3.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS gold")
    conn.execute("""
        CREATE TABLE sensor_latest (
            sensor_id     TEXT PRIMARY KEY,
            event_id      TEXT NOT NULL,
            event_time    TEXT NOT NULL,
            temperature_c REAL NOT NULL,
            received_at   TEXT NOT NULL,
            updated_at    TEXT)""")
    conn.execute("""
        CREATE TABLE alerts (
            event_id      TEXT NOT NULL,
            rule_id       TEXT NOT NULL,
            sensor_id     TEXT NOT NULL,
            event_time    TEXT,
            temperature_c REAL,
            threshold     REAL,
            created_at    TEXT,
            PRIMARY KEY (event_id, rule_id))""")
    conn.execute("""
        CREATE TABLE processed_events (
            event_id           TEXT PRIMARY KEY,
            sensor_id          TEXT,
            event_time         TEXT,
            temperature_c      REAL,
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
            event_count  INTEGER NOT NULL,
            avg_value    REAL NOT NULL,
            min_value    REAL NOT NULL,
            max_value    REAL NOT NULL,
            computed_at  TEXT,
            PRIMARY KEY (sensor_id, hour_start))""")
    conn.execute("""
        CREATE TABLE staging_stream_events (
            event_id        TEXT,
            sensor_id       TEXT,
            event_time      TEXT,
            temperature_c   REAL,
            received_at     TEXT,
            kafka_topic     TEXT,
            kafka_partition INTEGER,
            kafka_offset    INTEGER)""")
    conn.execute("""
        CREATE TABLE staging_hourly_agg (
            sensor_id    TEXT,
            hour_start   TEXT,
            event_count  INTEGER,
            avg_value    REAL,
            min_value    REAL,
            max_value    REAL)""")
    return conn


def stage_event(conn, event_id, sensor_id="TEMP_01", event_time="2026-09-15T01:05:00Z",
                temperature=30.0, received="2026-09-15T01:05:01Z",
                topic="sensor_raw", partition=0, offset=1):
    conn.execute(
        "INSERT INTO staging_stream_events VALUES (?,?,?,?,?,?,?,?)",
        (event_id, sensor_id, event_time, temperature, received, topic, partition, offset))


class SensorLatestTieBreakTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def latest(self):
        return self.conn.execute(
            "SELECT event_id, event_time, received_at, temperature_c "
            "FROM sensor_latest WHERE sensor_id='TEMP_01'").fetchone()

    def apply(self):
        self.conn.execute(pg_sink.SQL_UPSERT_LATEST)
        self.conn.execute("DELETE FROM staging_stream_events")

    def test_newer_event_overwrites(self):
        stage_event(self.conn, "e1", event_time="2026-09-15T01:00:00Z", temperature=20.0)
        self.apply()
        stage_event(self.conn, "e2", event_time="2026-09-15T02:00:00Z", temperature=25.0)
        self.apply()
        row = self.latest()
        self.assertEqual(row[0], "e2")
        self.assertEqual(row[3], 25.0)

    def test_older_late_event_does_not_overwrite(self):
        # newer first, then a LATE older event arrives — must NOT win
        stage_event(self.conn, "e_new", event_time="2026-09-15T02:00:00Z", temperature=25.0)
        self.apply()
        stage_event(self.conn, "e_old", event_time="2026-09-15T01:00:00Z", temperature=20.0)
        self.apply()
        self.assertEqual(self.latest()[0], "e_new")

    def test_same_timestamp_tie_break_by_received_at(self):
        stage_event(self.conn, "e_a", event_time="2026-09-15T02:00:00Z",
                    temperature=21.0, received="2026-09-15T02:00:05Z")
        self.apply()
        stage_event(self.conn, "e_b", event_time="2026-09-15T02:00:00Z",
                    temperature=22.0, received="2026-09-15T02:00:09Z")
        self.apply()
        row = self.latest()
        self.assertEqual(row[0], "e_b")          # later received_at wins
        self.assertEqual(row[3], 22.0)

    def test_same_timestamp_and_received_tie_break_by_event_id(self):
        stage_event(self.conn, "e_2", event_time="2026-09-15T02:00:00Z",
                    temperature=21.0, received="2026-09-15T02:00:05Z")
        self.apply()
        stage_event(self.conn, "e_10", event_time="2026-09-15T02:00:00Z",
                    temperature=22.0, received="2026-09-15T02:00:05Z")
        self.apply()
        # lexicographic row comparison: "e_2" > "e_10"
        self.assertEqual(self.latest()[0], "e_2")

    def test_insert_order_independent(self):
        # apply in the opposite order and compare with the ordered run
        stage_event(self.conn, "e_a", event_time="2026-09-15T02:00:00Z",
                    temperature=21.0, received="2026-09-15T02:00:05Z")
        stage_event(self.conn, "e_b", event_time="2026-09-15T02:00:00Z",
                    temperature=22.0, received="2026-09-15T02:00:09Z")
        self.apply()
        self.assertEqual(self.latest()[0], "e_b")


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

    def latest(self, sensor="TEMP_01"):
        return self.conn.execute(
            "SELECT event_id, event_time, received_at, temperature_c "
            "FROM sensor_latest WHERE sensor_id=?", (sensor,)).fetchone()

    def latest_count(self):
        return self.conn.execute("SELECT count(*) FROM sensor_latest").fetchone()[0]

    def apply(self):
        self.conn.execute(pg_sink.SQL_UPSERT_LATEST)
        self.conn.execute("DELETE FROM staging_stream_events")

    def test_same_sensor_multi_row_batch_picks_winner(self):
        # three events for TEMP_01 in ONE statement; middle event_time wins
        stage_event(self.conn, "e1", event_time="2026-09-15T01:00:00Z",
                    temperature=20.0, offset=1)
        stage_event(self.conn, "e2", event_time="2026-09-15T03:00:00Z",
                    temperature=30.0, offset=2)
        stage_event(self.conn, "e3", event_time="2026-09-15T02:00:00Z",
                    temperature=25.0, offset=3)
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
                stage_event(conn, event_id, event_time=ts, temperature=temp, offset=off)
            conn.execute(pg_sink.SQL_UPSERT_LATEST)
            row = conn.execute(
                "SELECT event_id FROM sensor_latest WHERE sensor_id='TEMP_01'"
            ).fetchone()
            conn.close()
            self.assertEqual(row[0], "e2", f"stage order {order} changed the winner")

    def test_multi_sensor_multi_row_in_one_batch(self):
        stage_event(self.conn, "a1", sensor_id="TEMP_01",
                    event_time="2026-09-15T02:00:00Z", temperature=21.0, offset=1)
        stage_event(self.conn, "a0", sensor_id="TEMP_01",
                    event_time="2026-09-15T01:00:00Z", temperature=20.0, offset=2)
        stage_event(self.conn, "b1", sensor_id="TEMP_02",
                    event_time="2026-09-15T01:30:00Z", temperature=31.0, offset=3)
        stage_event(self.conn, "b0", sensor_id="TEMP_02",
                    event_time="2026-09-15T00:30:00Z", temperature=30.0, offset=4)
        self.apply()
        self.assertEqual(self.latest_count(), 2)
        self.assertEqual(self.latest("TEMP_01")[0], "a1")
        self.assertEqual(self.latest("TEMP_02")[0], "b1")

    def test_existing_row_not_overwritten_by_older_multi_row_batch(self):
        stage_event(self.conn, "e_new", event_time="2026-09-15T03:00:00Z",
                    temperature=25.0)
        self.apply()
        # a later batch carrying only OLDER events (e.g. delayed replay)
        stage_event(self.conn, "e_old1", event_time="2026-09-15T01:00:00Z",
                    temperature=20.0, offset=2)
        stage_event(self.conn, "e_old2", event_time="2026-09-15T02:00:00Z",
                    temperature=22.0, offset=3)
        self.apply()
        self.assertEqual(self.latest()[0], "e_new")
        self.assertEqual(self.latest()[3], 25.0)

    def test_batch_with_newer_and_older_overwrites_with_newer(self):
        stage_event(self.conn, "e_mid", event_time="2026-09-15T02:00:00Z",
                    temperature=22.0)
        self.apply()
        stage_event(self.conn, "e_old", event_time="2026-09-15T01:00:00Z",
                    temperature=20.0, offset=2)
        stage_event(self.conn, "e_top", event_time="2026-09-15T03:00:00Z",
                    temperature=26.0, offset=3)
        self.apply()
        self.assertEqual(self.latest()[0], "e_top")
        self.assertEqual(self.latest()[3], 26.0)

    def test_same_timestamp_multi_row_tie_break_by_event_id(self):
        # same event_time AND received_at in one batch -> greater event_id wins
        stage_event(self.conn, "e_10", event_time="2026-09-15T02:00:00Z",
                    temperature=21.0, received="2026-09-15T02:00:05Z", offset=1)
        stage_event(self.conn, "e_2", event_time="2026-09-15T02:00:00Z",
                    temperature=22.0, received="2026-09-15T02:00:05Z", offset=2)
        self.apply()
        # lexicographic row comparison: "e_2" > "e_10"
        self.assertEqual(self.latest()[0], "e_2")


class AlertsIdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_db()
        self.sql = pg_sink.sql_insert_alerts(35.0)

    def tearDown(self):
        self.conn.close()

    def count(self):
        return self.conn.execute("SELECT count(*) FROM alerts").fetchone()[0]

    def test_threshold_crossing_event_inserted(self):
        stage_event(self.conn, "e_hot", temperature=36.2)
        self.conn.execute(self.sql)
        self.assertEqual(self.count(), 1)
        row = self.conn.execute("SELECT rule_id, threshold FROM alerts").fetchone()
        self.assertEqual(row[0], "temperature_high")
        self.assertEqual(row[1], 35.0)

    def test_replay_does_not_duplicate(self):
        stage_event(self.conn, "e_hot", temperature=36.2)
        self.conn.execute(self.sql)
        self.conn.execute(self.sql)  # replay
        self.conn.execute(self.sql)  # and again
        self.assertEqual(self.count(), 1)

    def test_below_threshold_no_alert(self):
        stage_event(self.conn, "e_cool", temperature=35.0)  # not strictly above
        self.conn.execute(self.sql)
        self.assertEqual(self.count(), 0)

    def test_different_event_ids_both_alert(self):
        stage_event(self.conn, "e1", temperature=36.0)
        stage_event(self.conn, "e2", temperature=41.0)
        self.conn.execute(self.sql)
        self.assertEqual(self.count(), 2)


class ProcessedEventsReplayTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def test_replay_bumps_counter_without_growing(self):
        stage_event(self.conn, "e1", temperature=30.0)
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        rows = self.conn.execute(
            "SELECT event_id, duplicate_count FROM processed_events").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 1)

    def test_payload_conflict_detected(self):
        stage_event(self.conn, "e1", temperature=30.0)
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        # replay same event_id with a DIFFERENT temperature
        self.conn.execute("DELETE FROM staging_stream_events")
        stage_event(self.conn, "e1", temperature=88.0)
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        conflicts = self.conn.execute(
            pg_sink.SQL_COUNT_PAYLOAD_CONFLICTS).fetchone()[0]
        self.assertEqual(conflicts, 1)

    def test_no_conflict_for_identical_replay(self):
        stage_event(self.conn, "e1", temperature=30.0)
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
            stage_event(self.conn, "e1", temperature=30.0, offset=off)
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
        stage_event(self.conn, "e1", temperature=30.0, offset=1)
        self.mark()                                   # fresh -> duplicate_count 0
        for off in (10, 11, 12):                      # replayed 3x in one batch
            stage_event(self.conn, "e1", temperature=30.0, offset=off)
        self.mark()
        # every occurrence of the second batch is a duplicate: 0 + 3 = 3
        self.assertEqual(self.row()[3], 3)

    def test_mixed_batch_fresh_and_replayed_events(self):
        stage_event(self.conn, "e_old", temperature=30.0, offset=1)
        self.mark()                                   # e_old processed (count 0)
        # one batch: e_old replayed 2x, e_new seen 3x
        stage_event(self.conn, "e_old", temperature=30.0, offset=5)
        stage_event(self.conn, "e_old", temperature=30.0, offset=6)
        stage_event(self.conn, "e_new", temperature=31.0, offset=7)
        stage_event(self.conn, "e_new", temperature=31.0, offset=8)
        stage_event(self.conn, "e_new", temperature=31.0, offset=9)
        self.mark()
        self.assertEqual(self.counts(), [("e_new", 2), ("e_old", 2)])
        self.assertEqual(self.row("e_old")[3], 2)     # 0 + 2 occurrences
        self.assertEqual(self.row("e_new")[3], 2)     # 3 occurrences - 1 first

    def test_payload_conflict_with_multi_row_staging(self):
        stage_event(self.conn, "e1", temperature=30.0, offset=1)
        self.mark()
        # replay same event_id 3x, two of which carry a DIFFERENT temperature
        stage_event(self.conn, "e1", temperature=88.0, offset=10)
        stage_event(self.conn, "e1", temperature=88.0, offset=11)
        stage_event(self.conn, "e1", temperature=30.0, offset=12)
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        # conflict query runs BEFORE staging is cleared (cf. apply_stream_batch)
        conflicts = self.conn.execute(
            pg_sink.SQL_COUNT_PAYLOAD_CONFLICTS).fetchone()[0]
        self.assertEqual(conflicts, 2)
        self.assertEqual(self.row()[3], 3)            # count grew by batch size

    def test_identical_multi_row_replay_no_conflict(self):
        stage_event(self.conn, "e1", temperature=30.0, offset=1)
        self.mark()
        for off in (2, 3):
            stage_event(self.conn, "e1", temperature=30.0, offset=off)
        self.conn.execute(pg_sink.SQL_MARK_PROCESSED)
        conflicts = self.conn.execute(
            pg_sink.SQL_COUNT_PAYLOAD_CONFLICTS).fetchone()[0]
        self.assertEqual(conflicts, 0)
        self.assertEqual(self.row()[3], 2)
        self.conn.execute("DELETE FROM staging_stream_events")


class HourlyAggregateIdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def stage_agg(self, sensor, hour, count, avg, mn, mx):
        self.conn.execute("INSERT INTO staging_hourly_agg VALUES (?,?,?,?,?,?)",
                          (sensor, hour, count, avg, mn, mx))

    def hourly(self, sensor="TEMP_01", hour="2026-09-15T01:00:00Z"):
        return self.conn.execute(
            "SELECT event_count, avg_value, min_value, max_value "
            "FROM gold.sensor_hourly WHERE sensor_id=? AND hour_start=?",
            (sensor, hour)).fetchone()

    def test_same_aggregate_rerun_does_not_accumulate(self):
        self.stage_agg("TEMP_01", "2026-09-15T01:00:00Z", 10, 30.0, 29.0, 31.0)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)  # rerun same window
        row = self.hourly()
        self.assertEqual(row[0], 10)  # NOT 20
        self.assertEqual(row[1], 30.0)

    def test_rerun_with_updated_data_replaces(self):
        # late data fixed by rerunning the hour: count 10 -> 12
        self.stage_agg("TEMP_01", "2026-09-15T01:00:00Z", 10, 30.0, 29.0, 31.0)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)
        self.conn.execute("DELETE FROM staging_hourly_agg")
        self.stage_agg("TEMP_01", "2026-09-15T01:00:00Z", 12, 30.5, 29.0, 33.0)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)
        row = self.hourly()
        self.assertEqual(row[0], 12)
        self.assertEqual(row[1], 30.5)
        self.assertEqual(row[3], 33.0)

    def test_multiple_sensors_hours_independent(self):
        self.stage_agg("TEMP_01", "2026-09-15T01:00:00Z", 10, 30.0, 29.0, 31.0)
        self.stage_agg("TEMP_02", "2026-09-15T01:00:00Z", 3, 21.0, 20.0, 22.0)
        self.stage_agg("TEMP_01", "2026-09-15T02:00:00Z", 7, 31.0, 30.0, 32.0)
        self.conn.execute(pg_sink.SQL_UPSERT_HOURLY)
        total_rows = self.conn.execute("SELECT count(*) FROM gold.sensor_hourly").fetchone()[0]
        self.assertEqual(total_rows, 3)
        self.assertEqual(self.hourly("TEMP_02")[0], 3)
        self.assertEqual(self.hourly(hour="2026-09-15T02:00:00Z")[0], 7)


if __name__ == "__main__":
    unittest.main()
