"""PostgreSQL sink helpers for Q3 (streaming) and the hourly Gold batch.

The SQL statements in this module are the SINGLE SOURCE OF TRUTH for
idempotency. They are executed verbatim by the Spark jobs AND re-executed by
implementation/tests/test_pg_semantics.py against SQLite (which supports the
same ON CONFLICT / row-value syntax) — so the SQL semantics are tested
without PostgreSQL or PySpark.

Guarantees (deliberately scoped — see docs/ARCHITECTURE.md):
  * sensor_latest: a row is overwritten ONLY by a strictly newer event with
    deterministic tie-break (event_time, received_at, event_id) compared
    lexicographically as a row value;
  * alerts: PRIMARY KEY (event_id, rule_id) + ON CONFLICT DO NOTHING =>
    replay/duplicate safe;
  * processed_events: replay marker; duplicates bump duplicate_count and the
    payload-conflict query detects re-sends with a different temperature;
  * gold.sensor_hourly: full-replace upsert => rerunning a window never
    accumulates (no double counting).

Key-uniqueness note (reviewer B1): PostgreSQL raises "ON CONFLICT DO UPDATE
command cannot affect row a second time" when the statement INPUT contains
more than one row per conflict key — which happens as soon as one micro-batch
carries several events for the same sensor or a replayed event_id (duplicate
fault injection). SQL_UPSERT_LATEST and SQL_MARK_PROCESSED therefore reduce
their input to exactly one row per key with a window function BEFORE the
INSERT ... ON CONFLICT runs. (ON CONFLICT DO NOTHING, as used by alerts, does
not have this restriction; staging_hourly_agg is already key-unique by
construction because it comes from a groupBy.)

Atomicity note: the JDBC write into a staging table and the transactional
SQL upserts are two steps; a crash in between leaves staging rows that the
next batch truncates. Parquet and PostgreSQL are NOT atomic across sinks —
reconciliation relies on the idempotent upserts above. No exactly-once claim
across the whole pipeline.
"""

import os
import time


def _pg_settings():
    return {
        "host": os.environ.get("PGHOST", "postgres"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "app"),
        "user": os.environ.get("PGUSER", "bigdata"),
        "password": os.environ.get("PGPASSWORD", ""),
    }


def connect_pg(max_tries=30, delay_s=2.0):
    """Open a psycopg2 connection with retry (the DB may still be starting)."""
    import psycopg2  # lazy import: unit tests import this module without it

    cfg = _pg_settings()
    last_exc = None
    for attempt in range(1, max_tries + 1):
        try:
            return psycopg2.connect(**cfg)
        except Exception as exc:  # noqa: BLE001 - retry any connection error
            last_exc = exc
            print(f"[pg_sink] connect attempt {attempt}/{max_tries} failed: {exc}",
                  flush=True)
            time.sleep(delay_s)
    raise RuntimeError(f"PostgreSQL unreachable after {max_tries} attempts: {last_exc}")


def jdbc_url():
    s = _pg_settings()
    return f"jdbc:postgresql://{s['host']}:{s['port']}/{s['dbname']}"


def jdbc_options():
    """Spark JDBC write options (PostgreSQL driver is baked into the image)."""
    s = _pg_settings()
    return {
        "url": jdbc_url(),
        "user": s["user"],
        "password": s["password"],
        "driver": "org.postgresql.Driver",
        "batchsize": "1000",
        "isolationLevel": "READ_COMMITTED",
    }


# ---------------------------------------------------------------------------
# SQL constants (SQLite-compatible syntax so the unit tests can run them verbatim).
# ---------------------------------------------------------------------------

# Input reduction: staging may hold several events per sensor in one batch, so
# the derived table keeps only ONE deterministic winner per sensor_id
# (greatest (event_time, received_at, event_id) row value) — otherwise
# PostgreSQL would abort the statement with "ON CONFLICT DO UPDATE command
# cannot affect row a second time". NULLS LAST is pinned explicitly because
# PostgreSQL defaults DESC to NULLS FIRST while SQLite sorts NULLs last: a
# NULL received_at must never outrank a known value on either engine (mirrors
# batch.py's desc_nulls_last() tie-break). event_time/event_id are already
# NOT NULL-filtered below; the explicit modifier keeps the rule self-contained.
# The strict-newer guard against the EXISTING row is unchanged (a winner that
# is not strictly newer than what is already stored must not overwrite it).
SQL_UPSERT_LATEST = """
INSERT INTO sensor_latest
    (sensor_id, event_id, event_time, temperature_c, received_at, updated_at)
SELECT winner.sensor_id, winner.event_id, winner.event_time, winner.temperature_c,
       winner.received_at, CURRENT_TIMESTAMP
FROM (
    SELECT s.sensor_id, s.event_id, s.event_time, s.temperature_c, s.received_at,
           ROW_NUMBER() OVER (
               PARTITION BY s.sensor_id
               ORDER BY s.event_time DESC NULLS LAST, s.received_at DESC NULLS LAST,
                        s.event_id DESC NULLS LAST
           ) AS rn
    FROM staging_stream_events s
    WHERE s.sensor_id IS NOT NULL AND s.event_id IS NOT NULL
      AND s.event_time IS NOT NULL AND s.temperature_c IS NOT NULL
) AS winner
WHERE winner.rn = 1
ON CONFLICT (sensor_id) DO UPDATE SET
    event_id      = EXCLUDED.event_id,
    event_time    = EXCLUDED.event_time,
    temperature_c = EXCLUDED.temperature_c,
    received_at   = EXCLUDED.received_at,
    updated_at    = CURRENT_TIMESTAMP
WHERE (EXCLUDED.event_time, EXCLUDED.received_at, EXCLUDED.event_id)
      > (sensor_latest.event_time, sensor_latest.received_at, sensor_latest.event_id)
"""

# Input reduction: one row per event_id (the "first occurrence" = lowest Kafka
# position, NULL provenance last so PostgreSQL and SQLite order NULLs the same
# way). Semantics per statement:
#   * event never processed  -> insert that first occurrence with
#     duplicate_count = in_batch_duplicates (n - 1 remaining occurrences);
#   * event already present  -> DO UPDATE adds EXCLUDED.duplicate_count + 1
#     = (n - 1) + 1 = n, i.e. every occurrence of this batch counts as one
#     duplicate of the previously stored event.
SQL_MARK_PROCESSED = """
INSERT INTO processed_events
    (event_id, sensor_id, event_time, temperature_c, kafka_topic,
     kafka_partition, kafka_offset, first_processed_at, duplicate_count)
SELECT ranked.event_id, ranked.sensor_id, ranked.event_time, ranked.temperature_c,
       ranked.kafka_topic, ranked.kafka_partition, ranked.kafka_offset,
       CURRENT_TIMESTAMP, ranked.in_batch_duplicates
FROM (
    SELECT s.event_id, s.sensor_id, s.event_time, s.temperature_c,
           s.kafka_topic, s.kafka_partition, s.kafka_offset,
           ROW_NUMBER() OVER (
               PARTITION BY s.event_id
               ORDER BY s.kafka_partition ASC NULLS LAST,
                        s.kafka_offset ASC NULLS LAST
           ) AS rn,
           COUNT(*) OVER (PARTITION BY s.event_id) - 1 AS in_batch_duplicates
    FROM staging_stream_events s
    WHERE s.event_id IS NOT NULL
) AS ranked
WHERE ranked.rn = 1
ON CONFLICT (event_id) DO UPDATE SET
    duplicate_count   = processed_events.duplicate_count
                      + EXCLUDED.duplicate_count + 1,
    last_duplicate_at = CURRENT_TIMESTAMP
"""

SQL_COUNT_PAYLOAD_CONFLICTS = """
SELECT count(*) AS payload_conflicts
FROM processed_events p
JOIN staging_stream_events s ON s.event_id = p.event_id
WHERE s.temperature_c IS NOT NULL AND p.temperature_c IS NOT NULL
  AND s.temperature_c <> p.temperature_c
"""

SQL_UPSERT_HOURLY = """
INSERT INTO gold.sensor_hourly
    (sensor_id, hour_start, event_count, avg_value, min_value, max_value, computed_at)
SELECT s.sensor_id, s.hour_start, s.event_count, s.avg_value, s.min_value,
       s.max_value, CURRENT_TIMESTAMP
FROM staging_hourly_agg s
WHERE s.sensor_id IS NOT NULL AND s.hour_start IS NOT NULL
ON CONFLICT (sensor_id, hour_start) DO UPDATE SET
    event_count = EXCLUDED.event_count,
    avg_value   = EXCLUDED.avg_value,
    min_value   = EXCLUDED.min_value,
    max_value   = EXCLUDED.max_value,
    computed_at = CURRENT_TIMESTAMP
"""

# The alert threshold is a numeric config constant (not user input), so it is
# interpolated as a plain float literal — this keeps the statement string
# usable by both psycopg2 and sqlite3.
SQL_INSERT_ALERTS_TEMPLATE = """
INSERT INTO alerts
    (event_id, rule_id, sensor_id, event_time, temperature_c, threshold, created_at)
SELECT s.event_id, 'temperature_high', s.sensor_id, s.event_time,
       s.temperature_c, {threshold}, CURRENT_TIMESTAMP
FROM staging_stream_events s
WHERE s.temperature_c IS NOT NULL AND s.temperature_c > {threshold}
ON CONFLICT (event_id, rule_id) DO NOTHING
"""


def sql_insert_alerts(threshold: float) -> str:
    """Render the alert insert for a concrete numeric threshold."""
    return SQL_INSERT_ALERTS_TEMPLATE.format(threshold=float(threshold))


# ---------------------------------------------------------------------------
# foreachBatch / batch entry points (used from the Spark driver side).
# ---------------------------------------------------------------------------

def apply_stream_batch(df, batch_id, threshold=None):  # noqa: ANN001
    """Q3 foreachBatch body: bounded micro-batch -> staging -> idempotent SQL.

    ``df`` is the Silver micro-batch (SILVER_SCHEMA). No collect()/toPandas():
    the batch rows go to PostgreSQL through the JDBC writer (executor side);
    only scalar counts cross back to the driver.
    """
    import validation

    threshold = validation.ALERT_THRESHOLD_C if threshold is None else float(threshold)
    print(f"[pg_sink] stream batch_id={batch_id}: starting", flush=True)

    conn = connect_pg()
    try:
        cur = conn.cursor()
        # fresh staging area for this micro-batch
        cur.execute("TRUNCATE TABLE staging_stream_events")
        conn.commit()

        # JDBC maps columns BY NAME: the select output must carry exactly the
        # column names declared for staging_stream_events in
        # implementation/sql/schema.sql — Silver's *_utc names are aliased
        # accordingly (event_time_utc -> event_time, received_at_utc ->
        # received_at). Guard test: implementation/tests/test_jdbc_columns.py.
        (df.select(
            "event_id", "sensor_id",
            df["event_time_utc"].alias("event_time"),
            "temperature_c",
            df["received_at_utc"].alias("received_at"),
            "kafka_topic", "kafka_partition", "kafka_offset",
         )
         .write.format("jdbc")
         .options(**jdbc_options(), dbtable="staging_stream_events")
         .mode("append")
         .save())

        cur.execute(SQL_UPSERT_LATEST)
        cur.execute(sql_insert_alerts(threshold))
        cur.execute(SQL_MARK_PROCESSED)
        cur.execute(SQL_COUNT_PAYLOAD_CONFLICTS)
        conflicts = cur.fetchone()[0]
        cur.execute("DELETE FROM staging_stream_events")
        conn.commit()  # single atomic transaction for all upserts
        if conflicts:
            # payload conflict: same event_id replayed with a different value
            print(f"[pg_sink] stream batch_id={batch_id}: {conflicts} payload "
                  "conflict(s) detected (same event_id, different temperature)",
                  flush=True)
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print(f"[pg_sink] stream batch_id={batch_id}: committed", flush=True)


def apply_hourly_batch(agg_df, window_start, window_end):  # noqa: ANN001
    """Batch path: hourly aggregate DataFrame -> staging -> full-replace upsert.

    Returns the (rows, event_count) summary for the window from PostgreSQL.
    """
    conn = connect_pg()
    try:
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE staging_hourly_agg")
        conn.commit()

        (agg_df.select("sensor_id", "hour_start", "event_count",
                       "avg_value", "min_value", "max_value")
         .write.format("jdbc")
         .options(**jdbc_options(), dbtable="staging_hourly_agg")
         .mode("append")
         .save())

        cur.execute(SQL_UPSERT_HOURLY)
        cur.execute("DELETE FROM staging_hourly_agg")
        conn.commit()

        cur.execute(
            "SELECT count(*), coalesce(sum(event_count), 0) FROM gold.sensor_hourly "
            "WHERE hour_start >= %s AND hour_start < %s",
            (window_start, window_end),
        )
        rows, events = cur.fetchone()
        cur.close()
        print(f"[pg_sink] hourly window [{window_start}, {window_end}): "
              f"{rows} sensor-hour rows, {events} events", flush=True)
        return rows, events
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
