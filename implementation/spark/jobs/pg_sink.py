"""PostgreSQL sink helpers for Q3 (streaming) and the hourly Gold batch.

The SQL statements in this module are the SINGLE SOURCE OF TRUTH for
idempotency. They are executed verbatim by the Spark jobs AND re-executed by
implementation/tests/test_pg_semantics.py against SQLite (which supports the
same ON CONFLICT / row-value syntax) — so the SQL semantics are tested
without PostgreSQL or PySpark.

Guarantees (deliberately scoped — see docs/ARCHITECTURE.md):
  * sensor_latest: a row is overwritten ONLY by a strictly newer event with
    deterministic tie-break (event_time, received_at, event_id) compared
    lexicographically as a row value; all six metric columns travel with the
    winner (station subsets leave absent metrics NULL);
  * alerts: PRIMARY KEY (event_id, rule_id) + ON CONFLICT DO NOTHING =>
    replay/duplicate safe. ONE row per (event, metric-over-threshold) pair,
    computed INSIDE this foreachBatch from the METRIC_CONFIG table in
    common.py (rule_id = metric name; no separate rules table, no separate
    alert step — user decision 2026-09-18);
  * processed_events: replay marker; duplicates bump duplicate_count and the
    payload-conflict query detects re-sends whose metric VECTOR differs;
  * gold.sensor_hourly: full-replace upsert per (sensor, hour, metric) =>
    rerunning a window never accumulates (no double counting).

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

from common import METRIC_NAMES, alert_metrics, alert_rule


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
        "batchsize": os.environ.get("JDBC_BATCH_SIZE", "1000"),
        "isolationLevel": "READ_COMMITTED",
    }


# ---------------------------------------------------------------------------
# SQL constants (SQLite-compatible syntax so the unit tests can run them verbatim).
# Column lists are derived from METRIC_NAMES so the statements follow the
# metric registry in common.py by construction.
# ---------------------------------------------------------------------------

_METRIC_LIST = ", ".join(METRIC_NAMES)


def _any_metric_non_null(prefix: str = "s.") -> str:
    """SQL predicate: at least one metric column is non-NULL on ``prefix``."""
    return " OR ".join(f"{prefix}{m} IS NOT NULL" for m in METRIC_NAMES)


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
# The winner must carry at least one metric (an identity-only row must not
# NULL-out a richer row); station subsets keep their absent metrics NULL.
SQL_UPSERT_LATEST = f"""
INSERT INTO sensor_latest
    (sensor_id, event_id, event_time, {_METRIC_LIST}, received_at, updated_at)
SELECT winner.sensor_id, winner.event_id, winner.event_time,
       {', '.join('winner.' + m for m in METRIC_NAMES)},
       winner.received_at, CURRENT_TIMESTAMP
FROM (
    SELECT s.sensor_id, s.event_id, s.event_time,
           {', '.join('s.' + m for m in METRIC_NAMES)}, s.received_at,
           ROW_NUMBER() OVER (
               PARTITION BY s.sensor_id
               ORDER BY s.event_time DESC NULLS LAST, s.received_at DESC NULLS LAST,
                        s.event_id DESC NULLS LAST
           ) AS rn
    FROM staging_stream_events s
    WHERE s.sensor_id IS NOT NULL AND s.event_id IS NOT NULL
      AND s.event_time IS NOT NULL
      AND ({_any_metric_non_null()})
) AS winner
WHERE winner.rn = 1
ON CONFLICT (sensor_id) DO UPDATE SET
    event_id      = EXCLUDED.event_id,
    event_time    = EXCLUDED.event_time,
    {', '.join(f'{m} = EXCLUDED.{m}' for m in METRIC_NAMES)},
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
SQL_MARK_PROCESSED = f"""
INSERT INTO processed_events
    (event_id, sensor_id, event_time, {_METRIC_LIST}, kafka_topic,
     kafka_partition, kafka_offset, first_processed_at, duplicate_count)
SELECT ranked.event_id, ranked.sensor_id, ranked.event_time,
       {', '.join('ranked.' + m for m in METRIC_NAMES)},
       ranked.kafka_topic, ranked.kafka_partition, ranked.kafka_offset,
       CURRENT_TIMESTAMP, ranked.in_batch_duplicates
FROM (
    SELECT s.event_id, s.sensor_id, s.event_time,
           {', '.join('s.' + m for m in METRIC_NAMES)},
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

# Payload conflict = same event_id replayed with a DIFFERENT metric VECTOR
# (any metric differs, NULL-safe via IS DISTINCT FROM on both engines).
SQL_COUNT_PAYLOAD_CONFLICTS = f"""
SELECT count(*) AS payload_conflicts
FROM processed_events p
JOIN staging_stream_events s ON s.event_id = p.event_id
WHERE {' OR '.join(f'(s.{m} IS DISTINCT FROM p.{m})' for m in METRIC_NAMES)}
"""

SQL_UPSERT_HOURLY = """
INSERT INTO gold.sensor_hourly
    (sensor_id, hour_start, metric, event_count, avg_value, min_value, max_value,
     computed_at)
SELECT s.sensor_id, s.hour_start, s.metric, s.event_count, s.avg_value, s.min_value,
       s.max_value, CURRENT_TIMESTAMP
FROM staging_hourly_agg s
WHERE s.sensor_id IS NOT NULL AND s.hour_start IS NOT NULL AND s.metric IS NOT NULL
ON CONFLICT (sensor_id, hour_start, metric) DO UPDATE SET
    event_count = EXCLUDED.event_count,
    avg_value   = EXCLUDED.avg_value,
    min_value   = EXCLUDED.min_value,
    max_value   = EXCLUDED.max_value,
    computed_at = CURRENT_TIMESTAMP
"""

# One alert rule per alertable metric of METRIC_CONFIG (rule_id = metric name,
# user decision 2026-09-18). The threshold/direction are numeric/string config
# constants (not user input), so they are interpolated as plain literals —
# this keeps the statement strings usable by both psycopg2 and sqlite3.
# ``value`` is the offending metric reading; ``temperature_c`` is kept only as
# a legacy/compat column (NULL for non-temperature alerts).
SQL_INSERT_ALERTS_TEMPLATE = """
INSERT INTO alerts
    (event_id, rule_id, sensor_id, event_time, metric, threshold, direction,
     value, temperature_c, created_at)
SELECT s.event_id, '{metric}', s.sensor_id, s.event_time,
       '{metric}', {threshold}, '{direction}', s.{metric}, s.temperature_c,
       CURRENT_TIMESTAMP
FROM staging_stream_events s
WHERE s.{metric} IS NOT NULL AND s.{metric} {op} {threshold}
ON CONFLICT (event_id, rule_id) DO NOTHING
"""


def sql_insert_alerts():
    """Render the alert INSERT statements, one per alertable metric.

    Generated from METRIC_CONFIG (common.py): direction "high" fires on
    value > threshold, "low" on value < threshold; metrics without a
    threshold produce no statement. Statements are idempotent per
    (event_id, rule_id) via ON CONFLICT DO NOTHING.
    """
    statements = []
    for name in alert_metrics():
        threshold, direction = alert_rule(name)
        op = ">" if direction == "high" else "<"
        statements.append(SQL_INSERT_ALERTS_TEMPLATE.format(
            metric=name, threshold=float(threshold), direction=direction, op=op))
    return tuple(statements)


# ---------------------------------------------------------------------------
# foreachBatch / batch entry points (used from the Spark driver side).
# ---------------------------------------------------------------------------

def apply_stream_batch(df, batch_id):  # noqa: ANN001
    """Q3 foreachBatch body: bounded micro-batch -> staging -> idempotent SQL.

    ``df`` is the Silver micro-batch (silver_schema()). No collect()/toPandas():
    the batch rows go to PostgreSQL through the JDBC writer (executor side);
    only scalar counts cross back to the driver. Alerts for all metrics of
    METRIC_CONFIG are computed HERE, inside the same transaction as the
    sensor_latest upsert.
    """
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
        # received_at); the metric columns are splatted from METRIC_NAMES
        # (same pattern as silver_projection in stream_app.py), so adding a
        # metric to the registry in common.py automatically extends the
        # staging write. The static JDBC guard test
        # (implementation/tests/test_jdbc_columns.py) resolves *METRIC_NAMES
        # to the registry keys and still pins every output column against
        # the DDL (a registry/DDL drift fails the guard loudly).
        (df.select(
            "event_id", "sensor_id",
            df["event_time_utc"].alias("event_time"),
            *METRIC_NAMES,
            df["received_at_utc"].alias("received_at"),
            "kafka_topic", "kafka_partition", "kafka_offset",
         )
         .write.format("jdbc")
         .options(**jdbc_options(), dbtable="staging_stream_events")
         .mode("append")
         .save())

        cur.execute(SQL_UPSERT_LATEST)
        for statement in sql_insert_alerts():
            cur.execute(statement)
        cur.execute(SQL_MARK_PROCESSED)
        cur.execute(SQL_COUNT_PAYLOAD_CONFLICTS)
        conflicts = cur.fetchone()[0]
        cur.execute("DELETE FROM staging_stream_events")
        conn.commit()  # single atomic transaction for all upserts
        if conflicts:
            # payload conflict: same event_id replayed with a different metric
            print(f"[pg_sink] stream batch_id={batch_id}: {conflicts} payload "
                  "conflict(s) detected (same event_id, different metric vector)",
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

    ``agg_df`` carries the metric dimension: (sensor_id, hour_start, metric,
    event_count, avg_value, min_value, max_value). Returns the (rows,
    event_count) summary for the window from PostgreSQL.
    """
    conn = connect_pg()
    try:
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE staging_hourly_agg")
        conn.commit()

        (agg_df.select("sensor_id", "hour_start", "metric",
                       "event_count", "avg_value", "min_value", "max_value")
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
              f"{rows} sensor-hour-metric rows, {events} event readings", flush=True)
        return rows, events
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
