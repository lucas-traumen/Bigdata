-- App database schema: serving tables + Gold + staging (idempotency contract).
-- Mounted into /docker-entrypoint-initdb.d on first init of the `app` DB.
-- Semantics are enforced by the SQL in implementation/spark/jobs/pg_sink.py
-- and documented in implementation/docs/ARCHITECTURE.md.
-- All timestamps are timestamptz (UTC instants).

CREATE TABLE IF NOT EXISTS sensor_latest (
    sensor_id      text PRIMARY KEY,
    event_id       text NOT NULL,
    event_time     timestamptz NOT NULL,
    temperature_c  double precision NOT NULL,
    received_at    timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Replay-safe alert log: PRIMARY KEY (event_id, rule_id) + ON CONFLICT
-- DO NOTHING in pg_sink.py means retries/replays never duplicate rows.
CREATE TABLE IF NOT EXISTS alerts (
    event_id       text NOT NULL,
    rule_id        text NOT NULL,
    sensor_id      text NOT NULL,
    event_time     timestamptz NOT NULL,
    temperature_c  double precision NOT NULL,
    threshold      double precision NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, rule_id)
);
CREATE INDEX IF NOT EXISTS alerts_created_at_idx ON alerts (created_at DESC);

-- Internal replay marker for Q3: one row per event_id ever applied to the
-- serving tables. Replays bump duplicate_count; the payload-conflict query in
-- pg_sink.py detects re-sends with a different temperature.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id            text PRIMARY KEY,
    sensor_id           text,
    event_time          timestamptz,
    temperature_c       double precision,
    kafka_topic         text,
    kafka_partition     integer,
    kafka_offset        bigint,
    first_processed_at  timestamptz NOT NULL DEFAULT now(),
    last_duplicate_at   timestamptz,
    duplicate_count     bigint NOT NULL DEFAULT 0
);

-- Gold: hourly aggregates in UTC, fully replaced on every batch rerun
-- (never accumulated). hour_start is the UTC hour boundary of event_time.
CREATE SCHEMA IF NOT EXISTS gold;
CREATE TABLE IF NOT EXISTS gold.sensor_hourly (
    sensor_id     text NOT NULL,
    hour_start    timestamptz NOT NULL,
    event_count   bigint NOT NULL,
    avg_value     double precision NOT NULL,
    min_value     double precision NOT NULL,
    max_value     double precision NOT NULL,
    computed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (sensor_id, hour_start)
);
CREATE INDEX IF NOT EXISTS sensor_hourly_hour_idx ON gold.sensor_hourly (hour_start);

-- Bounded micro-batch landing tables (truncated at the start of every batch;
-- see pg_sink.py). Only one writer at a time: the streaming app XOR the
-- hourly batch XOR the Airflow-triggered batch.
CREATE TABLE IF NOT EXISTS staging_stream_events (
    event_id        text,
    sensor_id       text,
    event_time      timestamptz,
    temperature_c   double precision,
    received_at     timestamptz,
    kafka_topic     text,
    kafka_partition integer,
    kafka_offset    bigint
);

CREATE TABLE IF NOT EXISTS staging_hourly_agg (
    sensor_id    text,
    hour_start   timestamptz,
    event_count  bigint,
    avg_value    double precision,
    min_value    double precision,
    max_value    double precision
);
