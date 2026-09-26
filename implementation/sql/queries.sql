-- Operational queries for the `app` database (read-only, run manually via:
--   docker compose exec postgres psql -U bigdata -d app -f - <<'SQL' ...
-- or copy-paste into psql). Timestamps are UTC.

-- \set data_rate: latest values per sensor (metric vector; absent metrics NULL)
SELECT sensor_id, event_id, event_time,
       temperature_c, humidity_pct, co2_ppm, pressure_hpa, pm25_ugm3, light_lux,
       received_at, updated_at
FROM sensor_latest
ORDER BY sensor_id;

-- recent alerts (newest first; rule_id = metric name from METRIC_CONFIG)
SELECT event_id, rule_id, metric, value, threshold, direction, sensor_id,
       event_time, created_at
FROM alerts
ORDER BY created_at DESC
LIMIT 50;

-- Gold hourly stats per metric for a UTC window
SELECT sensor_id, hour_start, metric, event_count,
       round(avg_value::numeric, 3) AS avg_value,
       round(min_value::numeric, 3) AS min_value,
       round(max_value::numeric, 3) AS max_value,
       computed_at
FROM gold.sensor_hourly
WHERE hour_start >= '2026-09-15T00:00:00Z' AND hour_start < '2026-09-16T00:00:00Z'
ORDER BY hour_start, sensor_id, metric;

-- replay / duplicate statistics (Q3 replay evidence)
SELECT count(*) AS total_events,
       count(*) FILTER (WHERE duplicate_count > 0) AS replayed_events,
       max(duplicate_count) AS max_replays
FROM processed_events;

-- per-event replay detail
SELECT event_id, duplicate_count, first_processed_at, last_duplicate_at
FROM processed_events
WHERE duplicate_count > 0
ORDER BY last_duplicate_at DESC
LIMIT 50;

-- overall pipeline summary
SELECT
    (SELECT count(*) FROM sensor_latest)      AS sensors,
    (SELECT count(*) FROM processed_events)   AS events_processed,
    (SELECT count(*) FROM alerts)             AS alerts,
    (SELECT count(*) FROM gold.sensor_hourly) AS sensor_hour_metric_rows;
