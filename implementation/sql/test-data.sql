-- Demo fixture for the `app` database: lets you verify the API and dashboard
-- BEFORE the pipeline produces data. Fully idempotent (ON CONFLICT DO
-- NOTHING). Apply manually:
--   docker compose exec -T postgres psql -U bigdata -d app < sql/test-data.sql

INSERT INTO sensor_latest (sensor_id, event_id, event_time, temperature_c, received_at, updated_at)
VALUES
    ('TEMP_01', 'fixture-000001', '2026-09-15T01:05:00Z', 30.5,  '2026-09-15T01:05:01Z', now()),
    ('TEMP_02', 'fixture-000002', '2026-09-15T01:10:00Z', 36.2,  '2026-09-15T01:10:01Z', now()),
    ('TEMP_03', 'fixture-000003', '2026-09-15T01:15:00Z', 21.75, '2026-09-15T01:15:01Z', now())
ON CONFLICT (sensor_id) DO NOTHING;

INSERT INTO alerts (event_id, rule_id, sensor_id, event_time, temperature_c, threshold, created_at)
VALUES
    ('fixture-000002', 'temperature_high', 'TEMP_02', '2026-09-15T01:10:00Z', 36.2, 35, '2026-09-15T01:10:01Z')
ON CONFLICT (event_id, rule_id) DO NOTHING;

INSERT INTO processed_events (event_id, sensor_id, event_time, temperature_c,
                              kafka_topic, kafka_partition, kafka_offset)
VALUES
    ('fixture-000001', 'TEMP_01', '2026-09-15T01:05:00Z', 30.5,  'sensor_raw', 0, 1),
    ('fixture-000002', 'TEMP_02', '2026-09-15T01:10:00Z', 36.2,  'sensor_raw', 1, 1),
    ('fixture-000003', 'TEMP_03', '2026-09-15T01:15:00Z', 21.75, 'sensor_raw', 2, 1)
ON CONFLICT (event_id) DO NOTHING;

INSERT INTO gold.sensor_hourly (sensor_id, hour_start, event_count, avg_value, min_value, max_value)
VALUES
    ('TEMP_01', '2026-09-15T01:00:00Z', 3, 30.2,  29.8, 30.5),
    ('TEMP_02', '2026-09-15T01:00:00Z', 2, 36.1,  36.0, 36.2),
    ('TEMP_03', '2026-09-15T01:00:00Z', 1, 21.75, 21.75, 21.75)
ON CONFLICT (sensor_id, hour_start) DO NOTHING;
