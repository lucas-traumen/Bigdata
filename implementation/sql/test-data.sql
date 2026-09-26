-- Demo fixture for the `app` database: lets you verify the API and dashboard
-- BEFORE the pipeline produces data. Fully idempotent (ON CONFLICT DO
-- NOTHING). Apply manually:
--   docker compose exec -T postgres psql -U bigdata -d app < sql/test-data.sql
-- Metric columns follow METRIC_CONFIG; station subsets leave absent metrics NULL.

INSERT INTO sensor_latest (sensor_id, event_id, event_time, temperature_c,
                           humidity_pct, co2_ppm, pressure_hpa, pm25_ugm3,
                           light_lux, received_at, updated_at)
VALUES
    ('zoneA_WEATHER_01', 'zoneA-000001', '2026-09-15T01:05:00Z', 30.5,  61.0, NULL, 1012.4, NULL,  30500.0, '2026-09-15T01:05:01Z', now()),
    ('zoneB_AIR_01',     'zoneB-000002', '2026-09-15T01:10:00Z', NULL,  NULL, 1240.0, NULL, 42.5, NULL, '2026-09-15T01:10:01Z', now()),
    ('zoneC_MULTI_01',   'zoneC-000003', '2026-09-15T01:15:00Z', 21.75, 55.2, 604.0, 1001.7, 12.3, 28010.0, '2026-09-15T01:15:01Z', now())
ON CONFLICT (sensor_id) DO NOTHING;

-- One alert per (event, metric) pair; rule_id = metric name, threshold and
-- direction mirror METRIC_CONFIG (temperature_c > 35 high, co2_ppm > 1000 high).
INSERT INTO alerts (event_id, rule_id, sensor_id, event_time, metric,
                    threshold, direction, value, temperature_c, created_at)
VALUES
    ('zoneB-000002', 'co2_ppm', 'zoneB_AIR_01', '2026-09-15T01:10:00Z', 'co2_ppm',
     1000, 'high', 1240.0, NULL, '2026-09-15T01:10:01Z')
ON CONFLICT (event_id, rule_id) DO NOTHING;

INSERT INTO processed_events (event_id, sensor_id, event_time, temperature_c,
                              humidity_pct, co2_ppm, pressure_hpa, pm25_ugm3,
                              light_lux, kafka_topic, kafka_partition, kafka_offset)
VALUES
    ('zoneA-000001', 'zoneA_WEATHER_01', '2026-09-15T01:05:00Z', 30.5,  61.0, NULL, 1012.4, NULL, 30500.0, 'sensor_raw', 0, 1),
    ('zoneB-000002', 'zoneB_AIR_01',     '2026-09-15T01:10:00Z', NULL,  NULL, 1240.0, NULL, 42.5, NULL, 'sensor_raw', 1, 1),
    ('zoneC-000003', 'zoneC_MULTI_01',   '2026-09-15T01:15:00Z', 21.75, 55.2, 604.0, 1001.7, 12.3, 28010.0, 'sensor_raw', 2, 1)
ON CONFLICT (event_id) DO NOTHING;

-- Gold grain is (sensor_id, hour_start, metric): one row per metric per hour.
INSERT INTO gold.sensor_hourly (sensor_id, hour_start, metric, event_count,
                                avg_value, min_value, max_value)
VALUES
    ('zoneA_WEATHER_01', '2026-09-15T01:00:00Z', 'temperature_c', 3, 30.2,  29.8, 30.5),
    ('zoneA_WEATHER_01', '2026-09-15T01:00:00Z', 'humidity_pct',  3, 60.4,  58.9, 61.0),
    ('zoneB_AIR_01',     '2026-09-15T01:00:00Z', 'co2_ppm',       2, 1105.0, 970.0, 1240.0),
    ('zoneC_MULTI_01',   '2026-09-15T01:00:00Z', 'light_lux',     1, 28010.0, 28010.0, 28010.0)
ON CONFLICT (sensor_id, hour_start, metric) DO NOTHING;
