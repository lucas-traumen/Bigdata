-- Run from the extracted archive root using DuckDB 1.5.6.
-- These read-only queries scan all Parquet and compare the archived Gold CSV.
SET TimeZone='UTC';
CREATE VIEW silver AS SELECT * FROM read_parquet('parquet/silver/*.parquet');
CREATE VIEW bronze AS SELECT * FROM read_parquet('parquet/bronze/*.parquet');
SELECT count(*) AS bronze_rows FROM bronze;
SELECT count(*) AS silver_rows FROM silver;
SELECT count(*) AS quarantine_rows FROM read_parquet('parquet/quarantine/*.parquet');
SELECT kafka_topic, kafka_partition, count(*) AS rows, min(kafka_offset) AS first_offset,
       max(kafka_offset) AS last_offset, count(DISTINCT kafka_offset) AS distinct_offsets
FROM bronze GROUP BY 1,2 ORDER BY 1,2;
SELECT sensor_type, count(*) AS rows, count(DISTINCT sensor_id) AS sensors,
       count(temperature_c) AS temperature_present, count(humidity_pct) AS humidity_present,
       count(co2_ppm) AS co2_present, count(pressure_hpa) AS pressure_present,
       count(pm25_ugm3) AS pm25_present, count(light_lux) AS light_present
FROM silver GROUP BY 1 ORDER BY 1;
CREATE VIEW readings AS
SELECT sensor_id, date_trunc('hour', event_time_utc) AS hour_start, 'temperature_c' AS metric, temperature_c AS value FROM silver WHERE temperature_c IS NOT NULL
UNION ALL
SELECT sensor_id, date_trunc('hour', event_time_utc) AS hour_start, 'humidity_pct' AS metric, humidity_pct AS value FROM silver WHERE humidity_pct IS NOT NULL
UNION ALL
SELECT sensor_id, date_trunc('hour', event_time_utc) AS hour_start, 'co2_ppm' AS metric, co2_ppm AS value FROM silver WHERE co2_ppm IS NOT NULL
UNION ALL
SELECT sensor_id, date_trunc('hour', event_time_utc) AS hour_start, 'pressure_hpa' AS metric, pressure_hpa AS value FROM silver WHERE pressure_hpa IS NOT NULL
UNION ALL
SELECT sensor_id, date_trunc('hour', event_time_utc) AS hour_start, 'pm25_ugm3' AS metric, pm25_ugm3 AS value FROM silver WHERE pm25_ugm3 IS NOT NULL
UNION ALL
SELECT sensor_id, date_trunc('hour', event_time_utc) AS hour_start, 'light_lux' AS metric, light_lux AS value FROM silver WHERE light_lux IS NOT NULL;
SELECT metric, count(*) AS readings, avg(value) AS mean, min(value) AS minimum,
       max(value) AS maximum FROM readings GROUP BY 1 ORDER BY 1;
CREATE VIEW recomputed AS SELECT sensor_id, hour_start, metric, count(*) AS event_count,
 avg(value) AS avg_value, min(value) AS min_value, max(value) AS max_value
 FROM readings GROUP BY 1,2,3;
CREATE VIEW gold_csv AS SELECT sensor_id, hour_start::TIMESTAMP AS hour_start,
 metric, event_count::BIGINT AS event_count, avg_value::DOUBLE AS avg_value,
 min_value::DOUBLE AS min_value, max_value::DOUBLE AS max_value
 FROM read_csv_auto('gold/sensor_hourly.csv');
SELECT count(*) AS joined_groups,
 count(*) FILTER (WHERE a.sensor_id IS NULL OR b.sensor_id IS NULL) AS missing_groups,
 count(*) FILTER (WHERE a.event_count <> b.event_count) AS count_mismatches,
 count(*) FILTER (WHERE a.min_value <> b.min_value OR a.max_value <> b.max_value) AS extrema_mismatches,
 count(*) FILTER (WHERE abs(a.avg_value-b.avg_value)>1e-6) AS mean_mismatches,
 max(abs(a.avg_value-b.avg_value)) AS max_mean_difference
FROM recomputed a FULL OUTER JOIN gold_csv b USING(sensor_id,hour_start,metric);
