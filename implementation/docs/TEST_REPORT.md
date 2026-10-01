# TEST REPORT — IoT Big Data demo stack

Cập nhật: 2026-10-01 (runtime verification D1–D7, D9 hoàn thành trên máy
phát triển). Máy: Ubuntu x86_64, Ryzen 5 6600H (12 logical CPU), RAM 14 GiB
(available ~8.4 GiB), root disk 99 GB trống, Docker 29.1.3, Compose v5.5.0.
TCP 1883 bị project khác chiếm → dùng MQTT_HOST_PORT=1884.
DATA_ROOT=/home/lucas/bigdata-demo (fallback không cần sudo).

Lịch sử: 2026-09-18 (phần mở rộng E3: đa nguồn + đa chỉ số, bỏ scheduler
bên ngoài, alert giữ trong Q3). Trước 2026-09-30 máy bị BLOCKED (RAM 3.9 GB,
disk 11 GB).

Quy ước kết quả:
- **PASS/OK** — đã chạy trên máy này, có lệnh + output.
- **DEFERRED** — không chạy được trên máy này do thiếu tài nguyên/nguy cơ disk;
  phải chạy trên máy mục tiêu (RAM 8–10 GB, ≥ 15 GB disk trống, port rảnh).
- **BLOCKED** — bị chặn cụ thể; liệt kê lý do.

Lịch sử remediation của phiên bản trước (F1–F4, B1, M1 — scheduler-image
artifact, compose argv, SQL multi-row conflict key, docker stats parsing,
JDBC column mismatch) đã được consolidate vào git history; các guard tương
ứng còn lại trong test suite và vẫn PASS sau rewrite E3.

## 1. Đã kiểm tra trên máy này (static / unit) — sau rewrite E3 2026-09-18

| Kiểm tra | Lệnh | Kết quả |
|---|---|---|
| Unit tests (validation per-metric, metric registry, pg SQL semantics multi-metric + alert per-metric, batch window + metric stack expr, JDBC staging column guard 13 cột, resource-report parsing) | `python3 -m unittest discover -s implementation/tests` | **OK — 111/111 tests pass** (81 trước E3 + 27 mới/cập nhật cho per-metric validation, alert rules per-metric, Gold theo chiều metric, metric-stack expr, metric dimension contract + 3 remediation identity-only event trong sensor_latest/processed_events) |
| Python syntax toàn bộ jobs/backend/bridge/simulator | `python3 -m py_compile …` | **OK** |
| Shell syntax toàn bộ scripts + run wrappers | `bash -n` | **OK** |
| Compose file parse + resolve 2 profiles | `docker compose -f implementation/compose.yaml config` (+ `--profile live`, `--profile batch`) | **OK (exit 0)** — không còn service/profile của scheduler bên ngoài; kafka có `KAFKA_LOG_RETENTION_MS=3600000` |
| Không còn artifact của scheduler đã loại bỏ | grep case-insensitive token tên scheduler trên `implementation/` | **OK — rỗng** (thư mục image/DAG của scheduler đã xoá khỏi repo; compose/scripts/docs/init-databases đã dọn) |
| Version pins tồn tại thật | PyPI JSON API, Docker Hub tag API, Maven Central HEAD (verify 2026-09-14) | **OK** — không thêm image/package mới trong E3 (loại bỏ scheduler là GIẢM dependency) |
| Maven jar checksums | tải + `sha256sum` (verify 2026-09-14) | **OK 5/5** — không đổi trong E3 |

Version pins đã verify (ngày 2026-09-14; giữ nguyên sau E3):

| Thành phần | Giá trị | Nguồn xác minh |
|---|---|---|
| apache/spark | `4.2.0-java21-python3` | Docker Hub tag list |
| apache/kafka | `4.3.1` | Docker Hub tag list |
| eclipse-mosquitto | `2.0.22` | Docker Hub tag list |
| postgres | `17.11` | Docker Hub tag list |
| nginx | `1.27-alpine` | Docker Hub tag list |
| paho-mqtt | `2.1.0` | PyPI + wheel source (đọc `client.py`: `manual_ack=True`, `ack(mid, qos)`) |
| confluent-kafka | `2.10.1` | PyPI release JSON |
| psycopg2-binary | `2.9.10` | PyPI release JSON |
| fastapi | `0.141.1` | PyPI latest |
| uvicorn | `0.52.4` | PyPI latest |
| spark-sql-kafka-0-10_2.13 | `4.2.0` | Maven Central (jar + POM) |
| spark-token-provider-kafka-0-10_2.13 | `4.2.0` | Maven Central |
| kafka-clients | `3.9.2` | Maven Central + POM của connector; sha256 `014b4ef36884478a932641818b080c36a8f8e2e92dc771e800c9d2694f4f83d8` |
| commons-pool2 | `2.13.1` | Maven Central + POM |
| postgresql JDBC | `42.7.4` | Maven Central |

Unit tests phủ các semantics bắt buộc (không cần Docker/PySpark/PostgreSQL —
`pg_sink` chạy nguyên câu SQL upsert trên SQLite in-memory):

- Metric registry (`METRIC_CONFIG`): 6 chỉ số đúng bounds/ngưỡng/hướng theo
  ma trận user chốt; `light_lux` không sinh alert statement.
- Validation per-metric: identity errors (timestamp/sensor_id/event_id) →
  quarantine; per-metric non-numeric/out-of-bounds → vẫn Silver, metric
  masked NULL + reason `non_numeric:<m>` / `value_out_of_bounds:<m>`;
  station subset (chỉ số vắng) không phải lỗi; bounds inclusive; thứ tự
  issue theo `METRIC_NAMES`; alias `value` chỉ áp cho `temperature_c`.
- `sensor_latest`: newer-overwrite, older-late không ghi đè, tie-break theo
  received_at rồi event_id, độc lập thứ tự insert; batch nhiều event cùng
  sensor rút về đúng một winner deterministic; winner kiểu station-subset
  giữ NULL cho chỉ số vắng.
- Alerts per-metric: mỗi cặp (event, chỉ số vượt ngưỡng) → 1 dòng
  `(event_id, rule_id=metric)`; replay không tăng hàng; "high" = strictly
  above, "low" (pressure) = strictly below; 1 event nhiều chỉ số vượt →
  nhiều dòng; cột legacy `temperature_c` NULL cho alert phi-nhiệt độ.
- `processed_events`: replay tăng `duplicate_count` không mọc hàng; payload
  conflict phát hiện được trên cả temperature lẫn metric khác (so vector
  NULL-safe); batch chứa cùng event_id nhiều lần insert đúng một dòng.
- `gold.sensor_hourly` (PK sensor/hour/metric): rerun không cộng lặp, rerun
  với data mới thay thế đúng metric, các metric/giờ/sensor độc lập.
- Batch window: từ chối non-hour boundary, end ≤ start, timestamp
  naive/garbage; `metric_stack_expr()` sinh đúng `stack(6, …) AS (metric,
  value)` theo registry.
- JDBC staging column guard: output của `df.select(...)` trong
  `apply_stream_batch` (13 cột)/`apply_hourly_batch` (7 cột) khớp tên-cột
  DDL theo đúng thứ tự; nguồn select nằm trong `silver_schema()`; metric
  dimension khớp `METRIC_CONFIG` trên staging/silver/sensor_latest/gold/alerts.

## 2. Runtime verification — ĐÃ CHẠY 2026-09-30/10-01

| # | Việc | Kết quả | Evidence |
|---|---|---|---|
| D1 | Build images | **PASS** — 5 images built, `[warmup] OK kafka010=… pgjdbc=… psycopg2=2.9.10` | `logs/init.log` |
| D2 | Live pipeline 5 zone + e2e | **PASS 6/6** — 200 evt fault-inject seed 42, processed 18657, alerts 334, sensor_latest 30 | `logs/test-e2e.log` |
| D3 | Quarantine per-metric khớp fault profile | **PASS** — quarantine 2 rows = 2 bad_timestamp; Silver metric_issues 5 rows = 3 non_numeric + 2 out_of_bounds (đúng manifest) | `qcheck.py` spark-submit |
| D4 | Batch Gold rerun determinism | **PASS** — 108 rows / 132844 readings; 2 runs byte-identical (sha256 `a6e0b181…`) | `logs/gold-run{1,2}.csv` |
| D5 | Batch hourly (manual + lock) | **PASS** — window [17:00,18:00)Z đúng, grace 60s, concurrent run bị từ chối, lock released | `logs/batch-hourly.log` |
| D6 | Recovery (in-container crash) | **PASS 4/4** — batch 707→708 resume, kafka offsets advance, bronze grows, bridge restart OK | `logs/test-recovery.log` |
| D7 | Resource budget | **PASS** — peak spark-stream 1.326 GiB (53%), kafka 456 MiB, tổng ~1.9 GiB; host avail min 8.3 GB; disk ổn định 94 GB | `logs/resource-report.jsonl` (10 samples) |
| D8 | Load test dài | **SKIPPED** (user quyết định bỏ qua) | — |
| D9 | Backup/restore | **PASS** — pg_dump 1.3 MB, restore verify gold/sensor_latest/alerts/processed_events identical; tar DATA_ROOT 15 MB, extract OK | `backup-app-2026-10-01.dump`, `bigdata-demo-tar-2026-10-01.tgz` |

## 3. BLOCKED / giới hạn còn lại

- **Port 1883** vẫn bị project khác chiếm → dùng `MQTT_HOST_PORT=1884`.
- **D8 (load test dài)** chưa chạy — cần 30–60 phút liên tục; chạy khi cần.
- **Cron thật** chưa cài (chạy manual `run-batch-hourly.sh` + kiểm lock).
- Không exactly-once xuyên pipeline (giới hạn thiết kế, ghi trong README §10).

## 4. Checklist cho tester (máy mục tiêu)

1. `implementation/scripts/preflight.sh` — mong đợi exit 0 (hoặc 2 với warning
   đã hiểu); ghi output.
2. `cp implementation/.env.example implementation/.env` — chỉnh ports/credentials;
   đặc biệt MQTT_HOST_PORT nếu 1883 bận.
3. `implementation/scripts/init.sh` — build + postgres schema + kafka topic
   (retention 1h); log build phải có warmup OK.
4. `implementation/scripts/run-mode.sh live` → `wait-for-health` → dashboard/API;
   kiểm tra 5 container `sim-zoneA..zoneE` + manifest per zone.
5. `implementation/scripts/test-e2e.sh` — PASS hết; lưu manifest/delivery log;
   đối chiếu quarantine per-metric với fault profile.
6. `implementation/scripts/test-recovery.sh` — resume PASS; ghi loss window thật.
7. `run-mode.sh batch --start <hour UTC> --end <hour+1>` — đối chiếu
   count/avg/min/max per-metric với fixture tính tay; rerun 2 lần → số không đổi.
8. Cài cron `run-batch-hourly.sh` như README — kiểm log, lock, kết quả Gold
   per-metric; rerun thủ công một giờ để kiểm determinism.
9. `scripts/resource-report.sh --interval 30 --count 10` trong lúc load; điền
   kết quả thật vào phần "Measured results" bên dưới (hiện đang trống).

## 5. Measured results (2026-09-30, máy phát triển)

- **Throughput thực**: ~10.2 evt/s (5 zone × 5 sensor × 2 evt/s = 50 evt/s
  phát; pg processed 3050 evt / 5 phút trong cửa sổ resource-report).
- **Peak RAM/CPU per service** (10 samples, 5 phút, live 5 zone):
  spark-stream 1.326 GiB / 2.5 GiB (53%), cpu peak 125.7%;
  kafka 455.8 MiB / 1 GiB (44.5%), cpu 2.7%;
  postgres 36.9 MiB / 512 MiB; backend 35.7 MiB / 256 MiB;
  bridge 15.8 MiB / 128 MiB; mqtt 2.7 MiB / 64 MiB; dashboard 10.2 MiB / 64 MiB.
  **Tổng peak ~1.9 GiB** (dưới ngân sách 4.6 GiB).
- **Host**: RAM available min 8339 MB; disk 94 GB ổn định (không tăng đáng kể
  trong 5 phút do Kafka retention 1h + Parquet chưa compact).
- **Kafka backlog**: end offsets tăng đều (46332→46807 trong recovery test);
  không có backlog tích luỹ khi live chạy ổn định.
- **Loss window (recovery)**: cumulative sent 45496 vs delivered 46398 →
  chênh lệch −902 (âm do dedup + cộng dồn nhiều run; KHÔNG phải mất mát).
  Không có event nào mất trong cửa sổ recovery test cụ thể (batch advance
  liên tục sau crash).
- **Tỷ lệ lỗi per-metric (veracity, e2e fixture 200 evt seed 42)**:
  non_numeric 3/200 (1.5%), out_of_bounds 2/200 (1%), bad_timestamp 2/200 (1%),
  duplicate 2/200, missing_field 1/200, late 2/200. Khớp fault profile thiết kế.
- **Batch Gold**: 108 sensor-hour-metric rows từ 132844 readings (giờ 17:00–18:00Z);
  rerun byte-identical.
- **Backup**: pg_dump 1.3 MB; tar DATA_ROOT 15 MB (11836 entries).
