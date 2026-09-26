# TEST REPORT — IoT Big Data demo stack

Cập nhật: 2026-09-18 (phần mở rộng E3: đa nguồn + đa chỉ số, bỏ scheduler
bên ngoài, alert giữ trong Q3). Máy dùng để viết code (không phải máy nghiệm
thu mục tiêu): Ubuntu x86_64, Ryzen 5 6600H (12 logical CPU), RAM 14 GiB
nhưng **available chỉ ~3.9 GiB**, root disk **còn ~11 GB**, TCP **1883 đã bị
project khác chiếm**.

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

## 2. DEFERRED — phải chạy trên máy mục tiêu

Mọi mục dưới đây **chưa được chạy trên bất kỳ máy nào** sau rewrite E3.
TEST REPORT này KHÔNG chứa bất kỳ số đo runtime nào; không được suy ra pass
từ static checks.

| # | Việc | Lệnh | Điều kiện |
|---|---|---|---|
| D1 | Build images (spark ≈ 1.4 GB, …) | `implementation/scripts/init.sh` | ≥ 15 GB disk trống; cần internet tới Maven Central |
| D2 | Live pipeline end-to-end + e2e fixture (5 zone simulators) | `run-mode.sh live && scripts/test-e2e.sh` | RAM available ≥ 5 GB; port 1883/9092/8000/8088/5432 rảnh (hoặc đổi qua `.env`) |
| D3 | Quarantine/manifest/assertions với fault-inject seed 42; tỷ lệ quarantine per-metric khớp fault profile | (trong test-e2e.sh + phân tích manifest/Silver) | — |
| D4 | Batch Gold per-metric + rerun determinism trên data thật | `run-mode.sh batch --start … --end …` (2 lần, đối chiếu) | Sau D2 |
| D5 | Batch hourly qua cron | crontab `run-batch-hourly.sh` theo README | Sau D4; kiểm log + lock behavior |
| D6 | Recovery: SIGKILL spark-stream, restart bridge, dừng Spark 60s (backlog) | `scripts/test-recovery.sh` | Sau D2; lưu ý Kafka retention 1h |
| D7 | Resource budget thực đo (live 5 zone ≈ 5 GiB) | `scripts/resource-report.sh --once` / `--interval` | Sau D2; ghi peak RAM/CPU, backlog, disk growth |
| D8 | Load 1 event/s × 10–15 phút và 10 event/s × 30–60 phút | simulator `--rate 1/--rate 10` | Máy mục tiêu; không tự thêm nếu thiếu RAM |
| D9 | Backup/restore thực hành (pg_dump app + tar DATA_ROOT) | xem README mục Backup | — |

## 3. BLOCKED trên máy hiện tại

- **Disk ~11 GB free** — dưới ngưỡng an toàn để build image + chạy dữ liệu
  demo → D1 bị chặn trên máy này.
- **RAM available ~3.9 GB** (đo 2026-09-14) — dưới ngân sách live ~5 GiB;
  đồng thời JVM Kafka + Spark có thể OOM → D2–D8 bị chặn/khuyến nghị không chạy.
- **Port 1883** đang được nghe trên 0.0.0.0 (project khác) — script
  KHÔNG tự dừng container ngoài; dùng `MQTT_HOST_PORT=1884` trong `.env` nếu
  phải chạy trên máy này.
- Static checks/unit tests/preflight vẫn chạy được và đã chạy (mục 1) vì không
  cần Docker image lớn.

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

## 5. Measured results (chỉ điền khi có evidence thật)

- End-to-end latency (ingest_time → API): _DEFERRED — chưa đo_
- Peak RAM/CPU per service (live 5 zone): _DEFERRED — chưa đo_
- Kafka/file backlog: _DEFERRED — chưa đo_
- Disk growth: _DEFERRED — chưa đo_
- Loss window (recovery): _DEFERRED — chưa đo_
- Tỷ lệ lỗi per-metric (veracity) theo cửa sổ: _DEFERRED — chưa đo_
