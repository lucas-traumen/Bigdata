# ARCHITECTURE — IoT Big Data demo stack (lightweight profile)

Trạng thái tài liệu: 2026-09-14, khớp source code trong `implementation/`.
Tài liệu này mô tả kiến trúc **đang triển khai**, khác với kiến trúc học thuật
trong `Documents/report/` (HDFS + Delta + Spark standalone) — phần độ lệch được
ghi rõ ở mục 9 và trong README.

## 1. Mục tiêu và phạm vi

Một máy Linux duy nhất (8–10 GB RAM) chạy pipeline end-to-end:

```text
                       MODE: live
  Simulator (MQTT pub, QoS1)
        │  sensors/<sensor_id>/telemetry
        v
  Mosquitto ────┐ persistent session, manual ACK
        │       │
        v       │
  Bridge (paho + confluent-kafka, bounded queue, ACK after Kafka delivery)
        │  key=sensor_id, headers=[mqtt_topic], acks=all, idempotent producer
        v
  Kafka 4.3.1 KRaft single broker ── topic sensor_raw (3 partitions, RF=1)
        │  maxOffsetsPerTrigger cap
        v
  ONE Spark Structured Streaming app (local[2], 1 SparkSession, 4 queries)
    Q1  ──> Bronze Parquet   (raw_payload + provenance, NO dedup, NO parse)
    Q2a ──> Silver Parquet   (validated rows)
    Q2b ──> Quarantine Parquet (invalid rows + error_reason)
    Q3  ──> PostgreSQL (sensor_latest upsert, alerts, processed_events)
        │                                │
        v                                v
  DATA_ROOT (bind mount)           MODE: batch / airflow
  {bronze,silver,quarantine,         spark-batch / Airflow LocalExecutor
   checkpoints,logs,control}         ── batch.py --start --end (UTC hours)
                                     ── gold.sensor_hourly upsert
                                     ── đọc Silver trực tiếp
                                     │
                                     v
                       PostgreSQL app DB ──> FastAPI ──> static dashboard
```

Không có trong profile mặc định: HDFS, Kubernetes, Spark standalone
master/worker, Kafka multi-broker, Delta Lake, Prometheus/Grafana, webserver
Airflow (xem mục 9).

## 2. Thành phần và ownership

| Service | Image (pin) | mem_limit | Vai trò |
|---|---|---|---|
| postgres | `postgres:17.11` | 512m | 2 database: `app` (serving+Gold) và `airflow_meta` |
| backend | `bigdata-iot-backend:1.0.0` (python:3.12-slim) | 256m | FastAPI 1 worker, pool ≤ 5 connections |
| dashboard | `bigdata-iot-dashboard:1.0.0` (nginx:1.27-alpine) | 64m | static UI + proxy `/api` và `/health` |
| mqtt | `eclipse-mosquitto:2.0.22` | 64m | MQTT 1883, anonymous, persistence off |
| kafka | `apache/kafka:4.3.1` | 1024m (heap 512m) | KRaft combined broker+controller, topic `sensor_raw` 3 partition RF=1, auto-create OFF |
| kafka-init | `apache/kafka:4.3.1` | 256m | one-shot tạo topic (`--if-not-exists`) |
| simulator | `bigdata-iot-simulator:1.0.0` | 64m | publish QoS1 + manifest JSONL + fault injection |
| bridge | `bigdata-iot-bridge:1.0.0` | 128m | MQTT→Kafka, bounded queue 5000, manual ACK |
| spark-stream | `bigdata-iot-spark:1.0.0` (apache/spark:4.2.0-java21-python3) | 2560m | 1 spark-submit local[2], driver heap 1g, 4 streaming queries |
| spark-batch | cùng image spark | 2560m | one-shot `batch.py --start --end` |
| airflow-init / airflow-scheduler | `bigdata-iot-airflow:1.0.0` (apache/airflow:2.10.5-python3.12 + JDK17 + Spark 4.2.0 COPY từ image spark) | 512m / 3072m | LocalExecutor, 1 DAG hourly, `max_active_runs=1`; scheduler limit = 384m scheduler + tối đa 2560m spark con + overhead |

Tổng mem_limit nhóm live + serving ≈ **4.6 GiB** (không tính OS/Docker cache;
JVM heap nằm trong limit, cần chừa overhead cho container OS).

## 3. Compose profiles và mode

File canonical: `implementation/compose.yaml`, project name `bigdata-iot`,
container names `bigdata-<service>`.

- Không profile: `postgres`, `backend`, `dashboard` (serving layer, luôn chạy).
- `live`: `mqtt`, `simulator`, `bridge`, `kafka`, `kafka-init`, `spark-stream`.
- `batch`: `spark-batch` (one-shot, gọi qua `scripts/run-mode.sh batch`).
- `airflow`: `airflow-init` (one-shot), `airflow-scheduler`.

Chuyển mode bằng `scripts/run-mode.sh`:

- `live`: stop airflow-scheduler + spark-batch trước → up shared → up kafka/mqtt
  (kèm kafka-init idempotent) → up bridge/simulator/spark-stream.
- `batch --start X --end Y`: drain (stop simulator+bridge → đợi Spark tiêu hết
  backlog Kafka, đối chiếu end offsets của broker với `stream-progress.jsonl` →
  SIGTERM spark-stream để checkpoint finalize) → chạy `run-batch.sh` → giữ
  PostgreSQL/dashboard + toàn bộ volume/checkpoint.
- `airflow`: drain như batch → `airflow-init` (db migrate + parse check) →
  scheduler. Chỉ một DAG, một active run.
- `status`: `docker compose ps -a` + 3 dòng cuối của progress log.

Không bao giờ `down`/`down -v` trong mode script; path phá hủy duy nhất là
`scripts/reset-pipeline.sh` với flag tường minh (`--data`, `--volumes`, `--yes`).

## 4. Data contract và đường đi dữ liệu

### 4.1 Payload canonical (5 trường bắt buộc)

```json
{
  "event_id": "sim01-000001",
  "sensor_id": "TEMP_01",
  "event_time": "2026-09-13T10:00:00+07:00",
  "temperature_c": 36.2,
  "ingest_time": "2026-09-13T03:00:00.120Z"
}
```

- Timestamp có offset được chuẩn hóa về UTC (session timezone UTC + cast).
- Trường legacy tùy chọn vẫn được nhận: `sensor_type`, `unit`, `location`,
  `sequence_no`; **`value` là legacy alias của `temperature_c`** — chỉ dùng khi
  `temperature_c` vắng/trống (thứ tự mirror ở cả Python và Spark:
  `coalesce(nullif(trim(temperature_c),''), nullif(trim(value),''))`).

### 4.2 Q1 — Bronze (lossless)

- Nguồn: Kafka `sensor_raw`, `startingOffsets=earliest` (lần đầu), cap
  `maxOffsetsPerTrigger=20000`, trigger 5s, `includeHeaders=true`.
- Cột: `raw_payload` (byte gốc), `mqtt_topic` (từ header), `kafka_topic`,
  `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `received_at_utc`
  (processing time lúc đọc).
- Không parse, không dedup → JSON lỗi/type sai vẫn nằm nguyên trong Bronze.
- Sink: Parquet append `DATA_ROOT/bronze/sensor`, checkpoint `q1`.

### 4.3 Q2a/Q2b — Silver / Quarantine (phân loại độc quyền)

- Nguồn: file stream Bronze (schema tường minh), cap `maxFilesPerTrigger=1000`.
- Cùng một DataFrame đã qua `validation.build_validity_columns()`; hai nhánh là
  hai predicate bổ sung của nhau (`is_valid` / `~is_valid`) → không record nào
  rơi ra ngoài cả hai nhánh, không record nào vào cả hai.
- Quy tắc (thứ tự kiểm tra cố định, cùng từ vựng reason ở cả Python mirror và
  Spark):
  1. `missing_or_invalid_event_time` — event_time vắng/không parse được;
  2. `missing_or_invalid_ingest_time` — ingest_time vắng/không parse được;
  3. `missing_field:sensor_id`;
  4. `missing_field:event_id`;
  5. `missing_or_non_numeric_temperature` — cả `temperature_c` lẫn alias `value`
     vắng/trống/không ép được số;
  6. `value_out_of_physical_bounds` — ngoài `[BOUNDS_MIN, BOUNDS_MAX]`
     (mặc định [-50, 200], cấu hình qua env).
- Valid → Silver: `event_time_utc` (UTC), `temperature_c` (double), provenance
  Kafka, `received_at_utc`, cờ `is_late` thông tin (processing-time based;
  ngưỡng `LATE_THRESHOLD_S=60`). Event trễ HỢP LỆ không bị quarantine.
- Invalid → Quarantine: `record_key` = `event_id` hoặc fallback
  `kafka_topic-kafka_partition-kafka_offset` khi event_id vắng, kèm
  `error_reason` + `raw_payload` gốc.
- Nhiệt độ > `ALERT_THRESHOLD_C` (35) là valid → vẫn vào Silver; alert là rule
  nghiệp vụ ở Q3, không phải validation.
- Sink: Parquet append `DATA_ROOT/silver/sensor` và
  `DATA_ROOT/quarantine/sensor`, checkpoint `q2a` / `q2b` riêng.

### 4.4 Q3 — PostgreSQL serving (foreachBatch + staging + upsert)

- Nguồn: file stream Silver, cap `maxFilesPerTrigger=500`.
- Mỗi micro-batch (bounded): JDBC append vào `staging_stream_events` (executor
  side, KHÔNG collect/toPandas) → một transaction driver-side gồm:
  1. `sensor_latest` upsert với điều kiện "mới hơn mới được ghi đè": input của
      statement được rút về **một winner mỗi sensor** (window
      `ROW_NUMBER() OVER (PARTITION BY sensor_id ORDER BY event_time DESC
      NULLS LAST, received_at DESC NULLS LAST, event_id DESC NULLS LAST)`,
      `NULLS LAST` để PostgreSQL (DESC mặc định NULLS FIRST) và SQLite xếp
      NULL giống nhau, khớp tie-break `desc_nulls_last()` của batch) vì
      PostgreSQL từ chối
      `ON CONFLICT DO UPDATE` khi input chạm cùng một dòng hai lần; điều kiện
      ghi đè giữ nguyên:
      `WHERE (EXCLUDED.event_time, EXCLUDED.received_at, EXCLUDED.event_id)
      > (sensor_latest.event_time, sensor_latest.received_at, sensor_latest.event_id)`
      — so sánh row-value từ điển, deterministic;
  2. `alerts` insert `ON CONFLICT (event_id, rule_id) DO NOTHING` (rule
      `temperature_high`, threshold = `ALERT_THRESHOLD_C`) — replay an toàn;
  3. `processed_events` upsert `ON CONFLICT (event_id) DO UPDATE` — input cũng
      được rút về một dòng mỗi `event_id` ("first occurrence" = vị trí Kafka
      thấp nhất, NULL sau): event mới lần đầu → insert với
      `duplicate_count = n - 1` (số lần lặp lại trong batch); event đã có sẵn
      → cộng thêm đúng `n` (mọi lần xuất hiện trong batch này đều là duplicate)
      — marker replay + đếm duplicate;
  4. query đếm payload conflict (cùng event_id, khác temperature) → log warn;
  5. `DELETE FROM staging_stream_events`.
- Checkpoint `q3`. Spark checkpoint không phải khóa chống trùng DB — khóa là
  các upsert/unique ở trên.

### 4.5 Batch — `gold.sensor_hourly`

- `batch.py --start S --end E`: cả hai phải UTC ISO-8601, đúng ranh giới giờ,
  `[start, end)` chứa ít nhất một giờ đầy đủ; input sai bị từ chối (exit 2)
  TRƯỚC khi ghi bất kỳ đâu.
- Đọc Silver Parquet trực tiếp; filter `event_time_utc ∈ [start, end)`;
  dedup `event_id` bằng window `orderBy(received_at_utc desc, kafka_partition
  desc, kafka_offset desc)` (deterministic, khác `dropDuplicates` thuần);
  aggregate `groupBy(sensor_id, date_trunc('hour', event_time_utc))` →
  count/avg/min/max.
- Ghi qua `staging_hourly_agg` → upsert **full-replace** (`ON CONFLICT
  (sensor_id, hour_start) DO UPDATE SET ...`) → rerun cùng khoảng cho kết quả
  xác định, không cộng lặp; dữ liệu đến muộn được sửa bằng cách rerun giờ liên quan.

### 4.6 Airflow

- Image: airflow 2.10.5-python3.12 + openjdk-17 + **bản Spark 4.2.0 copy nguyên
  từ image spark** (cùng jar Kafka/JDBC) → LocalExecutor chạy đúng
  `/opt/spark/run/run-batch.sh` trong container scheduler.
- DAG `iot_gold_hourly`: `schedule="0 * * * *"` (UTC), `max_active_runs=1`,
  `catchup=False`, retries 2 × 2 min, `execution_timeout=30m`. Task BashOperator
  truyền `{{ data_interval_start }}` / `{{ data_interval_end }}` (đúng ranh giới
  giờ UTC) vào `--start/--end`.
- Metadata DB: `airflow_meta` trên cùng PostgreSQL; logs bind mount
  `${DATA_ROOT}/logs/airflow` (AIRFLOW_UID=50000).
- Không webserver trong bản đầu (dashboard ứng dụng là UI chính).

## 5. API và dashboard

| Endpoint | Ý nghĩa |
|---|---|
| `GET /health` | `{status: ok|degraded, database, checked_at, version}`; luôn 200 để healthcheck phản ánh process sống |
| `GET /api/sensors/latest?sensor_id=&limit=&offset=` | `sensor_latest`, phân trang (limit ≤ 500), order sensor_id |
| `GET /api/alerts?limit=&offset=` | alerts mới nhất trước, phân trang |
| `GET /api/stats?start=&end=&sensor_id=` | `gold.sensor_hourly` theo UTC `[start,end)`, mặc định 24h cuối |
| `GET /api/progress` | counters nội bộ: events, duplicates, alerts, sensors, last_processed_at |

Dashboard (`/` trên host port 8088 mặc định): KPI, latest, alerts, hourly stats,
`last_updated_at`, banner lỗi kết nối; refresh mỗi 3s, đổi được bằng
`?interval=N`; khi stream dừng, dữ liệu đã lưu vẫn hiển thị kèm thời điểm cập nhật cuối.

## 6. Storage, volumes, network

```text
${DATA_ROOT} (bind mount, mặc định /data/bigdata, mọi container thấy ở /data/bigdata)
├── bronze/sensor/        Parquet Q1
├── silver/sensor/        Parquet Q2a
├── quarantine/sensor/    Parquet Q2b
├── checkpoints/{q1,q2a,q2b,q3}/   Spark checkpoints (không xóa khi restart)
├── logs/
│   ├── simulator-manifest.jsonl   mỗi event đã publish (event_id, fault, published)
│   ├── bridge-deliveries.jsonl    mỗi event delivered Kafka (topic/partition/offset)
│   ├── stream-progress.jsonl      mỗi micro-batch (batch_id, offsets, watermark)
│   ├── resource-report.jsonl      mẫu docker stats / RAM / disk / counts
│   └── airflow/                   task logs Airflow
└── control/stream-heartbeat       healthcheck của spark-stream

named volumes: bigdata-iot_pg_data (PostgreSQL), bigdata-iot_kafka_data (Kafka log.dirs)
network: bridge mặc định của compose project
host ports (đổi qua .env): MQTT 1883, Kafka 9092, Backend 8000, Dashboard 8088, PostgreSQL 5432
```

File stream chỉ đọc Parquet đã commit; input/output tách thư mục; không ghi đè
input. Quyền bind mount: demo `a+rwX` (máy đơn người dùng; ghi rõ là trade-off).

## 7. Idempotency, consistency, loss windows (điều kiện đúng của mọi claim)

| Điểm | Cơ chế | Giới hạn |
|---|---|---|
| Bronze | file sink + checkpoint (exactly-once per query của Spark file sink) | không dedup theo thiết kế |
| Silver/Quarantine | 2 query độc lập dùng cùng rule, predicate bổ sung của nhau | file sink idempotent qua checkpoint transaction log |
| sensor_latest | upsert có điều kiện "mới hơn" + tie-break deterministic | non-atomic với Parquet |
| alerts | PK (event_id, rule_id) + DO NOTHING | — |
| processed_events | PK event_id + duplicate_count | — |
| gold.sensor_hourly | full-replace upsert per (sensor, hour) | — |
| MQTT→Kafka | QoS1 + persistent session + manual ACK sau delivery callback | **loss window thật**: message publish khi bridge chết trước khi session giữ (persistence off sau broker restart), broker `max_queued_messages` tràn, crash giữa produce và ACK → redelivery (dup) hoặc mất; KHÔNG exactly-once |
| Parquet vs PostgreSQL | 2 sink không atomic; reconcile bằng rerun batch + idempotent upserts | crash giữa 2 sink → pending staging rows bị truncate ở batch sau |

Tuyên bố đúng: **at-least-once từ bridge về Kafka trong giới hạn trên; sink
idempotent; KHÔNG exactly-once xuyên pipeline.**

## 8. Recovery playbook

- Restart thông thường (`run-mode.sh live` lại sau `docker compose stop`):
  checkpoint/volume nguyên vẹn, các query tiếp tục từ offset đã commit.
- SIGKILL spark-stream: restart policy `unless-stopped` tự khởi động lại;
  `test-recovery.sh` kiểm chứng progress tăng trở lại.
- Bridge chết: broker giữ message chưa ACK trong persistent session
  (`clean_session=false`) và redeliver khi reconnect; nếu broker restart thì
  persistence=false → mất hàng chờ (giới hạn đã ghi).
- PG chết giữa chừng Q3: connect retry; transaction rollback; checkpoint Q3
  replay micro-batch → upsert idempotent không nhân bản.
- Batch rerun: an toàn (full-replace), dùng để sửa late data.
- Reset sạch: `reset-pipeline.sh --data --volumes` (prompt/`--yes`).

## 9. Độ lệch so với báo cáo học thuật

- HDFS → plain local Parquet trên bind mount; Delta Lake → plain Parquet +
  idempotency ở PostgreSQL/batch (không ACID/time travel).
- Spark standalone master/worker + 3 job runner → 1 app local[2].
- Simulator → Kafka trực tiếp → Simulator → MQTT → bridge → Kafka.
- Observability Prometheus/Grafana/JMX → scripts + docker stats
  (`resource-report.sh`); có thể thêm profile sau.
- Báo cáo (`Documents/report/`) KHÔNG bị sửa trong task này; bảng độ lệch được
  ghi trong `implementation/README.md`.
