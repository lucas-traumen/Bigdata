# ARCHITECTURE — IoT Big Data demo stack (lightweight profile)

Trạng thái tài liệu: 2026-09-18, khớp source code trong `implementation/` sau
phần mở rộng E3 (đa nguồn + đa chỉ số, bỏ scheduler bên ngoài, alert giữ
trong Q3) và khớp báo cáo học thuật đã rewrite cùng ngày.

## 1. Mục tiêu và phạm vi

Một máy Linux duy nhất (8–10 GB RAM) chạy pipeline end-to-end đa nguồn:

```text
                        MODE: live
  5 x Simulator (zoneA..zoneE; WEATHER/AIR/MULTI stations)
        │  sensors/<sensor_id>/telemetry   (payload = vector 6 chỉ số)
        v
  Mosquitto ────┐ persistent session, manual ACK
        │       │
        v       │
  Bridge (paho + confluent-kafka, bounded queue 5000, ACK after Kafka delivery)
        │  key=sensor_id, headers=[mqtt_topic], acks=all, idempotent producer
        v
  Kafka 4.3.1 KRaft single broker ── topic sensor_raw (3 partitions, RF=1,
        │  retention 1h)             maxOffsetsPerTrigger cap
        v
  ONE Spark Structured Streaming app (local[2], 1 SparkSession, 4 queries)
    Q1  ──> Bronze Parquet   (raw_payload + provenance, NO dedup, NO parse)
    Q2a ──> Silver Parquet   (valid rows; metric vector + metric_issues)
    Q2b ──> Quarantine Parquet (identity-invalid rows + error_reason)
    Q3  ──> PostgreSQL (sensor_latest upsert, per-metric alerts,
        │    processed_events) — alert tính TRONG foreachBatch
        v
  DATA_ROOT (bind mount)           MODE: batch (script/cron host)
  {bronze,silver,quarantine,       spark-batch / run-batch-hourly.sh
   checkpoints,logs,control}       ── batch.py --start --end (UTC hours)
                                   ── gold.sensor_hourly upsert
                                      theo chiều metric
                                   ── đọc Silver trực tiếp
                                   │
                                   v
                     PostgreSQL app DB ──> FastAPI ──> static dashboard
```

Không có trong profile mặc định: HDFS, Kubernetes, Spark standalone
master/worker, Kafka multi-broker, Delta Lake, Prometheus/Grafana, scheduler
bên ngoài (workflow orchestrator → Hướng phát triển; batch hourly kích hoạt
bằng cron host).

## 2. Thành phần và ownership

| Service | Image (pin) | mem_limit | Vai trò |
|---|---|---|---|
| postgres | `postgres:17.11` | 512m | 1 database `app` (serving + Gold + staging) |
| backend | `bigdata-iot-backend:1.0.0` (python:3.12-slim) | 256m | FastAPI 1 worker, pool ≤ 5 connections |
| dashboard | `bigdata-iot-dashboard:1.0.0` (nginx:1.27-alpine) | 64m | static UI + proxy `/api` và `/health` |
| mqtt | `eclipse-mosquitto:2.0.22` | 64m | MQTT 1883, anonymous, persistence off |
| kafka | `apache/kafka:4.3.1` | 1024m (heap 512m) | KRaft combined broker+controller, topic `sensor_raw` 3 partition RF=1, auto-create OFF, retention 1h (`KAFKA_LOG_RETENTION_MS=3600000`) |
| kafka-init | `apache/kafka:4.3.1` | 256m | one-shot tạo topic (`--if-not-exists`) |
| simulator (template) | `bigdata-iot-simulator:1.0.0` | 64m mỗi container | publish QoS1 + manifest JSONL per-zone + fault injection per-metric |
| sim-zoneA..zoneE | cùng image (ad-hoc `compose run -d`) | 64m mỗi container | 5 nguồn song song, prefix zoneA..zoneE, ~50 evt/s tổng |
| bridge | `bigdata-iot-bridge:1.0.0` | 128m | MQTT→Kafka, bounded queue 5000, manual ACK |
| spark-stream | `bigdata-iot-spark:1.0.0` (apache/spark:4.2.0-java21-python3) | 2560m | 1 spark-submit local[2], driver heap 1g, 4 streaming queries |
| spark-batch | cùng image spark | 2560m | one-shot `batch.py --start --end` |

Tổng mem_limit nhóm live + serving ≈ **4.6 GiB** + ~320 MiB cho 4 simulator
thêm (zone simulators) ≈ **5 GiB** (không tính OS/Docker cache; JVM heap nằm
trong limit, cần chừa overhead cho container OS).

## 3. Compose profiles và mode

File canonical: `implementation/compose.yaml`, project name `bigdata-iot`,
container names `bigdata-<service>` (sim zone containers: `sim-<zone>`).

- Không profile: `postgres`, `backend`, `dashboard` (serving layer, luôn chạy).
- `live`: `mqtt`, `simulator` (template), `bridge`, `kafka`, `kafka-init`,
  `spark-stream` + các container ad-hoc `sim-zoneA..zoneE`.
- `batch`: `spark-batch` (one-shot, gọi qua `scripts/run-mode.sh batch` hoặc
  `scripts/run-batch-hourly.sh`).

Chuyển mode bằng `scripts/run-mode.sh`:

- `live`: up shared → up kafka/mqtt (kèm kafka-init idempotent) → up
  bridge/spark-stream → `run-multi-sim.sh start` (5 zone simulators).
- `batch --start X --end Y`: drain (stop zone simulators + bridge → đợi Spark
  tiêu hết backlog Kafka, đối chiếu end offsets của broker với
  `stream-progress.jsonl` → SIGTERM spark-stream để checkpoint finalize) →
  chạy `run-batch.sh` → giữ PostgreSQL/dashboard + toàn bộ volume/checkpoint.
- `status`: `docker compose ps -a` + 3 dòng cuối của progress log.

Không bao giờ `down`/`down -v` trong mode script; path phá hủy duy nhất là
`scripts/reset-pipeline.sh` với flag tường minh (`--data`, `--volumes`, `--yes`).

## 4. Data contract và đường đi dữ liệu

### 4.1 Payload canonical — vector 6 chỉ số môi trường

Nguồn duy nhất cho bounds/ngưỡng: bảng `METRIC_CONFIG` trong
`spark/jobs/common.py` (validation, alert Q3, Gold cùng đọc từ đây; thêm chỉ
số = thêm 1 dòng config):

| Chỉ số | Trường | Bounds | Alert | Hướng |
|---|---|---|---|---|
| temperature_c | °C | [-50, 200] | > 35 | cao |
| humidity_pct | %RH | [0, 100] | > 80 | cao |
| co2_ppm | ppm | [0, 5000] | > 1000 | cao |
| pressure_hpa | hPa | [300, 1100] | < 950 | thấp |
| pm25_ugm3 | µg/m³ | [0, 1000] | > 35 | cao |
| light_lux | lux | [0, 200000] | — | không alert |

```json
{
  "event_id": "zoneA-000042",
  "sensor_id": "zoneA_MULTI_03",
  "event_time": "2026-09-18T10:00:00+07:00",
  "temperature_c": 36.2,
  "humidity_pct": 61.5,
  "co2_ppm": 612.0,
  "pressure_hpa": 1008.4,
  "pm25_ugm3": 18.6,
  "light_lux": 31200.0,
  "ingest_time": "2026-09-18T03:00:00.120Z"
}
```

- Định danh đa nguồn: `sensor_id = {prefix}_{TYPE}_{NNN}`,
  `event_id = {prefix}-{seq:06d}`; prefix phân biệt nguồn (zoneA..zoneE) để
  event_id không trùng chéo và truy vết được nguồn trong Bronze/manifest.
- Loại trạm: WEATHER (nhiệt/ẩm/áp/sáng), AIR (CO₂/PM2.5), MULTI (đủ 6);
  chỉ số vắng mặt = NULL, KHÔNG phải lỗi.
- Timestamp có offset được chuẩn hóa về UTC (session timezone UTC + cast).
- Trường legacy tùy chọn vẫn được nhận: `sensor_type`, `location`,
  `sequence_no`; **`value` là legacy alias của `temperature_c`** — chỉ dùng
  khi `temperature_c` vắng/trống (thứ tự mirror ở cả Python và Spark:
  `coalesce(nullif(trim(temperature_c),''), nullif(trim(value),''))`).

### 4.2 Q1 — Bronze (lossless)

- Nguồn: Kafka `sensor_raw`, `startingOffsets=earliest` (lần đầu), cap
  `maxOffsetsPerTrigger=20000`, trigger 5s, `includeHeaders=true`; topic
  retention 1h đủ cho bài demo dừng Spark 60s.
- Cột: `raw_payload` (byte gốc), `mqtt_topic` (từ header), `kafka_topic`,
  `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `received_at_utc`
  (processing time lúc đọc).
- Không parse, không dedup → JSON lỗi/type sai vẫn nằm nguyên trong Bronze.
- Sink: Parquet append `DATA_ROOT/bronze/sensor`, checkpoint `q1`.

### 4.3 Q2a/Q2b — Silver / Quarantine (phân loại độc quyền, per-metric)

- Nguồn: file stream Bronze (schema tường minh), cap `maxFilesPerTrigger=1000`.
- Cùng một DataFrame đã qua `validation.build_validity_columns()`; hai nhánh
  là hai predicate bổ sung của nhau (`is_valid` / `~is_valid`) → không record
  nào rơi ra ngoài cả hai nhánh, không record nào vào cả hai.
- Mô hình lỗi HAI TẦNG:
  1. **Lỗi định danh → cả row vào Quarantine** (`error_reason`):
     `missing_or_invalid_event_time`, `missing_or_invalid_ingest_time`,
     `missing_field:sensor_id`, `missing_field:event_id`.
  2. **Lỗi per-metric → row VẪN vào Silver**, chỉ số đó bị mask = NULL và
     reason ghi vào cột `metric_issues` (join ";", từ vựng có cấu trúc):
     `non_numeric:<metric>`, `value_out_of_bounds:<metric>`.
     Chỉ số VẮNG MẶT (station subset) là bình thường — không reason.
  - Bounds per-metric lấy từ `METRIC_CONFIG` (inclusive); ngưỡng alert KHÔNG
    phải validation rule — giá trị vượt ngưỡng vẫn Silver.
- Valid → Silver: `event_time_utc` (UTC), vector 6 cột chỉ số (masked),
  `metric_issues`, provenance Kafka, `received_at_utc`, cờ `is_late` thông
  tin (processing-time based; ngưỡng `LATE_THRESHOLD_S=60`). Event trễ HỢP
  LỆ không bị quarantine.
- Invalid → Quarantine: `record_key` = `event_id` hoặc fallback
  `kafka_topic-kafka_partition-kafka_offset` khi event_id vắng, kèm
  `error_reason` + `raw_payload` gốc.
- Tỷ lệ lỗi per-metric (veracity) tính được từ `metric_issues` trong Silver
  theo cửa sổ thời gian — chất lượng dữ liệu là một meta-stream đọc được.
- Sink: Parquet append `DATA_ROOT/silver/sensor` và
  `DATA_ROOT/quarantine/sensor`, checkpoint `q2a` / `q2b` riêng.

### 4.4 Q3 — PostgreSQL serving (foreachBatch + staging + upsert + alert)

- Nguồn: file stream Silver, cap `maxFilesPerTrigger=500`.
- Mỗi micro-batch (bounded): JDBC append vào `staging_stream_events` (executor
  side, KHÔNG collect/toPandas; 13 cột = identity + 6 metric + provenance,
  map-by-name được guard test pin tĩnh) → một transaction driver-side gồm:
  1. `sensor_latest` upsert với điều kiện "mới hơn mới được ghi đè": input của
      statement được rút về **một winner mỗi sensor** (window
      `ROW_NUMBER() OVER (PARTITION BY sensor_id ORDER BY event_time DESC
      NULLS LAST, received_at DESC NULLS LAST, event_id DESC NULLS LAST)`,
      `NULLS LAST` để PostgreSQL (DESC mặc định NULLS FIRST) và SQLite xếp
      NULL giống nhau) vì PostgreSQL từ chối `ON CONFLICT DO UPDATE` khi
      input chạm cùng một dòng hai lần; winner phải mang tối thiểu 1 chỉ số;
      điều kiện ghi đè giữ nguyên:
      `WHERE (EXCLUDED.event_time, EXCLUDED.received_at, EXCLUDED.event_id)
      > (sensor_latest.event_time, sensor_latest.received_at, sensor_latest.event_id)`
      — so sánh row-value từ điển, deterministic; 6 cột chỉ số đi theo winner,
      chỉ số vắng mặt giữ NULL.
  2. **Alerts per-metric tính TRỰC TIẾP trong foreachBatch** từ bảng ngưỡng
      `METRIC_CONFIG` (không hard-code từng chỉ số, không bảng rule riêng,
      không bước tiêu thụ riêng — user decision 2026-09-18): một INSERT per
      chỉ số có ngưỡng, direction "high" = strictly above, "low" = strictly
      below; `rule_id` = tên chỉ số; PK `(event_id, rule_id)` + DO NOTHING →
      replay an toàn; cột `metric`, `threshold`, `direction`, `value` lưu
      đầy đủ ngữ cảnh (`temperature_c` chỉ là cột legacy, NULL cho alert
      phi-nhiệt độ).
  3. `processed_events` upsert `ON CONFLICT (event_id) DO UPDATE` — input cũng
      được rút về một dòng mỗi `event_id` ("first occurrence" = vị trí Kafka
      thấp nhất, NULL sau): event mới lần đầu → insert với
      `duplicate_count = n - 1`; event đã có sẵn → cộng thêm đúng `n`.
  4. query đếm payload conflict (cùng event_id, KHÁC metric vector — so
      NULL-safe `IS DISTINCT FROM` trên cả 6 chỉ số) → log warn;
  5. `DELETE FROM staging_stream_events`.
- Checkpoint `q3`. Spark checkpoint không phải khóa chống trùng DB — khóa là
  các upsert/unique ở trên.

### 4.5 Batch — `gold.sensor_hourly` theo chiều metric

- `batch.py --start S --end E`: cả hai phải UTC ISO-8601, đúng ranh giới giờ,
  `[start, end)` chứa ít nhất một giờ đầy đủ; input sai bị từ chối (exit 2)
  TRƯỚC khi ghi bất kỳ đâu.
- Đọc Silver Parquet trực tiếp; filter `event_time_utc ∈ [start, end)`;
  dedup `event_id` bằng window `orderBy(received_at_utc desc, kafka_partition
  desc, kafka_offset desc)` (deterministic); unpivot vector chỉ số bằng
  `metric_stack_expr()` (sinh từ `METRIC_CONFIG`) thành các reading row,
  bỏ reading NULL; aggregate `groupBy(sensor_id, hour, metric)` →
  count/avg/min/max **cho từng chỉ số**.
- Ghi qua `staging_hourly_agg` (có cột `metric`) → upsert **full-replace**
  (`ON CONFLICT (sensor_id, hour_start, metric) DO UPDATE SET ...`) → rerun
  cùng khoảng cho kết quả xác định, không cộng lặp; dữ liệu đến muộn được sửa
  bằng cách rerun giờ liên quan.
- Kích hoạt: `scripts/run-mode.sh batch --start --end` (one-shot có drain) hoặc
  `scripts/run-batch-hourly.sh` qua cron host cho giờ UTC vừa kết thúc —
  script có mkdir-lock trong `${DATA_ROOT}/control/` chống chạy chồng, grace
  delay `BATCH_GRACE_S` cho event biên giờ, không cần scheduler bên ngoài.

## 5. API và dashboard

| Endpoint | Ý nghĩa |
|---|---|
| `GET /health` | `{status: ok|degraded, database, checked_at, version}`; luôn 200 để healthcheck phản ánh process sống |
| `GET /api/sensors/latest?sensor_id=&limit=&offset=` | `sensor_latest` (vector 6 chỉ số), phân trang (limit ≤ 500), order sensor_id |
| `GET /api/alerts?limit=&offset=` | alerts (metric/threshold/direction/value) mới nhất trước, phân trang |
| `GET /api/stats?start=&end=&sensor_id=` | `gold.sensor_hourly` theo chiều metric, UTC `[start,end)`, mặc định 24h cuối |
| `GET /api/progress` | counters nội bộ: events, duplicates, alerts, sensors, last_processed_at |

Dashboard (`/` trên host port 8088 mặc định): KPI, latest dạng vector 6 chỉ
số (tô màu vượt ngưỡng theo hướng cao/thấp), alerts per-metric, hourly stats
per-metric, `last_updated_at`, banner lỗi kết nối; refresh mỗi 3s, đổi được
bằng `?interval=N`; khi stream dừng, dữ liệu đã lưu vẫn hiển thị kèm thời
điểm cập nhật cuối.

## 6. Storage, volumes, network

```text
${DATA_ROOT} (bind mount, mặc định /data/bigdata, mọi container thấy ở /data/bigdata)
├── bronze/sensor/        Parquet Q1
├── silver/sensor/        Parquet Q2a (metric vector + metric_issues)
├── quarantine/sensor/    Parquet Q2b
├── checkpoints/{q1,q2a,q2b,q3}/   Spark checkpoints (không xóa khi restart)
├── logs/
│   ├── simulator-<zone>.jsonl       mỗi event đã publish (per zone, có dict metrics)
│   ├── bridge-deliveries.jsonl      mỗi event delivered Kafka (topic/partition/offset)
│   ├── stream-progress.jsonl        mỗi micro-batch (batch_id, offsets, watermark)
│   └── resource-report.jsonl        mẫu docker stats / RAM / disk / counts
└── control/
    ├── stream-heartbeat             healthcheck của spark-stream
    └── batch-hourly.lock/           mkdir-lock của run-batch-hourly.sh

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
| alerts | PK (event_id, rule_id) + DO NOTHING, rule per-metric từ METRIC_CONFIG | — |
| processed_events | PK event_id + duplicate_count | — |
| gold.sensor_hourly | full-replace upsert per (sensor, hour, metric) | — |
| MQTT→Kafka | QoS1 + persistent session + manual ACK sau delivery callback | **loss window thật**: message publish khi bridge chết trước khi session giữ (persistence off sau broker restart), broker `max_queued_messages` tràn, queue 5000 tràn, crash giữa produce và ACK → redelivery (dup) hoặc mất; KHÔNG exactly-once |
| Kafka retention | 1h (`KAFKA_LOG_RETENTION_MS=3600000`) | dừng Spark lâu hơn 1h → mất offset cũ (demo chấp nhận, recovery test đo loss window) |
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
- Batch rerun: an toàn (full-replace per metric), dùng để sửa late data;
  `run-batch-hourly.sh` skip nếu batch trước còn chạy (lock) và xử lý lock
  stale bằng PID.
- Reset sạch: `reset-pipeline.sh --data --volumes` (prompt/`--yes`); cũng
  dọn các container `sim-*` ad-hoc.

## 9. Ghi chú kiến trúc (so với báo cáo học thuật)

Báo cáo LaTeX (`Documents/report/`, rewritten 2026-09-18) phản ánh đúng kiến
trúc này. Các seam cố ý để mở rộng ngang (đã ghi trong Hướng phát triển của
báo cáo): nhân bản N bridge (`BRIDGE_INSTANCE_ID`), tách topic theo zone,
thêm broker + RF, cụm Spark nhiều máy, Delta Lake/HDFS, TLS/SASL/ACL,
workflow scheduler khi cần điều phối đa job, framework Stream DaQ đầy đủ.
