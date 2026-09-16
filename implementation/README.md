# Implementation — Nền tảng Big Data IoT (bản nhẹ cho máy 8–10 GB RAM)

Pipeline end-to-end trên **một máy Linux**:

```text
Simulator → Mosquitto MQTT (QoS1) → Bridge → Kafka sensor_raw (1 broker KRaft,
3 partitions, RF=1) → MỘT Spark Structured Streaming app (local[2], 4 queries
Q1/Q2a/Q2b/Q3) → Parquet Bronze/Silver/Quarantine (bind mount) + PostgreSQL
(sensor_latest, alerts, gold.sensor_hourly) → FastAPI → static Dashboard.
Batch hourly Gold đọc Silver trực tiếp (--start/--end UTC); Airflow
LocalExecutor gọi đúng chương trình batch đó.
```

> **Báo cáo LaTeX (`Documents/report/`) không bị sửa.** Kiến trúc học thuật
> (HDFS + Delta + Spark standalone) khác với bản triển khai này — bảng độ lệch
> ở mục "Độ lệch so với báo cáo" bên dưới. Runtime benchmark trên máy viết code
> bị **DEFERRED** (thiếu disk/RAM); mọi số đo phải đến từ máy mục tiêu — xem
> `docs/TEST_REPORT.md`.

## 1. Cấu trúc thư mục

```text
implementation/
├── compose.yaml                 # file canonical (profiles: live|batch|airflow)
├── .env.example                 # DATA_ROOT, ports, credentials, tuning
├── .dockerignore                  # allowlist context build airflow (chỉ spark/ + airflow/)
├── mosquitto/mosquitto.conf
├── simulator/                   # producer.py MQTT QoS1 + manifest + fault inject
├── bridge/                      # bridge.py MQTT→Kafka (manual ACK sau delivery)
├── spark/
│   ├── Dockerfile               # Spark 4.2.0 local + jar Kafka/JDBC bake sẵn
│   ├── install-jars.sh          # installer jar pin dùng chung (spark + airflow)
│   ├── jobs/{common,validation,pg_sink,stream_app,batch,warmup}.py
│   └── run/{run-stream.sh,run-batch.sh}
├── sql/{init-databases,schema,queries,test-data}.sql
├── backend/                     # FastAPI (main.py)
├── dashboard/                   # index.html + nginx proxy /api
├── airflow/                     # Dockerfile tự thân: airflow + JDK17 + tự build
│   |                            # lại Spark app layer (không phụ thuộc image spark)
│   └── dags/iot_pipeline.py     # DAG hourly gold.sensor_hourly
├── scripts/
│   ├── preflight.sh             # kiểm tra host (read-only)
│   ├── prepare-host.sh          # tạo DATA_ROOT + .env
│   ├── init.sh                  # build + postgres + kafka topic
│   ├── wait-for-health.sh
│   ├── run-mode.sh              # live | batch | airflow | status
│   ├── reset-pipeline.sh        # DUY NHẤT được phép phá hủy (có flag tường minh)
│   ├── resource-report.sh       # docker stats / RAM / disk / counts → JSONL
│   ├── test-e2e.sh
│   └── test-recovery.sh
├── tests/                       # unit tests (unittest, KHÔNG cần Docker)
└── docs/{ARCHITECTURE.md,TEST_REPORT.md}
```

## 2. Versions đã pin (verify qua registry/Maven ngày 2026-09-14)

| Thành phần | Version pin | Ghi chú |
|---|---|---|
| apache/spark | `4.2.0-java21-python3` | local mode `local[2]`, JDK 21, Scala 2.13 |
| Kafka connector | `spark-sql-kafka-0-10_2.13:4.2.0` + `spark-token-provider…4.2.0` + `kafka-clients 3.9.2` + `commons-pool2 2.13.1` | bake từ Maven Central lúc build qua `spark/install-jars.sh` (sha256 pin đủ 5/5 jar) |
| postgresql JDBC | `42.7.4` | bake cùng chỗ |
| apache/kafka | `4.3.1` | KRaft, không ZooKeeper |
| eclipse-mosquitto | `2.0.22` | |
| postgres | `17.11` | 1 instance, 2 database (`app`, `airflow_meta`) |
| apache/airflow | `2.10.5-python3.12` | LocalExecutor; image có JDK 17 và **tự build lại Spark app layer** từ cùng `spark/jobs`+`spark/run` + cùng `spark/install-jars.sh` (không phụ thuộc image spark có sẵn trong cache; build-time assertion + warmup chặn lỗi thiếu artifact) |
| paho-mqtt | `2.1.0` | manual ACK (`manual_ack=True`, `ack(mid, qos)`) |
| confluent-kafka | `2.10.1` | producer `acks=all`, idempotent |
| fastapi / uvicorn / psycopg2-binary | `0.141.1` / `0.52.4` / `2.9.10` | backend |
| nginx | `1.27-alpine` | dashboard |

Lệnh kiểm tra thực tế (máy mục tiêu): `docker compose -f implementation/compose.yaml
config`, và sau build: log build spark **và** log build airflow phải hiện
`[warmup] OK kafka010=… pgjdbc=…` (smoke classpath trong từng image; airflow chạy
warmup dưới combo thật Spark 4.2.0 + Java 17). Airflow build dùng context
`implementation/` với `.dockerignore` allowlist — không gửi `.env` vào daemon.

## 3. Yêu cầu máy mục tiêu

- Docker Engine + Compose plugin (đã thử với Docker 29.x / Compose v5.x).
- RAM: **available ≥ 4.6 GB** cho mode live+serving (tổng mem_limit ~4.6 GiB,
  chưa tính OS/cache); mode Airflow ~3 GiB cho scheduler + spark con (KHÔNG chạy
  đồng thời live và Airflow trên máy 8 GB).
- Disk: **≥ 15 GB trống** trước lần build đầu (image ~3.5 GB + dữ liệu demo).
- Ports (đổi được qua `.env`): 1883, 9092, 8000, 8088, 5432.
- `python3` trên host (script drain + resource report dùng để parse JSON).

## 4. Cài đặt và chạy lần đầu

```bash
cd implementation

# 0) kiểm tra host (read-only): docker, RAM, disk, ports, DATA_ROOT
scripts/preflight.sh

# 1) tạo thư mục dữ liệu + .env (nếu chưa có)
#    /data cần quyền: sudo mkdir -p /data && sudo chown "$USER" /data
#    hoặc dùng fallback: chỉnh DATA_ROOT=/home/<user>/bigdata-demo trong .env
scripts/prepare-host.sh
cp .env.example .env        # prepare-host đã tự copy nếu thiếu — chỉnh ports/creds

# 2) build images + khởi tạo postgres schema + kafka topic
scripts/init.sh

# 3) chạy pipeline streaming
scripts/run-mode.sh live
scripts/wait-for-health.sh spark-stream backend dashboard

# 4) dashboard
open http://localhost:8088        # API: http://localhost:8000/health
```

## 5. Modes (run-mode.sh)

```bash
scripts/run-mode.sh status                  # ps -a + progress log
scripts/run-mode.sh live                    # streaming pipeline
scripts/run-mode.sh batch --start 2026-09-15T01:00:00Z --end 2026-09-15T02:00:00Z
                                            # drain → hourly Gold → giữ nguyên data
BATCH_START=… BATCH_END=… scripts/run-mode.sh batch   # tương đương qua env
scripts/run-mode.sh airflow                 # scheduler hourly DAG (LocalExecutor)
```

- `batch`/`airflow` **tự drain**: stop simulator+bridge → đợi Spark tiêu hết
  backlog (đối chiếu broker end offsets với `logs/stream-progress.jsonl`) →
  SIGTERM spark-stream (checkpoint finalize) → chạy batch.
- Không mode nào xóa volume/checkpoint. Path phá hủy duy nhất:
  `scripts/reset-pipeline.sh [--data] [--volumes] [--yes]`.

### One-shot simulator (fixture thủ công)

```bash
docker compose --profile live run --rm simulator \
  python -u /app/producer.py --events 200 --rate 20 --fault-inject --seed 42
```

Fault profile (seeded, deterministic): 2% duplicate, 1% thiếu temperature_c,
1% temperature="hot", 1% event_time="not-a-timestamp", 1% value=9999,
0.5%+0.5% late 30s/5min (late hợp lệ vẫn vào Silver). Manifest mỗi event:
`$DATA_ROOT/logs/simulator-manifest.jsonl`.

## 6. Kiểm thử

```bash
# unit tests trên host (không cần Docker): 81 tests
python3 -m unittest discover -s implementation/tests

# sau khi live chạy trên máy mục tiêu:
scripts/test-e2e.sh        # 200 event fault-inject → API/quarantine/alerts assertions
scripts/test-recovery.sh   # SIGKILL spark-stream + restart bridge → resume check
scripts/resource-report.sh --once          # hoặc --interval 30 --count N
```

Chi tiết kết quả hiện tại + checklist máy mục tiêu: `docs/TEST_REPORT.md`
(chỉ ghi "đã chạy" khi có log/lệnh thật).

## 7. Mount paths, env, ports

- `DATA_ROOT` (mặc định `/data/bigdata`) bind mount vào `/data/bigdata` của
  container: `bronze/sensor`, `silver/sensor`, `quarantine/sensor`,
  `checkpoints/{q1,q2a,q2b,q3}`, `logs/`, `control/`. Không có quyền tạo `/data`
  → đặt `DATA_ROOT=<đường dẫn ghi được>` trong `.env`.
- Named volumes: `bigdata-iot_pg_data`, `bigdata-iot_kafka_data`.
- Host ports/env đầy đủ: xem `.env.example` (mỗi biến có chú thích).
- PostgreSQL: `app` (serving + Gold + staging) và `airflow_meta`, user
  `${POSTGRES_USER}`; chỉ expose host port 5432 để debug.

### Backup / restore

```bash
# PostgreSQL (app + airflow_meta)
docker compose exec postgres pg_dump -U bigdata -Fc app > backup-app-$(date +%F).dump
docker compose exec postgres pg_dump -U bigdata -Fc airflow_meta > backup-airflow-$(date +%F).dump
# khôi phục
cat backup-app-…dump | docker compose exec -T postgres pg_restore -U bigdata -d app --clean

# Parquet/checkpoints/logs (DATA_ROOT) — dừng mode đang chạy trước khi tar
tar czf bigdata-data-$(date +%F).tgz -C "$(dirname "$DATA_ROOT")" "$(basename "$DATA_ROOT")"

# Kafka offsets/messages: KHÔNG backup trong bản demo; mất volume Kafka =
# mất Bronze chưa tiêu; luôn có thể replay từ manifest + simulator.
```

## 8. API dashboard

| Endpoint | Mô tả |
|---|---|
| `GET /health` | trạng thái DB + thời điểm kiểm tra (200 kể cả degraded) |
| `GET /api/sensors/latest?sensor_id=&limit=&offset=` | latest per sensor, phân trang ≤ 500 |
| `GET /api/alerts?limit=&offset=` | alerts (PK event_id+rule_id), mới nhất trước |
| `GET /api/stats?start=&end=&sensor_id=` | Gold hourly theo UTC `[start,end)`, mặc định 24h |
| `GET /api/progress` | counters nội bộ (events, duplicates, alerts, last_processed_at) |

Dashboard refresh 3s (`?interval=N` để đổi); khi stream dừng vẫn hiển thị dữ
liệu đã lưu + last refresh + banner lỗi.

## 9. Giới hạn delivery/idempotency (đọc kỹ trước khi claim kết quả)

- **Không exactly-once xuyên pipeline.** Spark file sink idempotent per-query
  (checkpoint), PostgreSQL idempotent qua upsert/PK, nhưng:
- MQTT→bridge: QoS1 + persistent session + manual ACK **sau** Kafka delivery
  callback. Loss window còn lại (đã ghi trong docs/ARCHITECTURE.md §7):
  message publish khi bridge chưa kịp giữ session, broker restart
  (persistence=false), tràn `max_queued_messages`, crash giữa produce và ACK.
  Dup redelivery có thể xảy ra (dedup ở Q3/batch theo event_id).
- Parquet và PostgreSQL **không atomic với nhau**: crash giữa hai sink để lại
  staging rows (sẽ bị truncate ở batch sau) — reconcile bằng rerun.
- Spark checkpoint KHÔNG phải khóa chống trùng DB — khóa là upsert/unique.
- Event đến muộn sau khi Gold đã aggregate: rerun giờ liên quan (batch
  full-replace, không cộng lặp).
- Mosquitto anonymous, không TLS/SASL/ACL — demo only.

## 10. Độ lệch so với báo cáo

| Báo cáo | Bản triển khai này |
|---|---|
| HDFS 3.5 + Delta Lake | plain local Parquet trên bind mount; idempotency ở PG/batch |
| Spark standalone master + 2 workers + 3 job runners | 1 app local[2], 4 queries, 1 container |
| Simulator → Kafka trực tiếp | Simulator → MQTT → bridge → Kafka |
| RF=2 topic | RF=1 (1 broker; nâng khi có 3 broker) |
| Prometheus/Grafana/JMX | scripts + docker stats (`resource-report.sh`) |
| Gold streaming window 1m/5m | Gold **batch hourly** `gold.sensor_hourly` (UTC) |

## 11. Chưa verify (DEFERRED — máy viết code thiếu tài nguyên)

Docker build/runtime/e2e/recovery/load **chưa chạy trên máy nào**. Preflight đã
chạy thật trên máy này (exit 2, warnings đúng thực tế — xem
`docs/TEST_REPORT.md` mục 1/3). Host hiện tại BLOCKED bởi: ~11 GB disk trống,
~3.9 GB RAM available (đo 2026-09-14), port 1883 bị project khác chiếm (script
không tự dừng container ngoài). Danh sách việc runtime cụ thể + lệnh + điều
kiện: `docs/TEST_REPORT.md` mục 2 (D1–D10) và mục 4 (checklist tester).
