# Implementation — Nền tảng Big Data IoT (bản nhẹ cho máy 8–10 GB RAM)

Pipeline end-to-end đa nguồn trên **một máy Linux**:

```text
5 x Simulator (zoneA..zoneE, vector 6 chỉ số môi trường)
    → Mosquitto MQTT (QoS1) → Bridge → Kafka sensor_raw (1 broker KRaft,
    3 partitions, RF=1, retention 1h) → MỘT Spark Structured Streaming app
    (local[2], 4 queries Q1/Q2a/Q2b/Q3) → Parquet Bronze/Silver/Quarantine
    (bind mount) + PostgreSQL (sensor_latest, alerts, processed_events,
    gold.sensor_hourly theo chiều metric) → FastAPI → static Dashboard.
Batch hourly Gold đọc Silver trực tiếp (--start/--end UTC); kích hoạt bằng
scripts/run-batch-hourly.sh qua cron trên host (KHÔNG dùng scheduler bên
ngoài — workflow orchestrator là Hướng phát triển trong báo cáo học thuật).
```

> **Báo cáo LaTeX (`Documents/report/`) đã được rewrite (2026-09-18) để phản
> ánh đúng kiến trúc này.** Runtime stack đã được build/recreate và bounded smoke đã chạy trên máy này ngày
> **01/10/2026**; phép `load15-clean-20261002` đã ghi nhận 16,226 GB payload trong 4 giờ; mục tiêu tiếp theo 30 GB trong 4 giờ vẫn chưa chạy,
> nên chưa được coi là đạt. Evidence hiện có nằm trong `docs/TEST_REPORT.md` và
> hướng dẫn đầy đủ ở `docs/EVALUATION.md`.

## 1. Cấu trúc thư mục

```text
implementation/
├── compose.yaml                 # file canonical (profiles: live|batch)
├── .env.example                 # DATA_ROOT, ports, credentials, tuning
├── mosquitto/mosquitto.conf
├── simulator/                   # producer.py MQTT QoS1 + manifest + fault inject per-metric
├── bridge/                      # bridge.py MQTT→Kafka (manual ACK sau delivery, queue 5000)
├── spark/
│   ├── Dockerfile               # Spark 4.2.0 local + jar Kafka/JDBC bake sẵn
│   ├── install-jars.sh          # installer jar pin sha256
│   ├── jobs/{common,validation,pg_sink,stream_app,batch,warmup}.py
│   └── run/{run-stream.sh,run-batch.sh}
├── sql/{init-databases,schema,queries,test-data}.sql   # 1 database `app`
├── backend/                     # FastAPI (main.py)
├── dashboard/                   # index.html + nginx proxy /api
├── scripts/
│   ├── preflight.sh             # kiểm tra host (read-only)
│   ├── prepare-host.sh          # tạo DATA_ROOT + .env
│   ├── init.sh                  # build + postgres + kafka topic
│   ├── wait-for-health.sh
│   ├── run-mode.sh              # live | batch | status
│   ├── run-multi-sim.sh         # spawn/stop 5 simulator zoneA..zoneE
│   ├── run-batch-hourly.sh      # batch giờ vừa kết thúc (cron host, có lock)
│   ├── reset-pipeline.sh        # DUY NHẤT được phép phá hủy (có flag tường minh)
│   ├── resource-report.sh       # docker stats / RAM / disk / counts → JSONL
│   ├── test-e2e.sh
│   └── test-recovery.sh
├── tests/                       # unit tests (unittest, KHÔNG cần Docker)
└── docs/{ARCHITECTURE.md,TEST_REPORT.md}
```

## 2. Data contract — vector 6 chỉ số môi trường

Một bản tin = **vector data-fusion** của CÙNG 1 cảm biến tại CÙNG 1 thời
điểm. Nguồn duy nhất cho bounds/ngưỡng alert là bảng `METRIC_CONFIG` trong
`spark/jobs/common.py` (validation, alert Q3 và Gold đều đọc từ đây):

| Chỉ số | Trường | Đơn vị | Bounds | Alert | Hướng |
|---|---|---|---|---|---|
| Nhiệt độ | `temperature_c` | °C | [-50, 200] | > 35 | cao |
| Độ ẩm | `humidity_pct` | %RH | [0, 100] | > 80 | cao |
| CO₂ | `co2_ppm` | ppm | [0, 5000] | > 1000 | cao |
| Áp suất | `pressure_hpa` | hPa | [300, 1100] | < 950 | thấp |
| Bụi mịn | `pm25_ugm3` | µg/m³ | [0, 1000] | > 35 | cao |
| Ánh sáng | `light_lux` | lux | [0, 200000] | — | không alert |

Định danh đa nguồn: `sensor_id = {prefix}_{TYPE}_{NNN}`
(`zoneA_WEATHER_03`), `event_id = {prefix}-{seq:06d}` (`zoneA-000042`).
Loại trạm theo vòng `WEATHER → AIR → MULTI`:

- `WEATHER`: nhiệt/ẩm/áp/sáng; `AIR`: CO₂/PM2.5; `MULTI`: đủ 6.
- Chỉ số vắng mặt = NULL/absent, KHÔNG phải lỗi (station subset).
- `ingest_time` đo latency; timestamp có offset chuẩn hóa UTC trong Spark;
  `value` là legacy alias của `temperature_c`; legacy optional:
  `sensor_type`, `location`, `sequence_no`.

Validation per-metric: chỉ số non-numeric hoặc out-of-bounds → row VẪN vào
Silver với chỉ số đó = NULL + reason `non_numeric:<metric>` /
`value_out_of_bounds:<metric>` (cột `metric_issues`). Chỉ thiếu
`event_id`/`sensor_id`/timestamp → cả row vào Quarantine. Alert tính TRỰC
TIẾP trong foreachBatch Q3 theo bảng ngưỡng (rule_id = tên chỉ số, 1 dòng
mỗi cặp event–chỉ số vượt ngưỡng, idempotent), không có bảng rule riêng.

## 3. Versions đã pin (verify qua registry/Maven ngày 2026-09-14)

| Thành phần | Version pin | Ghi chú |
|---|---|---|
| apache/spark | `4.2.0-java21-python3` | local mode `local[2]`, JDK 21, Scala 2.13 |
| Kafka connector | `spark-sql-kafka-0-10_2.13:4.2.0` + `spark-token-provider…4.2.0` + `kafka-clients 3.9.2` + `commons-pool2 2.13.1` | bake từ Maven Central lúc build qua `spark/install-jars.sh` (sha256 pin đủ 5/5 jar) |
| postgresql JDBC | `42.7.4` | bake cùng chỗ |
| apache/kafka | `4.3.1` | KRaft, không ZooKeeper; `KAFKA_LOG_RETENTION_MS=3600000` (1h) |
| eclipse-mosquitto | `2.0.22` | |
| postgres | `17.11` | 1 instance, 1 database (`app`) |
| paho-mqtt | `2.1.0` | manual ACK (`manual_ack=True`, `ack(mid, qos)`) |
| confluent-kafka | `2.10.1` | producer `acks=all`, idempotent |
| fastapi / uvicorn / psycopg2-binary | `0.141.1` / `0.52.4` / `2.9.10` | backend |
| nginx | `1.27-alpine` | dashboard |

Lệnh kiểm tra thực tế (máy mục tiêu): `docker compose -f implementation/compose.yaml
config`, và sau build: log build spark phải hiện
`[warmup] OK kafka010=… pgjdbc=…` (smoke classpath trong image).

## 4. Yêu cầu máy mục tiêu

- Docker Engine + Compose plugin (đã thử với Docker 29.x / Compose v5.x).
- RAM: **available ≥ 4.6 GB** cho mode live+serving (tổng mem_limit ~4.6 GiB,
  chưa tính OS/cache; 5 simulator thêm ~320 MiB → ~5 GiB; batch mode dùng lại
  tối đa 2560 MiB).
- Disk: **≥ 15 GB trống** trước lần build đầu (image ~1.5 GB + dữ liệu demo).
- Ports (đổi được qua `.env`): 1883, 9092, 8000, 8088, 5432.
- `python3` trên host (script drain + resource report dùng để parse JSON).

## 5. Cài đặt và chạy lần đầu

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

# 3) chạy pipeline streaming (tự spawn 5 zone simulator zoneA..zoneE)
scripts/run-mode.sh live
scripts/wait-for-health.sh spark-stream backend dashboard

# 4) dashboard
open http://localhost:8088        # API: http://localhost:8000/health
```

## 6. Modes, đa nguồn và batch theo giờ

```bash
scripts/run-mode.sh status                  # ps -a + progress log
scripts/run-mode.sh live                    # streaming pipeline + 5 zone simulator
scripts/run-mode.sh batch --start 2026-09-15T01:00:00Z --end 2026-09-15T02:00:00Z
                                            # drain → hourly Gold → giữ nguyên data
BATCH_START=… BATCH_END=… scripts/run-mode.sh batch   # tương đương qua env

scripts/run-multi-sim.sh start|stop|status  # 5 simulator song song zoneA..zoneE
scripts/run-batch-hourly.sh                 # batch cho giờ UTC vừa kết thúc
```

- `run-mode.sh live` spawn 5 simulator qua `docker compose run -d --name
  sim-<zone>`, mỗi zone `SENSOR_PREFIX` + manifest riêng
  (`$DATA_ROOT/logs/simulator-<zone>.jsonl`); 5 × 5 sensor × 2 evt/s =
  ~50 evt/s tổng. `run-mode.sh batch` tự stop các zone trước khi drain.
- `batch` **tự drain**: stop simulators+bridge → đợi Spark tiêu hết backlog
  (đối chiếu broker end offsets với `logs/stream-progress.jsonl`) → SIGTERM
  spark-stream (checkpoint finalize) → chạy batch.
- **Batch theo giờ bằng cron trên host** (thay cho scheduler bên ngoài):

```cron
# crontab -e — chạy ở phút 0 mỗi giờ, batch cho giờ vừa kết thúc
0 * * * * /path/to/implementation/scripts/run-batch-hourly.sh >> /tmp/batch-hourly.log 2>&1
```

  Script tính cửa sổ `[hh:00, hh+1:00)` UTC vừa kết thúc, chờ
  `BATCH_GRACE_S` (mặc định 60s, chỉnh trong `.env`) để event biên giới kịp
  vào Silver, dùng file lock trong `${DATA_ROOT}/control/` để từ chối chạy
  chồng, rồi gọi `run-batch.sh --start --end`. Rerun mọi lúc an toàn
  (full-replace per sensor/giờ/metric).
- Không mode nào xóa volume/checkpoint. Path phá hủy duy nhất:
  `scripts/reset-pipeline.sh [--data] [--volumes] [--yes]`.

### One-shot simulator (fixture thủ công)

```bash
docker compose --profile live run --rm simulator \
  python -u /app/producer.py --events 200 --rate 20 --fault-inject --seed 42
```

Fault profile (seeded, deterministic; chỉ số bị lỗi chọn ngẫu nhiên trong các
chỉ số đang có của trạm): 2% duplicate, 1% thiếu một chỉ số, 1% chỉ số =
"hot", 1% event_time="not-a-timestamp", 1% chỉ số vượt 10× upper bound,
0.5%+0.5% late 30s/5min (late hợp lệ vẫn vào Silver). `--spike-pct` (mặc định
2%) cho 1 chỉ số ngẫu nhiên vượt ngưỡng alert để demo alert cho cả 5 chỉ số
có ngưỡng. Manifest mỗi event: `metrics` là dict đầy đủ các chỉ số đã gửi.

## 7. Kiểm thử

```bash
# unit tests trên host (không cần Docker): 113 tests
python3 -m unittest discover -s implementation/tests

# sau khi live chạy trên máy mục tiêu:
scripts/test-e2e.sh        # 200 event fault-inject → API/quarantine/alerts assertions
scripts/test-recovery.sh   # SIGKILL spark-stream + restart bridge → resume check
scripts/resource-report.sh --once          # hoặc --interval 30 --count N
```

Chi tiết kết quả hiện tại + checklist máy mục tiêu: `docs/TEST_REPORT.md`
(chỉ ghi "đã chạy" khi có log/lệnh thật). Phương pháp đo Q1–Q4, benchmark
run-id, đối soát và mục tiêu 30 GB/4 giờ: `docs/EVALUATION.md`.

## 8. Mount paths, env, ports

- `DATA_ROOT` (mặc định `/data/bigdata`) bind mount vào `/data/bigdata` của
  container: `bronze/sensor`, `silver/sensor`, `quarantine/sensor`,
  `checkpoints/{q1,q2a,q2b,q3}`, `logs/`, `control/`. Không có quyền tạo `/data`
  → đặt `DATA_ROOT=<đường path ghi được>` trong `.env`.
- Named volumes: `bigdata-iot_pg_data`, `bigdata-iot_kafka_data`.
- Host ports/env đầy đủ: xem `.env.example` (mỗi biến có chú thích).
- PostgreSQL: 1 database `app` (serving + Gold + staging), user
  `${POSTGRES_USER}`; chỉ expose host port 5432 để debug.

### Backup / restore

```bash
# PostgreSQL (app)
docker compose exec postgres pg_dump -U bigdata -Fc app > backup-app-$(date +%F).dump
# khôi phục
cat backup-app-…dump | docker compose exec -T postgres pg_restore -U bigdata -d app --clean

# Parquet/checkpoints/logs (DATA_ROOT) — dừng mode đang chạy trước khi tar
tar czf bigdata-data-$(date +%F).tgz -C "$(dirname "$DATA_ROOT")" "$(basename "$DATA_ROOT")"

# Kafka offsets/messages: KHÔNG backup trong bản demo; mất volume Kafka =
# mất Bronze chưa tiêu; luôn có thể replay từ manifest + simulator.
```

## 9. API dashboard

| Endpoint | Mô tả |
|---|---|
| `GET /health` | trạng thái DB + thời điểm kiểm tra (200 kể cả degraded) |
| `GET /api/sensors/latest?sensor_id=&limit=&offset=` | latest per sensor (đủ 6 chỉ số), phân trang ≤ 500 |
| `GET /api/alerts?limit=&offset=` | alerts (metric/threshold/direction/value; PK event_id+rule_id), mới nhất trước |
| `GET /api/stats?start=&end=&sensor_id=` | Gold hourly **theo chiều metric** theo UTC `[start,end)`, mặc định 24h |
| `GET /api/progress` | counters nội bộ (events, duplicates, alerts, last_processed_at) |

Dashboard refresh 3s (`?interval=N` để đổi); hiển thị vector 6 chỉ số (ô đổi
màu khi vượt ngưỡng theo hướng cao/thấp), alerts có metric+ngưỡng+hướng, Gold
per-metric; khi stream dừng vẫn hiển thị dữ liệu đã lưu + last refresh + banner lỗi.

## 10. Giới hạn delivery/idempotency (đọc kỹ trước khi claim kết quả)

- **Không exactly-once xuyên pipeline.** Spark file sink idempotent per-query
  (checkpoint), PostgreSQL idempotent qua upsert/PK, nhưng:
- MQTT→bridge: QoS1 + persistent session + manual ACK **sau** Kafka delivery
  callback. Loss window còn lại (đã ghi trong docs/ARCHITECTURE.md §7):
  message publish khi bridge chưa kịp giữ session, broker restart
  (persistence=false), tràn `max_queued_messages`/queue 5000, crash giữa
  produce và ACK. Dup redelivery có thể xảy ra (dedup ở Q3/batch theo event_id).
- Parquet và PostgreSQL **không atomic với nhau**: crash giữa hai sink để lại
  staging rows (sẽ bị truncate ở batch sau) — reconcile bằng rerun.
- Spark checkpoint KHÔNG phải khóa chống trùng DB — khóa là upsert/unique.
- Event đến muộn sau khi Gold đã aggregate: rerun giờ liên quan (batch
  full-replace per metric, không cộng lặp).
- Mosquitto anonymous, không TLS/SASL/ACL — demo only.

## 11. Ghi chú về báo cáo học thuật

Báo cáo LaTeX (`Documents/report/`) đã được viết lại ngày 2026-09-18 theo
đúng kiến trúc triển khai này (multi-source simulator, vector 6 chỉ số,
batch qua script/cron, alert trong Q3); không còn bảng "độ lệch" cần theo
dõi. Việc điều phối đa job đa lịch (workflow scheduler) chuyển thành Hướng
phát triển (khi cần điều phối đa job đa lịch).

## 12. Giới hạn phép đo còn lại

Docker build, runtime, e2e, recovery, resource và backup/restore đã được kiểm
tra trên máy này; kết quả được ghi ở `docs/TEST_REPORT.md`. Bounded smoke
`smoke03` cũng đã tạo summary và đối soát Bronze. Phép tải tiếp theo 30 GB/4 giờ chưa chạy
trong phiên triển khai, vì đây là phép thử riêng cần chọn rate, dung lượng đĩa,
ngưỡng backlog và thời gian xử lý bù trước khi bắt đầu. Không dùng các số đo
smoke hoặc số liệu minh họa để kết luận đạt 30 GB trong 4 giờ.
