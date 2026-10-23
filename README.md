# BigData — Nền tảng xử lý dữ liệu cảm biến IoT theo thời gian thực

Đề tài: **"Xây dựng và đánh giá nền tảng Big Data xử lý dữ liệu cảm biến IoT
theo thời gian thực"** — báo cáo môn học Big Data, Trường Đại học Công nghệ Kỹ
thuật TP. Hồ Chí Minh (HCMUTE).

Pipeline chạy end-to-end trên một máy Linux: simulator đa nguồn → MQTT → Kafka
→ Spark Structured Streaming → Parquet Bronze/Silver/Quarantine + PostgreSQL →
FastAPI → dashboard, kèm job batch tổng hợp Gold theo giờ.

```text
5 x Simulator (zoneA..zoneE, vector 6 chỉ số môi trường)
  -> Mosquitto MQTT (QoS1) -> Bridge -> Kafka sensor_raw
     (1 broker KRaft, 3 partition, RF=1, retention 1h)
  -> Spark Structured Streaming (local[2], 4 query Q1/Q2a/Q2b/Q3)
     -> Parquet Bronze/Silver/Quarantine (bind mount)
     -> PostgreSQL (sensor_latest, alerts, processed_events, gold.sensor_hourly)
  -> FastAPI -> Dashboard (Nginx, refresh 3s)
```

## Nội dung repository

| Thư mục | Nội dung |
|---|---|
| `implementation/` | Mã nguồn runtime: Docker Compose, simulator, bridge, Spark job, FastAPI, dashboard, script vận hành và benchmark |
| `implementation/docs/` | Kiến trúc, phương pháp đánh giá và báo cáo kiểm thử |
| `Documents/report/` | Báo cáo LaTeX (5 chương + phụ lục triển khai) |
| `homework/` | Bài tập các tuần |

Tài liệu chi tiết về runtime: [`implementation/README.md`](implementation/README.md).

## Triển khai nhanh bằng Docker Hub

Image đã build sẵn, không cần biên dịch lại. Yêu cầu: Docker Engine 29+ kèm
Compose plugin, RAM available ≥ 4,6 GiB, đĩa ≥ 15 GB trống.

### 1. Lấy mã nguồn và image

```bash
git clone https://github.com/lucas-traumen/Bigdata.git
cd Bigdata/implementation

docker pull trilucas/big_data:spark-1.0.0
docker pull trilucas/big_data:backend-1.0.0
docker pull trilucas/big_data:bridge-1.0.0
docker pull trilucas/big_data:simulator-1.0.0
docker pull trilucas/big_data:dashboard-1.0.0
```

### 2. Gắn tag cục bộ

`compose.yaml` tham chiếu tên cục bộ `bigdata-iot-*:1.0.0`, nên phải gắn tag
lại sau khi pull:

```bash
docker tag trilucas/big_data:spark-1.0.0     bigdata-iot-spark:1.0.0
docker tag trilucas/big_data:backend-1.0.0   bigdata-iot-backend:1.0.0
docker tag trilucas/big_data:bridge-1.0.0    bigdata-iot-bridge:1.0.0
docker tag trilucas/big_data:simulator-1.0.0 bigdata-iot-simulator:1.0.0
docker tag trilucas/big_data:dashboard-1.0.0 bigdata-iot-dashboard:1.0.0
```

### 3. Cấu hình

```bash
scripts/preflight.sh      # kiểm tra host, read-only
scripts/prepare-host.sh   # tạo DATA_ROOT và .env từ .env.example
```

Nếu `/data` không ghi được: `sudo mkdir -p /data && sudo chown "$USER" /data`,
hoặc trỏ `DATA_ROOT` sang đường dẫn khác trong `.env`. Rà lại cổng và
`POSTGRES_PASSWORD` trước khi chạy.

### 4. Khởi tạo dịch vụ nền

Khi dùng image từ registry, **bỏ qua** `scripts/init.sh` (script này build lại
toàn bộ image). Chạy trực tiếp:

```bash
docker compose up -d postgres
scripts/wait-for-health.sh --timeout 120 postgres

docker compose --profile live up -d kafka
scripts/wait-for-health.sh --timeout 180 kafka
docker compose --profile live run --rm --name kafka-init-once kafka-init
```

### 5. Chạy pipeline

```bash
scripts/run-mode.sh live
scripts/wait-for-health.sh spark-stream backend dashboard
```

Dashboard: <http://localhost:8088> — API: <http://localhost:8000/health>

### 6. Batch theo giờ, kiểm thử và dừng

```bash
scripts/run-mode.sh status
scripts/run-batch-hourly.sh        # batch cho giờ UTC vừa kết thúc
scripts/test-e2e.sh                # fixture 200 event qua API
scripts/test-recovery.sh           # SIGKILL spark-stream rồi restart
scripts/resource-report.sh --once

docker compose --profile live stop # dừng, giữ nguyên dữ liệu
```

Không dùng `docker compose down -v` cho thao tác thường ngày: cờ `-v` xóa named
volume của Kafka và PostgreSQL. Đường phá hủy duy nhất được thiết kế cho việc
đó là `scripts/reset-pipeline.sh --data --volumes`.

## Image phát hành

Repository: [`trilucas/big_data`](https://hub.docker.com/r/trilucas/big_data)

| Thành phần | Tag | Kích thước | Dùng cho |
|---|---|---|---|
| spark | `spark-1.0.0` | 817 MB | `spark-stream`, `spark-batch` |
| backend | `backend-1.0.0` | 57 MB | FastAPI serving layer |
| bridge | `bridge-1.0.0` | 53 MB | Bridge MQTT → Kafka |
| simulator | `simulator-1.0.0` | 49 MB | Simulator đa nguồn |
| dashboard | `dashboard-1.0.0` | 21 MB | Nginx + giao diện tĩnh |

Mỗi thành phần có thêm tag `<thành phần>-latest` trỏ tới cùng digest. Ba image
nền còn lại lấy trực tiếp từ Docker Hub: `postgres:17.11`,
`apache/kafka:4.3.1`, `eclipse-mosquitto:2.0.22`.

### Nhận dạng image

Image ID là băm cấu hình image nên thay đổi mỗi lần build lại, kể cả khi mã
nguồn không đổi. Vì vậy tài liệu trong repo không trích image ID cố định mà
dùng cặp **tag + digest**. Đọc digest của image đang có:

```bash
docker image inspect trilucas/big_data:spark-1.0.0 \
  --format '{{index .RepoDigests 0}}'
```

Công cụ benchmark ghi digest của các image đang chạy vào `run-spec.json` của
từng phiên đo, nên bằng chứng gắn với nội dung image thực tế.

## Build từ mã nguồn

Nếu không dùng registry:

```bash
scripts/preflight.sh
scripts/prepare-host.sh
scripts/init.sh            # build toàn bộ image + khởi tạo postgres + kafka topic
scripts/run-mode.sh live
```

Cần mạng tới Docker Hub cho base image và tới `repo1.maven.org` cho connector
Kafka + driver JDBC (khoảng 13 MB, có kiểm tra SHA256 trong
`spark/install-jars.sh`).

## Kiểm thử

```bash
python3 -m unittest discover -s tests   # unit tests, không cần Docker
```

Kết quả kiểm thử runtime và checklist máy đích: `implementation/docs/TEST_REPORT.md`.
Phương pháp đo, benchmark run-id và đối soát: `implementation/docs/EVALUATION.md`.

## Báo cáo

Báo cáo LaTeX nằm trong `Documents/report/`. Build:

```bash
cd Documents/report
latexmk -pdf -outdir=build main.tex
```

Yêu cầu TeX Live có `babel-vietnamese`, `vntex` và `IEEEtran.bst` (đính kèm
trong thư mục báo cáo). Phụ lục A của báo cáo trình bày chi tiết quy trình
triển khai bằng Docker Hub.

## Giới hạn đã biết

- Không exactly-once xuyên pipeline. MQTT QoS1 + manual ACK sau Kafka delivery
  thu hẹp cửa sổ mất mát nhưng không loại bỏ hoàn toàn; dedup dựa trên
  `event_id` ở Q3 và batch.
- Parquet và PostgreSQL không atomic với nhau: crash giữa hai sink để lại
  staging rows, xử lý bằng rerun.
- Mosquitto chạy anonymous, không TLS/SASL/ACL — chỉ dùng cho demo.
- Mục tiêu 30 GB trong 4 giờ chưa chạy; phiên `load15-clean-20261002` đạt
  15 GB trong 4 giờ.
