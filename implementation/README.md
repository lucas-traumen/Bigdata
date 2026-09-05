# Implementation — Nền tảng Big Data IoT (Phase 1)

Source code triển khai cho kiến trúc mô tả trong báo cáo
(`Documents/report/`): Sensor Simulator → Kafka (KRaft) → Spark Structured
Streaming → Delta Lake trên HDFS (Bronze → Silver → Gold), kèm observability
(Prometheus/Grafana/JMX).

> **Báo cáo LaTeX KHÔNG bị sửa.** Mọi độ lệch version giữa báo cáo và triển
> khai thực tế được ghi ở mục "Độ lệch so với báo cáo" bên dưới.

## Cấu trúc thư mục

```
implementation/
├── infra/
│   ├── docker-compose.yml        # toàn bộ service + mem_limit
│   ├── hadoop/common.env         # env→XML config cho image apache/hadoop
│   ├── jmx/kafka-config.yml      # cấu hình JMX exporter (scrape Kafka)
│   ├── prometheus/prometheus.yml
│   └── grafana/provisioning/datasources/datasource.yml
├── simulator/                    # Python producer (confluent-kafka)
│   ├── producer.py               # --rate / --duration / --events / --fault-inject
│   ├── requirements.txt
│   └── Dockerfile
├── spark/
│   ├── Dockerfile                # Spark 4.2.0 + Delta 4.4.0 + kafka connector
│   ├── conf/metrics.properties   # Prometheus servlet cho master/worker
│   ├── jobs/
│   │   ├── common.py             # path HDFS, schema, SparkSession
│   │   ├── bronze.py             # streaming Kafka → Bronze (dedup + watermark)
│   │   ├── silver.py             # batch Bronze → Silver + quarantine (MERGE)
│   │   ├── gold.py               # streaming window 1m/5m
│   │   ├── count-tables.py       # helper đếm row (smoke test)
│   │   └── check_kafka_topic.py  # helper chờ topic (run-bronze.sh)
│   └── run/                      # wrapper spark-submit
│       ├── run-bronze.sh
│       ├── run-silver.sh         # vòng lặp batch mỗi 30s (hoặc --once)
│       └── run-gold.sh
├── scripts/
│   ├── init-cluster.sh           # tạo dir HDFS + topic sensor.raw
│   └── smoke-test.sh             # 10k event → đếm Kafka/Bronze/Silver/Gold
└── README.md                     # file này
```

## Version thực tế đã chọn (và lý do)

| Thành phần | Báo cáo ghi | Triển khai thực tế | Lý do |
|---|---|---|---|
| Apache Spark | 4.2 | **4.2.0** (`apache/spark:4.2.0`, JDK 21, Scala 2.13) | Khớp đúng báo cáo; image chính thức tồn tại |
| Delta Lake | — (chỉ nói tương thích) | **4.4.0** (artifact `delta-spark_4.2_2.13`) | Delta 4.4.0 là release đầu tiên hỗ trợ Spark 4.2.0 (phát hành 2026-08) |
| Apache Kafka | 4.3 (KRaft, bỏ ZooKeeper) | **4.3.1** (`apache/kafka:4.3.1`) | Khớp đúng báo cáo; image chính thức, KRaft native |
| Hadoop/HDFS | 3.5 | **3.5.0** (`apache/hadoop:3.5.0`, JDK 17) | Khớp đúng báo cáo; image chính thức |
| confluent-kafka (Python) | — | **2.10.1** | bản ổn định mới nhất cho Python 3.12 |

Kết hợp Spark 4.2.0 + Delta 4.4.0 xác minh qua Maven Central (tồn tại
artifact `delta-spark_4.2_2.13:4.4.0`) và release note chính thức của Delta
("Delta Spark 4.4.0 is built for Apache Spark 4.2.0").

## Độ lệch so với báo cáo

1. **Replication factor của topic**: báo cáo/kế hoạch ghi RF=2. Stack này chỉ
   có 1 broker (giới hạn RAM), Kafka không cho phép RF > số broker → topic
   `sensor.raw` được tạo với **RF=1, 3 partitions**. Nâng lên 3 broker ở
   Phase 2/đường hướng phát triển thì đặt lại RF=2..3 được ngay trong
   `scripts/init-cluster.sh`.
2. **Số liệu benchmark** vẫn là minh hoạ (theo báo cáo); implementation này
   chỉ tạo điều kiện chạy thật sau này, không sinh số liệu giả.
3. Image `apache/hadoop:3.5.0` dùng cơ chế cấu hình bằng biến môi trường
   `CORE-SITE.XML_<key>=<value>` (xem `infra/hadoop/common.env`) — chi tiết
   triển khai, không thay đổi kiến trúc.

## Yêu cầu hệ thống

- Docker + Docker Compose v2 (đã kiểm tra: Docker 29.1.3, Compose v5.5.0).
- Tổng `mem_limit` của stack ≈ **13.4GB** — máy cần ≥16GB RAM vật lý
  (hoặc đóng bớt ứng dụng để available ≥ 13.5GB). Disk: image ≈ 4.5GB,
  dữ liệu demo vài trăm MB.

## Chạy từng bước

```bash
cd implementation/infra

# 1. Build 2 image tự build (spark + simulator)
docker compose build

# 2. Khởi động toàn bộ stack
docker compose up -d

# 3. Xem health từng service (chờ tất cả healthy)
docker compose ps

# 4. Init HDFS dir + Kafka topic (idempotent)
../scripts/init-cluster.sh

# 5. Smoke test end-to-end (10.000 event sạch)
../scripts/smoke-test.sh
```

### Chạy lại với dữ liệu khác / fault injection

```bash
# Simulator chạy nền với cấu hình riêng:
docker compose run --rm sensor-simulator python producer.py \
  --rate 1000 --duration 60 --fault-inject

# Silver một lần duy nhất (thay vì vòng lặp):
docker compose run --rm spark-silver /opt/spark/run/run-silver.sh --once
```

### Kiểm tra từng service

| Service | Cách kiểm tra |
|---|---|
| HDFS NameNode | `curl http://localhost:9870/jmx` hoặc UI http://localhost:9870 |
| HDFS data | `docker exec hdfs-namenode hdfs dfs -ls -R /delta \| head` |
| Kafka topic | `docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic sensor.raw` |
| Kafka offset | `docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sensor.raw` |
| Spark master UI | http://localhost:8080 (2 worker, job đang chạy) |
| Đếm row Delta | `docker compose run --rm spark-silver /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/count-tables.py` |
| Prometheus | http://localhost:9090/targets (tất cả phải UP) |
| Grafana | http://localhost:3000 (admin/bigdata), datasource Prometheus auto-provisioned |
| Kafka JMX metrics | `curl localhost:9090/api/v1/query?query=kafka_server_BrokerTopicMetrics_MessagesInPerSec_OneMinuteRate` (qua Prometheus) |
| Kafka exporter metrics | `curl` target kafka-exporter:9308 trong mạng nội bộ; xem qua Prometheus UI |

### Dừng và dọn dẹp

```bash
docker compose down            # dừng, giữ volume (data còn)
docker compose down -v         # dừng + xoá toàn bộ data HDFS/Kafka
```

## Thiết kế đáng chú ý

- **Bronze**: `from_json` giữ mọi field dạng STRING → schema không bao giờ vỡ
  vì 1 event lỗi; việc ép kiểu + lọc chuyển xuống Silver. Dedup dùng
  `withWatermark("event_ts", "10 minutes") + dropDuplicates(["event_id"])` —
  đúng thiết kế chương 3. Checkpoint trên HDFS (`/_checkpoints/bronze`).
- **Silver**: batch chạy vòng lặp 30s, theo dõi version đã xử lý của Bronze
  (file state trên HDFS) và dùng `MERGE INTO` theo `event_id` → idempotent,
  chạy lại không nhân bản row. Record lỗi vào `silver_quarantine` kèm
  `quarantine_reason` (`missing_or_invalid_event_time`,
  `missing_or_non_numeric_value`, `value_out_of_physical_bounds`,
  `missing_field:<ten>`).
- **Gold**: streaming từ Silver, tumbling window 1 phút và 5 phút theo
  `location × sensor_type`, agg avg/min/max/count.
- **Job runner tách riêng** (`spark-bronze/silver/gold`): driver JVM không
  chiếm RAM của `spark-master`; mỗi job tự restart khi gặp lỗi nhờ
  `restart: unless-stopped`.

## Bảng ngân sách bộ nhớ (mem_limit)

| Service | mem_limit | Ghi chú |
|---|---|---|
| hdfs-namenode | 1536m | heap 768m |
| hdfs-datanode-1/2 | 896m × 2 | heap 640m |
| kafka-1 | 1536m | heap 1024m |
| jmx-kafka | 256m | JVM nhỏ |
| kafka-exporter | 128m | Go, rất nhẹ |
| spark-master | 768m | driver job KHÔNG chạy ở đây |
| spark-worker-1/2 | 2176m × 2 | worker 1792m + daemon 256m + executor overhead |
| spark-bronze/silver/gold | 700m × 3 | driver client-mode (heap 512m + off-heap + python) |
| sensor-simulator | 384m | Python |
| prometheus | 384m | retention 2 ngày |
| grafana | 384m | |
| **Tổng** | **≈13.3g** | ≤ 13.5g theo kế hoạch |

Executor của mỗi job: 640m, nên cả 3 job (bronze + gold streaming, silver
batch) cùng chạy vẫn vừa trong 2 worker (worker memory 1792m/worker ≈ 2
executor/worker). Muốn tăng tốc thì sửa `spark.executor.memory` trong
`spark/run/*.sh`.

## Chưa verify do thiếu tài nguyên

Mục này ghi lại phần chưa chạy thử được trên máy thật (RAM available tại
thời điểm viết code thấp hơn ngân sách stack). Orchestrator/tester sẽ verify
lại khi máy đủ RAM:

- _(cập nhật sau khi chạy thử — xem `.ai/state/current-task.md`)_
