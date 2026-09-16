# TEST REPORT — IoT Big Data demo stack

Ngày lập: 2026-09-14. Máy dùng để viết code (không phải máy nghiệm thu mục tiêu):
Ubuntu x86_64, Ryzen 5 6600H (12 logical CPU), RAM 14 GiB nhưng **available chỉ
~3.6 GiB**, root disk **còn ~11 GB**, TCP **1883 đã bị project khác chiếm**.

Quy ước kết quả:
- **PASS/OK** — đã chạy trên máy này, có lệnh + output.
- **DEFERRED** — không chạy được trên máy này do thiếu tài nguyên/nguy cơ disk;
  phải chạy trên máy mục tiêu (RAM 8–10 GB, ≥ 15 GB disk trống, port rảnh).
- **BLOCKED** — bị chặn cụ thể; liệt kê lý do.

## 1. Đã kiểm tra trên máy này (static / unit)

| Kiểm tra | Lệnh | Kết quả |
|---|---|---|
| Preflight host (read-only) | `implementation/scripts/preflight.sh` | **Ran OK — exit 2 (READY WITH WARNINGS)**, đúng thực tế host: Docker 29.1.3 + Compose v5.5.0 OK; RAM available 3.9 GB < 4.6 GB budget → WARN; `/data` chưa tồn tại → WARN; **TCP 1883 busy (0.0.0.0:1883, project khác) → WARN với gợi ý `MQTT_HOST_PORT`**; 9092/8000/8088/5432 free; disk `/var/lib/docker` 11 GB → WARN |
| Unit tests (validation, tie-break, multi-row conflict-key, hourly idempotency, batch window, docker-stats parsing, JDBC staging column guard) | `python3 -m unittest discover -s implementation/tests` | **OK — 81/81 tests pass** (55 ban đầu + 23 cho reviewer findings B1/M1 + 3 guard cột JDBC staging cho F4) |
| Python syntax toàn bộ jobs/backend/bridge/simulator/DAG | `python3 -m py_compile …(10 file)` | **OK** |
| Shell syntax toàn bộ scripts + run wrappers | `bash -n` trên 11 file | **OK** |
| Compose file parse + resolve 3 profiles | `docker compose -f implementation/compose.yaml config` (thêm `--profile live|batch|airflow`) | **OK (exit 0)** — env anchors merge đúng, mem_limit resolve đúng (kafka 1GiB, spark-stream/batch 2560MiB, scheduler 3072MiB, …) |
| Version pins tồn tại thật | PyPI JSON API, Docker Hub tag API, Maven Central HEAD | **OK** — chi tiết bảng dưới |
| Maven jar checksums | tải + `sha256sum` | **OK 5/5** — 4 jar đầu xác minh 2026-09-14 lúc authoring; `kafka-clients-3.9.2.jar` (9,220,052 bytes) đã tải toàn phần + sha256 pin ngày 2026-09-14 trong remediation F1; cả 5 checksum tái xác minh bằng download thật 2026-09-14 (remediation) |

Version pins đã verify (ngày 2026-09-14):

| Thành phần | Giá trị | Nguồn xác minh |
|---|---|---|
| apache/spark | `4.2.0-java21-python3` | Docker Hub tag list |
| apache/kafka | `4.3.1` | Docker Hub tag list |
| eclipse-mosquitto | `2.0.22` | Docker Hub tag list |
| postgres | `17.11` | Docker Hub tag list |
| nginx | `1.27-alpine` | Docker Hub tag list |
| apache/airflow | `2.10.5-python3.12` | Docker Hub tag list |
| paho-mqtt | `2.1.0` | PyPI + wheel source (đọc `client.py`: `manual_ack=True`, `ack(mid, qos)`) |
| confluent-kafka | `2.10.1` | PyPI release JSON |
| psycopg2-binary | `2.9.10` | PyPI release JSON |
| fastapi | `0.141.1` | PyPI latest |
| uvicorn | `0.52.4` | PyPI latest |
| spark-sql-kafka-0-10_2.13 | `4.2.0` | Maven Central (jar + POM) |
| spark-token-provider-kafka-0-10_2.13 | `4.2.0` | Maven Central |
| kafka-clients | `3.9.2` | Maven Central + POM của connector (dependency khai báo); sha256 `014b4ef36884478a932641818b080c36a8f8e2e92dc771e800c9d2694f4f83d8` (tải toàn phần 2026-09-14) |
| commons-pool2 | `2.13.1` | Maven Central + POM |
| postgresql JDBC | `42.7.4` | Maven Central |
| spark-4.2.0-bin-hadoop3.tgz | 555,409,690 bytes | archive.apache.org HEAD (dùng gián tiếp qua COPY multi-stage) |

Unit tests phủ các semantics bắt buộc (không cần Docker/PySpark/PostgreSQL —
`pg_sink` chạy nguyên câu SQL upsert trên SQLite in-memory):

- Validation: thiếu field, timestamp sai/naive/offset, alias `value`, non-numeric,
  out-of-bounds (bao gồm biên ±50/200 theo nghĩa inclusive), >35 C vẫn valid,
  late valid không quarantine, thứ tự ưu tiên reason.
- `sensor_latest`: newer-overwrite, older-late không ghi đè, tie-break theo
  received_at rồi event_id, độc lập thứ tự insert; batch nhiều event cùng
  sensor rút về đúng một winner deterministic (B1).
- `alerts`: replay không tăng hàng, threshold dùng so sánh "strictly above".
- `processed_events`: replay tăng `duplicate_count` không mọc hàng; payload
  conflict phát hiện được; batch chứa cùng event_id nhiều lần insert đúng một
  dòng (first occurrence = vị trí Kafka thấp nhất) với `duplicate_count` =
  số dư trong batch, event đã có sẵn thì cộng đúng cỡ batch (B1).
- `resource-report` parsing: hàng `docker stats` TAB-delimited giữ nguyên
  `MemUsage` chứa khoảng trắng; hàng thiếu trường/rỗng bị skip an toàn (M1).
- `gold.sensor_hourly`: rerun không cộng lặp, rerun với data mới thay thế,
  độc lập giữa sensor/giờ.
- Batch window: từ chối non-hour boundary, end ≤ start, timestamp naive/garbage;
  `[start,end)` đúng ở cả hai biên; hour của mọi event trong window nằm trọn
  trong window.
- JDBC staging column guard (F4): output của `df.select(...)` trong
  `apply_stream_batch`/`apply_hourly_batch` khớp tên-cột DDL của
  `staging_stream_events`/`staging_hourly_agg` trong `schema.sql` (parse tĩnh
  stdlib-only), nguồn select nằm trong `SILVER_SCHEMA` — chặn regression
  JDBC map-by-name fail trên PostgreSQL.

### 1.1 Remediation F1 (2026-09-14) — Airflow image thiếu Spark application artifact

Tester blocker F1: stage `spark-base` của `airflow/Dockerfile` là raw
`apache/spark:4.2.0-java21-python3` nên `COPY --from=spark-base /opt/spark`
chỉ copy distribution — thiếu `jobs/`, `run/run-batch.sh` và connector/JDBC
jars → DAG fail chắc chắn. Đã sửa:

- `spark/install-jars.sh` (mới): installer jar pin dùng chung cho cả hai image,
  5/5 jar có sha256 (kafka-clients pin mới; cả 5 tái xác minh bằng download
  thật 2026-09-14).
- `spark/Dockerfile`: dùng installer dùng chung thay vì inline curl (nội dung
  image không đổi: cùng 5 jar, cùng vị trí, cùng warmup smoke).
- `airflow/Dockerfile`: viết lại multi-stage tự thân — stage `spark-app` tự
  bake jars + COPY `spark/jobs/` + `spark/run/`, stage airflow copy nguyên
  `/opt/spark` → image chứa đúng artifact như `bigdata-iot-spark:1.0.0`,
  KHÔNG phụ thuộc image spark có sẵn trong local cache. Thêm build-time
  assertions (`test -x run-batch.sh`, `test -f batch.py`, glob 3 jar,
  `spark-submit --version`) + warmup smoke dưới combo thật Spark 4.2.0 +
  Java 17. `COPY dags/` đổi thành `COPY airflow/dags/` theo context mới.
- `compose.yaml`: 2 service airflow build với `context: .` +
  `dockerfile: airflow/Dockerfile`; `airflow-init` chạy assertion runtime
  (`test -x run-batch.sh && test -f batch.py && ls jars`) trước
  `airflow db migrate`.
- `implementation/.dockerignore` (mới): allowlist chỉ gửi `spark/` + `airflow/`
  vào build context (loại `.env` khỏi daemon, context nhỏ).
- `scripts/init.sh`: bỏ bước build spark-first (không còn image build-order
  dependency) → một lệnh `build` (F2/F3 về sau chuẩn hoá thành
  `"${COMPOSE[@]}" --profile live --profile batch --profile airflow build`).

Checks chạy lại sau remediation (static, không Docker build): compose config
exit 0 cho default + 3 profiles (context resolve đúng `implementation/` +
`airflow/Dockerfile`, assertion command render đúng); `bash -n` OK cho
`install-jars.sh` + `init.sh`; py_compile DAG OK; unit tests 55/55 OK; đối
chiếu tĩnh mọi COPY source tồn tại và thuộc allowlist `.dockerignore`.
Docker build/runtime vẫn DEFERRED — không claim pass runtime.

## 2. DEFERRED — phải chạy trên máy mục tiêu

Mọi mục dưới đây **chưa được chạy trên bất kỳ máy nào**. TEST REPORT này KHÔNG
chứa bất kỳ số đo runtime nào; không được suy ra pass từ static checks.

| # | Việc | Lệnh | Điều kiện |
|---|---|---|---|
| D1 | Build images (spark ≈ 1.4 GB, airflow ≈ 2 GB, …) | `implementation/scripts/init.sh` | ≥ 15 GB disk trống; cần internet tới Maven Central. Airflow image build tự thân (không cần build spark trước) |
| D2 | Live pipeline end-to-end + e2e fixture | `run-mode.sh live && scripts/test-e2e.sh` | RAM available ≥ 4.6 GB; port 1883/9092/8000/8088/5432 rảnh (hoặc đổi qua `.env`) |
| D3 | Quarantine/manifest/assertions với fault-inject seed 42 | (trong test-e2e.sh) | — |
| D4 | Batch Gold + rerun idempotency trên data thật | `run-mode.sh batch --start … --end …` | Sau D2 |
| D5 | Airflow LocalExecutor chạy DAG hourly | `run-mode.sh airflow` + trigger thủ công | Sau D4; image Airflow build OK |
| D6 | Recovery: SIGKILL spark-stream, restart bridge | `scripts/test-recovery.sh` | Sau D2 |
| D7 | Resource budget thực đo | `scripts/resource-report.sh --once` / `--interval` | Sau D2; ghi peak RAM/CPU, backlog, disk growth |
| D8 | Load 1 event/s × 10–15 phút và 10 event/s × 30–60 phút (plan §9.12) | simulator `--rate 1/--rate 10` | Máy mục tiêu; không tự thêm nếu thiếu RAM |
| D9 | Backup/restore thực hành (pg_dump + tar DATA_ROOT) | xem README mục Backup | — |
| D10 | Warmup jar smoke bên trong image build | chạy tự động trong `spark/Dockerfile` VÀ `airflow/Dockerfile` (D1) | Log build phải hiện `[warmup] OK kafka010=… pgjdbc=…` cho CẢ HAI image; airflow build phải qua hết build-time assertions (test run-batch.sh/batch.py/jars + spark-submit --version) |

## 3. BLOCKED trên máy hiện tại

Preflight chạy thật (mục 1) xác nhận đúng các blocker sau:

- **Disk ~11 GB free** — dưới ngưỡng an toàn để build 2 image lớn (spark + airflow
  ~3.5 GB trước cache) và chạy dữ liệu demo → D1, D5 bị chặn trên máy này.
- **RAM available ~3.9 GB** (đo 2026-09-14) — dưới ngân sách live 4.6 GiB;
  đồng thời JVM Kafka + Spark có thể OOM → D2–D8 bị chặn/khuyến nghị không chạy.
- **Port 1883** đang được nghe trên 0.0.0.0 (project khác) — script
  KHÔNG tự dừng container ngoài; dùng `MQTT_HOST_PORT=1884` trong `.env` nếu
  phải chạy trên máy này.
- Static checks/unit tests/preflight vẫn chạy được và đã chạy (mục 1) vì không cần
  Docker image lớn.

## 4. Checklist cho tester (máy mục tiêu)

1. `implementation/scripts/preflight.sh` — mong đợi exit 0 (hoặc 2 với warning
   đã hiểu); ghi output.
2. `cp implementation/.env.example implementation/.env` — chỉnh ports/credentials;
   đặc biệt MQTT_HOST_PORT nếu 1883 bận.
3. `implementation/scripts/init.sh` — build + postgres schema + kafka topic;
   log build phải có warmup OK.
4. `implementation/scripts/run-mode.sh live` → `wait-for-health` → dashboard/API.
5. `implementation/scripts/test-e2e.sh` — PASS hết; lưu manifest/delivery log.
6. `implementation/scripts/test-recovery.sh` — resume PASS; ghi loss window thật.
7. `run-mode.sh batch --start <hour UTC> --end <hour+1>` — đối chiếu count/avg
   fixture tính tay; rerun 2 lần → số không đổi.
8. `run-mode.sh airflow` — unpause + trigger DAG, kiểm task log + kết quả Gold.
9. `scripts/resource-report.sh --interval 30 --count 10` trong lúc load; điền
   kết quả thật vào phần "Measured results" bên dưới (hiện đang trống).

## 5. Measured results (chỉ điền khi có evidence thật)

- End-to-end latency (ingest_time → API): _DEFERRED — chưa đo_
- Peak RAM/CPU per service: _DEFERRED — chưa đo_
- Kafka/file backlog: _DEFERRED — chưa đo_
- Disk growth: _DEFERRED — chưa đo_
- Loss window (recovery): _DEFERRED — chưa đo_
