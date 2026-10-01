# Plan: Triển khai Big Data IoT bằng Docker trên máy RAM 8–10 GB

## Trạng thái và approval gate

- **Trạng thái hiện tại:** `READY_FOR_HANDOFF — runtime acceptance deferred`.
- **Ngày lập plan:** 2026-09-13; user xác nhận triển khai artifact Docker ngày 2026-09-14.
- **Phạm vi approval:** user muốn triển khai code/config/documentation để đem sang
  máy khác chạy vào ngày kế tiếp; không yêu cầu runtime test trên host hiện tại.
- **Mặc định thực thi:** coder dùng các lựa chọn tối thiểu đã nêu ở mục 5 (plain
  Parquet, một PostgreSQL hai database, Airflow scheduler không webserver,
  observability ngoài profile mặc định), không tự mở rộng phạm vi. Nếu gặp quyết định
  làm thay đổi contract hoặc kiến trúc, phải dừng và báo orchestrator.
- **Không tự động commit hoặc push.**
- Task Tuần 3 trước đã được bảo toàn tại `.ai/plans/archive/2026-09-13-week3-lakehouse-awaiting-acceptance.md`; không coi việc archive là user acceptance của task đó.

## 1. Mục tiêu

Xây dựng một bản thử nghiệm chạy được trên **một máy Linux** với Docker Engine + Docker Compose plugin, ưu tiên một luồng end-to-end nhỏ nhưng đầy đủ:

```text
Simulator -> MQTT Broker -> Python Bridge -> Kafka sensor_raw
          -> một Spark Structured Streaming app (Q1/Q2a/Q2b/Q3)
          -> local Parquet (Bronze/Silver/Quarantine)
          -> PostgreSQL (serving + Gold)
          -> FastAPI -> static Dashboard
```

Spark batch đọc trực tiếp Silver để tính `gold.sensor_hourly`; Airflow dùng
`LocalExecutor` để gọi **đúng chương trình batch đó** theo lịch. Mục tiêu bản đầu là
đúng dữ liệu, idempotency ở các bảng phục vụ, có thể restart/replay, không OOM ở tải
thử và có bằng chứng đo RAM/CPU/backlog/dung lượng/độ trễ. Không tuyên bố
exactly-once xuyên toàn bộ nhiều sink.

## 2. Bằng chứng hiện trạng repository và máy

### Repository

- HEAD hiện tại là `bdf01f8`; `implementation/` đã có một stack cũ nhưng khác yêu cầu:
  Simulator -> Kafka trực tiếp -> Spark standalone (master + 2 worker + 3 job runner)
  -> Delta trên HDFS, tổng `mem_limit` README ghi khoảng 13.3 GB.
- Có thể tái sử dụng: generator/fault profile của simulator, schema/provenance cơ bản,
  validation bounds `[-50, 200]`, các ý tưởng checkpoint/retry/smoke test và cấu trúc
  README/version table.
- Phải thay: HDFS, Delta, Spark standalone, ba job/container riêng, Kafka trực tiếp từ
  simulator, cấu hình compose phẳng, các đường dẫn HDFS, và Airflow Dockerfile đang
  thiếu `implementation/airflow/dags/`.
- Chưa có MQTT bridge, PostgreSQL schema/app sink, FastAPI, dashboard, Compose profiles,
  local bind-mounted data layout, mode/resource/recovery scripts.
- `Documents/report/` và `homework/` là tài liệu khác; không sửa báo cáo chính trong
  task này trừ khi user phê duyệt mở rộng phạm vi riêng.
- GitNexus đã được kiểm tra nhưng repo này **chưa được index** (`gitnexus status` trả
  `Repository not indexed`). Không suy diễn kết quả graph; dùng filesystem/git diff
  làm bằng chứng. Có thể index sau khi implementation ổn định.

### Máy hiện tại (chỉ là môi trường phát hiện, chưa phải máy nghiệm thu mục tiêu)

Kết quả kiểm tra read-only ngày 2026-09-13:

- Ubuntu kernel 7.0.0-31, `x86_64`, AMD Ryzen 5 6600H, 6 physical/12 logical CPU.
- RAM vật lý khoảng 14 GiB, available khoảng 4.6 GiB tại thời điểm kiểm tra, swap 4 GiB.
- Docker Engine 29.1.3, Compose v5.5.0; Docker daemon báo khoảng 15.95 GB memory.
- Root filesystem 147 GB, chỉ còn khoảng 11 GB; không có `/data`.
- Có ba container của project khác đang chạy; `mobile_backend-mosquitto-1` đang chiếm
  host port 1883/9001. Script không được tự dừng các container ngoài project.

Vì vậy runtime acceptance trên máy này có thể bị **blocked** bởi disk/available RAM
và port conflict. Code/config vẫn có thể triển khai; báo cáo chỉ ghi “đã chạy” khi có
log/command evidence trên máy đủ điều kiện. Preflight phải phát hiện và báo rõ sai lệch.

## 3. Kiến trúc đích và semantics

### 3.1 Services và ownership

- `postgres`: một PostgreSQL instance, hai database tách biệt: `app` cho serving/Gold
  và `airflow_meta` cho metadata Airflow.
- `mqtt`: Mosquitto, subscribe topic `sensors/<sensor_id>/telemetry` từ simulator
  hoặc client ngoài.
- `bridge`: Python Paho MQTT QoS 1 -> Kafka producer `acks=all`, Kafka key là
  `sensor_id`, publish nguyên payload. Dùng bounded in-memory queue; lỗi publish có
  retry/backoff và log `event_id`/lý do.
- `kafka`: một broker KRaft, topic chính xác `sensor_raw`, 3 partitions, RF=1.
- `spark-stream`: **một** `spark-submit`, một `SparkSession`, chạy local mode
  `local[2]`; bốn query logic dùng checkpoint riêng.
- `backend`: FastAPI, một worker, kết nối PostgreSQL bằng service name `postgres`,
  pool nhỏ và retry khi DB chưa ready.
- `dashboard`: image tĩnh nhẹ (Nginx hoặc tương đương), proxy `/api` tới backend;
  browser chỉ cần truy cập host port dashboard.
- `spark-batch`: cùng Spark image/dependencies, one-shot `batch.py --start --end`.
- `airflow-scheduler`/`airflow-init`: profile Airflow, LocalExecutor, một DAG và
  tối đa một batch active.

Không có HDFS, Kubernetes, Spark standalone master/worker, Kafka multi-broker,
Delta Lake, Prometheus/Grafana trong profile mặc định của bản thử.

### 3.2 Contract và đường đi dữ liệu

Payload canonical bắt buộc có:

```json
{
  "event_id": "sim01-000001",
  "sensor_id": "TEMP_01",
  "event_time": "2026-09-13T10:00:00+07:00",
  "temperature_c": 36.2,
  "ingest_time": "2026-09-13T03:00:00.120Z"
}
```

`ingest_time` là thời điểm simulator phát bản tin để đo latency; timestamp có offset
được chuẩn hóa thành UTC trong Spark. Có thể giữ các trường tùy chọn tương thích code
cũ (`sensor_type`, `unit`, `location`, `sequence_no`) nhưng `temperature_c` là tên
chuẩn mới. Parser có thể hỗ trợ `value` như legacy alias nếu điều đó không làm mơ hồ
contract; README phải ghi rõ mapping.

- **Q1:** Kafka `sensor_raw` -> Bronze Parquet. Giữ `raw_payload`, topic, partition,
  offset, Kafka timestamp, MQTT topic/header nếu có, `received_at_utc`; không bỏ payload
  JSON lỗi và không dedup ở Bronze.
- **Q2a:** file stream Bronze mới -> parse/cast/normalize/validate -> Silver Parquet.
- **Q2b:** cùng DataFrame/rule phân loại với Q2a -> Quarantine Parquet, kèm
  `error_reason` và `record_key`. Hai nhánh loại trừ nhau, không để record biến mất.
- **Q3:** file stream Silver mới -> `foreachBatch`/staging transaction vào PostgreSQL:
  upsert `sensor_latest` và insert-idempotent `alerts`. Nhiệt độ hợp lệ vượt ngưỡng
  minh họa 35°C vẫn vào Silver và tạo alert, không vào quarantine.

Quy tắc validation tối thiểu: thiếu required field, timestamp không parse được hoặc
`temperature_c` không numeric -> quarantine; physical bounds mặc định `[-50, 200]` là
cấu hình; late event hợp lệ không tự động bị loại. Silver có thể chứa các lần gửi lặp
đã chuẩn hóa; dedup kết quả bằng `event_id` ở Q3 và batch, không dùng state dedup tăng
vô hạn. Nếu `event_id` thiếu, quarantine dùng khóa provenance
`topic-partition-offset` để không làm mất record.

### 3.3 PostgreSQL và idempotency

DDL phải có ít nhất:

- `sensor_latest(sensor_id primary key, event_id, event_time, temperature_c,
  received_at, updated_at, ...)`: chỉ cập nhật nếu event time mới hơn; nếu cùng
  timestamp dùng thứ tự xác định `received_at`, sau đó `event_id`.
- `alerts(event_id, rule_id primary key, sensor_id, event_time, temperature_c,
  threshold, created_at)`: retry không tạo thêm hàng.
- `gold.sensor_hourly(sensor_id, hour_start primary key, event_count, avg_value,
  min_value, max_value, computed_at, ...)`: upsert toàn bộ aggregate, không cộng lặp.
- Bảng nội bộ `processed_events`/staging để replay Q3 an toàn và phát hiện duplicate
  hoặc payload conflict; không coi Spark checkpoint là khóa chống trùng DB.

Q3 dùng bounded micro-batch và staging/upsert SQL hoặc `foreachPartition`, tuyệt đối
không `collect()`/`toPandas()` toàn bộ lịch sử. Transaction có thể không atomic giữa
Parquet và PostgreSQL; README phải nêu giới hạn và cách reconciliation.

Batch:

- nhận `--start` và `--end` theo UTC, khoảng `[start,end)`, cả hai phải ở ranh giới
  giờ và chứa ít nhất một giờ đầy đủ; input không hợp lệ bị từ chối trước khi ghi;
- đọc Silver trực tiếp, `dropDuplicates(event_id)`, group theo sensor/giờ, ghi
  `gold.sensor_hourly` bằng upsert;
- rerun cùng khoảng cho kết quả xác định; dữ liệu đến muộn được sửa bằng cách chạy
  lại giờ liên quan. Không cộng dồn kết quả cũ.

### 3.4 API và dashboard

API tối thiểu:

- `GET /health` — trạng thái DB và thời điểm kiểm tra;
- `GET /api/sensors/latest` — latest theo sensor hoặc toàn bộ có phân trang;
- `GET /api/alerts?limit=&offset=` — alerts phân trang;
- `GET /api/stats?start=&end=&sensor_id=` — đọc Gold theo UTC;
- endpoint tra event/progress nội bộ (nếu cần) để đo thời điểm API thấy event.

Dashboard gọi API khoảng 3 giây/lần (cấu hình được), hiển thị latest, alerts, thống kê,
`last_updated_at` và trạng thái lỗi/kết nối. Khi streaming dừng, dữ liệu đã lưu vẫn
hiển thị cùng thời điểm cập nhật cuối.

## 4. Profiles, storage, network và ngân sách

### 4.1 Compose profiles và chuyển mode

Canonical file là `implementation/compose.yaml`, có một bridge network. Shared
services (PostgreSQL/backend/dashboard) không bị profile làm mất; data-plane services
được gắn profile:

- `live`: MQTT, bridge, Kafka, simulator, `spark-stream`.
- `batch`: one-shot `spark-batch`; script dừng simulator/bridge/stream sau khi drain,
  rồi chạy batch, giữ PostgreSQL/dashboard và mọi volume.
- `airflow`: `airflow-init` (one-shot) và `airflow-scheduler`; trước khi bật phải
  dừng simulator/bridge/stream, và không tạo nhiều batch đồng thời.

`scripts/run-mode.sh live|batch|airflow` phải stop đúng service của mode trước; không
dùng `down -v`, không xóa checkpoint trong restart thông thường. Batch tạo một tín hiệu
drain/đợi Spark xử lý hết dữ liệu hiện có rồi mới dừng; trạng thái/offset tiến độ phải
được ghi ở bind-mounted logs để script có thể kiểm chứng. Airflow scheduler chỉ chạy
khi profile Airflow được bật.

### 4.2 Dữ liệu và volume

`.env.example` định nghĩa `DATA_ROOT` (mặc định đề xuất `/data/bigdata`; có thể đổi
sang thư mục local khi không có quyền/sufficient disk), host ports và credentials mẫu.
Bind mount cùng một đường dẫn container `/data/bigdata` cho Spark/Airflow/backend khi
cần, gồm:

```text
/data/bigdata/{bronze,silver,quarantine,checkpoints,logs,control}
```

Kafka và PostgreSQL dùng named volume. Q1/Q2a/Q2b/Q3 có checkpoint riêng:
`checkpoints/q1`, `q2a`, `q2b`, `q3`; không xóa khi restart. Spark chỉ đọc các file
Parquet đã commit hoàn tất; không sửa/ghi đè input của file stream.

### 4.3 Ngân sách khởi đầu

| Service | `mem_limit` | Cấu hình bắt buộc |
|---|---:|---|
| Kafka | 1024 MiB | một KRaft broker, heap 512 MiB, `sensor_raw`, 3 partition/RF=1 |
| Spark streaming | 2560 MiB | một app, `local[2]`, driver heap 1 GiB, shuffle partitions=2, input cap |
| PostgreSQL | 512 MiB | ít connection, `shared_buffers`/`max_connections` phù hợp |
| MQTT | 64 MiB | Mosquitto |
| Bridge | 128 MiB | bounded queue, retry/backoff, publish error log |
| Simulator | 64 MiB | rate/sensor count configurable |
| Backend | 256 MiB | FastAPI, một worker |
| Dashboard | 64 MiB | static build |

Tổng giới hạn nhóm khoảng 4.6 GiB, chưa tính OS/Docker cache. JVM heap là một phần
container memory; phải chừa overhead Python/off-heap. Airflow mode có ngân sách riêng:
Spark batch dùng lại tối đa 2560 MiB, scheduler khởi đầu khoảng 384 MiB và init là
one-shot nhỏ, tổng tiến trình Airflow + Spark batch khoảng 3 GiB trước khi đo lại.

Không chạy đồng thời live và Airflow trên máy 8 GB. Trên 10 GB chỉ thử đồng thời sau
khi có peak `docker stats` và available RAM chứng minh còn margin.

## 5. Mặc định cần user xác nhận

Các lựa chọn sau là đề xuất để tránh tự suy diễn từ stack cũ:

1. **Plain local Parquet, không Delta:** đúng câu chữ yêu cầu; đổi lại idempotency
   được thực hiện ở PostgreSQL/staging/upsert và batch, không có ACID/time travel của
   Delta.
2. **Contract temperature-first:** bốn trường mẫu là bắt buộc, `ingest_time` thêm để
   đo latency; các trường 9-field cũ chỉ là optional/legacy.
3. **Không bật Prometheus/Grafana mặc định:** đo tài nguyên bằng script + Docker stats
   để giữ ngân sách; có thể thêm profile observability sau, ngoài task này.
4. **Một PostgreSQL instance, hai database:** `app` và `airflow_meta`, thay vì thêm
   container metadata DB.
5. **Airflow scheduler không có webserver trong bản đầu:** log/DAG đủ nghiệm thu,
   dashboard ứng dụng là UI chính; thêm webserver chỉ khi đo RAM cho phép.
6. **Không sửa `Documents/report/`:** README/docs implementation ghi rõ độ lệch với
   báo cáo học thuật hiện còn mô tả HDFS/Delta/standalone.
7. **Host hiện tại chỉ dùng preflight/build nếu đủ disk:** port MQTT 1883 có thể đổi
   qua `.env`; không tự dừng project khác và không ghi runtime evidence giả.

Nếu user muốn một lựa chọn khác, coder phải cập nhật plan trước khi triển khai.

## 6. Phạm vi file dự kiến

### Tạo hoặc viết lại

- `implementation/compose.yaml`, `implementation/.env.example`, `.gitignore` entries.
- `implementation/mosquitto/mosquitto.conf`.
- `implementation/bridge/{bridge.py,Dockerfile,requirements.txt}`.
- `implementation/simulator/{producer.py,Dockerfile,requirements.txt}` với MQTT QoS 1,
  event manifest/log và fault injection.
- `implementation/spark/Dockerfile` (Spark local + Kafka/JDBC dependencies, không Delta),
  `spark/jobs/{common.py,validation.py,pg_sink.py,stream_app.py,batch.py}`,
  `spark/run/{run-stream.sh,run-batch.sh}`.
- `implementation/sql/{init-databases.sql,schema.sql,queries.sql,test-data.sql}`.
- `implementation/backend/{main.py,Dockerfile,requirements.txt}`.
- `implementation/dashboard/{index.html,nginx.conf,Dockerfile}`.
- `implementation/airflow/Dockerfile`, `airflow/dags/iot_pipeline.py`.
- `implementation/scripts/{preflight.sh,prepare-host.sh,init.sh,run-mode.sh,
  reset-pipeline.sh,resource-report.sh,test-e2e.sh,test-recovery.sh,wait-for-health.sh}`
  và helper publish/fixture nếu cần.
- `implementation/docs/{ARCHITECTURE.md,TEST_REPORT.md}`.
- Unit tests nhẹ trong `implementation/tests/` cho validation, tie-break latest,
  hour-boundary và aggregate idempotency.

### Xóa hoặc loại khỏi đường chạy (chỉ các file obsolete của stack cũ)

- HDFS config/service và toàn bộ Spark standalone master/worker/job-runner definitions.
- Delta-specific jobs/maintenance/count helpers và HDFS-based run scripts.
- JMX/Prometheus/Grafana configs không còn được dùng trong default profile.

Git history vẫn giữ code cũ; không xóa `Documents/report/`, `homework/`, `.opencode/`
hoặc các artifact ngoài scope.

## 7. Trình tự triển khai (tracer-bullet)

1. **M0 — preflight và hợp đồng:** lock versions, tạo `.env.example`, host directory
   checks, SQL skeleton, Compose network/healthchecks; không chạy destructive command.
2. **M1 — ingestion mỏng nhất:** Mosquitto + simulator MQTT + bridge + Kafka topic;
   test đối chiếu event IDs và Kafka delivery/ack/error log.
3. **M2 — Q1:** một Spark app local mode ghi raw Bronze Parquet, checkpoint riêng,
   bounded input; test giữ được cả payload lỗi và provenance Kafka.
4. **M3 — Q2a/Q2b:** parser/rule dùng chung, Silver/Quarantine tách đường dẫn,
   timestamp UTC, physical bounds, không bỏ sót; unit + e2e fixture.
5. **M4 — Q3/serving:** PostgreSQL DDL, staging/upsert/unique constraints, latest và
   alerts; FastAPI/dashboard; kiểm tra event cũ, duplicate và retry.
6. **M5 — batch:** `batch.py` với `[start,end)`, dedup event_id, hourly Gold và rerun;
   mode script drain/stop/launch một job.
7. **M6 — Airflow:** custom image có Spark/Java/dependencies thật, metadata DB riêng,
   LocalExecutor/parallelism=1, `max_active_runs=1`, DAG gọi command batch chính xác,
   retry có kiểm soát.
8. **M7 — recovery/measurement/docs:** restart giữ checkpoint/volume, kill bridge và
   Spark khi có dữ liệu, đo replay/loss window, chạy load 1 và 10 event/s theo lịch,
   ghi peak RAM/CPU/backlog/disk/latency thật, hoàn thiện README và test report.

Mỗi mốc phải để lại một luồng chạy được và log/check kết quả; không thêm tính năng mới
trước khi mốc trước có smoke evidence.

## 8. Ngoài phạm vi

- HDFS, Kubernetes, Spark cluster nhiều worker, Kafka nhiều broker/RF>1, Delta Lake,
  HA/production exactly-once, TLS/SASL/ACL, schema registry, cloud object storage.
- Adaptive watermark/runtime controller hoặc sửa cấu trúc/chương báo cáo học thuật.
- Tự động dừng/xóa container của project khác; tự động commit/push.
- Benchmark lớn hơn khả năng máy; không thay số minh họa trong báo cáo bằng số chưa đo.

## 9. Tiêu chí nghiệm thu khách quan

### Cấu hình và vận hành

1. `docker compose -f implementation/compose.yaml config` hợp lệ; image/package
   versions được pin và ghi trong README cùng lệnh kiểm tra thực tế.
2. `scripts/preflight.sh` kiểm OS/arch/CPU/RAM available/disk/Docker/port và báo rõ
   khi dưới ngân sách; `prepare-host.sh` tạo đúng bind directories/quyền.
3. `run-mode.sh live|batch|airflow` không để mode trước tiếp tục chạy, không xóa
   volume/checkpoint khi restart; healthcheck/retry/restart/log rotation có bằng chứng.

### Dữ liệu và correctness

4. Simulator -> MQTT -> bridge -> Kafka dùng đúng topic/path/key; manifest và log cho
   phép đối chiếu event ID gửi/ack/đến Kafka, kể cả fault injection.
5. Q1 lưu nguyên `raw_payload` và metadata Kafka; payload JSON/type lỗi vẫn hiện trong
   Bronze.
6. Q2a/Q2b dùng cùng rule và phân loại độc quyền: valid vào Silver, invalid vào
   Quarantine với lý do; giá trị >35°C vẫn Silver + alert, late valid không tự động
   quarantine.
7. Q3 `sensor_latest` không bị event cũ ghi đè; `alerts(event_id,rule_id)` không tăng
   khi replay/duplicate; conflict cùng timestamp xử lý theo tie-break đã ghi.
8. API trả latest/alerts phân trang/stats và dashboard hiển thị dữ liệu đã lưu cùng
   last update/error state khi stream dừng.
9. Batch từ chối range không ở ranh giới giờ/không đủ giờ; count/avg/min/max của
   fixture khớp tính tay; chạy lại cùng range không tạo hàng hoặc cộng lặp.
10. Airflow dùng LocalExecutor, một active DAG run/batch, gọi cùng `batch.py`, có log
    và retry; không giả định `spark-submit` tồn tại nếu image chưa chứa nó.

### Recovery và tài nguyên

11. Restart Spark/bridge trong lúc phát dữ liệu giữ checkpoint/volume và xử lý tiếp;
    test ghi rõ ID thiếu nếu crash xảy ra trước MQTT ACK/Kafka confirmation. Không
    dùng kết quả đó để claim exactly-once.
12. Có bằng chứng cho tải `1 event/s` trong 10–15 phút và `10 event/s` trong 30–60
    phút (hoặc ghi `BLOCKED` nếu máy không đủ): end-to-end latency từ `ingest_time`
    tới API, peak RAM/CPU, Kafka/file backlog, disk growth, OOM/restart. Event cố ý
    đến muộn được tách khỏi processing latency.
13. README có yêu cầu máy, cài/chạy, mount paths, API, mode/recovery/backup-restore,
    giới hạn QoS/ACK/loss window; `TEST_REPORT.md` chỉ ghi “đã chạy” khi có evidence.
    Secrets thật không nằm trong Git.

## 10. Verification bắt buộc sau implementation

Tester độc lập phải chạy, nếu tài nguyên cho phép:

```bash
docker compose -f implementation/compose.yaml config
implementation/scripts/preflight.sh
python -m unittest discover -s implementation/tests
implementation/scripts/init.sh
implementation/scripts/run-mode.sh live
implementation/scripts/test-e2e.sh
implementation/scripts/test-recovery.sh
implementation/scripts/resource-report.sh --once
implementation/scripts/run-mode.sh batch   # với khoảng UTC fixture
implementation/scripts/run-mode.sh airflow # manual DAG run/kiểm log
```

Tester không sửa production code hay chạy formatter/fixer. Nếu build/runtime bị chặn
bởi 11 GB disk, available RAM 4.6 GB hoặc port 1883 đang bận, báo `blocked` kèm lệnh
và output, không suy ra pass từ static inspection. Reviewer sau đó kiểm tra độc lập
diff, scope, idempotency, recovery semantics, security và test gaps.

## 11. Rủi ro và cách xử lý

- **RAM/disk không đúng giả định:** preflight fail-fast; dùng mode luân phiên, hạ tải,
  hoặc chuyển sang máy mục tiêu; không nới `mem_limit` âm thầm.
- **ACK MQTT/Kafka:** nếu Paho manual ACK khả dụng, chỉ ACK sau delivery callback;
  nếu bridge chết trước đó, log loss/retry window và kiểm thử riêng.
- **Hai sink không atomic:** PostgreSQL unique/upsert + staging và batch reconciliation;
  không claim exactly-once toàn pipeline.
- **File-stream consistency:** chỉ đọc committed Parquet, đường dẫn input/output
  tách biệt, checkpoint bất biến trong restart.
- **Late data:** không quarantine chỉ vì late; Gold theo UTC và rerun giờ liên quan,
  ghi rõ event đến muộn sau khi đã aggregate.
- **Airflow image/dependencies:** build smoke phải xác nhận Java, Spark, Kafka/JDBC
  connector và Python package cùng tồn tại; không chỉ cài Airflow.
- **Port/service ngoài project:** preflight cảnh báo; `.env` cho phép đổi host port;
  script không dùng `docker compose down` toàn hệ thống hoặc `docker system prune`.

## 12. Quyết định để user phê duyệt

User đã xác nhận theo hướng: **chỉ triển khai artifact; runtime sẽ chạy trên máy
khác**. Vì host hiện tại không đủ dung lượng/available RAM, coder không được coi việc
không chạy Docker ở đây là lỗi; phải cung cấp README/runbook để người dùng chạy ngày
kế tiếp và để tester xác minh trên máy mục tiêu khi có.

---

# PHẦN MỞ RỘNG — Rewrite báo cáo LaTeX + đa nguồn/đa chỉ số (2026-09-18)

> Phần này là task contract cho phạm vi MỚI, tách biệt khỏi §1–§12 ở trên (plan
> implementation bản nhẹ đã approve và đã commit ở `c4c01b5`). Phạm vi mới gồm hai
> nhánh song song: (A) rewrite báo cáo LaTeX cho khớp kiến trúc đã implement thật;
> (B) mở rộng implementation sang đa nguồn simulator + payload đa chỉ số môi trường.
> Cả hai nhánh đều CHỜ user approve trước khi giao coder/tester/reviewer. Không tự
> commit/push.

## E1. Trạng thái và approval gate (phần mở rộng)

- **Trạng thái:** `PLANNING — awaiting user approval`.
- **Ngày lập:** 2026-09-18.
- **Bằng chứng hiện trạng:** HEAD `c4c01b5` (implementation bản nhẹ đã commit);
  working tree sạch trừ `.opencode/` untracked. Báo cáo LaTeX (`Documents/report/`)
  hiện mô tả kiến trúc Delta Lake + HDFS + Spark standalone + Prometheus/Grafana —
  LỆCH hoàn toàn so với implementation thật (plain Parquet trên bind mount +
  PostgreSQL + Spark local[2] + MQTT/Bridge + script stats). Abstract + 5 chương đều
  chứa tham chiếu Delta/HDFS/Prometheus cần thay.
- **Nguyên tắc xuyên suốt (user chốt 2026-09-18):** demo dữ liệu/tài nguyên nhỏ
  nhưng kiến trúc giữ nguyên các seam cho phép mở rộng ngang; không co kiến trúc
  cho vừa dữ liệu nhỏ. Cái gì demo được trên 1 máy thì demo; TLS/HA đa máy chỉ ghi
  Hướng mở rộng. Code chỉ làm thứ ảnh hưởng trực tiếp đến chạy đúng; còn lại ghi
  vào tài liệu/báo cáo. "Kiến trúc có gì thì phản ánh hết vào báo cáo, đừng bớt."
- **Phân loại scope:** (i) VÀO CODE = thiếu thì pipeline không chạy/chạy sai;
  (ii) VÀO BÁO CÁO/HƯỚNG MỞ RỘNG = chỉ ghi, không đụng code implementation.

## E2. Nhánh A — Rewrite báo cáo LaTeX (phạm vi tài liệu, KHÔNG phải production code)

### E2.1 Mục tiêu

Thay thế nội dung báo cáo cũ/không liên quan (Delta/HDFS/Spark standalone/
Prometheus/Grafana/JMX) bằng nội dung phản ánh đúng hệ thống đã implement và sẽ
chạy, đồng thời đưa luồng dữ liệu mới (Simulator đa nguồn → MQTT → Bridge → Kafka →
Spark Bronze/Silver/Quarantine → PostgreSQL Gold → FastAPI/Dashboard) và payload đa
chỉ số môi trường vào lý thuyết. Giữ lại phần mô hình hình thức event-time/
watermark/veracity ở Chương 2 nếu vẫn áp dụng được cho kiến trúc mới.

### E2.2 Phạm vi file (tài liệu báo cáo — không phải implementation/)

- `Documents/report/chapters/abstract.tex` — viết lại theo stack thật.
- `Documents/report/chapters/01-introduction.tex` — stack, 5 RQ, phạm vi, đóng góp
  khớp implementation (Parquet/PostgreSQL/MQTT/Bridge/local[2]).
- `Documents/report/chapters/02-background.tex` — giữ mô hình event-time/watermark/
  veracity còn dùng; thay §công nghệ Delta/HDFS bằng plain Parquet + PostgreSQL +
  MQTT/Bridge; thêm related work cho 2 paper mới (Kafka benchmark practices,
  Stream DaQ).
- `Documents/report/chapters/03-implementation.tex` — sơ đồ TikZ vẽ lại theo luồng
  mới; data contract mở rộng 6 chỉ số môi trường; Docker Compose services khớp
  `implementation/compose.yaml`; pseudo-code Spark jobs khớp `spark/jobs/*.py` thật
  (Q1/Q2a/Q2b/Q3 + batch).
- `Documents/report/chapters/04-results.tex` — rà/sửa tham chiếu Delta/HDFS/
  Prometheus cũ; giữ phương pháp benchmark nhưng ánh xạ sang kịch bản demo được
  trên 1 máy (đa nguồn, backlog, partition, bão hòa ingestion, veracity per-metric).
- `Documents/report/chapters/05-conclusion.tex` — sửa tóm tắt stack; Hướng mở rộng
  gom các seam phi-bảo-mật + bảo mật TLS/SASL/ACL + HA đa máy + cụm Spark + Delta.
- `Documents/report/references.bib` — thêm entry paper Kafka (ĐÃ xác minh DOI) và
  Stream DaQ (CHƯA xác minh DOI — đánh dấu rõ, không bịa DOI).
- `Documents/report/README.md` — cập nhật mô tả nội dung chương cho khớp.

### E2.3 Ngoài phạm vi nhánh A

- Không sửa bất kỳ file nào trong `implementation/` ở nhánh A (đó là nhánh B).
- Không thêm package LaTeX mới trừ khi thật sự cần (hiện main.tex đã có tikz,
  pgfplots, listings, booktabs, tabularx — đủ). Nếu cần package mới phải hỏi user.
- Không thay đổi cấu trúc 5 chương + abstract + titlepage của main.tex.
- Số liệu Chương 4 vẫn là MINH HOẠ (ghi rõ nguồn) cho đến khi có số đo thật trên
  máy mục tiêu — không giả vờ đo thật (giữ quy ước AGENTS.md).

### E2.4 Xác minh nguồn (bắt buộc trước khi ghi bib)

- Paper Kafka `1afro78` — ĐÃ XÁC MINH qua Crossref (2026-09-18): Muzeeb Mohammad,
  "Analysis of Design Patterns and Benchmark Practices in Apache Kafka Event-Streaming
  Systems", 2025 5th ICICyTA, IEEE, pp. 612–617, DOI
  `10.1109/icicyta68677.2025.11362748`. → ghi vào bib bình thường.
- Paper Stream DaQ `1l2-9P` — CHƯA XÁC MINH được DOI/venue từ nguồn độc lập
  (Crossref trả kết quả không liên quan; Semantic Scholar 429; DBLP chặn Anubis).
  Có toàn văn local: Vasileios Papastergios & Anastasios Gounaris, Aristotle
  University of Thessaloniki. → ghi entry với thông tin có sẵn NHƯNG để trống DOI
  và thêm note "metadata xuất bản chưa xác minh từ nguồn độc lập (2026-09-18)".
  Không bịa DOI. User có thể bổ sung DOI sau.
- Mọi trích dẫn khác giữ nguyên các entry đã xác minh sẵn trong bib.

### E2.5 Tiêu chí nghiệm thu nhánh A

1. `latexmk -pdf -outdir=build main.tex` compile sạch, không lỗi undefined reference
   do tham chiếu Delta/HDFS/Prometheus cũ bị xóa.
2. Không còn mention Delta Lake / HDFS / Spark standalone master-worker /
   Prometheus/Grafana như một phần của kiến trúc TRIỂN KHAI (chỉ được xuất hiện ở
   Chương 5 Hướng mở rộng hoặc phần related work như công nghệ tham khảo).
3. Sơ đồ TikZ Chương 3 khớp luồng draw.io đã chốt (Simulator→MQTT→Bridge→Kafka→
   Bronze→Silver/Quarantine→PG/Gold→Dashboard + Airflow lập lịch batch).
4. Data contract Chương 3 phản ánh 6 chỉ số môi trường (E3.2).
5. bib: entry Kafka có DOI xác minh; entry Stream DaQ đánh dấu rõ chưa xác minh DOI.
6. Tester độc lập build PDF + grep audit không còn token kiến trúc cũ trong
   abstract/Chương 1/2/3/4 (trừ Chương 5 và related work).

## E3. Nhánh B — Mở rộng implementation: đa nguồn + đa chỉ số (production code)

> Nhánh B chạm vào `implementation/` → bắt buộc qua coder → tester → reviewer.
> Chờ user approve E3 riêng (sau hoặc cùng E2).

### E3.1 Simulator đa nguồn (vào code)

- 5 container simulator song song qua `docker compose run` ad-hoc (KHÔNG sửa
  `compose.yaml` service definition), mỗi container `--prefix` riêng
  (`zoneA`…`zoneE`) để `event_id` không trùng chéo và phân biệt nguồn trong Bronze.
- Định danh: `sensor_id = {prefix}_{TYPE}_{NNN}`, `event_id = {prefix}-{seq:06d}`.
- Tổng tải demo ~50 evt/s (5 × 5 sensor × ~2 evt/s, cấu hình qua env).
- Thêm `implementation/scripts/run-multi-sim.sh` (mới) để spawn/stop N container.
- Manifest per-container (source of truth "đã gửi gì") để đối chiếu sent-vs-delivered.

### E3.2 Payload đa chỉ số môi trường (vào code — lan xuống schema/validation/alert/Gold)

- 1 bản tin = vector data-fusion hợp nhất: tổ hợp nhiều chỉ số của CÙNG 1 cảm biến
  tại CÙNG 1 thời điểm (khớp khái niệm streaming data fusion).
- Bộ 6 chỉ số (user chốt "có gì thêm hết, đừng bớt"):

  | Chỉ số | Ký hiệu trường | Đơn vị | Miền vật lý (bounds) | Ngưỡng alert | Hướng alert |
  |---|---|---|---|---|---|
  | Nhiệt độ | `temperature_c` | °C | [-50, 200] | 35 | cao |
  | Độ ẩm | `humidity_pct` | %RH | [0, 100] | 80 | cao |
  | CO₂ | `co2_ppm` | ppm | [0, 5000] | 1000 | cao |
  | Áp suất | `pressure_hpa` | hPa | [300, 1100] | 950 | thấp |
  | Bụi mịn | `pm25_ugm3` | µg/m³ | [0, 1000] | 35 | cao |
  | Ánh sáng | `light_lux` | lux | [0, 200000] | — | không alert |

- Ma trận này ép thuật toán phải tổng quát: có metric ngưỡng cao, ngưỡng thấp
  (áp suất), metric không alert (ánh sáng), miền hẹp/rộng khác nhau, đơn vị khác
  nhau. Nếu validation/alert còn hard-code cho nhiệt độ sẽ lộ ngay.

### E3.3 Thay đổi Spark/schema kéo theo (vào code)

- **QUYẾT ĐỊNH TỐI ƯU 2026-09-18 (user chốt "bỏ cái nguy cơ lỗi nhất, triển khai lại"):**
  - **BỎ AIRFLOW hoàn toàn** khỏi implementation: xoá profile `airflow`,
    `airflow/Dockerfile`, `airflow/dags/`, database `airflow_meta` (PostgreSQL
    chỉ còn `app`), mọi tham chiếu trong scripts/docs. Batch hourly được thay
    bằng script `scripts/run-batch-hourly.sh` (cron trên host hoặc loop nội
    script gọi `run-batch.sh` với cửa sổ giờ vừa kết thúc). Lý do: Airflow là
    nguồn blocker F1 phức tạp nhất, image nặng, scheduler + metadata DB ăn
    ~3 GiB — đúng ngân sách cần cho 5 simulator; chức năng chỉ là "gọi batch
    mỗi giờ". Airflow chuyển thành Hướng phát triển (Chương 5).
  - **ALERT GIỮ TRONG Q3** (không tách, không bảng alert_rules): logic alert
    mở rộng từ 1 chỉ số nhiệt độ thành 6 chỉ số theo bảng ngưỡng cấu hình
    (hằng số module dùng chung với validation config) ngay trong foreachBatch
    Q3, insert idempotent vào `alerts` như hiện tại. Lý do: tránh bề mặt lỗi
    mới trên đường Q3 (nơi từng ra F4); ít thay đổi nhất so với code đã
    verify 81/81.
- Q1 (Kafka→Bronze): không đổi logic (raw lossless + provenance), payload to hơn.
- Q2a/Q2b (Bronze→Silver/Quarantine): Silver thành vector hợp nhất (thêm 6 cột chỉ
  số). Validation TỔNG QUÁT HÓA per-metric theo bảng cấu hình (thêm loại = thêm 1
  dòng config, không sửa logic). Reason vocabulary có cấu trúc `loại_lỗi:metric`
  (vd `value_out_of_bounds:co2`, `missing_field:humidity`). Khi 1 chỉ số out-of-bounds:
  row VẪN vào Silver với chỉ số đó = null + reason per-metric (chỉ loại cả row khi
  thiếu trường định danh bắt buộc event_id/sensor_id/time).
- Q3 (Silver→PostgreSQL): `sensor_latest` + `staging_stream_events` thêm cột chỉ số.
  Alert tính TRỰC TIẾP trong foreachBatch cho 6 chỉ số theo bảng ngưỡng cấu hình
  (không hard-code từng chỉ số, không bảng riêng, không bước tiêu thụ riêng),
  insert idempotent `alerts(event_id, rule_id, metric, threshold, direction)`.
- Batch (Silver→Gold): `gold.sensor_hourly` thêm chiều `metric` — PK
  `(sensor_id, hour_start, metric)`, full-replace upsert giữ nguyên. Trigger
  bằng script/cron, không dùng Airflow.
- Late-data nâng thành bài demo veracity chính thức + ghi tỷ lệ lỗi per-metric theo
  cửa sổ thời gian vào metric log (quality meta-stream).
- File Spark phải sửa: `spark/jobs/{common.py,validation.py,pg_sink.py,stream_app.py,
  batch.py}`, `sql/{schema.sql,queries.sql,test-data.sql}`, `backend/main.py`,
  `dashboard/index.html`, `simulator/producer.py`, tests tương ứng.
- File xoá/tinh gọn: `airflow/` (toàn bộ), compose profile airflow,
  `sql/init-databases.sql` bỏ airflow_meta, scripts bỏ mode airflow, docs đồng bộ.

### E3.4 Tham số tường minh (vào code)

- Kafka retention = 1 giờ (`KAFKA_LOG_RETENTION_MS=3600000`) trong `compose.yaml` —
  đủ cho bài demo dừng Spark 60s.
- Bridge queue = 5000 (giữ nguyên; queue đầy → redeliver = metric cần đo).

### E3.5 Bài demo thực nghiệm (chạy trên máy mục tiêu, số liệu thật cho Chương 4)

Mỗi bài là kịch bản chạy được trên 1 máy: (1) đa nguồn + cân bằng partition;
(2) backlog & recovery (dừng Spark 60s); (3) điểm bão hòa ingestion + 1-vs-3-vs-6
partition + 1-vs-N topic; (4) nhân bản N bridge (kèm `BRIDGE_INSTANCE_ID` duy nhất);
(5) persistence Mosquitto (kill bridge, đo redeliver); (6) veracity per-metric với 6
chỉ số + late-data; (7) batch rerun determinism với payload đa chỉ số.

### E3.6 Hướng mở rộng (CHỈ GHI vào Chương 5, không đụng code)

Nhân bản N bridge thành cấu hình mặc định; tách topic theo zone; thêm broker + nâng
RF (HA thật cần ≥2 máy); cluster Mosquitto; TLS/SASL/ACL; tăng partition; cụm Spark
standalone nhiều worker (cần nhiều máy); Delta Lake/HDFS; framework Stream DaQ đầy đủ;
dynamic-context check; ML dự báo trên stream.

### E3.7 Tiêu chí nghiệm thu nhánh B

1. Unit tests mở rộng pass (validation per-metric, pg semantics multi-metric, batch
   window, JDBC columns cho schema mới).
2. `docker compose config` + preflight OK trên máy mục tiêu (chỉ 2 profile
   live/batch; KHÔNG còn service/Dockerfile/database airflow nào trong repo).
3. e2e: 5 simulator phát đồng thời → Bronze/Silver/Quarantine/PG/Gold/API đúng;
   fault injection per-metric cho tỷ lệ quarantine khớp profile inject.
4. recovery: dừng Spark 60s → backlog tăng → bật lại đuổi kịp; không mất event.
5. resource-report ghi peak RAM/CPU/backlog/disk/latency thật; tổng live mode ≤ ngân
   sách (≤ ~5.1 GiB với 5 simulator; bỏ Airflow giải phóng ~3 GiB cho batch mode).
6. Batch hourly chạy được qua script/cron (không cần Airflow), rerun deterministic.
7. Reviewer xác nhận alert tính trong Q3 theo bảng ngưỡng cấu hình (không
   hard-code từng chỉ số), bounds per-metric tổng quát, Gold có chiều metric,
   không còn artifact Airflow.

## E4. Trình tự thực hiện (phần mở rộng)

1. Orchestrator: hoàn tất plan này (E1–E5) + cập nhật task state. ← đang làm
2. User approve E2 (nhánh A báo cáo) và/hoặc E3 (nhánh B code).
3. Nhánh A: orchestrator trực tiếp sửa tài liệu LaTeX (không phải production code
   implementation) → tester build PDF + grep audit → reviewer đọc diff báo cáo.
4. Nhánh B: giao coder implement trong scope E3 → tester verify độc lập → reviewer
   review độc lập. Lỗi route về coder kèm handoff.
5. Sau khi cả hai nhánh được user accept trên máy mục tiêu → promote durable memory.

## E5. Rủi ro (phần mở rộng)

- Stream DaQ chưa xác minh DOI → không bịa; ghi rõ hạn chế trong bib và báo cáo.
- Payload 6 chỉ số làm schema/validation/alert/Gold phình ra → kiểm soát bằng bảng
  cấu hình metric (thêm loại = thêm dòng config), tránh hard-code lan khắp code.
- Nhiều simulator → false duplicate cross-container nếu quên `--prefix` khác nhau →
  bắt buộc trong E3.1 và test E3.7.
- Sửa báo cáo là thao tác ghi đè nội dung đã viết → giữ bản cũ trong git history
  (đã commit ở `72c3c2e`/`c4c01b5`); không xóa file, chỉ viết lại nội dung.
- Runtime vẫn DEFERRED trên host hiện tại (disk/RAM/port); số liệu Chương 4 chỉ ghi
  "đã chạy" khi có evidence trên máy mục tiêu.

---

# PHẦN E6 — Rà soát lý thuyết Chương 1 và Chương 2 (2026-09-19)

## E6.1 Trạng thái và approval gate

- **Trạng thái:** `AWAITING_USER_ACCEPTANCE — tester PASS; reviewer APPROVE_WITH_NOTES`.
- **Yêu cầu hiện tại:** đọc toàn bộ Chương 1 và Chương 2, đối chiếu với
  implementation hiện có, rồi đề xuất câu văn và khung lý thuyết phù hợp.
- **Ranh giới đã duyệt:** viết lại `01-introduction.tex` và
  `02-background.tex`; không sửa `implementation/`, không tự thêm tài liệu tham
  khảo chưa xác minh, không tự commit hoặc push.
- **GitNexus:** registry hiện không có repository BigData; kết luận được đối chiếu
  bằng nội dung file, implementation và git diff hiện tại, không suy diễn từ graph.

## E6.2 Định hướng nội dung được đề xuất

1. Định vị báo cáo là **thiết kế và đánh giá một pipeline streaming IoT một máy**,
   trong đó veracity là ngữ nghĩa vận hành có thể kiểm tra; không định vị bản
   triển khai như một framework adaptive đã hoàn chỉnh.
2. Tách rõ hai lớp trong Chương 2:
   - **baseline đã triển khai:** Q1 Bronze, Q2a/Q2b Silver--Quarantine, Q3
     PostgreSQL, batch Gold, `is_late`, `processed_events` và upsert;
   - **mở rộng khái niệm:** watermark, FAST/CAUTIOUS, lifecycle
     PROVISIONAL/FINAL/CORRECTED và các giả thuyết cần đo.
3. Chuẩn hóa vốn từ: `event` là đơn vị dữ liệu logic; `message` là biểu diễn trên
   MQTT/Kafka; `row` là biểu diễn lưu trữ; `reading` là một thành phần metric.
   Phân biệt `ingestion lateness`, Kafka consumer lag và publication latency.
4. Sửa các điểm phải khớp implementation trước khi chốt văn bản: bốn trường
   định danh/thời gian bắt buộc thay vì năm; metric thiếu là station subset bình
   thường; metric sai được mask trong Silver thay vì đưa vào Quarantine; baseline
   không khai báo watermark; `processed_events` không có bounded state; và Gold
   chỉ được gọi là full-replace nếu code thực sự xoá các key cũ trong cửa sổ.
5. Không gọi các kịch bản benchmark là "đã chạy" khi TEST_REPORT vẫn đánh dấu
   runtime là DEFERRED; dùng "được thiết kế" hoặc "có runbook" cho đến khi có
   log/lệnh trên máy mục tiêu.

## E6.3 Quyết định đã chốt cho lượt viết lại

- **D1:** giữ watermark, policy và lifecycle ở Chương 2 nhưng gom thành phần mở
  rộng khái niệm, đánh dấu rõ `chưa triển khai` và không dùng chúng để mô tả
  baseline.
- **D2:** giữ metric lý thuyết để làm khung đánh giá, nhưng mỗi metric phải có
  nhãn `baseline đã thu thập`, `có thể suy ra từ log`, hoặc `extension chưa triển
  khai`.
- **D3:** sửa văn bản để phản ánh semantics code hiện tại; không mở task code
  trong lượt này. Không dùng cụm `full-replace` cho Gold nếu chỉ đang upsert các
  key xuất hiện trong staging.
- **D4:** giữ bối cảnh Việt Nam ở mức ngắn, không thêm claim định lượng hoặc
  citation mới chưa xác minh.
- **D5:** giữ Stream DaQ như tài liệu/bản thảo có toàn văn nội bộ; ghi rõ metadata
  xuất bản chưa xác minh và chỉ dùng khái niệm, không dùng số liệu hay gọi là
  nguồn đã kiểm chứng.

## E6.4 Phạm vi viết lại cụ thể

- Chương 1: viết lại theo trục `bối cảnh → vấn đề hệ thống → mục tiêu/RQ → phạm
  vi → đóng góp → cấu trúc`; giảm catalogue công nghệ; không đưa số đo runtime
  minh hoạ vào phần mở đầu.
- Chương 2: tổ chức theo trục `đặc thù dữ liệu IoT → mô hình event và timestamp →
  veracity baseline → extension event-time → công nghệ → nghiên cứu liên quan`.
- Chương 2 phải nêu rõ các semantics đã xác minh từ code:
  `event_id`, `sensor_id`, `event_time`, `ingest_time` là bốn trường identity/time;
  metric vắng mặt là hợp lệ; lỗi metric được mask thành `NULL` trong Silver;
  lỗi identity vào Quarantine; dedup Q3 dựa trên PostgreSQL; Gold hiện là
  idempotent upsert theo key có trong batch.
- Giữ các label/cross-reference đang được Chương 3 và Chương 4 sử dụng, hoặc
  tạo alias tương thích nếu đổi heading.
- Chỉ chỉnh hai file chương 1/2 trong lượt coder; nếu phát hiện Chương 3/4 lệch
  do câu chữ mới, tester ghi finding để mở lượt tài liệu sau, không tự mở rộng.

## E6.5 Tiêu chí nghiệm thu nếu user duyệt sửa tài liệu

1. Chương 1 và 2 có một thesis nhất quán, không hứa capability mà baseline không
   thực hiện.
2. Mọi định nghĩa thời gian và metric dùng cùng ký hiệu ở Chương 1, 2, 3 và 4.
3. Bảng veracity và các bất biến phân biệt rõ row-level, metric-level và
   event-level semantics.
4. Mọi claim về runtime, watermark, end-to-end latency, Gold full-replace và
   benchmark có trạng thái bằng chứng tương ứng.
5. Build LaTeX không có undefined citation/reference; sau khi sửa phải chạy
   mechanical audit và một lượt đọc semantic độc lập.

## E6.6 Kết quả verification và review

- Tester re-verification: `PASS`.
  - `git diff --check` sạch.
  - Build ép lại bằng `TEXINPUTS="build:" latexmk -gg -pdf -outdir=build main.tex`:
    65 trang, 0 undefined citation/reference, không lỗi LaTeX, không label trùng.
  - Các semantics baseline/extension, cross-reference, citation và phạm vi hai
    file đều đạt; runtime D1--D9 vẫn deferred.
- Reviewer độc lập: `APPROVE_WITH_NOTES`; không có blocker hoặc major finding
  ngăn nghiệm thu E6.
- Reviewer notes không blocking:
  1. nên ghi rõ `persistence false` của Mosquitto tạo thêm loss window khi broker
     restart;
  2. có thể làm heading I3 tự chứa caveat ``không bounded state``;
  3. citation `kafka_docs_4_3` cho mô tả MQTT QoS không phải nguồn MQTT chuyên
     biệt;
  4. có thể rút gọn phần lặp về quy ước baseline/extension và rà lại hai label
     lifecycle/policy cùng section.
- Các note trên chưa được tự sửa vì reviewer không yêu cầu để đóng acceptance,
  và mọi thay đổi thêm cần user quyết định. Các finding Chương 3/4/5 vẫn ngoài
   scope E6, được ghi nhận cho lượt tài liệu riêng.

---

# PHẦN E7 — Sửa lỗi dàn trang Mục 2.2.7 (2026-09-19)

## E7.1 Trạng thái và approval gate

- **Trạng thái:** `AWAITING_USER_ACCEPTANCE` (user đã duyệt triển khai bằng tin
  nhắn “vậy sửa”; coder, tester và reviewer đã hoàn tất, 2026-09-19).
- **Yêu cầu người dùng:** lỗi hiển thị ở Mục 2.2.7, trong đó nhãn dài của
  các bất biến bị tràn ra lề trái và phần đầu nhãn bị mất/che khuất trong PDF.
- **File production dự kiến:** chỉ `Documents/report/chapters/02-background.tex`.
- **Không tự động commit hoặc push.** Coder chỉ được sửa đúng file và markup
  trong phạm vi E7.
- **Coder implementation:** hoàn tất; tester độc lập `PASS`; reviewer độc lập
  `APPROVE_WITH_NOTES`; đang chờ user nghiệm thu cuối.

## E7.2 Bằng chứng hiện trạng và chẩn đoán

- Nguồn hiện tại dùng `\begin{itemize}` với optional label rất dài:
  `\item[I1 --- Kế toán record (\textbf{baseline}).]` và tương tự cho I2--I5.
- LaTeX giữ độ rộng nhãn mặc định của list; optional label dài hơn vùng nhãn
  sẽ tràn ngược ra lề trái thay vì làm rộng vùng nội dung. Vì vậy PDF hiện
  chỉ còn thấy các đuôi như `record (baseline).`, `mark (extension).`,
  `n vững (baseline).` ở sát mép trang, trong khi thân item bắt đầu ở cột
  thụt vào.
- `main.tex` không tải `enumitem`; không cần thêm package để sửa lỗi này.
- GitNexus không có index cho repository BigData, nên chẩn đoán dựa trên source,
  PDF đã build và `git diff`; không suy diễn quan hệ từ graph.

## E7.3 Phương án mục tiêu

Đổi riêng danh sách I1--I5 sang dạng list có nhãn ngắn, giữ tiêu đề bất biến
trong thân item để tiêu đề tự xuống dòng trong vùng văn bản:

```latex
\begin{description}
  \item[\textbf{I1.}] \textbf{Kế toán record (baseline).} Với mỗi khoảng quan sát, ...
\end{description}
```

Áp dụng cùng mẫu cho I2--I5. Phương án này không thêm package, không đổi nội
dung lý thuyết, công thức (2.9), label `eq:accounting`, cross-reference,
citation hoặc các mục H1--H4. Chỉ thay cơ chế dàn nhãn của I1--I5.

## E7.4 Ngoài phạm vi

- Không sửa semantics, câu chữ học thuật, công thức hoặc số thứ tự mục.
- Không đổi `main.tex`, preamble, package, geometry hay cấu trúc chương.
- Không sửa Chương 1, 3, 4, 5 dù có thể còn vấn đề dàn trang ở nơi khác.
- Không xử lý các reviewer note E6 không liên quan trực tiếp đến lỗi 2.2.7.

## E7.5 Trình tự thực hiện sau khi user duyệt

1. Giao coder sửa đúng một file `02-background.tex` theo E7.3.
2. Coder build PDF bằng lệnh chuẩn của repo và chạy `git diff --check`.
3. Coder thực hiện mechanical/style audit cho phần TeX đã đổi, không thay đổi
   prose ngoài phạm vi.
4. Giao tester độc lập kiểm tra diff scope, build, log cảnh báo, trích xuất/
   đọc trang chứa Mục 2.2.7 và xác nhận cả I1--I5 không còn tràn/cắt nhãn.
5. Giao reviewer độc lập đọc diff và xác nhận không có regression về công thức,
   tham chiếu, numbering hoặc phạm vi.

## E7.6 Tiêu chí nghiệm thu khách quan

1. PDF build thành công với `TEXINPUTS="build:" latexmk -pdf -outdir=build main.tex`.
2. Không có undefined citation/reference, label trùng hoặc lỗi LaTeX mới.
3. Trên trang chứa Mục 2.2.7, toàn bộ nhãn `I1.`--`I5.` và tiêu đề tương ứng
   hiển thị đầy đủ, không chạm/cắt lề trái; thân các item thẳng hàng và tự
   xuống dòng trong vùng text.
4. Công thức (2.9), `eq:accounting`, nội dung I1--I5 và H1--H4 không bị thay
   đổi ngoài phần markup cần cho dàn trang.
5. Diff production chỉ chạm `Documents/report/chapters/02-background.tex`.

## E7.8 Kết quả tester độc lập

- Tester trả `PASS` sau build thường và forced rebuild (`latexmk -gg`).
- PDF 65 trang; 0 LaTeX error, 0 undefined citation/reference, 0 duplicate
  label; công thức (2.9) và `eq:accounting` giữ nguyên.
- Bounding-box/PDF inspection xác nhận I1--I5 bắt đầu tại lề text bình thường,
  dòng tiếp theo thẳng hàng trong vùng list; không còn nhãn cắt/tràn ở Mục 2.2.7.
- Sáu overfull box còn lại trùng baseline và nằm ngoài vùng E7; tám cảnh báo
  duplicate PDF destination là cảnh báo cũ, không phải duplicate LaTeX label.
- Working tree vốn đã dirty; isolated pre-E7 comparison xác nhận patch E7 chỉ
  gồm 14 dòng trong `02-background.tex`.

## E7.9 Kết quả reviewer độc lập

- Reviewer trả `APPROVE_WITH_NOTES`; không có blocker hoặc major finding.
- Minor notes: worktree dirty nên cần giữ ranh giới E7 khi accept/commit; log
  vẫn có cảnh báo PDF baseline ngoài scope; các invariant mới nên tiếp tục dùng
  nhãn ngắn trong optional label và đặt tiêu đề dài ở phần thân.
- Reviewer xác nhận `description` là môi trường có sẵn, không thêm dependency;
  thay đổi chỉ ảnh hưởng trình bày Mục 2.2.7, không ảnh hưởng semantics,
  numbering, citation, cross-reference hay execution flow.

## E7.7 Rủi ro và lựa chọn cần user xác nhận

- `description` có thể làm khoảng cách nhãn--thân item khác nhẹ so với bản
  hiện tại; đây là trade-off cần thiết để không cắt nhãn. Nếu người dùng muốn
  giữ bullet thay vì nhãn `I1.` ngắn, có thể chọn phương án thay thế: itemize
  với nhãn rỗng và tiêu đề in đậm trong thân item.
- Mặc định đề xuất: **duyệt E7.3 với `description` và nhãn ngắn `I1.`--`I5.`**.
