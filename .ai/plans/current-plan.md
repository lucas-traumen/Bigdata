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
