# Hướng dẫn đánh giá runtime IoT Big Data trên một máy

Tài liệu này mô tả cách đo hệ thống tại commit tham chiếu `dc1c34d1b365f3dcf22e949c2ab0b166582231ac` trong kiến trúc một máy Docker:

```text
Simulator -> MQTT -> Bridge -> Kafka -> Spark -> Bronze/Silver/Quarantine
                                                -> PostgreSQL -> FastAPI -> Dashboard
```

Các query Spark là bằng chứng cho từng đoạn xử lý. `q1_bronze`, `q2a_silver`, `q2b_quarantine` và `q3_pg` không phải là bốn câu hỏi độc lập. Không cộng throughput của các query này thành throughput toàn hệ thống.

## 1. Chuẩn bị

Chạy từ thư mục `implementation/`:

```bash
scripts/preflight.sh
scripts/prepare-host.sh
scripts/init.sh
scripts/run-mode.sh live
scripts/wait-for-health.sh --timeout 300 spark-stream postgres backend dashboard
```

Đặt `DATA_ROOT` thành đường dẫn tuyệt đối có đủ dung lượng. Với máy đang có dịch vụ MQTT khác, dùng `MQTT_HOST_PORT=1884`; các container nội bộ vẫn kết nối `mqtt:1883`.

Baseline mặc định được giữ trong `.env.example`:

| Tham số | Baseline |
|---|---:|
| Spark trigger | 5 s |
| Spark master | `local[2]` |
| Kafka input cap | 20.000 offsets/trigger |
| Bronze/Silver file cap | 1.000 files/trigger |
| PostgreSQL file cap | 500 files/trigger |
| Shuffle partitions | 2 |
| JDBC batch size | 1.000 |
| Drain wait | 600 s |
| Downstream settle | 30 s |

Các biến tuning được truyền vào container thật. Chỉ thay một biến trong mỗi phép thử và ghi bản sao `.env`, `docker compose config` hoặc metadata của run.

## 2. Bốn vấn đề cần trả lời

- **Q1 — thu nhận:** Simulator → MQTT → Bridge → Kafka → Bronze. Đo rate thực, payload bytes, delivery, Kafka offsets, Bronze rows/bytes, backlog và sai lệch ID/provenance.
- **Q2 — chất lượng:** Bronze → Silver/Quarantine. Đo phân loại identity lỗi, metric bị mask thành `NULL`, lý do lỗi, late flag, throughput và đối soát provenance.
- **Q3 — trạng thái phục vụ:** Silver → PostgreSQL → API. Kiểm tra latest, alerts, processed ledger, replay/duplicate, tốc độ marker duy nhất và thời điểm đọc được kết quả.
- **Q4 — lịch sử:** Silver → dedup/stack/aggregate → Gold. Kiểm tra count/avg/min/max, cửa sổ UTC, late event, rerun và thời gian theo dung lượng.

## 3. Chạy một workload theo run id

Stack live phải đang chạy. Không dùng lại run id hoặc prefix đã có trong dữ liệu.

```bash
scripts/benchmark-run.sh run \
  --run-id pilot01 \
  --rate 50 \
  --duration 600 \
  --sensors 5 \
  --resource-interval 30
```

Fault injection là một workload riêng:

```bash
scripts/benchmark-run.sh run \
  --run-id quality01 \
  --rate 50 \
  --duration 600 \
  --sensors 5 \
  --fault-inject \
  --seed 42
```

Lệnh này không xóa dữ liệu, không xóa checkpoint và không reset Kafka/PostgreSQL. Nó tạo container có tên `sim-<run-id>-<zone>` và label riêng để resource report chỉ thu đúng simulator của project.

Theo dõi hoặc dừng một run:

```bash
scripts/benchmark-run.sh status --run-id pilot01
scripts/benchmark-run.sh snapshot --run-id pilot01
scripts/benchmark-run.sh stop --run-id pilot01
```

Chỉ dùng `stop` khi cần hủy workload. Container nguồn tự dừng sau `--duration`; file manifest vẫn được giữ trong `DATA_ROOT/logs/`.

## 4. Evidence theo run

Sau khi nguồn dừng và pipeline đã xử lý hết, summary được ghi tại:

```text
$DATA_ROOT/logs/bench-<run-id>/summary.json
```

Các evidence chính gồm:

```text
bench-<run-id>-metadata.json
bench-<run-id>-before-*.json/txt
bench-<run-id>-sources-finished-*.json/txt
bench-<run-id>/resource-report.stdout.log
summary.json
simulator-<run-id>-<zone>.jsonl
```

Manifest giữ các trường cũ và thêm:

- `payload_bytes`: byte UTF-8 của raw JSON đã phát;
- `published_payload_bytes`: byte được PUBACK xác nhận.

Bridge delivery log giữ các trường cũ và thêm `payload_bytes`. Đây là byte payload, chưa gồm overhead MQTT/Kafka, nén hoặc byte trên đĩa.

## 5. Đo Bronze

Chạy sau khi q1 đã đọc tới cuối nguồn:

```bash
BRONZE_STDOUT="$DATA_ROOT/logs/bench-pilot01/bronze-summary.stdout.log"
docker compose --profile batch run --rm --no-deps \
  -v "$PWD/spark/jobs/bench_bronze_summary.py:/opt/spark/jobs/bench_bronze_summary.py:ro" \
  spark-batch /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-local[2]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  /opt/spark/jobs/bench_bronze_summary.py \
  --prefix pilot01_ \
  | tee "$BRONZE_STDOUT"

python3 - "$BRONZE_STDOUT" \
  "$DATA_ROOT/logs/bench-pilot01/bronze-summary.json" <<'PY'
import json
import sys

source, target = sys.argv[1:]
with open(source, encoding="utf-8") as fh:
    records = [line.split("=", 1)[1].strip() for line in fh
               if line.startswith("BENCH_BRONZE_SUMMARY=")]
if not records:
    raise SystemExit("BENCH_BRONZE_SUMMARY record not found")
with open(target, "w", encoding="utf-8") as fh:
    json.dump(json.loads(records[-1]), fh, indent=2, sort_keys=True)
    fh.write("\n")
PY
```

Kết quả gồm `bronze_rows`, `payload_bytes`, `avg_payload_bytes` và số provenance Kafka phân biệt. Helper dùng Spark aggregation; không collect toàn bộ lịch sử về driver. Helper luôn in một dòng `BENCH_BRONZE_SUMMARY=...`; tùy chọn `--output` chỉ ghi được khi đường dẫn trong container có quyền ghi, nên cách `tee` rồi trích xuất trên host là cách dùng ổn định.

## 6. Các kịch bản

| Run | Workload | Mục đích |
|---|---|---|
| B0 | Fixture nhỏ, seed cố định, có lỗi | Đáp án phân loại, latest, alerts, Gold |
| B1 | Tăng rate theo các mức 10–15 phút | Tìm rate thực và nút thắt đầu tiên |
| B2 | Tăng số sensor/key | Kiểm tra phân bố Kafka partition và chi phí latest |
| B3 | Ít key, giữ rate | Kiểm tra hot key và lệch partition |
| B4 | Dừng Spark khoảng 60 giây | Đo backlog và thời gian xử lý bù |
| B5 | Fault injection ở mức tải chọn | Kiểm tra Q2, replay và ledger |
| B6 | Cửa sổ Gold 1 giờ/nhiều giờ, rerun | Kiểm tra thống kê và chi phí batch |
| B7 | Nguồn chạy liên tục 14.400 giây | Đối soát khối lượng và ổn định dài hạn |

B4, B5 và B6 không chạy xen vào B7 nếu muốn gọi B7 là tải ổn định.

## 7. Đối soát phiên chạy dài

B7 dùng nguồn chạy liên tục trong `14.400` giây để quan sát khối lượng dữ
liệu và trạng thái ổn định của pipeline. Sau khi nguồn dừng, giữ
Spark/Bridge chạy cho tới khi q1/q2/q3 ổn định. Ghi riêng:

- thời điểm nguồn bắt đầu/kết thúc;
- thời điểm q1 đọc hết Kafka;
- thời điểm Silver/Quarantine/PostgreSQL ổn định;
- thời điểm Gold hoàn thành;
- lượng backlog và thời gian xử lý bù;
- payload bytes và provenance tại Source, Bridge và Bronze.

Không coi rate đặt, số query hoàn thành hoặc Dashboard hiển thị được là
bằng chứng về khối lượng đã ghi. Kết luận phải dựa trên summary, Bronze
summary và đối soát provenance.

## 8. Đối soát

Sau mỗi run:

1. Manifest `published` đối chiếu Bridge delivery theo `event_id` và payload bytes.
2. Bronze đối chiếu theo `(kafka_topic, kafka_partition, kafka_offset)`.
3. Mỗi Bronze provenance hợp lệ thuộc đúng một nhánh Silver hoặc Quarantine.
4. Mỗi `event_id` Silver hợp lệ có marker tương ứng trong `processed_events`.
5. Replay không tạo thêm khóa, nhưng có thể tăng `duplicate_count` theo semantics hiện tại.
6. Latest chọn đúng winner theo `(event_time, received_at, event_id)`.
7. Alerts đúng khóa `(event_id, rule_id)` và threshold/direction.
8. Gold dedup trước aggregate, bỏ metric `NULL`, đúng count/avg/min/max.
9. Rerun bỏ qua `computed_at` khi so sánh thống kê.

Không dùng công thức `sent - delivered` giữa nhiều run khác nhau để kết luận mất dữ liệu. `published=false` chỉ là trạng thái timeout quan sát được; cần đối chiếu lại delivery và Kafka.

## 9. Latency và resource

`duration_ms` trong `stream-progress.jsonl` là thời gian trigger của một query, không phải latency đầu-cuối. Khi báo cáo latency, phải ghi rõ cặp mốc, ví dụ `ingest_time → Bronze observed` hoặc `ingest_time → API observed`.

Resource report ghi CPU/RAM container, RAM host, disk free, Kafka offsets, file counts và PostgreSQL counts. Nó bao gồm các service `bigdata-*` và simulator benchmark mang label `iot.simulator=1`, không lấy simulator của project khác.

## 10. Phiên đánh giá khoảng 8 giờ

Một phiên chính có thể tổ chức như sau:

| Khoảng | Công việc |
|---|---|
| 0:00–0:30 | Preflight, B0 và kiểm tra đáp án |
| 0:30–1:00 | Pilot, đo payload bytes |
| 1:00–2:15 | B1, các mức rate đại diện |
| 2:15–3:00 | B2/B3 hoặc một tuning độc lập |
| 3:00–3:40 | B4/B5 |
| 3:40–4:00 | B6 nhỏ và rerun |
| 4:00–8:00 | B7 đúng 4 giờ |
| 7:00–8:00 | Drain, Gold, đối soát và lưu metadata |

Nếu backlog lớn hoặc cần chạy đủ mọi biến thể B1–B3, phải ghi đó là một
phiên khác. Không trộn các phiên có workload hoặc cấu hình khác nhau khi
đối soát khối lượng và độ ổn định.

Phiên chạy dài đã hoàn thành với 50.760.004 bản ghi và 16.226.141.856 byte
payload trong 14.400 giây. Đây là evidence cho khối lượng đã đi qua các ranh
giới chính và cho trạng thái drain của pipeline. Các kết quả trong báo cáo
phải ghi commit, hardware, cấu hình, khoảng thời gian, file evidence, đúng/
sai dữ liệu, throughput/latency/backlog, nút thắt và thay đổi tuning trước–sau.
