# Nguồn hình và số liệu

Ngày biên tập: 2026-10-02. Snapshot tài liệu: `lucas-traumen/Bigdata@6343a180f84c46b611fbef73db1e394fb201abe3`.
Phiên runtime được đối chiếu từ metadata ghi commit `dc1c34d1b365f3dcf22e949c2ab0b166582231ac`.
Hai snapshot này được ghi tách biệt vì tài liệu và runtime không được tạo từ cùng một trạng thái working tree.

## Danh mục hình

| File | Nội dung | Cơ sở |
|---|---|---|
| `ch1-rq-map.tex` | Đặc thù IoT → yêu cầu pipeline → năm RQ | Sơ đồ khái niệm, không phải kết quả đo |
| `ch2-time-taxonomy.tex` | Các mốc thời gian của event | Data contract và metadata Kafka/Spark |
| `ch2-validation-levels.tex` | Validation row/metric và khử trùng event | `validation.py`, `stream_app.py`, `pg_sink.py`, `batch.py` |
| `ch2-partition-concurrency.tex` | Kafka partition và slot xử lý Spark | Sơ đồ minh họa tính song song; không khẳng định key được phân bố đều |
| `ch2-mqtt-ack.tex` | Hai vòng PUBACK và ACK thủ công của Bridge | MQTT 3.1.1 và `bridge.py`; trình tự khái niệm |
| `ch3-architecture.tex` | Kiến trúc năm tầng của baseline | `compose.yaml` và các thành phần runtime |
| `ch3-event-classification.tex` | Phân loại event và xử lý metric | `validation.py`; sơ đồ khái niệm |
| `ch3-q3-transaction.tex` | Staging và giao dịch PostgreSQL của Q3 | `pg_sink.py`; không biểu diễn giao dịch nguyên tử xuyên Parquet/PostgreSQL |
| `ch4-microbatch.tex` | P50/P95 thời gian xử lý Q1/Q2a/Q2b/Q3 | Summary và stream progress của phiên chạy dài |
| `ch4-payload-accounting.tex` | Đối soát Source/Bridge/Bronze | Payload bytes và số event tại ba ranh giới |
| `ch4-memory-last-sample.tex` | Mức RAM cao nhất của Spark/PostgreSQL | Resource report; không phải trung bình toàn phiên |

## Số liệu chương 4

| Query | P50 (ms) | P95 (ms) |
|---|---:|---:|
| Q1 / Bronze | 344 | 422 |
| Q2a / Silver | 1.305 | 1.465 |
| Q2b / Quarantine | 537 | 694 |
| Q3 / PostgreSQL | 1.457 | 1.651 |

Phiên chạy dài ghi nhận 50.760.004 event và 16.226.141.856 byte payload tại
source, Bridge delivery và Bronze. Tổng byte tương đương 16,226141856 GB với
quy ước 1 GB = 10^9 byte. Ba tổng bằng nhau là đối soát ở cấp tổng; chúng
tự nó không chứng minh mọi `event_id` duy nhất hoặc không có thiếu hụt, nên
báo cáo vẫn tách kiểm tra provenance và anti-join.

Resource report ghi nhận mức cao nhất khoảng 1,878 GiB / 2,5 GiB cho Spark
(75,1%), 466,3 MiB / 512 MiB cho PostgreSQL (91,1%), 575,3 MiB / 1 GiB cho
Kafka (56,2%) và 22,46 MiB / 128 MiB cho Bridge (17,6%). Dung lượng đĩa
trống thấp nhất được ghi nhận xấp xỉ 21,4 GB.

## Tài liệu kiểm chứng

- Chương 4 đối chiếu `summary.json`, `bronze-summary.json`, `stream-progress.jsonl`, `resource-report.jsonl` và health check của phiên chạy dài.
- `implementation/docs/TEST_REPORT.md` cung cấp evidence độc lập cho unit test, fixture veracity, recovery, Gold rerun và backup/restore.
- `implementation/` là cơ sở đối chiếu cho mô tả kiến trúc và triển khai.

Log runtime nằm ngoài repository. Lần biên tập này đã đọc các file summary,
bronze summary, health check, stream progress và resource report; không chạy
lại workload. Các phép đo rate-sweep, recovery định lượng, anti-join đầy đủ
và Gold cho prefix của phiên dài vẫn là nội dung cần kiểm chứng riêng.
