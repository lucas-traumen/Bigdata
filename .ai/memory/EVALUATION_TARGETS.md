# Mục tiêu đánh giá được user chốt ngày 2026-10-01

Hai phép tải chính cho báo cáo: **15 GB/4 giờ** và **30 GB/4 giờ**.
GB là 10^9 byte raw payload đã tới Bronze. Chạy lần lượt trên cùng một máy
Docker, cùng cấu hình và fault profile, run-id/prefix riêng. Rate và đĩa
cần pilot; khối lượng thực phải đối soát.

Campaign 8 giờ ở rate thấp `campaign01` đã dừng theo hướng này; giữ log
nhưng không tính là hoàn thành 8 giờ hoặc là một trong hai phép tải mục tiêu.

Tổng 8 giờ là thời gian hai nguồn workload (4+4). Fixture lỗi/late/replay,
pilot, drain, đối soát và Gold/rerun cần thời gian thêm. Q1–Q3 dùng bằng
chứng streaming; Q4 dùng batch trên Silver với đáp án độc lập và rerun.
Recovery/tuning cần phép thử riêng nếu báo cáo đưa ra các kết luận đó.

15 GB/3 giờ là mục tiêu cũ trong tài liệu tham chiếu; 15 GB/4 giờ không
chứng minh đạt mốc 3 giờ. Không bỏ log bản tin, không đổi processed_events,
không reset hoặc xóa checkpoint/volume.

## Cấu hình ingress được kiểm chứng trước phép 15 GB

Ngày 2026-10-01, probe `p15w200-20261001` 180s tại 5×705/s xác nhận
634.505 bản tin và 194.772.497 byte source→Bridge→Bronze, PG 634.505 marker.
Bridge callback idle poll đổi 0,5→0,01s; MQTT inflight đổi 20→200, queue
5.000 giữ nguyên. Cấu hình20 và100 có evidence drop riêng được giữ lại.
Hai phép tải15/30GB phải dùng cùng cấu hình cuối để so sánh; không gọi cấu
hình đã chỉnh là baseline nguyên bản tại commit tham chiếu. Log runtime
không commit; Bronze byte count/đối soát cuối phép mới quyết định đạt mục tiêu.

## Kết quả phép 15 GB được kiểm tra ngày 2026-10-02

Run cũ `load15-20261001` bị suspend dài và PUBACK timeout; đã dừng,
không dùng làm phép tải liên tục sạch. Serving rows prefix cũ đã xóa theo
user; Parquet/Kafka/manifests/logs cũ còn để giữ evidence.

Run mới `load15-clean-20261002` trên DATA_ROOT
`/home/lucas/bigdata-demo-rerun15-20261002` và Kafka topic
`sensor_raw_load15_20261002` đã hoàn thành bình thường ngày 02/10/2026:
nguồn 03:42:50–07:42:56 +07, mỗi nguồn 4 giờ; collection xong 07:53:55.
Nguồn/Bridge/Bronze/PG đều 50.760.004 bản tin/event_id; Bronze payload
16.226.141.856 byte, đạt lượng ít nhất 15 GB. PUBACK/delivery/queue-drop
và PG duplicate occurrences đều 0. Không quan sát suspend/OOM trong run.

Evidence riêng: DATA_ROOT/logs/bench-load15-clean-20261002/, bao gồm
summary.json, bronze-summary.json, target-observation.json và
health-check-20261002.json. Cấu hình ingress đã chỉnh (inflight200,
poll0,01s); không gọi là baseline nguyên bản. Count khớp chưa thay thế
anti-join ID/provenance, kiểm đúng quality/latest/alerts hay Gold/rerun.
Gold prefix mới chưa chạy; 30 GB chưa chạy. Đĩa lúc kiểm tra còn ~25,4 GB:
phải giải quyết ngân sách đĩa trước phép 30 GB, giữ evidence 15 GB.
