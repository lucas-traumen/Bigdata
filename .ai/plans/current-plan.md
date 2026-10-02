# Plan: Bộ công cụ đánh giá runtime IoT Big Data Q1–Q4

## Trạng thái và approval

- **Trạng thái:** APPROVED — user yêu cầu triển khai ngày 2026-10-01.
- **Mục tiêu:** triển khai một bộ công cụ đo và lưu bằng chứng cho hệ thống một máy
  Docker hiện có, đủ dùng cho một phiên đánh giá khoảng 8 giờ.
- **Không tự động commit hoặc push.**
- **Không reset pipeline, không xóa volume/checkpoint/dữ liệu hiện có.**

## Phạm vi đã chốt

1. Giữ nguyên kiến trúc MQTT → Bridge → Kafka → Spark → Parquet/PostgreSQL →
   FastAPI/Dashboard.
2. Wire các tham số Spark/JDBC từ environment vào Compose và Spark wrappers,
   nhưng giữ baseline mặc định hiện tại: trigger 5 giây, local[2], shuffle 2,
   JDBC batch 1000, các cap hiện tại.
3. Bổ sung công cụ chạy benchmark theo run id riêng, không trộn manifest giữa
   các lần chạy.
4. Bổ sung helper đo Bronze payload bytes và summary JSONL bằng Spark, không
   collect toàn bộ lịch sử về Python.
5. Bổ sung snapshot/summary cho source, Bridge, Kafka, Spark progress, resource,
   API/PostgreSQL và Gold.
6. Mở rộng resource report để thu simulator benchmark được gắn label.
7. Bổ sung payload byte counters vào manifest/delivery evidence mà không xóa
   hoặc đổi các trường log hiện có.
8. Bổ sung drain/settle kiểm tra downstream q2a/q2b/q3 trước khi chạy batch;
   thời gian chờ có thể tăng tới 600 giây qua environment.
9. Bổ sung tài liệu đánh giá trong `implementation/docs/EVALUATION.md`.
10. Chạy unit/static verification và bounded smoke/pilot nhỏ; không tự chạy
    B7 3 giờ trong lượt triển khai này.

## Ngoài phạm vi

- Không thêm HDFS, Kubernetes, scheduler mới hoặc service phân tán.
- Không thay đổi schema `processed_events`.
- Không bỏ log từng bản tin hiện có.
- Không sửa báo cáo LaTeX.
- Không tuyên bố đạt 15 GB/3 giờ khi chưa có phép chạy và đối soát tương ứng.
- Không cài cron thật, không chạy destructive reset.

## File dự kiến

- `implementation/compose.yaml`
- `implementation/.env.example`
- `implementation/spark/run/run-stream.sh`
- `implementation/spark/run/run-batch.sh`
- `implementation/spark/jobs/common.py`
- `implementation/spark/jobs/batch.py`
- `implementation/spark/jobs/pg_sink.py`
- `implementation/spark/jobs/stream_app.py`
- `implementation/simulator/producer.py`
- `implementation/bridge/bridge.py`
- `implementation/scripts/resource-report.sh`
- `implementation/scripts/run-mode.sh`
- `implementation/scripts/benchmark-run.sh`
- `implementation/scripts/benchmark-summary.py`
- `implementation/spark/jobs/bench_bronze_summary.py`
- `implementation/tests/test_benchmark_summary.py`
- `implementation/docs/EVALUATION.md`

## Acceptance criteria

- Baseline behavior stays unchanged when no new environment variables are set.
- `docker compose --profile live --profile batch config --quiet` passes.
- Unit tests pass, including summary/percentile/manifest aggregation tests.
- All shell scripts pass `bash -n`; Python files pass `py_compile`.
- Benchmark run creates run-scoped metadata, source manifests, resource snapshots,
  and a machine-readable summary.
- Resource report includes `bigdata-*` services and labeled `iot.simulator=1`
  benchmark containers without including unrelated projects.
- Bronze helper reports rows, payload bytes, average payload bytes and prefix.
- Drain helper does not claim success from q1 alone when q2/q3 are still moving.
- Existing logs and data remain readable; no destructive command is introduced.
- Documentation explicitly distinguishes measured results from proposed runs.

## Verification sequence

1. Tester runs unit/static checks and inspects generated helper output.
2. Coder fixes only failures in approved scope.
3. Reviewer performs read-only review against this plan.
4. Orchestrator reports remaining step: user may run/accept the 8-hour campaign.


## Điều chỉnh mục tiêu báo cáo — 2026-10-01

User chốt hai phép tải chính: 15 GB trong 4 giờ và 30 GB trong 4 giờ.
Không cần campaign riêng 8 giờ ở rate thấp; `campaign01` đã được dừng, log
và dữ liệu được giữ nguyên, metadata ghi `cancelled`.

- Hai phép tải chạy lần lượt, run-id riêng, cùng cấu hình và fault profile
  để so sánh tác động của việc tăng tải; đây là so sánh tải, chưa phải tuning.
- Lượng dữ liệu là payload byte thực đã tới Bronze (GB thập phân), không
  phải dung lượng log/Parquet/volume. Rate tính từ pilot và năng lực đạt được.
- Thời gian nguồn là 4 giờ mỗi phép, tổng 8 giờ. Preflight, pilot, fixture,
  xử lý bù, đối soát và batch/rerun cần thêm thời gian.
- Q1–Q3 lấy số đo trong hai phép tải và đối soát sau mỗi phép. Q2/Q3 cần
  fixture nhỏ có đáp án cho lỗi, NULL, late và duplicate/replay.
- Q4 chạy Gold trên Silver của hai phép tải, so đáp án độc lập và rerun.
- Recovery chỉ kết luận nếu có phép B4 riêng; không suy ra từ hai phép
  ổn định. Tuning chỉ kết luận khi chạy lại cùng workload, đổi một tham số.
- Mục tiêu 15 GB/3 giờ trong tài liệu ban đầu là điều kiện cũ; không coi
  đạt 15 GB/4 giờ là đạt 15 GB/3 giờ. Chưa có kết quả hai phép mới.


## Điều kiện thực thi phép 15 GB được user yêu cầu

User yêu cầu chạy 15 GB trước. Phép calibration đã tái hiện nghẽn/dropped
messages ở MQTT→Bridge (source PUBACK 317.255, Bridge 22.141). Phạm vi
triển khai để phép chạy có giá trị: sửa idle Kafka callback polling trong
Bridge, giữ manual ACK sau delivery thành công; thử lại từng cấu hình bằng
probe trước 4 giờ. Summary cần counters/unique-count trên đĩa thay vì RAM.
Giữ nguyên payload, log bản tin, schema và một máy Docker. Cấu hình đổi
phải ghi trong metadata và có before/after; không gọi cấu hình đổi là
cấu hình baseline nguyên bản. Không khởi chạy 30 GB trong lượt này.
