# Ghi chú bằng chứng — bản phân tích 10/10/2026

Nguồn chính: load15-clean-20261002-final.tar.zst do người thực hiện cung cấp trên Drive.
Mã lưu trữ do người thực hiện tự quy ước: load15-clean-20261002. Tên này chỉ phục vụ truy vết dữ liệu, không phải tên mô hình, phương pháp nghiên cứu hay tiêu chuẩn benchmark. Topic trong phiên thu thập là sensor_raw_load15_20261002. Runtime commit dc1c34d1; run-spec ghi cây làm việc có thay đổi, vì vậy commit không phải snapshot đầy đủ của mã chạy.

## Phân tích trực tiếp
- Toàn bộ 8.647 Bronze, 5.765 Silver, 2.883 Quarantine Parquet.
- Toàn bộ CSV Gold 450 dòng.
- Định danh, sequence, offset; hiện diện chỉ số theo loại trạm; thống kê và cảnh báo.
- Progress: batch duy nhất 2..2883 trong cửa sổ; phân vị thời gian và startup gap.
- Resource: 390 mẫu, chu kỳ khoảng 30 s.
- 17.351 tệp đã trích xuất khớp SHA256. Không khẳng định checksum của mọi thành viên chưa trích xuất trong tar.

## Đối chiếu qua bằng chứng lưu trữ
Source/Bridge counts từ summary.json; PostgreSQL theo run từ health-check-20261002.json.
Manifest nguồn và log Bridge được đọc lại toàn bộ trong lượt kiểm toán chỉ-đọc 20-21/10/2026 (evidence/AUDIT_LOAD15_REPORT.md): tập event_id, tổng payload_bytes, offset Kafka.
Chưa restore bản dump PostgreSQL (chỉ đọc mục lục, WSL không có pg_restore); bộ đếm PostgreSQL vẫn dựa trên health-check.
Không so payload từng dòng giữa Bronze và Silver; đối soát bằng số dòng, event_id và tập offset.
Health log_scan là kết quả lưu trữ; lượt kiểm toán 20-21/10 quét lại các tệp log trong runtime-evidence (0 ERROR/FATAL/OOM/PUBACK timeout).

## Giới hạn đối soát
- Số test đã công bố cùng archive là 127/127 (CURRENT_VERIFICATION.md, 10/10/2026).
- Archive có trước khi manifest/Bridge log ghi SHA-256; chỉ một workload mới có hash đủ ba ranh giới mới xác nhận payload giống nhau từng byte.
- Báo cáo không dựa vào công cụ đối soát nào chưa có trong commit công khai.

## Kết quả và đơn vị
Bronze = Silver = 50.760.004; Quarantine = 0.
Payload = 16.226.141.856 byte (GB thập phân), không phải dung lượng Parquet.
Reading = 182.736.016. Alert = 878.187 lượt theo metric.
Gold = 450 nhóm UTC; count là số reading trong nhóm metric.
Mean sai khác tối đa 8,258e-10; dung sai 1e-6. Count/min/max khớp toàn bộ nhóm.
Delay Kafka timestamp → Q1 received_at_utc gồm thời gian chờ batch, không phải latency tới dashboard.
Progress thiếu quan sát 9.127 dòng khởi động; offset và dữ liệu lưu vẫn đầy đủ.
PG tổng cuối chứa dữ liệu cũ; dùng bộ lọc run hoặc hiệu hai mốc quan sát.
CSV computed_at ngày 09/10 không chứng minh lịch cron hoặc publication latency.

## Tệp kèm theo
verification_summary.json: các kết quả tính trực tiếp.
checksum_verification.json: số tệp đã kiểm tra và mismatch.
VERIFY_DATA.sql: các truy vấn đối soát chính, chạy từ root dữ liệu trích xuất.
TEST_REPORT.md, EVALUATION.md, original-04-results.tex: tài liệu lịch sử của dự án, giữ để truy vết; không thay thế các số liệu phân tích mới.

## Format material
L2: phương pháp → cấu hình → kết quả → thảo luận.
L0: kiến trúc theo lớp, sơ đồ luồng.
L14: công bố cấu hình và giới hạn benchmark.
Giữ năm chương, khổ A4 một cột. Không đưa số liệu benchmark của bài tham khảo vào kết quả dự án.

## Biên tập nội dung 21/10/2026
- Lấy nguồn trực tiếp từ dự án Overleaf hiện tại, giữ nguyên năm RQ gốc.
- Làm rõ đóng góp tích hợp hệ thống và phạm vi single-node.
- Bổ sung so sánh nội dung L0/L2/L14; chuẩn hóa cách gọi Bronze, latency, RAM/CPU.
- Biểu đồ partition chỉ biểu thị lại số đếm 18.273.603 / 16.243.200 / 16.243.201 trong verification_summary.json.
- Phân biệt đối soát độc lập Gold 450 nhóm với rerun fixture D4 108 dòng.
- Chuyển mục tiêu tải cao hơn sang hướng phát triển; chưa thực hiện lượt chạy mới.
- Không thay dữ liệu hay các tệp kiểm chứng; chưa tải/phân tích lại toàn bộ archive Drive trong lượt biên tập này.

## Biên tập 21/10/2026 (tối)
- Thêm Mục kiểm toán độc lập (Bảng tab:independent-audit) và AUDIT_LOAD15_REPORT.md vào evidence/.
- Giải thích phân bố partition 36/32/32% bằng CRC32 trên 25 sensor_id (9/8/8); danh sách 9 sensor của partition 0 trùng dữ liệu Bronze.
- Điền giới hạn Simulator 64 MiB vào bảng tài nguyên; thay 137/137 bằng 127/127; làm rõ rerun Gold ở workload dài chỉ có số đếm trước/sau.
