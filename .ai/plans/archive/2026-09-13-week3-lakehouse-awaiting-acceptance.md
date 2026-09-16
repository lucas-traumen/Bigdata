# Archived plan: Bài tập thực hành Tuần 3 — Phần 2: Lưu trữ phân tán & Lakehouse

> Plan được giữ nguyên để không làm mất trạng thái của task trước khi chuyển
> sang task triển khai IoT mới. Task này vẫn ở trạng thái chờ user acceptance.

## Trạng thái
- **Approved by user:** 2026-09-13 (user: "triển khai đi").
- **Execution phase:** implementation completed; independent verification and user acceptance were pending.
- **No automatic commit or push.**

## Mục tiêu

Làm toàn bộ bài tập thực hành Tuần 3 theo slide `homework/week3/Tuan3_P2_LuuTruPhanTan_ThucHanh.pdf` (24 trang, đã trích xuất bằng pdftotext — nội dung các lab nằm trong plan này): 6 lab (tính replication, đọc code HDFS, mô phỏng rack-aware placement, CSV vs Parquet, Delta Lake transaction log, chọn table format) + bài tập thiết kế nhóm + bài tập về nhà + checklist ôn tập. Mọi số liệu trong báo cáo phải là kết quả thực chạy từ script.

## Bằng chứng hiện trạng

- `homework/week3/` chỉ chứa slide PDF; chưa có code hay report.
- Venv dùng chung `homework/bigdata-env` (Python 3.12.3, pandas 3.0.5, scikit-learn 1.9.0, matplotlib, mlxtend). **Chưa có pyarrow** (numpy có sẵn như dependency của scikit-learn — sẽ kiểm tra lại khi chạy).
- `TaiLieuThamKhao/P2_LuuTruPhanTan/` **không tồn tại trong repo** → Lab 2 và Lab 5 làm theo excerpt trong slide, ghi rõ giới hạn trong báo cáo, không giả vờ đã chạy Hadoop/Spark thật.
- GitNexus: repo chưa được index → không dùng, không bịa kết quả graph.
- Format mẫu: `homework/week2/code/run_all.py` (pattern chạy tuần tự + lưu outputs) và `homework/week2/report/main.tex` (preamble + cấu trúc 4 mục con mỗi lab).
- `.gitignore` hiện có; cần thêm mục cho file benchmark lớn của Lab 4.

## Quyết định đã khóa

1. Bìa báo cáo: giữ "Lê Đức Trí - 23139049", đổi dòng ngày thành "Tháng 9 năm 2026", tiêu đề "Báo cáo thực hành Tuần 3" + "Phần 2: Lưu trữ phân tán & Lakehouse".
2. Bài tập về nhà làm đầy đủ: Lab 4 bản 20 cột = script riêng `lab4b`; Lab 1 bản 2PB/RF=2 gộp vào cuối script lab1; các bước Iceberg Docker liệt kê trong báo cáo và ghi rõ chưa chạy thật.
3. Được phép `pip install pyarrow` vào `homework/bigdata-env`.
4. Task báo cáo lớn adaptive-veracity đã archive; không đụng `Documents/report/`.

## Phạm vi file

- `homework/week3/code/` — các script lab, `run_all.py`, `outputs/`, `bench/` và excerpt Java.
- `homework/week3/report/main.tex` — build output trong `report/build/`.
- `.gitignore` — thêm `homework/week3/code/bench/`.
- Venv `homework/bigdata-env` — chỉ thêm pyarrow.

## Ngoài phạm vi

- Không sửa `Documents/report/`, `implementation/`, `homework/week2/`, `AGENTS.md`.
- Không thêm package LaTeX mới; không chạy Docker/Spark/Hadoop thật; không bịa kết quả.
- Không commit, push.

## Tiêu chí và verification

- `python run_all.py` exit 0, các lab đều chạy.
- Lab 1 khớp 1500TB / 900TB / 40%; bài về nhà 4000TB / 3200TB / 20%.
- Lab 3 seed=1 trả về 3 node tuân thủ chính sách; ghi trung thực kết quả.
- Lab 4 Parquet nhỏ hơn CSV; ghi trung thực benchmark phụ thuộc máy.
- `latexmk -pdf -outdir=build main.tex` exit 0, PDF tiếng Việt đọc được.
- Listing ASCII và diff đúng scope.

## Ghi chú chuyển task

Implementation đã được coder tạo và commit `bdf01f8`; tester/reviewer/user acceptance
của task này chưa hoàn tất tại thời điểm archive. Xem state cũ trong git history nếu
cần tiếp tục task.
