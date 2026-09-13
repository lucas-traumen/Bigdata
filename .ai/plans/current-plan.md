# Approved Plan: Bài tập thực hành Tuần 3 — Phần 2: Lưu trữ phân tán & Lakehouse

## Trạng thái
- **Approved by user:** 2026-09-13 (user: "triển khai đi").
- Format bám đúng cấu trúc Tuần 2: `homework/week3/code/` (script + `run_all.py` + `outputs/labN.txt`) và `homework/week3/report/main.tex` (build bằng latexmk, nhúng code/output qua `\lstinputlisting`).
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

## Quyết định đã khóa (defaults do user duyệt gộp)

1. Bìa báo cáo: giữ "Lê Đức Trí - 23139049", đổi dòng ngày thành "Tháng 9 năm 2026", tiêu đề "Báo cáo thực hành Tuần 3" + "Phần 2: Lưu trữ phân tán & Lakehouse".
2. Bài tập về nhà làm đầy đủ: Lab 4 bản 20 cột = script riêng `lab4b`; Lab 1 bản 2PB/RF=2 gộp vào cuối script lab1 (kèm nhận xét đánh đổi độ tin cậy); các bước Iceberg Docker liệt kê trong báo cáo dạng mô tả các bước chuẩn (clone repo, docker compose up, vào spark-sql, CREATE TABLE iceberg) — ghi rõ là liệt kê theo hướng dẫn chung, chưa chạy thật vì repo chưa tải về.
3. Được phép `pip install pyarrow` vào `homework/bigdata-env`.
4. Task báo cáo lớn (adaptive-veracity) đã archive; không đụng `Documents/report/`.

## Phạm vi file được tạo/sửa

- `homework/week3/code/` — 8 script: `lab1_storage_cost.py`, `lab2_hdfs_api.py`, `lab3_rack_aware.py`, `lab4_csv_vs_parquet.py`, `lab4b_csv_vs_parquet_20cols.py`, `lab5_delta_log.py`, `lab6_table_format.py`, `run_all.py` + file `FileSystemCat.java` (excerpt ASCII từ slide) + `outputs/` (kết quả labN.txt) + `bench/` (file benchmark lớn, gitignored).
- `homework/week3/report/main.tex` — build output trong `report/build/` (đã match pattern gitignore `build/`).
- `.gitignore` — thêm: `homework/week3/code/bench/`.
- Venv `homework/bigdata-env` — chỉ thêm pyarrow.

## Ngoài phạm vi

- Không sửa `Documents/report/`, `implementation/`, `homework/week2/`, `AGENTS.md`.
- Không thêm package LaTeX mới — dùng đúng preamble tuần 2 (inputenc utf8, fontenc T5, babel vietnamese, amsmath/amssymb, booktabs/tabularx/array, listings/xcolor + style textout, float, caption, geometry 2.5cm, setspace onehalf, hyperref hidelinks).
- Không chạy Docker/Spark/Hadoop thật; không bịa kết quả chạy thật.
- Không commit, push.

## Nội dung 6 lab (theo slide đã trích, số liệu slide là chuẩn đối chiếu)

### Lab 1 — `lab1_storage_cost.py`
- Câu 1: 500TB × RF 3 = **1500TB** vật lý.
- Câu 2: hot 20% = 100TB × 3 = 300TB; cold 80% = 400TB × EC(6+3, overhead 1.5) = 600TB; tổng **900TB**; tiết kiệm **(1500−900)/1500 = 40%**.
- Bài về nhà (in kèm, phần riêng "[Bai ve nha]"): 2PB (= 2000TB) × RF 2 = 4000TB; hot 400TB × 2 = 800TB; cold 1600TB × 1.5 = 2400TB; tổng 3200TB; tiết kiệm 20%. Nhận xét: RF=2 chỉ chịu mất 1 DataNode/rack failure, tiết kiệm EC cũng giảm — đánh đổi độ tin cậy.

### Lab 2 — `lab2_hdfs_api.py` + `FileSystemCat.java`
- File `FileSystemCat.java` chứa excerpt từ slide 117 (ASCII, comment tiếng Anh).
- Script in file ra stdout, sau đó phân tích: dòng nào lấy cấu hình (`new Configuration()`), dòng nào kết nối NameNode (`FileSystem.get(URI.create(uri), conf)`), dòng nào bắt đầu đọc trực tiếp từ DataNode (`fs.open(new Path(uri))`). Ghi rõ: repo `hadoop-book-code` chưa có trong máy, dùng excerpt slide.

### Lab 3 — `lab3_rack_aware.py`
- Code theo slide 119: dict nodes 8 node / 3 rack (r1: n1-n3, r2: n4-n6, r3: n7-n8), hàm `place_replicas(client_node, nodes, seed=1)`, chạy với "n1".
- Kết quả mong đợi seed=1: **('n1','r1'), ('n5','r2'), ('n4','r2')** — nếu thực chạy khác do phiên bản random, ghi trung thực kết quả thật và ghi rõ seed (nguyên tắc tuần 2).
- Thêm 2 phần trả lời câu hỏi thảo luận bằng code minh họa:
  - 1 rack duy nhất: `random.choice([])` → IndexError ở bước chọn rack khác; lý giải nguy hiểm chịu lỗi (mất cả rack = mất toàn bộ bản sao).
  - RF=5: viết hàm mở rộng đặt tối đa 2 bản sao/rack, trải qua ít nhất 3 rack (1 node gốc + 2 rack khác × 2 node).

### Lab 4 — `lab4_csv_vs_parquet.py` + `lab4b_csv_vs_parquet_20cols.py`
- Lab 4: DataFrame 200.000 dòng 5 cột theo đúng slide 122 (id, khu_vuc choice HCM/HN/DN/CT/HP, gia_tri exponential(50000), so_luong randint 1-20, ngay từ 2026-01-01 + 0-240 ngày), `np.random.seed(42)`. Ghi `bench/bench.csv` và `bench/bench.parquet` (snappy, engine pyarrow). So dung lượng (MB) + thời gian đọc 1 cột `gia_tri` (usecols vs columns).
- Lab 4b (bài về nhà): 20 cột (mở rộng: thêm các cột số học học sinh tự đặt tên ASCII), đọc đúng 2 cột, so sánh tỷ lệ tiết kiệm I/O so với 5 cột — đối chiếu nhận xét slide: tiết kiệm tăng rõ rệt khi cột nhiều hơn.
- Nhận xét báo cáo: parquet thường 15–30% dung lượng CSV; con số chính xác phụ thuộc máy — ghi trung thực số đo thật.

### Lab 5 — `lab5_delta_log.py`
- Mô phỏng (không cần Spark): tạo thư mục `outputs/delta_demo/_delta_log/` với `000000.json` (INSERT: commitInfo + add), `000001.json` (UPDATE: commitInfo + remove + add — đúng cấu trúc slide 125), `000002.json` (DELETE: commitInfo + remove).
- Script đọc lại từng file JSON line, in tóm tắt số add/remove mỗi version, sau đó trả lời 2 câu hỏi thảo luận: (1) vì sao UPDATE tạo cặp remove+add thay vì sửa file cũ → immutable của block HDFS/GFS; (2) time travel về trước UPDATE → engine replay các commit JSON version 0..k, chỉ dùng các file chưa bị remove tại thời điểm đó.

### Lab 6 — `lab6_table_format.py`
- Script in bảng tình huống → lựa chọn + lý do ngắn (ASCII):
  - A (upsert IoT liên tục) → **Apache Hudi** (thế mạnh upsert/near real-time).
  - B (đa engine Spark/Trino/Flink) → **Apache Iceberg** (thiết kế đa engine, hidden partitioning).
  - C (Databricks toàn bộ) → **Delta Lake** (tích hợp sâu, đơn giản vận hành).

### Bài tập thiết kế nhóm (prose trong báo cáo, không script)
- Luồng review (văn bản tự do + điểm sao 1–5, mỗi chuyến 1 review, hiếm khi sửa): Bronze giữ JSON/raw gần nguyên bản (tái dùng cho NLP Phần 6); Silver Parquet/Delta, partition theo tháng (khối lượng nhỏ); Gold aggregate + feature cho sentiment. Không cần Hudi (update hiếm); không cần EC/cross-region ở quy mô này — giải thích bằng chữ, có bảng tóm tắt.

## Báo cáo LaTeX — cấu trúc `report/main.tex`

1. Titlepage (theo quyết định đã khóa).
2. TOC.
3. Mục tiêu (theo slide 113: 6 lab bám 6 khối lý thuyết + thiết kế nhóm + checklist).
4. Môi trường thực hành — bảng phiên bản THẬT (python, pandas, numpy, pyarrow, sklearn nếu dùng) + lệnh chạy (listing bash: activate venv, cd, run_all.py).
5. Lab 1–6: mỗi lab 4 mục con đúng pattern tuần 2: "Mục đích và lý thuyết" / "Mã nguồn" (`\lstinputlisting`) / "Kết quả thực chạy" (`\lstinputlisting` style textout từ outputs) / "Nhận xét".
6. Bài tập thiết kế nhóm.
7. Bài tập về nhà: Lab 4b (4 mục con đầy đủ như lab chính), Iceberg Docker steps (prose/listing các bước, ghi rõ chưa chạy), Lab 1 2PB/RF=2 (nhận xét đánh đổi).
8. Checklist ôn tập Phần 2 (6 mục theo slide 134).
9. Chuẩn bị Tuần 4 (MapReduce/Spark; cài Java JDK + PySpark).
10. Kết luận (pattern tuần 2: tổng hợp, ghi chú trung thực các giới hạn — repo tham khảo chưa tải, benchmark phụ thuộc máy).

## Trình tự thực hiện

1. `pip install pyarrow` vào venv; cập nhật `.gitignore`.
2. Viết `FileSystemCat.java` + 7 script lab + `run_all.py` (copy pattern tuần 2, thêm lab4b vào danh sách).
3. Chạy `run_all.py`, kiểm tra `outputs/` đủ 8 file (lab1..lab6 + lab4b) + `delta_demo/`.
4. Viết `report/main.tex`.
5. Build `latexmk -pdf -outdir=build main.tex` trong `report/`, kiểm log.

## Tiêu chí chấp nhận khách quan

1. `python run_all.py` (venv bigdata-env) exit 0, 8/8 lab OK.
2. Lab 1 khớp số slide: 1500TB / 900TB / 40%; bài về nhà 4000TB / 3200TB / 20%.
3. Lab 3 seed=1 trả về 3 node tuân thủ chính sách (1 gốc + 2 cùng rack khác); kết quả thật được ghi trung thực.
4. Lab 4: dung lượng parquet < csv; nếu thời gian đọc 1 cột không nhanh hơn trên máy này → ghi trung thực như cách tuần 2 xử lý Lab 3.
5. `latexmk -pdf -outdir=build main.tex` exit 0; PDF tiếng Việt đọc được; không lỗi font/undefined reference.
6. Mọi `lstlisting` body chỉ ASCII (comment code tiếng Anh không dấu); tiếng Việt chỉ ở caption/văn bản ngoài.
7. Diff chỉ nằm trong `homework/week3/`, `.gitignore`, venv; không đụng `Documents/report/`, `implementation/`, `homework/week2/`.
8. Số liệu trong báo cáo 100% lấy từ `\lstinputlisting` outputs — không chép tay.

## Verification bắt buộc (tester độc lập)

- `source homework/bigdata-env/bin/activate && cd homework/week3/code && python run_all.py`.
- `latexmk -pdf -outdir=build main.tex` từ `homework/week3/report/` + `pdftotext` kiểm nội dung.
- Đối chiếu số Lab 1/Lab 3 với slide; kiểm ASCII listing; kiểm diff scope.

## Rủi ro

- Kết quả benchmark Lab 4 phụ thuộc máy → ghi trung thực, kèm nhận xét độ lớn thay vì con số tuyệt đối.
- Repo tham khảo (hadoop-book-code, delta-lake-examples, iceberg quickstart) không có trong máy → mọi phân tích dựa excerpt slide, phải ghi rõ giới hạn.
- pandas 3.x khác biệt hiển thị (đã biết từ ISSUE-005) — chỉ là cosmetic.
- Nếu thực chạy Lab 3 ra khác slide do hành vi random theo phiên bản Python → ghi kết quả thật + seed, không chỉnh sửa để khớp.
