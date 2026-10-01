# Plan: Bài tập Tuần 7 — PySpark Buổi 1 (RDD): chạy notebook tối ưu + bài tập về nhà + báo cáo

## Trạng thái và approval gate

- **Trạng thái:** `APPROVED — IMPLEMENTATION IN PROGRESS`.
- **Ngày lập plan:** 2026-09-29; user approve ("ok") cùng ngày, chấp nhận mặc
  định cho cả 3 câu hỏi mở §8: (1) không có slide → báo cáo ghi giới hạn;
  (2) có tạo zip nộp bài; (3) giữ tiêu đề đề xuất.
- **Ngày lập plan:** 2026-09-29.
- Task cũ (IoT implementation, awaiting acceptance) đã archive sang
  `plans/archive/2026-09-13-iot-implementation-*.md`; không coi việc archive là
  user acceptance của task cũ.
- **Không tự động commit hoặc push.**

## 1. Mục tiêu

Làm bài tập Tuần 7 theo đúng quy trình đề xuất trong lecture note §0.3
(`homework/week7/LectureNote_Buoi1.pdf`):

1. Chạy notebook thực nghiệm `buoi1_toiuu.ipynb` (10 thí nghiệm đo hiệu năng
   và tính đúng đắn của các nguyên tắc tối ưu RDD).
2. Làm bài tập về nhà: **tính min/max latency theo endpoint**.
3. Viết báo cáo LaTeX `homework/week7/report/main.tex` đúng format các tuần
   trước (week6: titlepage + mục lục + Mục tiêu + Môi trường + mỗi bài một
   section với 4 subsection Mục đích/Lý thuyết, Mã nguồn, Kết quả thực chạy,
   Nhận xét + Kết luận), build PDF bằng latexmk.
4. (Tùy user duyệt) Tạo zip nộp bài `Tuan7_LeDucTri_23139049.zip` theo pattern
   week6 (main.pdf + notebook đã execute).

## 2. Đề bài và những gì KHÔNG làm được

Lecture note §0.3 đề xuất 5 bước; hiện trạng repo cho phép:

| Bước trong đề | Khả thi | Ghi chú |
|---|---|---|
| 1. start_jupyter, chạy buoi1_rdd.ipynb từng ô | **KHÔNG** | Notebook bài giảng + slide không có trong repo |
| 2. (gộp trong 1) | — | — |
| 3. Mở Spark UI quan sát Jobs/Stages | OK | Qua REST API trong notebook |
| 4. Chạy buoi1_toiuu.ipynb (~3 phút) | **OK** | Có trong `homework/week7/`; DATA trỏ về access.log week6 |
| 5. Trả lời 8 câu hỏi ôn tập ở cuối slide | **KHÔNG** | Slide_Buoi1_RDD_v2.pdf không có trong repo → báo cáo ghi rõ giới hạn (pattern "giới hạn trung thực" như week6 với document.txt) |
| 5. Bài tập về nhà min/max latency theo endpoint | **OK** | Viết script riêng `bt_minmax.py` |

Quyết định về slide/zip sẽ hỏi user trước khi triển khai (câu hỏi mở ở §8).

## 3. Bằng chứng hiện trạng

- `homework/week7/` chỉ có: `buoi1_toiuu.ipynb` (46 cells: chuẩn bị + TN1–TN10
  + tổng hợp + biểu đồ), `LectureNote_Buoi1.pdf` (62 trang, 19 câu hỏi lý
  thuyết, tham chiếu slide Phần 4B/4C/5).
- Notebook cần `DATA = "../Slides_Tuan4_Phan3_XuLyTheoLo/data"` (không tồn tại);
  `access.log` 6000 dòng có tại
  `homework/week6/BaiTap _Mapreduce/data/access.log` (path có khoảng trắng).
- Môi trường: `homework/bigdata-env` (Python 3.12.3, PySpark 4.2.0, pandas
  3.0.5, matplotlib 3.11.1, ipykernel có sẵn; JAVA_HOME → JDK 17.0.20) — cùng
  env đã chạy week6. Lecture note chạy trên Spark 3.4.4; ta chạy 4.2.0 — báo
  cáo ghi rõ khác biệt phiên bản, không so sánh tuyệt đối số giây với slide.
- Pattern week6 cần bắt chước: `code/` tách notebook thành script riêng +
  `run_all.py` chạy tuần tự lưu stdout vào `code/outputs/labN_*.txt`; symlink
  `code/data` né path có khoảng trắng; report nhúng code/output bằng
  `lstinputlisting`; chỉ số thật, ghi trung thực điểm khác/nhiễu.
- Pattern nộp bài week6: `Tuan6_LeDucTri_23139049.zip` = main.pdf +
  notebook đã execute (2 file).

## 4. Thiết kế code (thư mục `homework/week7/code/`)

```
code/
  data                     -> symlink "../../week6/BaiTap _Mapreduce/data"
  common.py                — init Spark + parse/counted/bench/stage_metrics/
                             run_in_group/nhan_ban/save Results (giữ nguyên logic notebook)
  tn00_moitruong.py        — cells 0–6: Spark version, master, core, UI, CPU/RAM,
                             nhân bản 6000×200 = 1.2M dòng, số partition (JSON tn00.json)
  tn01_lazy_cache.py       — TN1 (cells 9–11)
  tn02_loc_truoc_map.py    — TN2 (12–14)
  tn03_loc_truoc_shuffle.py— TN3 (15–17)
  tn04_bot_cot_gop_cuc_bo.py — TN4 (18–20)
  tn05_khong_collect_thua.py — TN5 (21–23)
  tn06_cache_dung_lai.py   — TN6a+6b (24–28); ghi work/access_x200
  tn07_mappartitions.py    — TN7 (29–31)
  tn08_coalesce.py         — TN8 (32–34)
  tn09_tinh_dung_dan.py    — TN9 (35–37)
  tn10_pipeline_tong_hop.py— TN10 (38–39)
  bt_minmax.py             — BÀI TẬP VỀ NHÀ: min/max latency theo endpoint
  tonghop.py               — gộp outputs/tnNN.json → ketqua_toiuu.json +
                             bảng tổng hợp (text) + bieudo_toiuu.png
  run_all.py               — chạy tuần tự, stdout → outputs/<tên>.txt (như week6)
  outputs/                 — *.txt (nhúng report) + tnNN.json + ketqua_toiuu.json + png
  work/                    — access_x200 (intermediate TN6b, xoá được)
```

Nguyên tắc chuyển đổi (bắt buộc):

- **Giữ nguyên thuật toán/đo lường** của notebook: SCALE=200, REPEAT=3 lấy min
  sau warmup, Accumulator đếm lần gọi, stage_metrics đọc REST Spark UI,
  assert kết quả hai cách bằng nhau. Mỗi script tự dựng lại `base`
  (flatMap×200 + cache + count) vì mỗi script một SparkSession riêng.
- Logic chung đưa vào `common.py` không copy-paste; hàm đo giữ nguyên tên
  (`parse`, `counted`, `bench`, `stage_metrics`, `run_in_group`, `nhan_ban`).
- Mỗi TN in phần tử RESULTS dạng bảng text gọn + ghi JSON slice
  `outputs/tnNN.json` (để `tonghop.py` gộp lại y như cell 41–43 của notebook).
- `bt_minmax.py` (bài tập về nhà): min/max latency theo endpoint bằng
  `reduceByKey` với cặp `(min, max)` — min/max có tính kết hợp + giao hoán;
  (a) chạy với nhiều số partition (1/4/8/32) chứng minh kết quả không đổi;
  (b) so với cách ngây thơ `collect()` + Python thuần trên Driver về thời gian
  và số dòng về Driver; (c) in bảng min/max/mean từng endpoint 6 dòng.
- Comment trong code: ASCII tiếng Việt không dấu (quy ước repo cho lstlisting).

## 5. Báo cáo `homework/week7/report/main.tex`

Format copy week6 (documentclass 12pt article, babel vietnamese T5, listings
+ booktabs + tabularx + float + caption + geometry 2.5cm + hyperref hidelinks
+ `\lstdefinestyle{textout}`). Cấu trúc:

1. Titlepage: "Báo cáo thực hành Tuần 7 — PySpark cơ bản (Buổi 1): RDD và
   tối ưu hiệu năng"; sinh viên Lê Đức Trí - 23139049; Tháng 9 năm 2026.
2. Mục tiêu — 3 việc làm được + 2 giới hạn trung thực (không có
   buoi1_rdd.ipynb + slide 8 câu ôn tập; Spark 4.2.0 ≠ 3.4.4 tài liệu).
3. Môi trường thực hành — bảng env + cách chạy `run_all.py` + dữ liệu
   access.log (6000 dòng ×200 → 1,2 triệu dòng, 8 partition) + mô tả công cụ
   đo 3 lớp (accumulator tất định / shuffle metric từ REST / thời gian min 3
   lần sau warmup) + bảng "3 loại chỉ số và độ tin cậy".
4. TN0 chuẩn bị (1 section như Lab0 week6).
5. TN1–TN10: mỗi TN 1 section × 4 subsection (Mục đích và lý thuyết / Mã
   nguồn lstinputlisting / Kết quả thực chạy textout / Nhận xét và trả lời
   câu hỏi — viết lại kết luận markdown của notebook bằng số đo thật của ta).
6. Bài tập về nhà min/max: section riêng, giải thích vì sao min/max an toàn
   trong reduce (khác phép chia/trung bình TN9), mã nguồn, kết quả, nhận xét.
7. Kết luận — bảng tổng hợp 10 nguyên tắc (Trước/Sau/Giảm chỉ số/Tăng tốc bằng
   số thật), các điểm khớp/khác so với slide, giới hạn (local mode, 1 máy,
   thời gian min-of-3; số liệu máy này ≠ số liệu slide).

Số liệu trong báo cáo CHỈ lấy từ `code/outputs/*.txt` thực chạy; không lấy số
giả định của slide. Ngôn ngữ tiếng Việt; body listing chỉ ASCII.

## 6. Acceptance criteria

- [ ] `run_all.py` chạy hết 14 script (tn00..tn10 + bt_minmax + tonghop) không
      lỗi, exit 0, tổng kết "N/N OK"; outputs đầy đủ file txt + json + png.
- [ ] Mọi assert trong các TN (hai cách cùng kết quả) pass thật.
- [ ] `bt_minmax.py`: min/max đồng nhất giữa mọi số partition; bảng 6 endpoint
      khớp kiểm chứng độc lập (pandas thuần đọc access.log).
- [ ] PDF build sạch bằng `latexmk -pdf -outdir=build main.tex`; không lỗi
      undefined reference; mọi số trong report truy vết được về outputs/.
- [ ] Notebook gốc `buoi1_toiuu.ipynb` KHÔNG bị sửa; file gốc week7 khác không
      bị đụng; chỉ thêm file mới trong `homework/week7/{code,report}/`.
- [ ] (Nếu user duyệt) `buoi1_toiuu_executed.ipynb` + zip nộp bài tạo đúng
      pattern week6.
- [ ] Không commit/push.

## 7. Verification plan (tester độc lập)

- Re-run `run_all.py` từ đầu trong bigdata-env; so JSON/text output lần 1 với
  lần 2 (chỉ số tất định như shuffle bytes/accumulator phải ổn định; thời
  gian được dao động).
- Kiểm chứng bt_minmax bằng pandas đọc thẳng access.log.
- Build PDF độc lập + grep số liệu report ↔ outputs; kiểm scope bằng
  `git status` (không file ngoài week7 bị đổi).
- Notebook gốc so hash trước/sau.

## 8. Câu hỏi mở cho user (chờ trả lời trước khi implement)

1. Slide `Slide_Buoi1_RDD_v2.pdf` có muốn cung cấp để trả lời 8 câu ôn tập
   không? Mặc định: KHÔNG — báo cáo ghi giới hạn như §2.
2. Có tạo zip nộp bài `Tuan7_LeDucTri_23139049.zip` (PDF + executed notebook)
   như tuần 6 không? Mặc định: CÓ (làm sau khi report xong).
3. Tên file/tiêu đề report: "Báo cáo thực hành Tuần 7 — PySpark cơ bản (Buổi
   1): RDD và tối ưu hiệu năng" — được không?

## 9. Phân công

- **Coder:** tạo code/ + report/ theo §4–§5, chạy run_all.py lấy số thật,
  build PDF.
- **Tester:** verify độc lập theo §7.
- **Reviewer:** review diff + report so plan này (read-only).
- **Orchestrator:** giữ state, không sửa production code.
