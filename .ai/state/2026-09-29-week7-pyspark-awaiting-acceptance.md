# Task: Bài tập Tuần 7 — PySpark Buổi 1 (RDD) + báo cáo

## Trạng thái: AWAITING_USER_ACCEPTANCE — coder xong, tester PASS, reviewer APPROVE_WITH_NOTES

- Plan: `.ai/plans/current-plan.md` (APPROVED).
- User approve "ok" 2026-09-29, chấp nhận 3 mặc định: không slide (ghi giới
  hạn), có zip nộp bài, giữ tiêu đề "Báo cáo thực hành Tuần 7 — PySpark cơ bản
  (Buổi 1): RDD và tối ưu hiệu năng".
- Task cũ (IoT implementation) đã archive: `plans/archive/2026-09-13-iot-implementation-*.md`.

## Yêu cầu user (2026-09-29)

"đọc week7 làm theo assignment, và viết report theo format như tuần khác"

## Phát hiện khi đọc đề (LectureNote_Buoi1.pdf §0.3)

- Đề 5 bước: chạy buoi1_rdd.ipynb từng ô (notebook KHÔNG có trong repo);
  chạy buoi1_toiuu.ipynb (CÓ, 46 cells, 10 thí nghiệm);
  quan sát Spark UI; trả lời 8 câu ôn tập cuối slide (slide KHÔNG có);
  bài tập về nhà min/max latency theo endpoint (làm được).
- buoi1_toiuu.ipynb cần DATA "../Slides_Tuan4_Phan3_XuLyTheoLo/data" không tồn
  tại → trỏ về `homework/week6/BaiTap _Mapreduce/data/access.log` (6000 dòng).
- Env sẵn: bigdata-env Python 3.12.3 + PySpark 4.2.0 + JDK 17 (đã chạy week6).
- Pattern tuần trước (week6) để làm theo: tách notebook thành code/ scripts +
  run_all.py + outputs/*.txt, report main.tex 4-subsection/labı, symlink
  code/data né khoảng trắng, zip nộp = main.pdf + executed notebook.

## Checkpoints

- [x] Đọc week7 (notebook + lecture note PDF).
- [x] Đối chiếu format report week2/3/6 + cấu trúc code week6 + zip nộp bài.
- [x] Kiểm env: pyspark 4.2.0, pandas, matplotlib, ipykernel, JAVA_HOME 17.
- [x] Archive task cũ, viết plan mới.
- [x] User approve plan (defaults: no slide / có zip / giữ tiêu đề).
- [x] Coder implement code/ + report/ (2026-09-29: 13/13 script OK — tn00..tn10,
      bt_minmax, tonghop; PDF 35 trang 0 error; executed notebook 18/18 cell;
      zip Tuan7_LeDucTri_23139049.zip 2 file; số chính: TN2 16,4×/2,1×, TN3
      4,8×/1,9×, TN4 4782,9×/3,1×, TN7 3000×/102,9×, TN8 50×/2,7×, TN10
      4426,5×/6,3×, bt_minmax khớp pandas + đồng nhất 1/4/8/32 partition).
- [x] Tester verify độc lập (2026-09-29: PASS_WITH_NOTES — 13/13 re-run OK,
      3m22s; số tất định 0 khác biệt giữa 2 lần chạy; bt_minmax khớp pandas
      độc lập từng giá trị; 36/36 số trong PDF truy vết được về outputs;
      rebuild PDF sạch 35 trang; zip nhất quán. 2 minor: (1) số thời gian
      trong prose là của một lần chạy cụ thể — nếu re-run sẽ lệch listing;
      (2) câu "biểu đồ kèm bài nộp tại code/outputs/bieudo_toiuu.png" trong
      khi zip chỉ có PDF + notebook (chart nằm trong executed notebook).
      Ghi chú: plan ghi 14 script nhưng thật là 13 — lỗi số học của plan).
- [x] Coder sửa 2 minor (2026-09-29: reword câu biểu đồ ở Kết luận — nhúng
      trong executed notebook + PNG ở repo nguồn; thêm 1 câu "một lần chạy
      chính thức" cuối phần công cụ đo; rebuild 35 trang sạch; zip cập nhật
      main.pdf mới, notebook giữ nguyên sha ccfe2a28).
- [x] Tester quick re-verify sau fix (2026-09-29: PASS cả 5 mục — rebuild
      sạch 0 error/0 undefined, 2 câu sửa đúng vị trí + có trong PDF, 12/12
      số tất định giữ nguyên giá trị lẫn số lần xuất hiện, zip nội dung
      pdftotext identical từng byte, scope sạch).
- [x] Coder sửa 2 minor reviewer (2026-09-29: "Phep chia"→"Phép chia" 1 chỗ
      prose (dòng 352, listing ASCII không đụng); 4 chỗ "Mục~N" hardcode →
      \ref{sec:muctieu|sec:moitruong|sec:btvn}, thêm 2 label mới; rebuild
      35 trang 0 error/0 undefined; zip cập nhật main.pdf mới (sha f8bfcb3e).
- [x] Tester final quick check (2026-09-29: PASS 5/5 — rebuild sạch; diff
      pdftotext toàn PDF chỉ đúng 1 dòng chính tả; 13/13 số tất định giữ
      nguyên giá trị + số lần xuất hiện; \ref render đúng số như cũ, 0 chỗ
      hardcode còn; zip text-identical, notebook ccfe2a28 giữ nguyên; scope
      + hash 2 file gốc sạch).
- [x] Reviewer review (2026-09-29: APPROVE_WITH_NOTES — code trung thành
      notebook 100% ở mức dòng, ~30 claim số truy vết hết, artifacts đúng
      pattern, không blocker/major. 2 minor: (1) chính tả "Phep chia" →
      "Phép chia" main.tex:350; (2) 4 chỗ hardcode "Mục~14/1/2" thay vì
      \ref{sec:...} (sec:btvn đã có label). 6 note để lần sau: attribution
      bảng env, TN8 câu mơ hồ, framing "khớp slide", SCALE import thừa,
      bất đối xứng đo bt_minmax, erratum "14 script" của plan).
- [ ] User acceptance → memory promotion.

## Câu hỏi mở chờ user

1. Cung cấp slide để trả lời 8 câu ôn tập? (mặc định: không, ghi giới hạn).
2. Tạo zip nộp Tuan7_LeDucTri_23139049.zip? (mặc định: có, làm cuối).
3. Tiêu đề "Báo cáo thực hành Tuần 7 — PySpark cơ bản (Buổi 1): RDD và tối ưu
   hiệu năng"? (mặc định: dùng).

## Recovery

Đọc AGENTS.md + `.ai/plans/current-plan.md` + file này. Chưa có code nào
được viết cho task này; `homework/week7/` còn nguyên vẹn (2 file gốc).
Không commit/push.

### Coder checkpoint 2026-09-29 — Week 7 (PySpark Buổi 1 RDD)

Hoàn thành toàn bộ deliverable theo plan `.ai/plans/current-plan.md`:

- `homework/week7/code/`: common.py + tn00..tn10 + bt_minmax.py + tonghop.py + run_all.py (13 script), symlink data → week6 access.log. `run_all.py` chính thức: **13/13 OK, ~3m24s** (outputs/*.txt + 12 json + ketqua_toiuu.json + bieudo_toiuu.png).
- Số chính (A→B): TN1 12000→6000 lần parse; TN2 1.200.000→73.200 parse (16.4×), tốc 2.1×; TN3 shuffle 1.761.756→366.986 byte (4.8×), tốc 1.9×; TN4 shuffle 11.880.712→2.484 (4782.9×), tốc 3.1×; TN5 1.2M→73.200 dòng về Driver (16.4×), tốc 3.1×; TN6a cache rẻ 1.0× / TN6b đắt 2.2×; TN7 24000→8 lần chuẩn bị, tốc 102.9×; TN8 800→16 task (50×), tốc 2.7×; TN10 shuffle 11.880.712→2.684, tốc 6.3×. bt_minmax: min 1 / max 906–920 / mean 445.19–465.52 ms, khớp pandas + đồng nhất 1/4/8/32 partition, collect() ngây thơ 1.359s vs 0.428s (3.2×).
- bt_minmax fix 1 bug: pandas check phải mô phỏng nhan_ban x200 (đối chiếu trên cùng 1.2M dòng), assert đầu tiên fail đã sửa.
- Report `homework/week7/report/main.tex`: build OK, **35 trang**, 0 error, 0 undefined ref, overfull max ~10pt.
- Notebook executed: `code/work/nb/buoi1_toiuu_executed.ipynb`, 18/18 code cell OK, DATA trỏ absolute week6, output rơi trong work dir.
- Zip: `homework/week7/Tuan7_LeDucTri_23139049.zip` (main.pdf + notebook, 304K).
- Scope sạch: sha256 2 file gốc week7 không đổi (d4531d6d…, a742c95f…); chỉ thêm mới trong homework/week7/ (+ checkpoint này). Chưa commit.
