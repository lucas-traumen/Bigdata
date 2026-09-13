# Current task: Bài tập thực hành Tuần 3 — Phần 2: Lưu trữ phân tán & Lakehouse

## Status: IMPLEMENTING (2026-09-13)

## Approved scope

User duyệt ("triển khai đi") plan `.ai/plans/current-plan.md`: làm toàn bộ bài tập Tuần 3
theo slide `homework/week3/Tuan3_P2_LuuTruPhanTan_ThucHanh.pdf`, bám đúng format Tuần 2
(`code/` + `run_all.py` + `outputs/`, `report/main.tex` nhúng kết quả qua `\lstinputlisting`).

## Defaults đã khóa (user duyệt gộp khi duyệt triển khai)

1. Bìa: "Lê Đức Trí - 23139049", "Tháng 9 năm 2026", "Báo cáo thực hành Tuần 3 — Phần 2".
2. Bài về nhà làm đầy đủ: lab4b (20 cột) script riêng; Lab 1 2PB/RF=2 gộp cuối lab1;
   Iceberg Docker liệt kê bước trong báo cáo (chưa chạy thật).
3. Được phép cài pyarrow vào `homework/bigdata-env`.
4. Task báo cáo lớn adaptive-veracity đã archive (`.ai/plans/archive/2026-09-03-*.md`),
   vẫn đang chờ user acceptance riêng — không đụng `Documents/report/`.

## Checkpoints

- [x] Slide Tuần 3 trích xuất bằng pdftotext (24 trang, đầy đủ 6 lab + thiết kế nhóm + bài về nhà + checklist).
- [x] Plan cũ đã archive; plan Tuần 3 được duyệt và ghi.
- [x] Coder triển khai code + report trong approved scope (2026-09-13: run_all.py exit 0
      7/7 lab OK; latexmk exit 0, PDF 33 trang, 0 undefined ref; lab1 khớp slide
      1500/900/40% + 4000/3200/20%; lab3 seed=1 khớp slide (n1,r1)(n5,r2)(n4,r2);
      lab4: CSV 6.40MB vs Parquet 2.93MB (45.8%), đọc 1 cột 9.1x nhanh; lab4b:
      CSV 30.86MB vs Parquet 23.48MB (76.1%), đọc 2 cột 26x nhanh).
      Lưu ý đếm: plan nói "8/8 lab" nhưng danh sách lab chỉ có 7 script
      (lab1,lab2,lab3,lab4,lab4b,lab5,lab6) + run_all.py = 8 file code/.
- [ ] Tester xác minh độc lập (run_all.py, latexmk, đối chiếu slide, diff scope).
      SỰ CỐ: 6 lần dispatch tester liên tiếp bị rate_limit_exceeded
      (ses_f673c6fe5ffeAurTNDDpoIy2Q, ses_f673b2fd3ffe9AOTLVzIFoI4Pe,
      ses_f6739efc0ffe399klZYgKsDhtc, ses_f67389e6fffeSKFp0TuA3flFLV,
      ses_f67366c84ffe1g3KJHggaSM7tg, ses_f673097aeffewAYg6dhQJBWKuo,
      ses_f672e5542ffekekcEUi6GJScCN, ses_f673e0aeffew6mq9Uk4SYY0e1),
      gồm các lần sau cooldown 1 phút, 5 phút và 10 phút. KHÔNG được suy ra PASS/FAIL
      từ các lần này. Orchestrator đã chạy smoke check read-only 2026-09-13:
      run_all.py exit 0 (7/7 lab), latexmk clean-build exit 0 (33 trang, 0 undefined ref,
      0 missing char), diff scope sạch (không đụng thư mục cấm) — nhưng đây KHÔNG phải
      verification độc lập; tester vẫn phải được re-dispatch khi rate limit hết,
      sau đó mới đến reviewer.
- [ ] Reviewer rà soát độc lập.
- [ ] User acceptance; chỉ sau đó promote memory.

## Ràng buộc quan trọng (tóm tắt cho coder/tester/reviewer)

- Repo tham khảo `TaiLieuThamKhao/` KHÔNG có trong máy → Lab 2/5 dùng excerpt slide, ghi rõ giới hạn.
- Số liệu chuẩn đối chiếu: Lab 1 = 1500TB/900TB/40%; bài về nhà 2PB/RF=2 = 4000TB/3200TB/20%.
- Lab 3 seed=1 mong đợi ('n1','r1'), ('n5','r2'), ('n4','r2') — nếu khác, ghi trung thực.
- lstlisting body chỉ ASCII; tiếng Việt ngoài listing.
- Không commit/push; không sửa Documents/report, implementation/, homework/week2/.

## Recovery instructions

Đọc `.ai/plans/current-plan.md`, file này, `git status` trước khi tiếp tục.
Nếu coder/tester/reviewer thất bại, ghi failure cụ thể vào đây và route handoff rõ ràng.
