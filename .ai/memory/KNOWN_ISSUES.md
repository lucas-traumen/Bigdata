# Project Memory — Known Issues

## ISSUE-001 — Repository chưa init git
- Status: open
- Severity: minor
- Evidence: `git status` trả "fatal: not a git repository".
- Impact: Không dùng được GitNexus indexing, không có version control, `git diff`-based recovery không hoạt động.
- Workaround: Fallback sang filesystem inspection. Cần user quyết định `git init` (không tự làm trong bootstrap).
- Related files/modules: toàn bộ repo.

## ISSUE-002 — Bootstrap spec yêu cầu file `VIBE_CODING_V1_BOOTSTRAP.md` nhưng không có trong repo
- Status: mitigated
- Severity: minor
- Evidence: Nội dung spec được cung cấp trực tiếp trong conversation, không tồn tại file trong repo.
- Impact: Không có file spec durable trong repo để session sau tham chiếu.
- Workaround: Có thể lưu file spec vào repo nếu user yêu cầu.
- Related files/modules: repo root.

## ISSUE-003 — TikZ hình kiến trúc dùng toạ độ hardcoded
- Status: open
- Severity: minor
- Evidence: Reviewer ghi nhận (task refactor kiến trúc 2026-08): cả `fig:architecture` lẫn `fig:event-flow` đặt node bằng toạ độ tuyệt đối trong `chapters/03-implementation.tex`.
- Impact: Thêm/xoá node hoặc đổi kích thước khung tầng dễ gây lệch toàn bộ bố cục; debug bằng mắt là chính.
- Workaround: Khi sửa hình, edit từng toạ độ + rebuild kiểm tra bằng mắt; hoặc refactor sang relative positioning (`positioning` library) nếu sửa lớn.
- Related files/modules: `Documents/report/chapters/03-implementation.tex`.

## ISSUE-004 — Overfull hbox pre-existing ở tab:versions, tab:components, hyperref title
- Status: open
- Severity: minor
- Evidence: Log `latexmk` khi build 2026-08-28: overfull tại `tab:versions`, `tab:components` (ch3), một số chỗ ch1/2/4/5, và hyperref PDF-string cho subsection title chứa `$\rightarrow$`.
- Impact: Thẩm mỹ; không ảnh hưởng nội dung hay build.
- Workaround: Chưa xử lý (ngoài scope task refactor kiến trúc). Xử lý nếu user yêu cầu hoàn thiện trình bày.
- Related files/modules: `Documents/report/chapters/*.tex`.

## ISSUE-005 — pandas trong bigdata-env là 3.x, slide môn học có thể dựa trên 2.x
- Status: open (advisory)
- Severity: minor
- Evidence: `homework/bigdata-env` cài pandas 3.0.5 (2026-08-30); pandas 3 đã gỡ nhiều API deprecated của 2.x.
- Impact: Code lab các tuần sau nếu theo slide cũ có thể gặp lỗi API lạ.
- Workaround: Nếu gặp lỗi liên quan pandas, nghi ngờ đầu tiên là khác biệt 2.x/3.x; fix nhanh bằng `pip install "pandas<3"` trong venv.
- Related files/modules: `homework/bigdata-env`.

## ISSUE-006 — Số liệu slide Tuần 2 có điểm không khớp thực nghiệm (K-Means Lab 3)
- Status: documented in report
- Severity: informational
- Evidence: KMeans(n_clusters=3) trên dữ liệu gốc của slide ra nhãn `[1 1 1 1 1 1 2 0 2]`, không phải 3 phân khúc như slide kỳ vọng (inertia tối ưu 12.404 vs cách chia slide 48.600). Chỉ sau StandardScaler mới ra đúng 3 cụm.
- Impact: Không lỗi môi trường; là tính chất đúng của K-Means khi 2 đặc trưng lệch thang đo.
- Workaround: Đã ghi trung thực cả 2 phần (a/b) trong `homework/week2/report/` kèm giải thích — giữ nguyên cách tiếp cận này nếu slide các tuần sau có sai lệch tương tự.
- Related files/modules: `homework/week2/`.

Do not convert temporary failures into durable issues unless they remain unresolved and matter to future work.
