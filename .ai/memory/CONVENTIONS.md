# Project Memory — Conventions

Only record conventions supported by repository evidence or explicit user instruction.

## Naming

- Chapter files: `chapters/NN-name.tex` (01-introduction, 02-background, ...).

## Project structure

- Báo cáo LaTeX nằm trọn trong `Documents/report/`; build output trong `Documents/report/build/` (không commit, đã có `.gitignore`).
- Bài thực hành môn học: mỗi tuần một thư mục `homework/weekN/` với cấu trúc chuẩn `code/` (script + `run_all.py` + `outputs/labN.txt`) và `report/` (LaTeX, build ra `report/build/main.pdf`). Code chạy bằng venv dùng chung `homework/bigdata-env` (Python 3.12). Code/output nhúng vào báo cáo qua `\lstinputlisting` để số liệu trong PDF luôn khớp kết quả chạy thật.

## Error handling

Không áp dụng (không có application code).

## Testing

Không có test framework. Verification = build LaTeX thành công (`latexmk -pdf -outdir=build main.tex`) và đọc PDF/log để kiểm tra nội dung.

## Logging

Không áp dụng.

## API / interface conventions

Không áp dụng.

## Other

- `lstlisting` body chỉ chứa ASCII (comment tiếng Anh); tiếng Việt chỉ ở caption và văn bản ngoài listing.
- Hình kiến trúc TikZ (`fig:architecture`, `fig:event-flow`): khung dashed từng tầng, mũi tên có nhãn tên job, màu nhất quán (compute/xanh, storage/cam, orchestration/tím, observability/hồng, serving optional nét đứt). Chỉ dùng TikZ library đã load trong `main.tex` (shapes, arrows.meta, positioning, fit, backgrounds); library mới phải hỏi user trước.
- Khi thay đổi mô tả job ở chương 3: phải kiểm prose khớp pseudo-code listing (đặc biệt execution model: Silver = batch, Bronze/Gold = streaming).
- Subsection Silver job không có `\label` — tham chiếu bằng `Listing~\ref{lst:silver}`; `subsec:goldjob` và `subsec:airflowdag` là label duy nhất của mục cài đặt chi tiết.
- Số liệu benchmark là minh hoạ có nguồn (ShuffleBench 2024, fault recovery benchmark 2024, Confluent docs) — luôn ghi nguồn trong caption, không giả vờ đo thật.
- Thêm package LaTeX mới hoặc đổi cấu trúc chương phải hỏi user trước.
