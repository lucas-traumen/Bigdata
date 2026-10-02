# Báo cáo Big Data — IoT Sensor Streaming Platform

Báo cáo LaTeX cho đề tài: **"Xây dựng và đánh giá nền tảng Big Data xử lý dữ liệu cảm biến IoT theo thời gian thực"**.

Phiên bản hiện tại (2026-10-02) mô tả kiến trúc triển khai thật: MQTT/Mosquitto → Bridge Python → Kafka KRaft → Spark Structured Streaming local[2] → Parquet cục bộ + PostgreSQL → FastAPI/Dashboard, với payload vector 6 chỉ số môi trường.

## Cấu trúc

```
report/
├── main.tex                  # File chính: preamble + include 7 chapter
├── references.bib            # 19+ entry BibTeX (paper + official docs + engineering)
├── chapters/
│   ├── titlepage.tex         # Bìa
│   ├── abstract.tex          # Tóm tắt
│   ├── 01-introduction.tex   # Bối cảnh, 5 RQ, phạm vi, đóng góp
│   ├── 02-background.tex     # Event-time/veracity model, Parquet, PG, MQTT/Bridge, related work
│   ├── 03-implementation.tex # Kiến trúc 5 tầng (TikZ), data contract 6 chỉ số, compose, Spark jobs
│   ├── 04-results.tex        # phương pháp benchmark, số đo 15 GB/4 giờ và số liệu tham chiếu
│   └── 05-conclusion.tex     # Tóm tắt + hướng phát triển
├── images/                   # Trống — sơ đồ và biểu đồ vẽ trực tiếp bằng TikZ/pgfplots
└── build/                    # Output build (không commit)
```

## Build

```bash
# Build đầy đủ — bắt buộc có TEXINPUTS để pdflatex tìm thấy build/main.bbl
# (vì \input{main.bbl} tìm theo search path từ thư mục gốc, không tự vào build/)
TEXINPUTS="build:" latexmk -pdf -outdir=build main.tex

# Hoặc build từng bước (tương đương):
rm -rf build && mkdir -p build
pdflatex -output-directory=build main.tex
bibtex build/main
TEXINPUTS="build:" pdflatex -output-directory=build main.tex
TEXINPUTS="build:" pdflatex -output-directory=build main.tex

# Output
build/main.pdf   # khoảng 65 trang
```

**Lưu ý quan trọng:**
- Nếu thư mục gốc có file `main.aux` / `main.bbl` / `main.blg` cũ (từ lần build không dùng `-outdir`), **phải xóa trước khi build** (`rm -f main.aux main.bbl main.blg`) — bibtex/pdflatex sẽ đọc nhầm file cũ này thay vì `build/`, làm mọi citation mới thành `[?]`.
- BibTeX ghi `.bbl` vào `build/` cùng chỗ `.aux`; `TEXINPUTS="build:"` cho pdflatex tìm thấy nó.
- Không dùng `rm -rf build` giữa các lần build thông thường (incremental build ổn).

## Quy ước kỹ thuật

- **Tiếng Việt**: `babel[vietnamese]` + `fontenc[T5]` qua pdflatex.
- **Hình ảnh**: TikZ cho sơ đồ kiến trúc 5 tầng (`fig:architecture`, `fig:event-flow`), pgfplots cho biểu đồ benchmark (`fig:rq-a`, `fig:rq-b`, `fig:rq-c`). Compile cùng pdflatex, không cần tool ngoài.
- **Code block**: `listings`. Body chỉ chứa ASCII (comment tiếng Anh) để tránh lỗi UTF-8 với `listings`. Tiếng Việt nằm ở caption + đoạn văn xung quanh.
- **BibTeX style**: `IEEEtran` (đính kèm `IEEEtran.bst` trong thư mục báo cáo) — chuẩn IEEE hiện đại, đánh số tham khảo theo thứ tự xuất hiện trong văn bản, tên tác giả viết tắt. Lưu ý: caption của figure/table chứa `\cite` bắt buộc phải có short caption `[...]` không chứa `\cite`, nếu không `\cite` sẽ chạy trong Danh sách Hình/Bảng (được xử lý trước nội dung) và làm lệch thứ tự đánh số.
- **Số liệu benchmark** trong chương 4 được tách thành số đo runtime của `load15-clean-20261002` (16,226 GB payload trong 4 giờ) và số liệu tham chiếu dựa trên Kafka patterns benchmark 2025, ShuffleBench 2024, fault recovery benchmark 2024, và tài liệu Confluent. Mục tiêu tiếp theo là 30 GB trong 4 giờ; chưa chạy. Luôn ghi rõ nguồn trong caption bảng/biểu đồ.

## Dọn file tạm

```bash
latexmk -C -outdir=build
rm -rf build/
```