# Báo cáo Big Data — IoT Sensor Streaming Platform

Báo cáo LaTeX cho đề tài: **"Xây dựng và đánh giá nền tảng Big Data xử lý dữ liệu cảm biến IoT theo thời gian thực"**.

## Cấu trúc

```
report/
├── main.tex                  # File chính: preamble + include 7 chapter
├── references.bib            # 17 entry BibTeX (paper + official docs + engineering)
├── chapters/
│   ├── titlepage.tex         # Bìa
│   ├── abstract.tex          # Tóm tắt (200-300 từ)
│   ├── 01-introduction.tex   # Bối cảnh, 5 RQ, phạm vi, đóng góp
│   ├── 02-background.tex     # 5V, Kafka, Spark, Delta, HDFS, Airflow, observability, related work
│   ├── 03-implementation.tex # Kiến trúc (TikZ), data contract, Docker Compose, Spark jobs
│   ├── 04-results.tex        # Methodology, 6 benchmark scenario, 11 metric, 3 pgfplots
│   └── 05-conclusion.tex     # Tóm tắt + 4 hướng phát triển
├── images/                   # Trống — sơ đồ và biểu đồ vẽ trực tiếp bằng TikZ/pgfplots
└── build/                    # Output build (không commit)
```

## Build

```bash
# Build đầy đủ (xử lý bibliography tự động)
latexmk -pdf -outdir=build main.tex

# Output
build/main.pdf   # khoảng 37 trang
```

## Quy ước kỹ thuật

- **Tiếng Việt**: `babel[vietnamese]` + `fontenc[T5]` qua pdflatex.
- **Hình ảnh**: TikZ cho sơ đồ kiến trúc (`fig:architecture`), pgfplots cho biểu đồ benchmark (`fig:rq-a`, `fig:rq-b`, `fig:rq-c`). Compile cùng pdflatex, không cần tool ngoài.
- **Code block**: `listings`. Body chỉ chứa ASCII (comment tiếng Anh) để tránh lỗi UTF-8 với `listings`. Tiếng Việt nằm ở caption + đoạn văn xung quanh.
- **BibTeX style**: `ieeetr` (sort theo author).
- **Số liệu benchmark** trong chương 4 là **minh hoạ** dựa trên ShuffleBench 2024, fault recovery benchmark 2024, và tài liệu Confluent. Luôn ghi rõ nguồn trong caption bảng/biểu đồ.

## Dọn file tạm

```bash
latexmk -C -outdir=build
rm -rf build/
```