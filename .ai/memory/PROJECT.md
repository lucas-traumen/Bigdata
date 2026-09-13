# Project Memory — Project

## Purpose

Tài liệu báo cáo môn Big Data. Đề tài: "Xây dựng và đánh giá nền tảng Big Data xử lý dữ liệu cảm biến IoT theo thời gian thực". Hiện tại repo chỉ chứa báo cáo LaTeX; source code triển khai (Docker Compose, Spark job...) chưa có.

## Current architecture

Không có application code. Repo gồm:

- `Documents/report/` — báo cáo LaTeX (5 chương + abstract + titlepage), build bằng `latexmk`.
- `.claude/` — artifact từ tool khác (`.cc-writes`), không đụng tới.

## Important modules

- `Documents/report/main.tex` — entry point LaTeX.
- `Documents/report/chapters/*.tex` — nội dung từng chương, include từ `main.tex`.
- `Documents/report/references.bib` — 17 entry BibTeX, style `ieeetr`.
- `chapters/03-implementation.tex` — chứa cả hai hình kiến trúc: `fig:architecture`
  (kiến trúc 5 tầng + dải observability, vẽ lại 2026-08) và `fig:event-flow`
  (hành trình 1 event: sensor → Kafka → Bronze → Silver/quarantine → Gold → UI,
  có watermark/dedup). Báo cáo hiện 40 trang PDF.

## Architecture truth (báo cáo)

- Kiến trúc mô tả theo 5 tầng + dải ngang observability: (1) Ingestion,
  (2) Stream processing (Bronze job), (3) Storage/Lakehouse (HDFS + Delta
  Bronze/Silver/Gold + quarantine), (4) Batch & orchestration (Silver job batch,
  Gold job streaming, Airflow), (5) Serving & UI (Spark SQL + Web UI optional).
- Execution model chính thức trong văn bản: Bronze job = streaming long-running;
  Silver job = **batch** (đọc bảng Bronze, ghi append Silver); Gold job =
  **streaming** (readStream từ Silver, window aggregation); Airflow khởi động/dừng
  hoặc chạy theo lịch. Dữ liệu không "tự chảy" giữa các tầng.
- Web UI (Streamlit/FastAPI gợi ý) là thành phần optional, đọc bảng Gold,
  vẽ nét đứt + nhãn "(optional)" trên hình.

## External systems / protocols

Không có. Báo cáo mô tả Kafka/Spark/Delta Lake/HDFS/Airflow nhưng chưa triển khai thật.

## Build and runtime environment

- LaTeX: pdflatex + `babel[vietnamese]` + `fontenc[T5]`; `latexmk` đã xác minh tại `/usr/bin/latexmk`.
- Hình vẽ: TikZ + pgfplots inline (không cần tool ngoài).
- Build: `latexmk -pdf -outdir=build main.tex` trong `Documents/report/` → `build/main.pdf`.

## GitNexus

- Available: CLI installed (v1.6.9) nhưng **chưa usable** — repo chưa init git nên không index được.
- Notes: Cần `git init` + `gitnexus analyze` trước khi dùng. Không bịa kết quả GitNexus.
