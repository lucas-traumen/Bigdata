# AGENTS.md

## Repo status

Repository chứa tài liệu báo cáo môn Big Data. Đề tài hiện tại:
**"Xây dựng và đánh giá nền tảng Big Data xử lý dữ liệu cảm biến IoT theo thời gian thực"**.

Báo cáo LaTeX đã viết đầy đủ (5 chương + abstract + titlepage), build PDF thành công.
Chưa có source code triển khai (Docker Compose, Spark job, ...); đó là phạm vi ngoài
báo cáo và sẽ thêm khi có máy chạy được. Chưa init git.

## Cấu trúc

- `Documents/report/` — báo cáo LaTeX (xem `Documents/report/README.md` cho chi tiết cấu trúc và lệnh build).
- `Documents/report/build/` — output PDF tạm, không commit.
- `.opencode/skills/` — project skills cho opencode: 6 skill của Vibe Coding V1 workflow + 3 skill viết học thuật (MIT, bản quyền kèm trong từng thư mục): `paper-writing` (phương pháp viết paper systems/CS — đọc `author_profile/` khi dùng), `research-paper-writing` (cấu trúc section, khớp claim–evidence, tự rà theo góc nhìn phản biện), `humanizer` (làm sạch văn phong AI). Dùng khi rà/sửa chương báo cáo. **Rào chắn**: 3 skill này theo quy ước học thuật tiếng Anh — áp dụng nguyên tắc (logic, cấu trúc, tránh phóng đại) cho nội dung tiếng Việt, không áp template câu tiếng Anh nguyên văn. Không dùng kỹ thuật "né AI detector" (như `sci-polish` của aut-sci-write) cho bài nộp.

## Build báo cáo

```bash
cd Documents/report
latexmk -pdf -outdir=build main.tex   # output: build/main.pdf (khoảng 40 trang)
```

- Tiếng Việt qua `babel[vietnamese]` + `fontenc[T5]` (pdflatex).
- Hình kiến trúc vẽ bằng **TikZ** trong `chapters/03-implementation.tex` (không cần file PNG ngoài).
- Biểu đồ benchmark trong chương 4 vẽ bằng **pgfplots** (compile cùng pdflatex, không cần tool ngoài).
- Code block dùng package `listings`. **Quy ước**: nội dung bên trong `lstlisting` chỉ chứa ASCII (comment tiếng Anh) để tránh lỗi UTF-8 với `listings`. Tiếng Việt nằm ngoài listing (caption, đoạn văn xung quanh).
- `references.bib` có 19 entry hợp lệ (6 paper, 6 official docs, 4 engineering docs, 2 paper mới 2026: Kafka benchmark đã xác minh DOI + Stream DaQ chưa xác minh DOI). BibTeX style: `IEEEtran` (file `IEEEtran.bst` đính kèm trong `Documents/report/`) — đánh số theo thứ tự xuất hiện. Caption chứa `\cite` phải có short caption để không lệch thứ tự đánh số qua LoF/LoT.

## Nội dung báo cáo

| Chapter | Nội dung |
|---|---|
| `01-introduction` | Bối cảnh IoT + Big Data, 5 RQ (RQ-A throughput/latency, RQ-B scalability, RQ-C fault recovery, RQ-D veracity, RQ-E storage/query), phạm vi, đóng góp |
| `02-background` | 5V, Kafka 4.3, Spark 4.2 Structured Streaming, Delta Lake, HDFS 3.5, Airflow, Prometheus/Grafana/JMX; 6 related work |
| `03-implementation` | Kiến trúc 2 mức (baseline + nâng cao), sơ đồ TikZ, data contract 8 trường, mô hình Bronze/Silver/Gold, Docker Compose services, pseudo-code 3 Spark job + Airflow DAG |
| `04-results` | Phương pháp benchmark (6 kịch bản mapping 5 RQ), 11 nhóm metric, công thức latency/throughput, 3 biểu đồ pgfplots (RQ-A latency theo rate, RQ-B throughput theo workers, RQ-C recovery curve), 3 bảng số liệu minh hoạ (ghi rõ nguồn) |
| `05-conclusion` | Tóm tắt + 4 hướng phát triển (K8s/Strimzi, HDFS HA + Kafka 3 broker, TLS/SASL/ACL, edge/fog) |

## Guidance cho agents

- Nội dung chương nằm trong `Documents/report/chapters/*.tex`, mỗi chương một file, include từ `main.tex`.
- Hình ảnh TikZ/pgfplots đặt trực tiếp trong file .tex; không cần file PNG ngoài.
- `lstlisting` body chỉ dùng ASCII — Vietnamese chỉ ở caption + đoạn văn ngoài.
- Số liệu trong chương 4 là **minh hoạ** dựa trên ShuffleBench 2024, fault recovery benchmark 2024, Confluent tuning notes. KHÔNG giả vờ đo thật — luôn ghi rõ nguồn.
- Hỏi user trước khi thêm package LaTeX mới hoặc thay đổi cấu trúc chương.
- Cập nhật file này khi có source code hoặc toolchain mới được thêm vào repo.

## Vibe Coding V1 workflow

Project dùng bốn role-based agents:

- `orchestrator`: requirement analysis, planning, delegation, recovery, synthesis, memory promotion.
- `coder`: implementation owner.
- `tester`: independent verification.
- `reviewer`: independent read-only code review.

Required flow:

`discuss → plan → user approve → implement → verify → review → user accept → memory promotion`

### Guardrails

- Orchestrator không sửa production code.
- Coder chỉ làm trong approved scope.
- Tester không repair production code.
- Reviewer không repair production code.
- Subagents không delegate subagent khác.
- Không push tự động.
- Không commit tự động trừ khi user yêu cầu.
- `.ai/plans/current-plan.md` là approved task contract.
- `.ai/state/current-task.md` là recoverable execution checkpoint.
- Dùng GitNexus cho code relationship / impact analysis khi khả dụng.
- Durable decisions phải nằm trong `.ai/memory/`, không chỉ ở session history.