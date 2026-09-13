# Archived Plan — Refactor kiến trúc + vẽ lại mô hình trong báo cáo

Status: COMPLETED (user duyệt 2026-08-27, hoàn thành + review APPROVE + user accept 2026-08-28; memory promotion cùng ngày)
Created: 2026-08-27
Completed: 2026-08-28

Traceability: tester V1–V4 PASS (build sạch 40 trang, prose↔TikZ nhất quán),
reviewer REQUEST_CHANGES → fix M1 (Silver batch, không phải streaming) →
re-check R1–R4 PASS → APPROVE. File thay đổi chính:
`Documents/report/chapters/03-implementation.tex`.

## Goal

Refactor phần kiến trúc trong báo cáo LaTeX và vẽ lại sơ đồ mô hình cho rõ
ràng, bổ sung **Web UI / Serving layer như thành phần tuỳ chọn (optional)**
của kiến trúc. Chỉ sửa LaTeX trong `Documents/report/`, KHÔNG chạy code,
KHÔNG triển khai pipeline.

## Current state

- Báo cáo 5 chương đã build được PDF (~37 trang), kiến trúc mô tả ở chương 3
  (`chapters/03-implementation.tex`).
- Sơ đồ TikZ duy nhất về kiến trúc: `03-implementation.tex:19-76` (fig:architecture).
  Chương 4 chỉ có 3 biểu đồ pgfplots benchmark, không liên quan kiến trúc.
- Vấn đề sơ đồ hiện tại (đã phân tích với user):
  1. Không phân tầng — node thả nổi, chỉ HDFS có khung.
  2. Mũi tên Bronze→Silver→Gold gây hiểu nhầm dữ liệu tự chảy (thực tế là job riêng).
  3. Airflow mũi tên cong không rõ trigger gì.
  4. Tầng serving chỉ 1 node "Spark SQL", thiếu UI cho data scientist.
  5. Prometheus/Grafana trôi nổi không thuộc tầng nào.
  6. Thiếu watermark/checkpoint/quarantine trên hình.

## Target state

Chương 3 mục "Kiến trúc tổng thể" được refactor:

1. **Kiến trúc mô tả theo 5 tầng + 1 dải ngang**, mỗi tầng có đoạn văn giải
   thích vai trò (viết thêm nội dung chữ, không chỉ đổi hình):
   - Tầng 1 Ingestion: Sensor Simulator (+ external producers) → Kafka `sensor.raw`
   - Tầng 2 Stream processing: Spark Structured Streaming (Bronze job: parse,
     watermark, dedup)
   - Tầng 3 Storage / Lakehouse: HDFS + Delta (Bronze/Silver/Gold + quarantine)
   - Tầng 4 Batch & orchestration: Silver job, Gold job, compaction; Airflow
     điều phối các job có điểm bắt đầu/kết thúc
   - Tầng 5 Serving & UI: Spark SQL + **Web UI (optional)** cho data scientist
   - Dải ngang Observability: Prometheus/Grafana/JMX exporter cắt qua các tầng
2. **Sơ đồ TikZ chính vẽ lại** (thay fig:architecture): khung dashed từng tầng,
   luồng trên→dưới, mũi tên ghi nhãn tên job (`Bronze job`, `Silver job`,
   `Gold job`, `Airflow trigger`), màu nhất quán theo loại thành phần
   (compute/xanh, storage/cam, orchestration/tím, observability/hồng,
   serving/web UI/tím nhạt hoặc xanh nhạt, khung dashed xám).
   Web UI vẽ với đường viền nét đứt + nhãn "(optional)" để thể hiện đây là
   thành phần tuỳ chọn.
3. **Thêm 1 hình phụ mới (optional nhưng khuyến nghị)**: sơ đồ luồng sự kiện
   — hành trình 1 event từ sensor → Kafka → Bronze → Silver (rẽ quarantine nếu
   lỗi) → Gold → UI, có đánh dấu `event_time`/`ingest_time`, watermark bound,
   dedup theo `event_id`. Trực quan hoá RQ-A và RQ-D.
4. Web UI là **optional component**: mô tả vai trò (data scientist truy vấn
   Gold qua web, xem dashboard lịch sử/quarantine), công nghệ gợi ý
   (Streamlit/FastAPI), ghi rõ "tuỳ chọn triển khai, không thuộc phạm vi
   bắt buộc của đồ án". KHÔNG thêm RQ mới, KHÔNG sửa chương 4.
5. Build lại PDF bằng `latexmk -pdf -outdir=build main.tex` để xác minh
   TikZ compile được, không lỗi thiếu package.

## Scope

- SỬA: `chapters/03-implementation.tex` (mục Kiến trúc tổng thể + sơ đồ + thêm
  subsection Web UI optional + cập nhật tóm tắt chương nếu cần).
- CÓ THỂ SỬA rất nhỏ: `chapters/05-conclusion.tex` nếu cần nhắc Web UI trong
  tóm tắt kiến trúc (tối đa 1–2 câu).
- KHÔNG SỬA: chương 1, 2, 4; `references.bib` (không thêm trích dẫn mới trừ
  khi dùng entry đã có); `main.tex` (đủ package TikZ rồi); các chương còn lại.
- KHÔNG chạy Docker/code.

## Out of scope

- Triển khai source code (plan Phase 1/2 cũ tạm gác, sẽ mở lại sau).
- Thêm package LaTeX mới.
- Thay đổi 5 RQ hoặc phương pháp benchmark.

## Architecture decisions

- Web UI nằm ở tầng Serving, đánh dấu optional (nét đứt + chữ "(optional)"),
  đọc bảng Gold — KHÔNG đọc trực tiếp Kafka/Bronze.
- Giữ medallion Bronze/Silver/Gold không đổi.
- Airflow chỉ trigger job batch (Silver/Gold/compaction), vẽ mũi tên có nhãn rõ.

## Relevant files/modules

- `Documents/report/chapters/03-implementation.tex` — chính
- `Documents/report/chapters/05-conclusion.tex` — chỉnh nhỏ nếu cần
- `Documents/report/main.tex` — không sửa, chỉ build

## Implementation steps

1. Viết lại đoạn văn mục "Kiến trúc tổng thể" theo 5 tầng + dải observability.
2. Vẽ lại sơ đồ TikZ chính: khung tầng dashed, nhãn mũi tên, màu nhất quán,
   Web UI optional nét đứt.
3. Vẽ sơ đồ luồng sự kiện (hình phụ) — hành trình event có watermark/dedup/quarantine.
4. Thêm subsection ngắn "Tầng serving và Web UI (tuỳ chọn)" mô tả vai trò,
   công nghệ gợi ý, phạm vi optional.
5. Cập nhật tóm tắt chương 3 (danh sách bullet cuối chương) cho khớp.
6. Build PDF, kiểm tra không lỗi, kiểm tra hình render đúng trang.

## Constraints

- Body `lstlisting` vẫn ASCII-only (quy ước repo); tiếng Việt chỉ ngoài listing.
- TikZ không dùng package ngoài những gì `main.tex` đã `\usetikzlibrary`
  (shapes, arrows.meta, positioning, fit, backgrounds); nếu cần library mới
  phải hỏi user trước.
- Giữ nhãn `\ref{fig:architecture}` ổn định để không vỡ tham chiếu chỗ khác;
  hình mới dùng nhãn mới.
- Giữ giọng văn và cấu trúc heading hiện có của báo cáo.

## Acceptance criteria

1. `latexmk -pdf -outdir=build main.tex` build thành công, không lỗi
   undefined reference, không overfull hbox nghiêm trọng ở phần sửa.
2. Sơ đồ chính có 5 tầng + dải observability, mũi tên có nhãn job, màu nhất
   quán, Web UI optional nét đứt.
3. Hình luồng sự kiện thể hiện đủ: sensor → Kafka → Bronze → Silver (nhánh
   quarantine) → Gold → UI, có watermark/dedup được đánh dấu.
4. Đoạn văn kiến trúc giải thích đúng luồng: dữ liệu không "tự chảy" giữa
   các tầng mà được chuyển bởi job cụ thể.
5. Web UI xuất hiện đúng vị trí optional, không làm thay đổi 5 RQ hay bảng
   benchmark.
6. Tóm tắt chương 3 và (nếu chỉnh) chương 5 khớp với kiến trúc mới.

## Required tests / verification (tester)

- V1: build PDF sạch (`latexmk`), đối chiếu log không lỗi.
- V2: kiểm `\ref`/`\label` không vỡ (grep nhãn hình trong toàn chapters).
- V3: kiểm nội dung: đếm số tầng trong văn bản khớp số khung trong TikZ;
  nhãn mũi tên khớp tên job trong mục cài đặt chi tiết.
- V4: PDF mở được, hình không tràn lề (tester xem build/main.pdf).

## Risks / open questions

1. Số hình phụ: hiện plan gồm 1 hình phụ (luồng sự kiện). Nếu user muốn thêm
   hình triển khai (Docker Compose/mem_limit) → thêm bước, tăng phạm vi.
   → ĐÃ CHỐT với user: Web UI optional = CÓ; các câu Q2/Q3/Q4 cũ coi như chấp
   nhận phạm vi trong plan này.
2. TikZ phức tạp có thể gây lỗi compile khó debug — mitigated bằng build sớm,
   lặp từng phần.
3. Không có GitNexus (chưa init git) — verification dựa vào grep + build PDF.
