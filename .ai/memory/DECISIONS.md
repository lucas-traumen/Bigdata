# Project Memory — Decisions

Store durable architectural/product decisions here.

<!--
## ADR-XXX — <title>
- Status: accepted | superseded | deprecated
- Date:
- Context:
- Decision:
- Rationale:
- Consequences:
- Related files/modules:
-->

## ADR-001 — Kiến trúc báo cáo: 5 tầng + dải observability, Web UI optional
- Status: accepted
- Date: 2026-08-27 (user duyệt plan)
- Context: Sơ đồ kiến trúc cũ (node thả nổi, mũi tên Bronze→Silver→Gold không nhãn) gây hiểu nhầm dữ liệu tự chảy giữa các tầng.
- Decision: Mô tả kiến trúc theo 5 tầng + dải ngang observability; vẽ lại `fig:architecture` với khung dashed từng tầng, mũi tên ghi nhãn job (`Bronze job`, `Silver job`, `Gold job`, `Airflow trigger`), màu nhất quán theo loại thành phần; thêm hình phụ `fig:event-flow` (hành trình event có watermark/dedup/quarantine); Web UI đặt ở tầng Serving, vẽ nét đứt + nhãn "(optional)".
- Rationale: Làm rõ phân tầng và ownership của từng luồng dữ liệu; Web UI optional phản ánh đúng phạm vi đồ án.
- Consequences: Mọi chỉnh sửa kiến trúc chương 3 sau này phải giữ mô hình 5 tầng + nhãn job trên mũi tên; giữ ổn định label `fig:architecture`.
- Related files/modules: `Documents/report/chapters/03-implementation.tex`.

## ADR-002 — Execution model: Silver batch, Bronze/Gold streaming
- Status: accepted
- Date: 2026-08-28 (fix M1 của reviewer)
- Context: Prose cũ nói cả Silver và Gold là streaming query — sai và mâu thuẫn với pseudo-code listing.
- Decision: Văn bản thống nhất: Bronze job = streaming long-running; Silver job = **batch** (Spark batch đọc bảng Bronze, kiểm tra chất lượng, ghi append Silver); Gold job = streaming query (readStream từ Silver, window aggregation); Airflow khởi động/dừng/chạy theo lịch.
- Rationale: Khớp pseudo-code và thiết kế thực tế; tránh hiểu nhầm khi triển khai code sau này.
- Consequences: Bất kỳ văn bản nào sau này nói về Silver job phải ghi "batch"; tham chiếu tới Silver job dùng `Listing~\ref{lst:silver}` (subsection Silver không có `\label`).
- Related files/modules: `Documents/report/chapters/03-implementation.tex`.

Do not invent historical decisions during bootstrap.
