# Approved Plan: Theory-first report — adaptive veracity-aware event-time framework

## Trạng thái
- **Approved by user:** 2026-09-01 (user: “vậy triển khai đi”).
- **Execution phase:** implementation delegated to one `coder`; verification and review
  must be performed independently by `tester` and `reviewer`.
- **No automatic commit or push.**

## Mục tiêu

Hoàn thiện phần lý thuyết của báo cáo chính tại `Documents/report/` về nền tảng
Big Data xử lý dữ liệu cảm biến IoT theo thời gian thực, giữ nguyên cấu trúc năm
chương hiện tại. Báo cáo phải phân biệt rõ kiến trúc baseline đang được mô tả với
một adaptive framework được đề xuất ở mức khái niệm/giả thuyết; không được mô tả
adaptive behavior như đã triển khai hoặc đã đo nếu repository chưa có bằng chứng.

## Bằng chứng hiện trạng

- Báo cáo chính gồm `abstract.tex`, năm chương và `references.bib`, build bằng
  `latexmk -pdf -outdir=build main.tex` trong `Documents/report/`.
- Cấu trúc thực thi được chấp nhận trong project memory/ADR:
  Bronze job = streaming, Silver job = batch, Gold job = streaming; Airflow chỉ
  điều phối các task có điểm bắt đầu/kết thúc rõ ràng.
- Implementation thực tế có tồn tại nhưng **ngoài phạm vi thay đổi**: Bronze dùng
  watermark cố định 10 phút và dedup `event_id`; Silver batch dùng physical bounds
  và quarantine; Gold tạo window aggregate.
- Chương 4 hiện công khai rằng số liệu là minh họa, không phải runtime benchmark.
- GitNexus chưa khả dụng vì repository chưa có commit; không được bịa kết quả graph.
- Corpus tài liệu ngoài repo có metadata `verified_against_source: false`; chỉ dùng
  nguồn đã xác minh hoặc ghi rõ giới hạn xác minh.

## Định vị đóng góp và claim policy

Được phép trình bày đóng góp là một **mô hình khái niệm/tổng hợp có thể kiểm chứng**
gồm định nghĩa, mapping cơ chế, bất biến và giả thuyết cho pipeline Kafka–Spark–Delta–HDFS.
Không được claim:

- novelty tuyệt đối của event-time, watermark, data quality, Lambda reconciliation
  hoặc adaptive query processing;
- adaptive watermark đã chạy trong Spark;
- cải thiện latency/throughput/accuracy đã đo;
- exactly-once xuyên nhiều sink Delta;
- HA production cho Kafka/HDFS;
- kết quả runtime từ các bảng minh họa trong Chương 4.

Mọi adaptive result phải được gọi là `đề xuất`, `mô hình`, `giả thuyết` hoặc
`việc cần kiểm chứng`; mọi số liệu Chương 4 tiếp tục được đánh dấu `minh họa`.

## Mô hình lý thuyết bắt buộc

Chèn vào Chương 2, trước phần related work, một section formal về xử lý event-time
có nhận thức veracity. Nội dung tối thiểu:

1. **Event tuple và time taxonomy.** Định nghĩa event với 9 trường contract hiện có;
   phân biệt `event_time`, producer/gateway `ingest_time`, platform receive time
   (`ingest_time_spark`) và publication/output time. Nêu giả định clock đồng bộ hoặc
   bounded clock skew khi so sánh timestamp.
2. **Lateness và window.** Định nghĩa observed lateness
   `L_i = max(0, t_platform_i - t_event_i)`, event-time window và out-of-order arrival;
   không đồng nhất lateness với riêng network delay.
3. **Watermark semantics.** Nêu watermark là tiến độ/điểm cắt ở query/window level,
   có tính đơn điệu; dùng để giới hạn state và hỗ trợ finalization, không có nghĩa
   mọi event muộn hơn đều bị xóa hoặc tự động vào quarantine.
4. **Veracity vector.** Định nghĩa các chiều completeness, validity, uniqueness,
   timeliness và consistency; mapping lần lượt tới required-field checks, physical
   bounds, event-id dedup, lateness/watermark metrics và cross-field/time checks.
5. **Rolling quality metrics.** Định nghĩa p95 lateness, late-event ratio,
   duplicate ratio, invalid/quarantine ratio, watermark lag và policy transition count.
6. **Policy state.** Mô tả hai state `FAST`/`CAUTIOUS`, ngưỡng trên/dưới, hysteresis
   theo số batch liên tiếp và hard safety bounds không bị policy nới. Nêu rõ đây là
   policy epoch/feedback design, không phải per-record native Spark watermark.
7. **Publication lifecycle.** Định nghĩa aggregate lifecycle
   `PROVISIONAL -> FINAL -> CORRECTED`, logical key theo window + dimensions, và
   yêu cầu reconciliation/idempotency. Phân biệt mục tiêu của extension với baseline
   hiện chưa có đầy đủ lifecycle này.
8. **Invariants/properties.** Ít nhất gồm record accounting (accepted + quarantine +
   explicitly dropped), watermark monotonicity, bounded dedup state, uniqueness/
   idempotent key, và correction convergence dưới giả định bounded lateness + replay
   đầy đủ. Gắn mỗi property với cơ chế/chương liên quan.
9. **Hypotheses.** Đưa H1–H4 thành giả thuyết cần kiểm chứng:
   - H1: allowed lateness tăng retention nhưng tăng state/publication delay;
   - H2: quality/quarantine accounting bám profile lỗi trong sai số lấy mẫu;
   - H3: reconciliation giảm sai lệch aggregate ở window đủ cũ;
   - H4: adaptive switching chỉ có lợi khi workload/quality shift kéo dài hơn chi phí
     chuyển policy; cần ablation fixed/adaptive và có/không hysteresis.

## Related work và nguồn

Mở rộng related work bằng bảng gap theo các cột: event-time/watermark, stream data
quality, runtime adaptation, batch correction/reconciliation và IoT/lakehouse
operationalization. Định vị tối thiểu:

- Dataflow Model/MillWheel: nền tảng event-time, logical time, watermark và trade-off
  correctness–latency–cost; không nhận là đóng góp mới.
- Structured Streaming: watermark/dedup theo query semantics.
- Stream DaQ: stream-first quality checks, windowing, dynamic constraints/meta-stream;
  không gán cho báo cáo này novelty về stream quality monitoring.
- Strider: adaptive query-plan/runtime switching; chỉ dùng metadata đã xác minh.
- Lambda ADS-B/reliability-aware work: batch correction/eventual consistency; phân biệt
  correction architecture với framework veracity-aware của báo cáo.

Chỉ thêm BibTeX entries khi kiểm tra được title, author, venue/year và DOI/URL từ
nguồn đáng tin. Dataflow Model dùng DOI đã xác minh `10.14778/2824032.2824076`;
MillWheel dùng DOI `10.14778/2536222.2536229`; Lambda ADS-B dùng DOI
`10.3390/s23177580`; Strider có thể dùng bản ISWC 2017 với DOI
`10.1007/978-3-319-68288-4_33` nếu nội dung được đối chiếu. Không đưa Stream DaQ
vào bibliography nếu chưa xác minh metadata xuất bản. Sửa hoặc loại các entry hiện
có metadata đáng ngờ thay vì giữ các cụm “extended 2024” không có căn cứ.

## Phạm vi file được phép chỉnh

- `Documents/report/references.bib`
- `Documents/report/chapters/abstract.tex`
- `Documents/report/chapters/01-introduction.tex`
- `Documents/report/chapters/02-background.tex`
- `Documents/report/chapters/03-implementation.tex`
- `Documents/report/chapters/04-results.tex`
- `Documents/report/chapters/05-conclusion.tex`

Không chỉnh `implementation/`, `homework/week2/`, `main.tex` preamble, TikZ library,
title-page placeholders hoặc project memory/AGENTS.md trong task này.

## Trình tự thực hiện

1. **Bibliography audit.** Xác minh nguồn cần dùng; thêm Dataflow/MillWheel/Lambda/
   Strider nếu đủ metadata; sửa citation key/metadata để không còn claim sai. Nếu
   nguồn không đủ chắc chắn, giữ nội dung ở mức khái quát và không thêm entry.
2. **Chapter 2 first.** Thêm formal model, definitions, properties, hypotheses và
   related-work gap table; cập nhật phần summary. Sử dụng labels ổn định cho equations
   và definitions để các chương sau tham chiếu.
3. **Introduction.** Giữ đúng 5 RQ; reframe RQ-D để bao gồm veracity/lateness policy;
   đổi ba đóng góp thành bốn, thêm contribution về conceptual framework; sửa contract
   thành 9 trường và mô tả adaptive extension là chưa triển khai.
4. **Implementation chapter.** Giữ ADR-001/ADR-002. Gắn mọi “10 phút” là baseline;
   nêu seam nơi policy extension sẽ can thiệp mà không đổi execution model. Đồng bộ
   prose/listings với code truth ở mức report: Silver batch, Bronze/Gold streaming,
   provenance timestamp, second-pass batch dedup, quarantine semantics, Gold schema.
   Không thêm adaptive code listing giả.
5. **Results chapter.** Giữ disclaimer minh họa. Tách ingestion lateness khỏi
   publication latency và dùng một taxonomy nhất quán; bổ sung công thức quality
   metrics/hypothesis-to-experiment mapping; sửa bảng RQ-D để bảo toàn record và làm
   late-event semantics khớp cơ chế; sửa các count/phase/recovery/header drift đã audit.
   Không kết luận H1–H4 đã được xác nhận.
6. **Conclusion and abstract.** Đồng bộ contribution/RQ wording, counts, baseline vs
   proposal, limitations và future work; không biến proposal thành implemented feature.
7. **Consistency pass.** Tìm các occurrence của watermark, RQ-D, latency, scenario/
   metric counts, “minh họa”, citations và kiểm tra mọi listing vẫn ASCII.

## Ngoài phạm vi

- Mọi production/application code, Spark job, simulator, Docker Compose, Airflow DAG,
  checkpoint migration và runtime adaptive controller.
- Runtime Docker benchmark hoặc thay số liệu minh họa bằng số liệu chưa đo.
- Thêm chương mới hoặc package/library LaTeX mới.
- Sửa thông tin bìa còn placeholder.
- Commit, amend, push hoặc thay đổi Git config.

## Tiêu chí chấp nhận khách quan

1. `Documents/report/` vẫn giữ cấu trúc 5 chương và build thành công bằng:
   `latexmk -pdf -outdir=build main.tex`.
2. Log không có undefined citation/reference; mọi citation key dùng trong `.tex`
   tồn tại trong `references.bib`.
3. Chỉ có một định nghĩa nhất quán cho từng loại latency; ingestion lag,
   event-time lateness và output/publication latency được phân biệt rõ.
4. RQ-D có wording nhất quán ở introduction, results, conclusion và abstract; tổng
   RQ vẫn là 5.
5. RQ-D record table bảo toàn tổng: input = accepted + quarantine + explicitly dropped.
6. Mọi occurrence của watermark 10 phút được gắn nhãn baseline; adaptive hook được
   mô tả như proposal, không phải native per-event Spark behavior.
7. Silver vẫn được mô tả là batch; Bronze/Gold semantics không bị đổi âm thầm.
8. Số scenario, metric groups, contributions, related-work count và future-work count
   khớp nội dung thực tế.
9. Mọi H1–H4 và adaptive claim được đánh dấu untested/proposed; số liệu Chương 4
   vẫn có “minh họa” và nguồn/giới hạn phù hợp.
10. `lstlisting` bodies vẫn ASCII; PDF không có lỗi font tiếng Việt hoặc công thức/
    tham chiếu bị hỏng.
11. Diff chỉ chứa các file report được phép và `.ai/` bookkeeping do orchestrator;
    không có thay đổi trong `implementation/` hay `homework/week2/`.

## Verification bắt buộc

- `latexmk -pdf -outdir=build main.tex` từ `Documents/report/`.
- Kiểm tra log cho `undefined`, `LaTeX Warning`, citation/reference và listings.
- Text search các pattern: `RQ-D`, `10 phút`, `10 minutes`, `Latency`, `minh họa`,
  scenario/metric/contribution counts; kiểm tra thủ công các kết quả.
- `pdftotext build/main.pdf` (nếu có) để xác nhận section/definition/hypothesis/gap
  table xuất hiện và tiếng Việt đọc được.
- Tester phải báo cáo riêng lỗi pre-existing, lỗi do thay đổi và giới hạn do chưa có
  runtime benchmark.

## Rủi ro và giả định

- Corpus chưa được xác minh toàn bộ; bibliography có thể phải dùng ít nguồn hơn dự kiến.
- Gold hiện là window aggregate nên không được định nghĩa per-event output latency nếu
  không bổ sung field/semantics phù hợp trong report.
- `foreachBatch`/nhiều Delta sink không tự bảo đảm atomicity; không viết claim mạnh hơn
  implementation truth.
- Chưa có GitNexus graph vì repo chưa có commit; dùng filesystem/read-only audit thay thế.
- Nếu phát hiện yêu cầu cần đổi execution model Silver hoặc thêm package/chương, coder
  phải dừng và báo orchestrator thay vì tự mở rộng scope.
