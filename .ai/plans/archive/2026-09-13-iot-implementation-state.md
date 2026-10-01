# Current task: Triển khai Big Data IoT bằng Docker trên máy RAM 8–10 GB

## Status: IN_PROGRESS — phần mở rộng 2026-09-18. **Nhánh A (báo cáo) HOÀN TẤT + TỐI ƯU KIẾN TRÚC (bỏ Airflow, alert trong Q3 — PDF 63 trang, 0 undefined). Nhánh B (code) ĐÃ IMPLEMENT + TEST + REVIEW + REMEDIATE + RE-VERIFY xong chuỗi đầy đủ (2026-09-18 chiều): coder implement E3 → tester PASS-with-conditions (không blocker) → reviewer APPROVE-with-conditions (SF1-SF3) → coder remediate (SF1 `*METRIC_NAMES` + guard AST Starred, SF2 comment GNU date, SF3 label `iot.simulator=1` + filter project-scoped, T1 +3 test identity-only event, docs 111 tests) → tester re-verify PASS (negative control độc lập: 10 test fail khi thêm metric thiếu DDL — guard mạnh hơn claim; filter project-name resolution khớp `name: bigdata-iot` top-level compose). 111/111 unit tests OK. TOÀN BỘ THAY ĐỔI CHƯA COMMIT — chờ user duyệt + quyết định commit. Runtime Docker/e2e (D1-D9) vẫn defer máy mục tiêu.**

## NHÁNH A2 — Tái cấu trúc Chương 2 "làm kĩ" (2026-09-18 tối, user chốt "dừng, làm kỹ 2 chương trước")

- **Skill mới:** 3 skill học thuật cập nhật 18:44 hôm nay (paper-writing mở rộng: author_profile, figure_templates/tikz_skeletons, red_team_protocol, writing_checklists; research-paper-writing, humanizer refresh). Đã load research-paper-writing cho đợt này (áp nguyên tắc: 1 đoạn 1 thông điệp, claim-evidence alignment, thuật ngữ ổn định — theo rào chắn AGENTS.md: áp nguyên tắc cho tiếng Việt, không áp template câu tiếng Anh).
- **Cấu trúc ch2 (theo TOC user duyệt):** 2.1 hạ 2 mục con vào thân; 2.2 9→5 (event tuple+lateness gộp; veracity+rolling gộp; policy+lifecycle gộp; invariants+hypotheses gộp); 2.3 7→5 (Parquet+PostgreSQL gộp thành "Lưu trữ"; batch-orchestration+observability gộp); 2.4 9→5 (Dataflow/MillWheel/SS gộp; Strider+ADS-B gộp; Kafka-practices+benchmarks gộp; khảo sát hấp thụ vào đoạn mở; gap table giữ). Chương 2 rút 24→23 trang.
- **Sửa claim-evidence (phát hiện khi đọc toàn bộ ch2):** event tuple 9 trường đơn giá trị → vector (4 trường định danh + m ∈ (R∪⊥)^6) khớp data contract thật; XOÁ claim "watermark θ=10 phút baseline" (implementation không có watermark — dùng is_late + LATE_THRESHOLD_S=60s); bảng veracity-mapping mechanism cập nhật theo cơ chế thật (Q2a/Q2b, per-metric NULL+reason, processed_events PK, dropDuplicates batch); I3 (state dedup) + I4 (khoá Gold) viết lại theo implementation; đoạn "Khoảng cách với baseline" lifecycle viết lại theo batch full-replace thật; Kafka producer claim sửa (Bridge là producer, không phải simulator); logical key K: location×sensor_type (field đã chết) → sensor_id×metric; bỏ hết "pass 1/pass 2", "Bronze/Silver/Gold job", "silver_quarantine", "9 trường".
- **Label giữ nguyên toàn bộ** (kể cả 4 label bị ch3/ch4 tham chiếu: subsec:event-tuple, watermark, policy, hypotheses); label gộp (lateness, rolling-metrics, lifecycle, hypotheses) gắn lại vào heading đã gộp — 0 undefined.
- **Kết quả build:** 63 trang, 0 undefined citation/reference, overfull max 14.9pt (không tăng). Trùng n-gram 79→96 nhưng toàn bộ phần tăng là thuật ngữ contract chuẩn (danh sách 6 chỉ số, trường định danh, "không cộng dồn") — nhất quán thuật ngữ có chủ đích ch2↔ch3, không phải trùng văn phong.
- **Chưa làm (theo lệnh "dừng ở ch2"):** gộp 5.2 9→4 mục của Chương 5 trong đề xuất TOC — hoãn, chờ ch1+ch2 được user duyệt xong.
- **2026-09-18 tối (tiếp): user chọn option (a) — BỎ toàn bộ mục "Tóm tắt chương"** ở ch2/ch3/ch4 (ch5 giữ "5.1 Tóm tắt" vì đó là tóm tắt toàn báo cáo, thành phần chuẩn của chương Kết luận). Mỗi chương giờ kết bằng đoạn chuyển 2–4 câu không heading (ch2: "Với nền tảng khái niệm và công nghệ đã rõ..."; ch3: "Với kiến trúc, data contract..."; ch4: giữ caveat "Toàn bộ kết luận định lượng vẫn là minh hoạ..." + câu chuyển). Lý do: đoạn mở chương đã làm roadmap + ch5 đã là lớp tóm tắt + audit cho thấy các mục này là nguồn lặp câu. Kết quả: 63→61 trang, mục lục mất 3 dòng, "Tóm tắt chương" = 0 trong PDF, 0 undefined, overfull không đổi.
- **2026-09-18 tối (tiếp nữa): user hỏi "sao 1.1 trích dẫn [4]?" + duyệt chuyển sang IEEE hiện đại.**
  - **Chẩn đoán gốc rễ (không phải do ieeetr sort theo author!):** 3 caption hình ch4 (fig:rq-a/b/c) chứa `\cite` và KHÔNG có short caption → caption dài (kèm `\cite`) bị ghi vào `.lof`, khi Danh sách Hình được typeset ở ĐẦU document, các `\cite` này chạy TRƯỚC nội dung → chiếm số [1]-[3], đẩy trích dẫn đầu §1.1 (iot_big_data_survey) xuống [4].
  - **Chẩn đoán gốc rễ (không phải do ieeetr sort theo author!):** 3 caption hình ch4 (fig:rq-a/b/c) chứa `\cite` và KHÔNG có short caption → caption dài (kèm `\cite`) bị ghi vào `.lof`, khi Danh sách Hình được typeset ở ĐẦU document, các `\cite` này chạy TRƯỚC nội dung → chiếm số [1]-[3], đẩy trích dẫn đầu §1.1 (iot_big_data_survey) xuống [4].
  - **Fix 1:** thêm short caption (không `\cite`) cho fig:rq-a/b/c → LoF sạch, thứ tự đánh số theo đúng thứ tự xuất hiện trong văn bản.
  - **Fix 2 (user duyệt):** chuyển `\bibliographystyle` từ `ieeetr` sang `IEEEtran` (v1.14, tải từ CTAN, lưu `Documents/report/IEEEtran.bst` — file mới, PHẢI commit kèm). Format: tên tác giả viết tắt kiểu IEEE, đánh số theo thứ tự xuất hiện.
  - **Kết quả verify:** trích dẫn đầu tiên của báo cáo (§1.1, iot_big_data_survey) = **[1]** ✓; bibliography [1] Y. Sasaki / [2] M. Fragkoulis / [3] M. Mohammad... theo đúng thứ tự văn bản; LoF/LoT không còn số [n]; 61 trang, 0 undefined.
  - **README.md + AGENTS.md đã đồng bộ** (style IEEEtran + quy ước caption chứa cite phải có short caption).
  - **Về mục 1.2:** user hỏi format liệt kê RQ bằng description list có chuẩn học thuật không → TRẢ LỜI: có, dạng "RQ-A (Throughput & Latency): câu hỏi?" là chuẩn systems/CS papers, giữ nguyên.
  - **Kiến thức bền (đưa vào memory khi promote):** caption LaTeX chứa `\cite` không có short caption sẽ leak vào LoF/LoT và chạy trước body → lệch numbering với mọi style đánh số theo thứ tự xuất hiện (IEEEtran/unsrt).
- **2026-09-18 tối (tiếp nữa nữa): RÀ CHƯƠNG 1 theo kiểu "đánh giá + sửa từng mục" (user duyệt từng phần):**
  - §1.2: user bắt lỗi câu mở strawman ("Khác với cài Kafka + Spark rồi chạy thử") + câu "dùng Spark để tính nhiệt độ trung bình" — viết lại thành khẳng định hướng tiếp cận; 5 RQ viết lại: mỗi RQ đúng 1 câu hỏi, nhãn tiếng Việt, bỏ ngoặc chen chi tiết kịch bản, RQ-D chốt về kế toán bản ghi. GIỮ RQ-A→RQ-E: user đưa paper "Extracting, Detecting, and Generating Research Questions" (Elsevier/UvA, 555k CS articles) làm dẫn chứng dạng RQ là chuẩn (không vào bib — lệch domain).
  - §1.3 "Phạm vi đề tài" (user đổi tên mục): 4 bullet catalogue → 4 đoạn văn trôi (bản đầu 3 nhãn đậm "cộc lóc" — user chê, viết lại lần 2 mới đạt): hệ thống 1 máy không liệt version; dữ liệu tự sinh + seed kèm LÝ DO; ngoài phạm vi mở bằng câu cầu "Cùng nguyên tắc tiết chế tài nguyên"; nguyên tắc seam mở bằng phủ định có chủ đích. Loại 10 tên trường + số phiên bản + kịch bản đối chứng (đều đã có ch3/ch4).
  - §1.4: bullet 1 stack-catalogue → quyết định thiết kế (tách giao thức, idempotency ở PG, BSG Parquet cục bộ); bullet 3 bỏ 7-tên-kịch bản → "bảy kịch bản ánh xạ 5 RQ + điều kiện ghi tường minh"; bullet 4 "Số liệu minh hoạ và/hoặc đo thật" → "Cách trình bày kết quả minh bạch"; "ngôn ngữ chung" → "hệ quy chiếu chung" (hết trùng abstract). Giữ enumerate (đúng chuẩn contribution list).
  - §1.5: đọc + đánh giá — OK giữ nguyên.
  - Kết quả: 60 trang, 0 undefined, citation [1] tuần tự IEEEtran. Ch1 giờ mỗi thông tin chỉ nằm ở chương đúng của nó; văn phong khẳng định, không strawman.

## NHÁNH B — checkpoint chi tiết (2026-09-18)

- **Coder đã làm:** xoá `airflow/` + `.dockerignore` + profile airflow khỏi compose (còn 10 service, 2 profile live/batch); tạo `scripts/run-batch-hourly.sh` (window UTC vừa kết thúc, mkdir-lock + PID staleness + grace 60s, gọi run-batch.sh) và `scripts/run-multi-sim.sh` (spawn/stop/status 5 zone qua `docker compose run -d --rm --label iot.simulator=1`, manifest per-zone); `METRIC_CONFIG` trong `common.py` (PySpark-free, single source 6 chỉ số bounds/threshold/direction, schema chuyển thành cached builder); validation 2 tầng (identity → quarantine; per-metric → Silver NULL + `metric_issues` `type:metric`; metric vắng = station subset WEATHER/AIR/MULTI); alert trong Q3 foreachBatch (rule_id = tên chỉ số, high strictly >, low strictly <, ON CONFLICT DO NOTHING, cùng transaction upsert latest); Gold PK (sensor_id, hour_start, metric) qua `stack()` unpivot; schema/backend/dashboard 6 chỉ số; Kafka retention 3600000 + bridge queue 5000.
- **Quyết định thiết kế đáng chú ý coder tự đưa (reviewer đã approve từng cái):** non-numeric → Silver không quarantine; batch hourly KHÔNG drain live (chạy song song stream, RAM 2×2560m cần đo D5/D7); không xây meta-stream riêng (dữ liệu có sẵn qua metric_issues); alerts giữ cột temperature_c legacy (nullable).
- **Tester findings minor còn mở (không chặn):** M4 fault-inject `missing_field` giờ là no-op dữ liệu (metric thiếu = subset hợp lệ) — cần chỉnh kỳ vọng demo D3; batch song song live cần đo RAM đỉnh.
- **Không commit/push. Runtime D1-D9 defer máy mục tiêu** (xem docs/TEST_REPORT.md).

## Phần mở rộng 2026-09-18 — rewrite báo cáo + đa nguồn/đa chỉ số

- **Bối cảnh:** HEAD `c4c01b5` (implementation bản nhẹ đã commit, working tree sạch trừ
  `.opencode/` untracked). Báo cáo LaTeX (`Documents/report/`) mô tả kiến trúc
  Delta/HDFS/Spark standalone/Prometheus — LỆCH so với implementation thật (plain
  Parquet + PostgreSQL + Spark local[2] + MQTT/Bridge + script stats). User yêu cầu
  xóa/thay nội dung cũ cho khớp flow + lý thuyết mới.
- **Nguyên tắc xuyên suốt (user chốt):** demo dữ liệu/tài nguyên nhỏ nhưng kiến
  trúc giữ seam mở rộng ngang; cái gì demo được trên 1 máy thì demo, TLS/HA đa máy
  chỉ ghi Hướng mở rộng; code chỉ làm thứ ảnh hưởng chạy đúng; "kiến trúc có gì
  phản ánh hết vào báo cáo, đừng bớt".
- **Xác minh nguồn (2026-09-18):** paper Kafka `1afro78` ĐÃ XÁC MINH qua Crossref
  (Muzeeb Mohammad, ICICyTA 2025, IEEE, pp.612-617, DOI
  10.1109/icicyta68677.2025.11362748). paper Stream DaQ `1l2-9P` CHƯA XÁC MINH DOI
  (Crossref không liên quan, Semantic Scholar 429, DBLP chặn Anubis) — ghi entry
  với thông tin toàn văn local, để trống DOI + note rõ, KHÔNG bịa DOI.
- **Payload 6 chỉ số (user chốt "có gì thêm hết"):** temperature_c [-50,200]/alert>35;
  humidity_pct [0,100]/alert>80; co2_ppm [0,5000]/alert>1000; pressure_hpa
  [300,1100]/alert<950; pm25_ugm3 [0,1000]/alert>35; light_lux [0,200000]/không alert.
  Row có 1 chỉ số out-of-bounds → vẫn vào Silver, chỉ số đó = null + reason per-metric.
- **Phạm vi nhánh A (tài liệu, orchestrator làm trực tiếp — không phải production
  code implementation):** abstract + 01..05 + references.bib + README báo cáo. Giữ
  main.tex structure + package hiện có. Số liệu Chương 4 vẫn MINH HOẠ đến khi có
  số đo máy mục tiêu.
- **Phạm vi nhánh B (code, chờ approve → coder/tester/reviewer):** 5 simulator ad-hoc
  + run-multi-sim.sh; payload vector 6 chỉ số; validation per-metric theo bảng config;
  alert tách khỏi Spark sang bảng alert_rules + bước riêng; Gold thêm chiều metric;
  Kafka retention 1h; bridge queue 5000; 7 bài demo. Chi tiết E3 trong plan.
- **Checkpoint nhánh A (HOÀN TẤT 2026-09-18):** [x] plan; [x] task state; [x] bib;
  [x] abstract; [x] ch1; [x] ch2; [x] ch3; [x] rà ch4/ch5; [x] build PDF (63 trang,
  0 undefined); [x] README build instructions (TEXINPUTS + cảnh báo file stale);
  [x] vẽ lại fig:architecture + fig:event-flow theo ảnh tay user (khối Cảm biến
  môi trường 6 ô chỉ số + nhãn publish/subscribe/produce acks=all);
  [x] pass rà soát format + trùng lặp (Overfull 52→4, trùng n-gram 199→79);
  [x] **TỐI ƯU HÓA KIẾN TRÚC 2026-09-18 (user chốt "bỏ cái nguy cơ lỗi nhất,
  triển khai lại"):** BỎ AIRFLOW hoàn toàn khỏi báo cáo (chỉ còn 3 mention hợp
  lệ: khi nào cần điều phối đầy đủ ở ch2, lý do thiết kế script ở ch3, hướng
  nâng cấp K8s ở ch5; T4 đổi thành "Batch (kích hoạt bằng script/cron)" với node
  run-batch-hourly + cron host trong sơ đồ TikZ; PostgreSQL 1 database app thay
  vì 2; profile compose chỉ còn live/batch); ALERT QUAY VÀO Q3 (bỏ mục "Bước
  cảnh báo tách rời" + bảng alert_rules, thay bằng mục "Cảnh báo theo ngưỡng
  trong Q3" \label{subsec:alert-q3}: dùng chung bảng ngưỡng cấu hình với
  validation, insert idempotent (event_id, rule_id), chia sẻ transaction với
  upsert sensor_latest); sửa luôn bullet Tóm tắt ch2 còn sót stack cũ
  (Delta/HDFS/Prometheus → Parquet/PG/script).
- **Kỹ thuật rà format đã dùng:** (1) `\emergencystretch=3em` trong main.tex
  chống tràn do token `\texttt{}` dài; (2) short caption `[...]` cho
  figure/table caption dài (chống tràn LoF/LoT); (3) tách chuỗi slash
  "A/B/C" thành "A, B, C" khi gây tràn; (4) tách `\texttt{a, b, c}` nhiều
  cột thành các `\texttt` riêng; (5) bảng hẹp: footnotesize + rút gọn nhãn
  cột + dời cite ra ngoài cell.
- **Kiến thức build bền (đã verify 2026-09-18):** build đúng =
  `TEXINPUTS="build:" latexmk -pdf -outdir=build main.tex` từ `Documents/report/`;
  phải xóa `main.aux`/`main.bbl`/`main.blg` stale ở thư mục gốc nếu tồn tại (bibtex
  đọc nhầm aux cũ → .bbl thiếu entry mới → citation `[?]`); bibtex ghi .bbl vào
  `build/` cùng chỗ aux; pdflatex tìm main.bbl theo search path từ CWD nên cần
  TEXINPUTS trỏ vào build/.
- Không sửa file trong `implementation/` ở nhánh A. Không commit/push.

---

## (Lịch sử) Status cũ: AWAITING_USER_ACCEPTANCE — reviewer APPROVE; transfer-to-target decision pending (2026-09-16)

## Runtime transfer checkpoint (2026-09-16)

- User chạy **stack CŨ** trên máy mục tiêu (`ferb27@gtr:~/Bigdata/implementation`):
  smoke-test 10000 events @ 2000 evt/s → **6 passed, 0 failed** (screenshot).
  Kết luận: máy mục tiêu đủ tài nguyên (chạy nổi stack cũ ~13.3 GB → thừa ngân
  sách cho stack mới ~4.6 GiB).
- **Lịch sử git phân kỳ:** máy gtr ở HEAD `d01118b` — không tồn tại trong repo
  máy này (local chỉ có `72c3c2e` → `bdf01f8`; `git cat-file -t d01118b` fail;
  origin/master = bdf01f8). `git pull` trên gtr sẽ không hoạt động như mong đợi.
- **Bản implementation mới CHƯA commit** — chỉ tồn tại trong working tree máy
  này; GitHub chưa có. Cần user phê duyệt commit + push để gtr nhận code
  (fresh clone hoặc `git fetch origin && git reset --hard origin/master` trên
  gtr — có tính phá hủy local commits của gtr, phải user tự quyết).
- Runbook trên máy gtr (stack mới): stop stack cũ đang chạy trên gtr trước
  (giải phóng RAM + ports 1883/9092/5432/8000/8088) → `scripts/preflight.sh` →
  `scripts/prepare-host.sh` → `scripts/init.sh` → `scripts/run-mode.sh live` →
  `scripts/wait-for-health.sh spark-stream backend dashboard` →
  `scripts/test-e2e.sh` → `scripts/test-recovery.sh` →
  `scripts/resource-report.sh --once`. Lưu ý: `smoke-test.sh` cũ đã bị xoá;
  thay thế là `test-e2e.sh`. Chưa có lỗi open nào đã biết trong code mới
  (81/81 unit tests, reviewer APPROVE); lỗi runtime (nếu có) sẽ được báo về
  orchestrator để đưa coder sửa.
- **2026-09-16: user phê duyệt commit + push.** Commit này chuyển toàn bộ
  implementation mới + `.ai/` bookkeeping + `.gitignore` lên GitHub
  (origin/master) để máy gtr fresh clone. `.opencode/**` vẫn giữ ngoài
  scope/untracked. Sau push: chờ user chạy runbook trên gtr, rồi bàn luận
  sâu theo yêu cầu user. KHÔNG tự claim runtime pass.

## Current phase

Coder đã rewrite toàn bộ `implementation/` theo approved plan. Tester độc lập đã
đóng blocker Airflow F1: Dockerfile multi-stage tự thân (tự bake jars + COPY
jobs/run, không phụ thuộc image local), thêm build-time + runtime assertions.
Tester phát hiện F2 minor và F3 blocker; coder đã sửa cả hai. Tester re-verify
độc lập bằng stub-docker harness, static checks và unit checks đã PASS; 29/29
call-site Compose dùng array expansion đúng, F1 Airflow artifact vẫn PASS.
Reviewer yêu cầu B1 (pg_sink multi-row SQL) và M1 (resource-report parsing); coder
đã remediate cả hai + minor cleanups. Tester xác nhận B1/M1 PASS nhưng phát hiện
F4 blocker trong cùng đường Q3: JDBC select dùng `received_at_utc` thay vì cột
staging `received_at`, khiến Spark JDBC write fail deterministic trên PostgreSQL.
Tester re-verify sau F4 đã PASS toàn bộ static/unit/harness; reviewer re-review
đã APPROVE với điều kiện runtime deferred. Không còn blocker code/static hoặc
regression. Runtime Docker/benchmark vẫn DEFERRED (máy hiện tại: ~11 GB disk,
~3.6 GB RAM available, port 1883 bận). Chưa commit.

## Durable inputs

- Plan hiện hành: `.ai/plans/current-plan.md` (APPROVED — IMPLEMENTATION IN PROGRESS).
- Repository HEAD: `bdf01f8`; working tree chứa thay đổi scope này + bookkeeping
  `.ai/**` + untracked `.opencode/**` (giữ nguyên ngoài scope).
- GitNexus: chưa index repo (đã kiểm tra ở phase trước); dùng filesystem/git diff
  làm bằng chứng.

## Implemented (bởi coder, theo plan §6)

- Rewrite `implementation/compose.yaml` (profiles live|batch|airflow), `.env.example`,
  mosquitto.conf, simulator MQTT QoS1 + manifest + fault inject, bridge manual-ACK
  sau Kafka delivery, ONE Spark app (Q1/Q2a/Q2b/Q3, checkpoint riêng, local[2]),
  batch.py UTC [start,end) + dedup deterministic + hourly upsert, pg_sink SQL
  idempotent (latest tie-break, alerts PK, processed_events, gold full-replace),
  SQL init/schema/queries/test-data, FastAPI + dashboard, Airflow 2.10.5 + JDK17
  + Spark copy (LocalExecutor, max_active_runs=1), 9 scripts (preflight,
  prepare-host, init, wait-for-health, run-mode, reset-pipeline, resource-report,
  test-e2e, test-recovery), 55 unit tests (stdlib-only), README + docs.
- Removed obsolete: `implementation/infra/` (HDFS/Delta/Prometheus/Grafana),
  spark jobs Delta cũ, spark conf metrics, run-{bronze,silver,gold}.sh,
  init-cluster.sh, smoke-test.sh, simulator __pycache__.
- Version pins đã verify thật (PyPI/Docker Hub/Maven, 2026-09-14) — chi tiết
  trong `implementation/docs/TEST_REPORT.md`.

## Evidence / checks đã chạy trên máy này

- `python3 -m unittest discover -s implementation/tests` → **55/55 OK** (3 suite:
  validation rules, pg SQL semantics trên SQLite, batch window contract).
- `python3 -m py_compile` 10 file Python → OK.
- `bash -n` 9 script shell + 2 run wrapper → OK.
- `docker compose -f implementation/compose.yaml config` (+ từng profile
  live/batch/airflow) → exit 0 (đã fix: Go-yaml không nhận 2 merge key `<<`;
  kafka/mqtt phải có `profiles: ["live"]`; `entrypoint: []` cho spark services).
- `scripts/preflight.sh` chạy thật → exit 2 READY WITH WARNINGS, đúng thực tế
  host (RAM available 3.9 GB < 4.6 GB budget; port 1883 busy bởi project khác;
  /data chưa tồn tại; disk 11 GB).
- Version pins verify thật 2026-09-14 (PyPI/Docker Hub/Maven Central; paho
  manual_ack API đọc từ wheel source; sha256 pin cho 4/5 jar Maven).
- Runtime Docker build/e2e/recovery/load → **DEFERRED/BLOCKED** (disk/RAM/port).
  Không có bất kỳ số đo runtime nào được claim.

## Tester re-verification / findings F2 and F3

- Tester re-verify status: `PASS` cho F1 và toàn bộ unit/syntax/compose/static
  checks; runtime vẫn `DEFERRED/BLOCKED`.
- **F2 minor:** `implementation/scripts/init.sh` gọi `docker compose build` không
  có `--profile live --profile batch --profile airflow`, nên Compose chỉ build
  backend và dashboard khi init chạy. Điều này lệch với echo/README và khiến
  image còn lại build lazy. Cần coder thêm profile flags rồi tester chạy lại
  static checks; không build thật trên host này.
- **F3 blocker:** `COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")` được
  gọi tại các call-site bằng `$COMPOSE` thay vì `"${COMPOSE[@]}"` trong
  `init.sh`, `run-mode.sh`, `reset-pipeline.sh`, `test-e2e.sh`,
  `test-recovery.sh`, `resource-report.sh`. Bash chỉ mở rộng phần tử đầu
  (`docker`), nên `init.sh` dừng với `unknown flag --profile` và các lệnh
  `up/exec` đều sai. Coder phải sửa toàn bộ call-site và tester phải chạy
  stub-docker harness để chứng minh argv.

## Reviewer B1/M1 and tester F4 handoff

- Reviewer verdict: `request_changes` vì B1 + M1; coder đã xử lý và thêm tests.
- Tester re-verify B1/M1: SQL input reduction, duplicate-count semantics,
  resource stats parser và 78/78 tests đều PASS.
- **F4 blocker (ĐÃ SỬA — xem section remediation F4):** `implementation/spark/
  jobs/pg_sink.py` trong `apply_stream_batch()` chọn `"received_at_utc"` để
  JDBC ghi vào `staging_stream_events`, trong khi `implementation/sql/schema.sql`
  khai báo cột `received_at`. Spark JDBC map theo tên, nên Q3 micro-batch fail
  trước khi upsert. Coder đã sửa thành `df["received_at_utc"].alias("received_at")`
  và thêm guard test pin JDBC staging columns. Chỉ sửa trong implementation,
  không build/runtime trên host này.

## Coder remediation F4 (ĐÃ SỬA 2026-09-14)

- **Fix chính:** `implementation/spark/jobs/pg_sink.py` — `apply_stream_batch()`
  đổi `"received_at_utc"` thành `df["received_at_utc"].alias("received_at")`
  (JDBC map theo tên cột; output select giờ khớp chính xác 8 cột DDL
  `staging_stream_events`: event_id, sensor_id, event_time, temperature_c,
  received_at, kafka_topic, kafka_partition, kafka_offset). Thêm comment ghi
  rõ contract map-by-name.
- **NULLS LAST cho ordering (đúng phạm vi cho phép):** window của
  `SQL_UPSERT_LATEST` thêm `NULLS LAST` vào cả 3 key (`event_time DESC
  NULLS LAST, received_at DESC NULLS LAST, event_id DESC NULLS LAST`). Lý do:
  PostgreSQL mặc định DESC = NULLS FIRST còn SQLite (nơi unit tests chạy SQL
  verbatim) xếp NULL cuối — nếu `received_at` NULL, hai engine chọn winner
  KHÁC nhau; `NULLS LAST` đồng bộ cả hai và khớp tie-break
  `desc_nulls_last()` sẵn có của batch.py (Spark side). `event_time`/`event_id`
  đã được WHERE lọc NOT NULL nên modifier là no-op với dữ liệu hiện tại.
- **Audit các JDBC cột khác (không còn mismatch):** chỉ có 2 JDBC write trong
  repo — `staging_stream_events` (F4, đã sửa) và `staging_hourly_agg`
  (`apply_hourly_batch`: sensor_id, hour_start, event_count, avg_value,
  min_value, max_value — khớp DDL, không cần sửa). Static audit 4 câu INSERT
  bảng đích (`sensor_latest`, `alerts`, `processed_events`,
  `gold.sensor_hourly`) so với schema.sql → toàn bộ khớp (PASS).
- **Guard test mới:** `implementation/tests/test_jdbc_columns.py` (3 tests,
  stdlib-only `ast` + regex, không PG/PySpark/SQLite): pin output của
  `df.select(...)` trong `apply_stream_batch`/`apply_hourly_batch` khớp
  tên-cột DDL của 2 bảng staging (so thứ tự), và pin nguồn select nằm trong
  `SILVER_SCHEMA` (common.py) — bắt cả 2 nửa của lớp lỗi F4 (thiếu alias và
  alias cột không tồn tại). Select arg không parse tĩnh được → test fail có
  thông báo rõ (giữ write contract luôn checkable tĩnh). Negative control:
  chạy guard trên bản pg_sink.py pre-F4 (trong /tmp, không sửa repo) → guard
  từ chối đúng; bản đã sửa → pass.
- **Checks sau remediation (static, không Docker build/runtime):**
  - `python3 -m unittest discover -s implementation/tests` → **81/81 OK**
    (78 cũ + 3 guard mới).
  - `python3 -m py_compile` toàn bộ jobs + backend/bridge/simulator/DAG +
    stats_parse + 5 test file → OK.
  - `bash -n` toàn bộ .sh (scripts + run wrappers + install-jars) → OK
    (không đổi shell script nào trong F4).
  - `docker compose -f implementation/compose.yaml config --quiet` default +
    cả 3 profile → exit 0.
  - Negative control + ánh xạ cột in evidence (trong /tmp) như trên.
- **Docs đồng bộ:** `docs/TEST_REPORT.md` (số test 81/81 + bullet coverage
  guard), `README.md` (81 tests), `docs/ARCHITECTURE.md` §4.4 (mô tả window
  có NULLS LAST + lý do).
- Runtime Docker/benchmark vẫn **DEFERRED/BLOCKED** (disk ~11 GB, RAM
   available ~3.6 GB, port 1883 bận) — hành vi JDBC trên PostgreSQL thật xác
   nhận bằng e2e trên máy mục tiêu; KHÔNG claim pass runtime.

## Final tester/reviewer disposition (2026-09-14)

- Tester sau F4: `PASS` — 81/81 unit tests, py_compile 16/16, bash -n 12/12,
  Compose config/profiles, JDBC/schema audit, B1/M1/F1/F2/F3 regression audit.
- Reviewer re-review: `APPROVE` với điều kiện runtime deferred. Không còn
  blocker; scope và docs được xác nhận đúng.
- Minor observations không chặn bàn giao: drain offset inclusive/exclusive cần
  quan sát trên máy mục tiêu; staging nullable theoretical edge; một vài wording/
  count docs nhỏ; e2e dùng `eval` cho expression nội bộ; duplicate_count replay
  cần diễn giải đúng. Không mở remediation mới trước runtime.
- Runtime obligations còn lại: build thật + warmup, live e2e/fault injection,
  batch rerun, Airflow DAG, recovery/loss window, resource/load measurement và
  backup/restore theo `implementation/docs/TEST_REPORT.md` D1–D10. Không ghi số
  vào report nếu chưa có command/log evidence.

## Tester re-verification after F4 (PASS 2026-09-14)

- `python3 -m unittest discover -s implementation/tests`: **81/81 OK**;
  F4 JDBC guard 3/3, B1 multi-row SQL 12/12, M1 parser 11/11.
- `py_compile` 16/16, `bash -n` 12/12, Compose config default + từng profile
  exit 0; cả ba profile liệt kê đúng 12 service.
- Independent AST/regex audit xác nhận hai JDBC staging write khớp DDL và bốn
  `ON CONFLICT` target khớp primary keys; `received_at_utc` chỉ đổi thành
  `received_at` tại biên JDBC đúng chủ đích.
- F1/F2/F3 không regression; 29/29 Compose array call-site vẫn dùng
  `"${COMPOSE[@]}"`; init vẫn build đủ profiles.
- Negative control pre-F4 làm guard fail đúng, bản hiện tại pass; harness/log ở
  `/tmp` ngoài repo. Không sửa file và không build/runtime.
- Runtime PostgreSQL/Docker/e2e/Airflow/load/resource vẫn DEFERRED/BLOCKED trên
  host này, cần tester chạy ở máy mục tiêu.

## Tester re-verification after F3 (PASS 2026-09-14)

- Tester độc lập status `PASS` trong phạm vi static/harness; không có blocker mới.
- `bash -n` 12/12, unit tests 55/55, py_compile 13/13, Compose config default
  và profiles exit 0.
- Stub-docker harness ghi nhận 37 invocation, 29/29 call-site Compose đúng
  `"${COMPOSE[@]}"`; init giữ đủ ba `--profile` và chạy exit 0. Negative control
  bare `$COMPOSE` tái hiện exit 125, xác nhận harness bắt đúng F3.
- F1 Airflow multi-stage/artifact assertions, source paths, shared installer và
  runtime assertion được kiểm tra tĩnh và PASS.
- Runtime build/e2e/recovery/load vẫn DEFERRED/BLOCKED đúng yêu cầu user; không
  có runtime result nào được claim. Harness/log nằm ngoài repo dưới `/tmp`.
- Minor observations không chặn: `LIVE_SERVICES` dead variable và câu lịch sử
  `$COMPOSE build` trong TEST_REPORT; reviewer quyết định có cần dọn không.

## Coder remediation F2 (ĐÃ SỬA 2026-09-14)

- `implementation/scripts/init.sh`: `$COMPOSE build` → `$COMPOSE --profile live
  --profile batch --profile airflow build` (dạng `--profile` trước subcommand,
  giống pattern sẵn có ở reset-pipeline.sh/run-mode.sh, tương thích Compose v2;
  `--profile` là repeatable flag). Thêm comment giải thích semantics Compose v2
  (không profile → chỉ build 2 service unprofiled backend/dashboard).
  Không sửa file nào khác — echo/README trước đó đã mô tả đúng hành vi sau fix.
- Static checks sau remediation: `bash -n init.sh` OK; `docker compose config
  --quiet` default exit 0; `config --services` với cả 3 profile chọn đủ 12
  services (gồm 8 service có `build:` → 6 image). Chưa build Docker trên host
  này (theo plan §12); không claim runtime.

## Coder remediation F3 (ĐÃ SỬA 2026-09-14)

- Sửa toàn bộ **29 call-site** `$COMPOSE …` → `"${COMPOSE[@]}" …` trong 6 file,
  không thay đổi dòng nào khác (đối chiếu diff với bản chụp trước sửa):
  - `implementation/scripts/init.sh` (9 site; giữ nguyên profile flags F2:
    `"${COMPOSE[@]}" --profile live --profile batch --profile airflow build`)
  - `implementation/scripts/run-mode.sh` (14)
  - `implementation/scripts/reset-pipeline.sh` (1)
  - `implementation/scripts/test-e2e.sh` (1)
  - `implementation/scripts/test-recovery.sh` (2)
  - `implementation/scripts/resource-report.sh` (2, nằm trong `$( )`)
- Không đổi definition `COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")`,
  không đổi các lệnh docker raw (`inspect/kill/stats`, wait-for-health) — đúng
  yêu cầu "không sửa lệnh không dùng array". Không thêm helper lib/test file
  mới vào repo: plan §6 không liệt kê shell-argv test và stub harness là artifact
  của tester; harness dùng để kiểm chứng nằm ngoài repo `/tmp/opencode/f3-harness/`.
- Checks (static, không Docker build/runtime):
  - `bash -n` 12/12 .sh OK.
  - grep audit: 0 occurrence `$COMPOSE` ngoài dạng `"${COMPOSE[@]}"`; tổng 29
    call-site (init 9, run-mode 14, reset 1, e2e 1, recovery 2, resource 2).
  - Stub-docker harness (log argv từng lần exec, mô phỏng inspect/psql/kafka):
    trước sửa — stub nhận `ARG[0]=--profile` (thiếu `compose -f`), init exit
    125 tái hiện tester; reset-pipeline thoát 0 "im lặng" nhờ `|| true`.
    Sau sửa — 6 script chạy qua harness: reset/init/live/batch/airflow/status/
    resource exit 0; 36/36 lời gọi compose mang đủ
    `compose -f <impl>/compose.yaml` + đúng thứ tự profile/subcommand/service
    args (build giữ 3 `--profile` trước `build`; batch truyền
    `--start/--end` nguyên vẹn; e2e/recovery dừng ở poll-loop chờ pipeline
    thật — gọi compose đã log, exit 124 là chủ đích harness).
  - `docker compose config --quiet` default + `--profile live --profile batch
    --profile airflow config --services` (thứ tự flag đúng như scripts) →
    exit 0, liệt kê đủ 12 services.
  - `python3 -m unittest discover -s implementation/tests` → 55/55 OK.
- Runtime Docker build/e2e/recovery/load vẫn **DEFERRED/BLOCKED** (disk/RAM/
  port 1883). Không có số đo runtime nào được claim.
- Ghi chú nhỏ cho reviewer: `implementation/docs/TEST_REPORT.md` dòng ~90 còn
  câu "`$COMPOSE build` đơn giản" trong phần log remediation F1 (lịch sử, mô tả
  trạng thái trước F2) — ngoài scope F3 nên không sửa; có thể cập nhật khi
  tester/reviewer thấy cần.

## Coder remediation reviewer findings B1 + M1 (ĐÃ SỬA 2026-09-14)

- **B1 blocker — pg_sink SQL multi-row conflict key.** `SQL_UPSERT_LATEST` và
  `SQL_MARK_PROCESSED` cũ cho phép input có nhiều dòng cùng conflict key trong
  một statement → PostgreSQL lỗi "ON CONFLICT DO UPDATE command cannot affect
  row a second time" (xảy ra khi một micro-batch có nhiều event cùng sensor hoặc
  duplicate fault injection). Fix bằng derived table + window function (cú pháp
  chung PostgreSQL/SQLite, chạy verbatim trên SQLite trong unit tests):
  - `SQL_UPSERT_LATEST`: input rút về **1 winner mỗi sensor** qua
    `ROW_NUMBER() OVER (PARTITION BY sensor_id ORDER BY event_time DESC,
    received_at DESC, event_id DESC)` (rn=1); guard strict-newer với row hiện
    có giữ nguyên (row-value comparison).
  - `SQL_MARK_PROCESSED`: input rút về **1 dòng mỗi event_id** — "first
    occurrence" = vị trí Kafka thấp nhất (`ORDER BY kafka_partition ASC NULLS
    LAST, kafka_offset ASC NULLS LAST` — NULLS LAST để PG/SQLite order NULL
    giống nhau); insert `duplicate_count = n-1` (số dư in-batch duplicates,
    dùng `COUNT(*) OVER (PARTITION BY event_id) - 1`); khi event đã có sẵn,
    `DO UPDATE` cộng `EXCLUDED.duplicate_count + 1 = n` đúng cỡ batch.
    `SQL_COUNT_PAYLOAD_CONFLICTS` không đổi (join/count, không ON CONFLICT).
  - `SQL_INSERT_ALERTS` (DO NOTHING) và `SQL_UPSERT_HOURLY` (input đã key-unique
    do groupBy ở batch.py) không đổi — ghi chú lý do trong docstring module.
- **M1 — resource-report.sh docker stats parsing.** MemUsage dạng
  `50MiB / 1GiB` chứa space làm lệch cột khi `split()` whitespace. Đổi format
  sang tab (`{{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}`) và tách
  toàn bộ parse/JSON-assembly ra module mới `implementation/scripts/stats_parse.py`
  (script gọi `python3 scripts/stats_parse.py <9 args>`). Parser skip an toàn
  hàng malformed/thiếu trường/rỗng, tolerance trailing tab; record shape JSONL
  giữ nguyên.
- **Tests mới (23):** `test_pg_semantics.py` +12 (multi-row same-sensor: winner
  deterministic bất kể thứ tự stage, multi-sensor một batch, old-batch không
  ghi đè, mixed new/old, tie-break event_id cùng timestamp; multi-row
  processed_events: duplicate delta n-1/n, first occurrence giữ lowest Kafka
  position, mixed fresh+replayed, payload conflict với multi-row staging) và
  `tests/test_resource_report.py` +11 (canned docker stats: mem_usage giữ
  nguyên khoảng trắng, skip hàng không tab/thiếu trường/rỗng/trailing tab,
  build_record shape + null placeholders). Tổng 78/78 pass.
- **Minor cleanups (trong scope, rẻ):** bỏ biến chết `LIVE_SERVICES` trong
  run-mode.sh; cập nhật câu stale "`$COMPOSE build` đơn giản" trong
  TEST_REPORT.md §1.1 (mô tả trạng thái trước F2/F3); đồng bộ docs:
  ARCHITECTURE.md §4.4 (input reduction + duplicate_count theo batch),
  TEST_REPORT.md (bảng check 78/78 + coverage list), README.md (số test 78).

### Checks sau remediation B1/M1 (static, không Docker build/runtime)

- `python3 -m unittest discover -s implementation/tests` → **78/78 OK**.
- `python3 -m py_compile` 15 file (jobs + backend/bridge/simulator/DAG +
  scripts/stats_parse.py + 4 test file) → OK.
- `bash -n` toàn bộ .sh (scripts + run wrappers + install-jars) → OK.
- `docker compose -f implementation/compose.yaml config --quiet` default và
  cả 3 profile → exit 0.
- Smoke gọi thật `python3 scripts/stats_parse.py <args>` với canned rows tab +
  1 hàng space-separated malformed → JSON đúng, hàng malformed bị skip.
- Negative note: nếu docker CLI không dịch `\t` (hành vi documented của docker
  formatter), parser sẽ skip hết hàng → `containers: {}` thay vì lệch cột —
  degradation an toàn, không sai số im lặng.
- PostgreSQL syntax được đối chiếu thủ công (derived table + window +
  `ASC NULLS LAST` + row-value comparison đều hợp lệ PG); xác nhận thực tế
  trên PG nằm trong e2e DEFERRED trên máy mục tiêu (không claim).

## Tester handoff / remediation F1 (ĐÃ SỬA 2026-09-14)

- Tester độc lập status `fail` ngày 2026-09-14.
- PASS (trước remediation): 55/55 unit tests, py_compile, bash -n, compose
  default + 3 profiles, preflight expected warning, static scope checks.
- BLOCKER F1 (đã remediate): `airflow/Dockerfile` stage `spark-base` là raw
  `apache/spark:4.2.0-java21-python3` → `COPY --from` chỉ lấy distribution,
  thiếu `jobs/`, `run/run-batch.sh`, connector/JDBC jars.
- Remediation: `spark/install-jars.sh` (mới, installer pin dùng chung 2 image,
  5/5 jar sha256 — kafka-clients `014b4ef3…` pin mới, cả 5 tái xác minh bằng
  download thật); `spark/Dockerfile` dùng installer chung (image content giữ
  nguyên); `airflow/Dockerfile` multi-stage tự thân (stage `spark-app` bake
  jars + COPY spark/jobs + spark/run; stage airflow COPY /opt/spark + assertions
  `test -x run-batch.sh` / `test -f batch.py` / glob 3 jar / `spark-submit
  --version` + warmup dưới combo Spark 4.2.0 + Java 17); compose 2 service
  airflow build `context: .` + `dockerfile: airflow/Dockerfile`, `airflow-init`
  thêm runtime assertion trước `airflow db migrate`; `.dockerignore` (mới,
  allowlist spark/+airflow/, loại .env khỏi context); `init.sh` bỏ build-order
  dependency; README + TEST_REPORT cập nhật cách build.
- Checks sau remediation (static): compose config exit 0 default + 3 profiles;
  bash -n toàn bộ scripts + install-jars.sh; py_compile 10 file; 55/55 unit
  tests; đối chiếu tĩnh mọi COPY source tồn tại + thuộc allowlist. Docker
  build/runtime vẫn DEFERRED — không claim runtime pass.

## Approved scope

Xem `.ai/plans/current-plan.md` §3–§6. Coder không mở rộng scope; plain Parquet,
1 PostgreSQL 2 database, Airflow scheduler không webserver, observability bằng
scripts — đã thực hiện đúng các default được duyệt.

## Checkpoints

- [x] Read user requirements.
- [x] Inspect repository, existing implementation, git status and host resources.
- [x] Check GitNexus availability (not indexed).
- [x] Archive prior current plan/state without deleting it.
- [x] Write new plan.
- [x] User approval (implementation only; runtime deferred to another machine).
- [x] Implement by coder (artifact hoàn chỉnh; runtime deferred).
- [x] Independent verification by tester (static/unit pass; found F1 blocker).
- [x] Coder remediation for F1 + focused re-verification (2026-09-14).
- [x] Tester re-verify độc lập (PASS static/unit; đóng F1, mở F2 minor).
- [x] Coder remediation F2: init.sh build truyền đủ 3 profile (2026-09-14).
- [x] Tester re-verify độc lập sau F2 (phát hiện F3 blocker).
- [x] Coder remediation F3 + focused re-verification (2026-09-14, xem bên dưới).
- [x] Tester re-verify độc lập sau F3 (stub-docker harness + static checks).
- [x] Coder remediation reviewer findings B1 (blocker) + M1 + minor cleanups
      (2026-09-14, xem section tương ứng).
- [x] Tester re-verify B1/M1 (PASS, phát hiện F4 blocker).
- [x] Coder remediation F4 + focused re-verification (2026-09-14, xem section
      "Coder remediation F4"; 81/81 tests, guard có negative control).
- [x] Tester re-verify độc lập sau F4 (PASS static/unit/harness, 2026-09-14).
- [x] Independent review by reviewer (APPROVE với runtime conditions, 2026-09-14).
- [ ] User acceptance/runtime acceptance trên máy mục tiêu.
- [ ] Promote durable memory only after acceptance.

## Recovery instructions

Đọc `AGENTS.md`, `.ai/plans/current-plan.md`, file này, `git status`/`git diff`.
F1 + F2 + F3 + B1 + M1 + F4 đã xử lý và tester PASS; bước tiếp theo là reviewer
re-review độc lập. Điểm remediation nằm ở:
F1: `implementation/{airflow/Dockerfile,spark/install-jars.sh,spark/Dockerfile,
compose.yaml,.dockerignore,scripts/init.sh,README.md,docs/TEST_REPORT.md}`;
F2: `implementation/scripts/init.sh` (chỉ dòng lệnh build + comment, thay đổi
1 dòng thực thi);
F3: `"${COMPOSE[@]}"` tại 29 call-site trong `implementation/scripts/{init.sh,
run-mode.sh,reset-pipeline.sh,test-e2e.sh,test-recovery.sh,resource-report.sh}`
(9/14/1/1/2/2 — chi tiết + evidence trong section "Coder remediation F3");
B1: `implementation/spark/jobs/pg_sink.py` (SQL_UPSERT_LATEST + SQL_MARK_
PROCESSED dùng derived table + ROW_NUMBER/COUNT window, input key-unique) và
`implementation/tests/test_pg_semantics.py` (lớp MultiRowSensorLatestTests +
MultiRowProcessedEventsTests);
M1: `implementation/scripts/resource-report.sh` (format docker stats TAB),
`implementation/scripts/stats_parse.py` (mới — parse/record builder testable),
`implementation/tests/test_resource_report.py` (mới — 11 test canned sample);
F4: `implementation/spark/jobs/pg_sink.py` (alias `received_at` trong
apply_stream_batch + NULLS LAST trong SQL_UPSERT_LATEST) và
`implementation/tests/test_jdbc_columns.py` (mới — 3 guard test cột JDBC
staging, stdlib-only);
Minor: `implementation/scripts/run-mode.sh` (bỏ LIVE_SERVICES chết),
`implementation/docs/{ARCHITECTURE.md,TEST_REPORT.md}`, `implementation/README.md`
(đồng bộ semantics + số test 81).
Không tự sửa plan/memory; không commit/push.

---

## Checkpoint 2026-09-19 — đọc và rà Chương 1/2, chưa chỉnh tài liệu

- **Trạng thái:** `AWAITING_USER_ACCEPTANCE — tester PASS; reviewer APPROVE_WITH_NOTES`.
- Đã đọc toàn bộ `Documents/report/chapters/01-introduction.tex` và
  `Documents/report/chapters/02-background.tex`, cùng implementation liên quan:
  `common.py`, `validation.py`, `stream_app.py`, `batch.py`, `pg_sink.py`,
  `schema.sql`, bridge, simulator, compose và test report.
- Đã đối chiếu các điểm chính: contract vector sáu metric; bốn trường
  identity/time bắt buộc; metric vắng là station subset; metric lỗi được mask
  trong Silver; identity lỗi mới vào Quarantine; `is_late` dùng
  `LATE_THRESHOLD_S`; baseline không gọi `withWatermark`; dedup Q3 dùng
  `processed_events`; Gold key có chiều metric; runtime D1--D9 vẫn DEFERRED.
- Các vấn đề ưu tiên cao đã phát hiện để trao đổi với user:
  1. Chương 2 còn dùng watermark/policy/lifecycle như phần lớn của mô hình dù
     chúng chưa chạy; cần tách baseline và extension.
  2. `processed_events` không phải bounded state, nên bất biến I3 hiện tại không
     khớp semantics baseline.
  3. Văn bản gọi Gold là `full-replace`, trong khi code hiện tại upsert các key có
     trong staging và chưa xoá key cũ vắng khỏi cửa sổ.
  4. Một số claim metric/quarantine/watermark và batch drain cần hạ hoặc sửa để
     khớp code thật; Chương 4 cũng cần đồng bộ sau khi user duyệt hướng sửa.
  5. Có lỗi văn bản rõ ràng quanh dòng 505 của Chương 2 (mảnh câu còn sót).
- Đã chạy audit cơ học sơ bộ trên Chương 1/2: không có passive-voice hit theo
  regex; còn nhiều `---`/dash trong prose và bảng, cùng hai nhóm từ cần xem lại
  (`finalize`, `replication factor`). Đây là đầu vào cho lượt sửa, chưa coi là
  pass cuối.
- GitNexus không có index cho repository BigData; không dùng kết quả graph.
- **User đã duyệt viết lại hai chương:** D1 giữ watermark/policy/lifecycle như
  extension có nhãn; D2 giữ metric lý thuyết nhưng phân loại trạng thái bằng chứng;
  D3 chỉ sửa tài liệu, không sửa code Gold; D4 giữ bối cảnh Việt Nam ngắn; D5 giữ
  Stream DaQ với cảnh báo metadata chưa xác minh.
- **Bước tiếp theo:** giao coder chỉ sửa hai file `01-introduction.tex` và
  `02-background.tex`, build sơ bộ nếu có thể; sau đó giao tester và reviewer độc
  lập. Không mở rộng sang Chương 3/4 nếu chưa có finding cụ thể.

### Coder checkpoint 2026-09-19

- Coder đã viết lại toàn bộ hai file được duyệt:
  `Documents/report/chapters/01-introduction.tex` và
  `Documents/report/chapters/02-background.tex`.
- Coder báo cáo đã giữ label/cross-reference, không thêm citation, sửa semantics
  theo code và build được PDF 64 trang với 0 undefined citation/reference.
- Coder ghi nhận sáu finding ngoài scope ở Chương 3/4/5 (trường bắt buộc, ví dụ
  quarantine, thuật ngữ Gold full-replace, watermark lag, số nhóm metric). Không
  tự sửa các file đó.
- Chưa coi task hoàn tất: tester phải xác minh độc lập build, diff scope,
  cross-reference, claim-evidence và mechanical audit; reviewer đọc lại hai
  chương sau tester.

### Tester findings and coder remediation checkpoint 2026-09-19

- Tester độc lập ban đầu trả `FAIL`: một lỗi MAJOR ở mô tả đường cron batch và
  bốn lỗi MINOR về observability, khả năng nhân bản Bridge, loss window và cỡ
  claim khảo sát. Tester xác nhận các lỗi đều nằm trong phạm vi E6 của Chương 1/2;
  các finding Chương 3/4/5 được tách riêng, không dùng để đánh trượt lượt này.
- Coder đã sửa đúng phạm vi:
  - mô tả `run-batch-hourly` là cron wrapper có lock/PID + grace, chạy song song
    live; phân biệt với `run-mode.sh batch` one-shot có drain;
  - tách query progress, bộ đếm PostgreSQL/API, resource-report JSONL và nguồn
    dashboard theo đúng implementation;
  - hạ nhân bản Bridge thành seam mở rộng vì `client_id` hiện hard-code;
  - ghi rõ giới hạn `max_queued_messages` và loss window khi broker bão hoà;
  - đồng nhất và giảm mức khẳng định về cỡ dữ liệu trong Chương 1/2.
- Coder báo cáo `git diff --check` sạch và build bằng lệnh repo đạt 65 trang,
  0 undefined citation/reference. Đây là báo cáo của coder, cần tester re-run
  độc lập trước khi chuyển reviewer.

### Coder checkpoint 2026-09-19 (lần 2) — remediation 5 finding E6 của tester

- Phạm vi tuân thủ đúng yêu cầu: chỉ sửa
  `Documents/report/chapters/02-background.tex` (4 finding) +
  `01-introduction.tex` (chỉ câu survey, finding 5). Không đụng file khác,
  không commit/push, không sửa finding Ch3/4/5 ngoài phạm vi.
- **F-major (batch cron):** đoạn §2.3.5 bỏ claim sai "script dừng nguồn phát,
  đợi drain"; viết đúng theo `run-batch-hourly.sh`: khoá mkdir+PID (từ chối
  lần chạy chồng, dọn khoá stale), cửa sổ giờ UTC vừa kết thúc,
  `\texttt{BATCH\_GRACE\_S}`, chạy SONG SONG live không drain; phân biệt
  tường minh với `run-mode.sh batch` (one-shot: dừng simulator+Bridge, chờ
  Spark tiêu hết backlog Kafka rồi mới dừng stream job và chạy batch — khớp
  `stop_live()` trong run-mode.sh).
- **Observability:** viết lại khớp `resource-report.sh` + backend/dashboard:
  4 luồng bằng chứng độc lập (i) Spark query progress → JSONL (numInputRows,
  thời lượng trigger, offset đầu/cuối — khớp record thật của stream_app.py,
  bỏ "processedRowsPerSecond/batchDuration" không có trong record), (ii) bộ
  đếm PostgreSQL/API (processed/duplicates/alerts/last_processed — khớp
  `/api/progress`, không còn gọi là "bộ đếm nội bộ JSONL"), (iii) docker
  stats + host RAM/disk, (iv) backlog (Kafka end offset, số tệp Parquet, số
  dòng PG). Script chỉ lấy mẫu (iii)-(iv) vào `resource-report.jsonl`; nói
  rõ JSONL là bằng chứng cho operator/benchmark, dashboard đọc trực tiếp
  endpoint ứng dụng, KHÔNG tiêu thụ JSONL. Câu sau chỉnh antecedent
  ("trích được từ progress API, thống kê container và bộ đếm PostgreSQL").
- **Bridge scaling:** recast thành seam tương lai — mỗi instance cần định
  danh phiên MQTT + producer Kafka riêng; ghi rõ bản hiện tại dùng một định
  danh cố định hai phía (`client_id="iot-bridge"` cả MQTT lẫn Kafka, không
  có cấu hình instance) và trỏ sang Ch5 future work (khớp sẵn câu
  "nhân bản Bridge (kèm client.id duy nhất)" ở conclusion).
- **Bridge loss:** bỏ claim tuyệt đối "không có bản tin nào bị mất âm thầm";
  at-least-once có điều kiện "chừng nào broker còn giữ được bản tin trong
  phiên bền"; nêu cửa sổ mất `max_queued_messages` của Mosquitto (đặt 5000,
  khớp mosquitto.conf; bridge.py docstring ghi loss window này).
- **Thang số survey (Ch1 + Ch2):** cả hai chapter giờ dùng "từ hàng triệu đến
  hàng tỷ". Đã tải PDF open-access Sasaki 2022 (DOI hợp lệ) và xác minh:
  survey KHÔNG nêu khoảng bản ghi/ngày; survey chỉ nêu Gartner "20 tỷ thiết bị
  IoT 2025" + IDC "175 ZB 2025". Ch1 viết lại để citation chỉ gắn với fact có
  trong survey (dự báo thiết bị, khối lượng tăng), khoảng/ngày thành ước lượng
  "có thể" của báo cáo; Ch2 giữ hedge "có thể" + thang mới.
- Checks: `git diff --check` cả 2 chapter = sạch; build
  `TEXINPUTS="build:" latexmk -pdf -outdir=build main.tex` OK — 65 trang,
  0 undefined citation/reference; 6 overfull (max 14.9pt, đúng baseline, không
  box nào trong vùng sửa; 3 box thuộc Ch3 có sẵn).
- **Concern còn lại cho orchestrator/tester (out of scope của lượt này):**
  Ch3 vẫn có claim lệch cùng loại — mục "Tầng 4" nói "Hai container
  spark-stream và spark-batch không bao giờ chạy đồng thời" và §"Điều phối
  batch bằng script và cron" nói run-batch-hourly "kiểm tra pipeline live đã
  drain"; bảng service nói resource-report "Đọc docker stats + query progress
  JSONL" (script không đọc JSONL progress). Cần lượt tài liệu sau cho Ch3.
   Ngoài ra mosquitto.conf có `persistence false` (bản tin chờ cho bridge
   offline mất khi broker restart) — Ch2 chưa nêu window này vì ngoài danh sách
   fix tester chỉ định; reviewer cân nhắc khi đọc lại.

### Reviewer checkpoint 2026-09-19

- Reviewer độc lập kết luận `APPROVE_WITH_NOTES`; không có BLOCKER/MAJOR nào
  chặn nghiệm thu E6. Reviewer xác nhận toàn bộ acceptance chính: thesis/RQ,
  flow Ch2, 4 required fields, station subset, per-metric masking, identity
  quarantine, `is_late`, không watermark baseline, marker `processed_events`,
  Gold idempotent upsert, extension labeling, cron-vs-one-shot semantics,
  observability, Bridge loss/scaling, citations và labels.
- Notes không blocking cần giữ lại khi user quyết định:
  1. Ch2 chưa nêu `persistence false` của Mosquitto nên broker restart tạo thêm
     loss window;
  2. heading I3 có thể ghi thẳng ``không bounded state`` để tránh đọc nhầm;
  3. `kafka_docs_4_3` không phải nguồn MQTT chuyên biệt cho câu QoS;
  4. có lặp nhẹ ở quy ước baseline/extension và hai label `policy`/`lifecycle`
     cùng section.
- Không tự sửa các note này trước user acceptance. Các lỗi Ch3/4/5 (O1--O4)
  tiếp tục ngoài scope E6.
- Verification đã đủ điều kiện chuyển sang approval gate của user. Chưa promote
  memory và chưa archive task state.

---

## Checkpoint 2026-09-19 — lỗi dàn trang Mục 2.2.7

- **Trạng thái:** `AWAITING_USER_ACCEPTANCE` (user xác nhận sửa bằng tin nhắn
  “vậy sửa”, 2026-09-19; coder/tester/reviewer đã hoàn tất).
- User báo PDF ở Mục 2.2.7 bị lỗi; ảnh cho thấy nhãn bất biến dài bị cắt/tràn
  sang lề trái (ví dụ chỉ còn `record (baseline).`).
- Đã đối chiếu source `Documents/report/chapters/02-background.tex` với PDF
  `Documents/report/build/main.pdf`: I1--I5 đều dùng optional label dài trong
  `itemize`, gây overflow do label width mặc định không đủ.
- Đề xuất đã ghi trong `.ai/plans/current-plan.md` §E7: đổi riêng danh sách
  I1--I5 sang `description` với nhãn ngắn `I1.`--`I5.` và đưa tên bất biến vào
  thân item in đậm; không thêm package và không đổi semantics/prose/công thức.
- Phạm vi production chỉ một file `02-background.tex`; đã chuyển giao coder
  triển khai phương án E7.3. Orchestrator không tự sửa production.
- GitNexus không có index cho BigData; bằng chứng hiện tại là source/PDF/git diff.
- Coder đã hoàn tất E7: đổi I1--I5 sang `description` với nhãn ngắn, chỉ sửa
  `Documents/report/chapters/02-background.tex`, không commit/push.
- Coder báo cáo `git diff --check` và build LaTeX pass, 65 trang, 0 undefined
  reference/citation; cần tester chạy lại độc lập, không coi báo cáo này là
  verification cuối.
- Tester độc lập đã `PASS`: chạy build thường và `latexmk -gg`, kiểm tra PDF,
  bbox và log; 0 LaTeX error/undefined citation/reference/duplicate label,
  công thức (2.9) giữ nguyên, I1--I5 không còn tràn lề. Overfull còn lại là
  baseline ngoài vùng E7; isolated diff vẫn chỉ 14 dòng ở `02-background.tex`.
- Bước kế tiếp bắt buộc: reviewer độc lập đọc plan, diff và bằng chứng tester;
  chưa user-accept cuối và chưa promote memory.
- Reviewer độc lập đã `APPROVE_WITH_NOTES`, không blocker/major: fix layout được
  xác nhận đúng scope và đủ bằng chứng; notes chỉ nhắc giữ isolation boundary,
  không mô tả build là zero-warning toàn cục, và duy trì quy ước nhãn ngắn.
- E7 hiện sẵn sàng để user nghiệm thu; chưa commit/push và chưa promote memory.
