# Current task: Triển khai Big Data IoT bằng Docker trên máy RAM 8–10 GB

## Status: AWAITING_USER_ACCEPTANCE — reviewer APPROVE; transfer-to-target decision pending (2026-09-16)

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
