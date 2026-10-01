# Task: Triển khai runtime IoT Big Data stack (D1–D7, D9) trên máy hiện tại

## Trạng thái: AWAITING_USER_ACCEPTANCE — D1–D7+D9 hoàn thành

- Plan: `.ai/plans/current-plan.md` (APPROVED — user "triển khai luôn đi"
  2026-09-30, chấp nhận mặc định DATA_ROOT fallback + bỏ qua D8).
- Task week7 vẫn AWAITING_USER_ACCEPTANCE riêng:
  `.ai/plans/archive/2026-09-29-week7-pyspark-awaiting-acceptance.md`.

## Cấu hình đã chốt

- `.env`: `DATA_ROOT=/home/lucas/bigdata-demo`, `MQTT_HOST_PORT=1884`
  (1883 bị project khác chiếm).
- Máy: RAM available 8.4 GB, disk 99 GB, Docker 29.1.3, Compose v5.5.0,
  preflight exit 2 (chỉ warnings 1883 + /data).

## Checkpoints

- [x] Đọc dự án + đo hiện trạng máy (preflight, free, df, ports).
- [x] Archive plan/state week7, viết plan triển khai mới.
- [x] User approve ("triển khai luôn đi").
- [x] Coder §5 bước 1–13 (unit tests → .env → prepare → init → live →
      resource → e2e → recovery → batch → batch-hourly → backup → docs → restart live).
- [ ] Tester verify độc lập (§7) — hoặc user tự kiểm.
- [ ] Reviewer review.
- [ ] User acceptance → memory promotion.

## Recovery

Đọc `AGENTS.md` + `.ai/plans/current-plan.md` + file này. Coder ghi nhật ký
từng bước vào mục "Coder log" bên dưới (append). Pipeline dữ liệu nằm ở
`/home/lucas/bigdata-demo`; container compose project trong `implementation/`.
Không commit/push. `reset-pipeline.sh` chỉ chạy khi user yêu cầu.

## Coder log

Orchestrator tự chạy (user: "bạn tự chạy khỏi coder" 2026-09-30 — không dùng
subagent coder do lỗi model premium). Log evidence trong `/home/lucas/bigdata-demo/logs/`.

- 2026-09-30 Step 1: `python3 -m unittest discover -s implementation/tests`
  → **111/111 OK** (0.032s). `.env` xác nhận git-ignored (`git check-ignore`).
- Step 2: `.env` tạo từ `.env.example`, `DATA_ROOT=/home/lucas/bigdata-demo`,
  `MQTT_HOST_PORT=1884`.
- Step 3: `prepare-host.sh` → tạo đủ 10 subdir dưới DATA_ROOT.
- Step 4 (D1): `init.sh` → exit 0 (log `init.log`). Build 5 images OK;
  `[warmup] OK kafka010=class ...KafkaSourceProvider pgjdbc=class org.postgresql.Driver`;
  schema check = 6 tables; serving tables đủ 6 (sensor_latest, alerts,
  processed_events, gold.sensor_hourly, staging_stream_events, staging_hourly_agg);
  topic `sensor_raw` PartitionCount=3 RF=1 retention.ms=3600000. **D1 PASS**.
- Step 5 (D2): `run-mode.sh live` chạy lần 1 → FAIL ở `run-multi-sim.sh:
  Permission denied`. **Bug #1**: 2 script thiếu mode execute (`100644` trong
  git index): `run-multi-sim.sh`, `run-batch-hourly.sh`. Fix: `chmod +x` cả hai.
  Kèm fix cosmetic: `${#ZONES}` (độ dài chuỗi=29) → `ZONE_COUNT` (wc -w = 5).
  Spawn 5 sim zone OK sau đó.
- **Bug #2 (blocker)**: spark-stream Restarting — query q3_pg crash
  `ModuleNotFoundError: No module named 'psycopg2'` trong foreachBatch
  (`spark/jobs/pg_sink.py:59`, lazy import nên warmup cũ không bắt được).
  Fix: `spark/Dockerfile` thêm `RUN pip3 install --no-cache-dir
  psycopg2-binary==2.9.10` (cùng pin backend); `warmup.py` thêm
  `import psycopg2` để build fail-fast lần sau. Rebuild spark-stream+
  spark-batch OK: `[warmup] OK … psycopg2=2.9.10`. Force-recreate
  spark-stream → healthy, hết restart loop.
- 2026-09-30 ~16:59Z: **D2 PASS** — 12 container up (5 sim-zone, spark-stream
  healthy), API /health ok database=true, /api/progress events=5561 sensors=25
  alerts=92, /api/sensors/latest trả đúng vector 6 chỉ số + station-subset NULL
  (zoneA_AIR_02 chỉ có co2/pm25), Bronze 79 parquet / Silver 47 parquet,
  manifest 5 zone ~5712 dòng. Log: `run-live.log`, `rebuild-spark.log`.
- Step 6 (D7): `resource-report.sh --interval 30 --count 10` — lần 1 bị shell
  tool timeout kill (4 mẫu, lưu `resource-report-partial.jsonl`); re-run detached
  bằng `setsid` (PID 146585). Mẫu 1 (17:00:53Z): spark-stream 1.07GiB/2.5GiB
  (42.75%) cpu 141%, kafka 451MiB/1GiB, postgres 29MiB, backend 34MiB, bridge
  15MiB, mqtt 1.7MiB, dashboard 10MiB; host avail RAM 8894MB, disk 94GB;
  Bronze 127 / Silver 79 / Quarantine 41 parquet; pg: sensor_latest 25,
  alerts 108, processed 6361.   Tổng RSS stack ~1.6 GiB (dưới ngân sách 4.6 GiB).
- Step 6 (D7) đủ 10 mẫu (17:12:03Z→17:17:06Z, 5 phút): peak spark-stream
  1.326GiB/2.5GiB (53%) cpu 125.7%, kafka 455.8MiB/1GiB (44.5%), postgres
  36.9MiB, backend 35.7MiB, bridge 15.8MiB, mqtt 2.7MiB, dashboard 10.2MiB.
  **Tổng peak ~1.9 GiB**. Host avail RAM min 8339MB, disk 94GB ổn định.
  Throughput: pg processed 13061→16111 = 3050 evt/5min ≈ 10.2 evt/s (khớp
  5 zone × 2 evt/s). Bronze 529→712, Silver 347→469, Quarantine 175→236 parquet.
  JSONL: `logs/resource-report.jsonl`.
- Step 7 (D2/D3): `test-e2e.sh` → **PASS 6/6** (log `test-e2e.log`). Simulator
  200 evt fault-inject seed 42: faults=10 duplicates=2 puback_fail=0. Manifest
  fault breakdown: non_numeric 3, out_of_bounds 2, bad_timestamp 2, duplicate 2,
  late_30s 1, late_5min 1, missing_field 1. processed 18657, alerts 334,
  sensor_latest 30, quarantine 284 file, silver 565 file. Đối chiếu D3 (đọc
  parquet qua spark-submit, `qcheck.py`): **quarantine 2 row đúng = 2
  bad_timestamp** (error_reason `missing_or_invalid_event_time`); Silver
  `metric_issues` STRING, 5 row flagged = 3 non_numeric + 2 out_of_bounds —
  khớp CHÍNH XÁC fault profile e2e. Silver 24259 row. **D3 PASS**.
  Ghi chú: `metric_issues` là StringType (không phải array) — script qcheck
  lần đầu dùng F.size() fail, sửa thành F.length(); KHÔNG phải bug pipeline.
- Step 8 (D6): `test-recovery.sh` chạy lần 1 → **FAIL 2/4** (spark-stream
  không resume). Điều tra + kiểm chứng độc lập (alpine cả `always` lẫn
  `unless-stopped`): `docker kill --signal=KILL <container>` bị Docker coi là
  MANUAL STOP → **tắt restart policy** (restarts=0, exited). Nhưng crash thật
  TRONG container (`pkill -9 stream_app.py` → JVM exit → container exit) thì
  policy `unless-stopped` CÓ restart (đã verify: restarts=1, resume batch
  405→414). **Bug #3 (test design)**: test-recovery mô phỏng sai kịch bản
  crash. **Bug #4 (run-mode + test-recovery)**: `end_offset`/`start_offset`
  trong `stream-progress.jsonl` là JSON **string** nhưng 2 script parse giả
  định **dict** (`isinstance(end,dict)`) → offset sum luôn = 0 →
  `run-mode.sh batch` KHÔNG BAO GIỜ xác nhận drain (rơi timeout warning).
  Fix #3: thay `docker kill bigdata-spark-stream` bằng in-container
  `pkill -9 -f stream_app.py` (kèm comment giải thích + scenario note).
  Fix #4: parse `json.loads(end)` nếu là str, trong CẢ `run-mode.sh`
  (`progress_q1_offsets_sum`) và `test-recovery.sh` (`progress_snapshot`).
  Re-run → **PASS 4/4** (log `test-recovery.log`): baseline batch 707
  offsets 46236 (≠0 nữa), resumed batch 708 offsets 46311, kafka 46332→46807,
  bronze 2130→2142, bridge restart OK. Loss-window cumulative −902 (evidence,
  không assert; âm do dedup + cộng dồn nhiều run). **D6 PASS**.
- Step 9 (D4): batch Gold `run-mode.sh batch --start 2026-09-30T17:00:00Z
  --end 2026-09-30T18:00:00Z` chạy 2 lần (detached, log `batch-run1.log` /
  `batch-run2.log`). Cả 2: **108 sensor-hour-metric rows, 132844 readings**.
  Snapshot `gold.sensor_hourly` (bỏ `computed_at`) → `gold-run1.csv` /
  `gold-run2.csv`: **IDENTICAL byte-for-byte, sha256 trùng
  `a6e0b181…bfbf4a`** cả avg/min/max float. 6 prefix (sim01 + zoneA..E) × 18
  row. **D4 PASS (rerun determinism)**.
- Step 10 (D5): `run-batch-hourly.sh` chạy tay (detached, log
  `batch-hourly.log`): tính đúng cửa sổ [17:00,18:00)Z, grace 60s, chạy Gold
  → 108 rows/132844 readings (lần 3, vẫn identical). Lock test: chạy instance
  thứ 2 trong lúc instance 1 đang grace-sleep → bị từ chối
  "previous batch still running (pid 424388) — skipping"; lock dir
  `${DATA_ROOT}/control/batch-hourly.lock/pid` held rồi released khi xong.
  KHÔNG cài crontab thật (ngoài scope). **D5 PASS**.
- Step 11 (D9): backup pg_dump 1.3MB + tar DATA_ROOT 15MB. Restore verify:
  tạo DB tạm `app_restore`, pg_restore, so gold/sensor_latest/alerts/
  processed_events → **IDENTICAL hết**. Tar extract 11836 entries OK.
  Dọn temp artifacts. **D9 PASS**.
- Step 12: Cập nhật `docs/TEST_REPORT.md` mục 2 (runtime table), 3 (blocked
  còn lại), 5 (measured results với số thật).
- Step 13: Restart live (`run-mode.sh live` lần 2) → 12 container up healthy.
  Pipeline ĐANG CHẠY. Không dừng (user có thể dùng dashboard).

## Tóm tắt kết quả

| Mục | Kết quả |
|---|---|
| D1 build | PASS |
| D2 live + e2e | PASS 6/6 |
| D3 quarantine/veracity | PASS (khớp fault profile) |
| D4 batch determinism | PASS (sha256 identical) |
| D5 batch-hourly + lock | PASS |
| D6 recovery | PASS 4/4 |
| D7 resource | PASS (peak ~1.9 GiB) |
| D8 load dài | SKIPPED |
| D9 backup/restore | PASS |

Bug đã sửa: (1) chmod +x 2 script thiếu exec bit; (2) psycopg2 thiếu trong
spark image; (3) test-recovery dùng docker kill thay vì in-container crash;
(4) end_offset parse string-vs-dict trong run-mode.sh + test-recovery.sh.
