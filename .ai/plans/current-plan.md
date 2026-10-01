# Plan: Triển khai runtime IoT Big Data stack trên máy hiện tại (D1–D7, D9)

## Trạng thái và approval gate

- **Trạng thái:** `APPROVED — IMPLEMENTATION IN PROGRESS`.
- User: "triển khai dự án" rồi "triển khai luôn đi" (2026-09-30) — chấp nhận
  mặc định: (1) DATA_ROOT fallback không cần sudo; (2) phạm vi D1–D7 + D9,
  bỏ qua D8 (load test dài).
- Task cũ week7 vẫn `AWAITING_USER_ACCEPTANCE`, đã archive sang
  `plans/archive/2026-09-29-week7-pyspark-awaiting-acceptance.md`; việc triển
  khai này KHÔNG phải acceptance của week7.
- **Không tự động commit hoặc push.**

## 1. Mục tiêu

Chạy thật toàn bộ phần DEFERRED (D1–D9) của `implementation/docs/TEST_REPORT.md`
trên máy hiện tại — máy trước đây bị BLOCKED (RAM 3.9 GB, disk 11 GB) nay đã
đủ (RAM available 8.4 GB, disk 99 GB, Docker 29.1.3 / Compose v5.5.0):

1. Build images + init (D1).
2. Live pipeline 5 zone + e2e fault-inject (D2, D3).
3. Batch Gold + rerun determinism (D4). Cron (D5) chỉ verify script chạy tay
   đúng, KHÔNG cài crontab thật trên máy user trừ khi user yêu cầu.
4. Recovery SIGKILL (D6).
5. Resource report thực đo (D7).
6. Backup/restore round-trip (D9).
7. Điền số đo thật vào TEST_REPORT mục 5 + cập nhật mục 2/3, README §12.

## 2. Bằng chứng hiện trạng (đo 2026-09-30)

- `scripts/preflight.sh` → exit 2 "READY WITH WARNINGS": RAM 8.4 GB OK,
  disk /var/lib/docker 99 GB OK, **TCP 1883 bị chiếm** (project khác),
  `/data` chưa tồn tại.
- Port 9092/8000/8088/5432 rảnh.
- Unit tests: 111/111 pass (2026-09-18, cần re-run xác nhận trước khi build).
- Toàn bộ runtime D1–D9 chưa chạy trên máy nào (TEST_REPORT §2).

## 3. Cấu hình môi trường (quyết định)

- `.env` từ `.env.example` với 2 chỉnh sửa:
  - `DATA_ROOT=/home/lucas/bigdata-demo` (fallback không cần sudo, theo
    chú thích trong .env.example).
  - `MQTT_HOST_PORT=1884` (1883 bận; simulator/bridge nối nội bộ compose
    network qua port 1883 của container nên không ảnh hưởng pipeline).
- Không đổi credentials mặc định (demo only, máy local).

## 4. Scope

**Trong scope:**

- Tạo `.env` (git-ignored), chạy `prepare-host.sh`, `init.sh`,
  `run-mode.sh live`, `wait-for-health.sh`, `test-e2e.sh`, `run-mode.sh batch`
  (2 lần đối chiếu determinism), `run-batch-hourly.sh` (chạy tay 1 lần),
  `test-recovery.sh`, `resource-report.sh`, backup/restore (pg_dump + tar).
- Sửa bug runtime NẾU phát hiện (coder sửa trong `implementation/`, ghi rõ
  từng thay đổi vào state + TEST_REPORT).
- Cập nhật docs: `implementation/docs/TEST_REPORT.md` (mục 2/3/5),
  `implementation/README.md` §12, và `AGENTS.md` nếu trạng thái repo đổi
  (đã có implementation + đã verify runtime).

**Ngoài scope:**

- D8 load test 10–60 phút (user không yêu cầu).
- Cài crontab thật trên máy user.
- Thay đổi kiến trúc, thêm package, sửa báo cáo LaTeX `Documents/report/`.
- Commit/push.
- Dọn dẹp container/project khác đang chiếm 1883.

## 5. Các bước thực hiện (coder)

1. `python3 -m unittest discover -s implementation/tests` — xác nhận 111/111.
2. `cp .env.example .env` + sửa DATA_ROOT, MQTT_HOST_PORT=1884 (mục 3).
3. `scripts/prepare-host.sh` — tạo cây thư mục DATA_ROOT.
4. `scripts/init.sh` (D1) — build images; log build phải có
   `[warmup] OK kafka010=… pgjdbc=…`; postgres schema + kafka topic.
   Cần internet tới Docker Hub + Maven Central.
5. `scripts/run-mode.sh live` → `scripts/wait-for-health.sh spark-stream
   backend dashboard` (D2) — kiểm 5 container `sim-zoneA..zoneE` + manifest
   per zone; dashboard http://localhost:8088, API /health.
6. Để live chạy ≥ 10 phút cho có dữ liệu; `scripts/resource-report.sh
   --interval 30 --count 10` (D7) — ghi peak RAM/CPU, backlog, disk growth.
7. `scripts/test-e2e.sh` (D2/D3) — 200 event fault-inject seed 42 →
   assertions API/quarantine/alerts PASS; đối chiếu tỷ lệ quarantine
   per-metric với fault profile (2% dup, 1% thiếu, 1% non-numeric, 1% bad ts,
   1% out-of-bounds, 1% late, 2% spike).
8. `scripts/test-recovery.sh` (D6) — SIGKILL spark-stream, restart bridge;
   ghi loss window thật.
9. Batch (D4): `run-mode.sh batch --start <giờ UTC vừa kết thúc> --end <+1h>`;
   rerun lần 2 → đối chiếu count/avg/min/max per-metric không đổi.
10. `BATCH_START/END` như cũ, chạy `scripts/run-batch-hourly.sh` tay (D5) —
    kiểm log + lock file trong `${DATA_ROOT}/control/`.
11. Backup/restore (D9): `pg_dump -Fc app` → restore vào DB tạm hoặc
    `--clean`; `tar czf` DATA_ROOT (sau khi dừng mode). Xác minh restore.
12. Cập nhật TEST_REPORT: mục 5 số đo thật (latency e2e, peak RAM/CPU per
    service, backlog, disk growth, loss window, tỷ lệ lỗi per-metric);
    mục 2 đổi DEFERRED → kết quả từng dòng D1–D7/D9 kèm lệnh + ngày chạy;
    mục 3 BLOCKED → gỡ (trừ 1883, ghi workaround 1884). README §12 tương ứng.
13. Dừng pipeline gọn (`run-mode.sh` về trạng thái stop theo script),
    KHÔNG xóa data (reset-pipeline chỉ chạy khi user yêu cầu).

Mỗi bước ghi checkpoint vào `.ai/state/current-task.md` (orchestrator giữ,
coder báo cáo lại). Nếu bước nào FAIL vì bug code: coder sửa tối thiểu,
ghi diff vào state, tester sẽ re-verify độc lập.

## 6. Acceptance criteria

- [ ] Unit tests 111/111 pass trước và sau triển khai.
- [ ] D1: init.sh exit 0, log build có warmup OK, topic `sensor_raw` tồn tại
      (3 partitions), schema postgres đủ bảng.
- [ ] D2: live + 5 sim zone chạy; wait-for-health PASS; dashboard render
      dữ liệu thật; `/api/sensors/latest` trả vector 6 chỉ số.
- [ ] D3: test-e2e.sh PASS; tỷ lệ quarantine khớp fault profile (±nhiễu mẫu).
- [ ] D4: batch Gold per-metric chạy 2 lần → số liệu identical.
- [ ] D5: run-batch-hourly.sh chạy tay OK, lock activity đúng.
- [ ] D6: test-recovery.sh PASS, loss window được ghi thật (không suy đoán).
- [ ] D7: resource-report có số liệu; tổng RAM live ≤ ngân sách ~5 GiB.
- [ ] D9: backup + restore verify OK.
- [ ] TEST_REPORT mục 5 điền số thật có evidence; không còn dòng DEFERRED
      cho D1–D7/D9; mọi claim "đã chạy" kèm lệnh + output.
- [ ] Không commit/push; không sửa file ngoài `implementation/` + `.ai/` +
      `AGENTS.md`; `.env` không lọt vào git.

## 7. Verification plan (tester độc lập)

- Đọc TEST_REPORT mới, chọn ngẫu nhiên ≥ 5 claim số → đối chiếu evidence
  (log, JSONL, psql query) hoặc re-run lệnh read-only.
- Re-run `preflight.sh`, `unittest discover`, `docker compose config`.
- Kiểm API/dashboard đang phản ánh data thật (curl các endpoint).
- Re-run `test-e2e.sh` một lần độc lập.
- `git status` — scope sạch, `.env` ignored.

## 8. Rủi ro

- **RAM**: 8.4 GB available, live ~5 GiB — đủ nhưng không chạy thêm workload
  nặng song song; nếu OOM, giảm SIM_RATE hoặc dừng bớt app khác (hỏi user).
- **Internet**: build cần Docker Hub + Maven Central; nếu fail giữa chừng,
  ghi log và báo user.
- **Bug runtime chưa từng chạy**: khả năng cao phát hiện lỗi (đây là lần
  chạy thật đầu tiên sau rewrite E3) — quy trình: coder sửa tối thiểu + ghi
  chép, không refactor mở rộng.
- Kafka retention 1h: các test cần dữ liệu phải chạy trong cửa sổ, batch
  ngay sau khi live đủ lâu.

## 9. Phân công

- **Coder:** thực hiện §5, sửa bug runtime nếu có, cập nhật docs.
- **Tester:** verify độc lập theo §7.
- **Reviewer:** review thay đổi (code fix + docs) so với plan này.
- **Orchestrator:** giữ plan/state, không sửa production code.
