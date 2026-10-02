# Task: Bộ công cụ đánh giá runtime IoT Big Data Q1–Q4

## Trạng thái: TOOLKIT COMPLETE; PHÉP 15 GB/4 GIỜ ĐÃ HOÀN THÀNH — 2026-10-02

- Plan: `.ai/plans/current-plan.md` — APPROVED bởi user ngày 2026-10-01.
- Baseline commit: `dc1c34d1b365f3dcf22e949c2ab0b166582231ac`.
- Runtime stack một host Docker đang hoạt động. Phép chạy lại `load15-clean-20261002` đã hoàn thành; phạm vi dữ liệu cũ bị xóa/cách ly được ghi trong incident/rerun bên dưới.
- B7/15 GB trong 3 giờ chưa chạy; không được tuyên bố đạt khi chưa có phép chạy và đối soát.
- Phiên này dùng GPT-6.1 cho triển khai và xác minh; không triển khai model khác.

## Đã hoàn thành

- [x] Đọc yêu cầu và xác định phạm vi benchmark toolkit.
- [x] Wire tham số Spark/JDBC từ environment vào Compose và wrappers, giữ baseline hiện tại.
- [x] Thêm benchmark runner theo run id, metadata, snapshot, resource evidence và summary.
- [x] Giữ log bản tin hiện có; bổ sung payload byte evidence, không đổi `processed_events`.
- [x] Bổ sung Bronze helper và hướng dẫn lưu kết quả an toàn từ stdout trên host.
- [x] Bổ sung drain/settle q1/q2/q3 và tài liệu `implementation/docs/EVALUATION.md`.
- [x] Cập nhật `AGENTS.md` và README để phản ánh runtime stack.

## Verification

- Unit tests: **113/113 PASS**.
- `bash -n scripts/*.sh spark/run/*.sh`: PASS.
- `py_compile` toàn bộ Python implementation: PASS.
- `docker compose --profile live --profile batch config --quiet`: PASS.
- `git diff --check`: PASS.
- Docker images build thành công: simulator, bridge, Spark.
- Spark dependency warmup: Kafka integration, PostgreSQL JDBC và psycopg2 PASS.
- Runtime health: Spark Streaming, Bridge, Kafka, MQTT, PostgreSQL, Backend và Dashboard đang healthy/running.
- Effective baseline: trigger 5 seconds, `local[2]`, shuffle 2, JDBC batch 1000, caps 20.000/1.000/500.
- Bounded smoke `smoke03`: 11 manifest records, 11 published, 0 PUBACK failures, 3.028 payload bytes, 11 Bridge deliveries; q1/q2/q3 drain/settle PASS.
- Bronze helper: 11 rows, 3.028 payload bytes, 11 distinct Kafka provenance records; fallback stdout và host extraction PASS.

## Evidence

- Smoke summary: `/home/lucas/bigdata-demo/logs/bench-smoke03/summary.json`.
- Bronze summary: `/home/lucas/bigdata-demo/logs/bench-smoke03/bronze-summary.json`.
- Source manifest: `/home/lucas/bigdata-demo/logs/simulator-smoke03-A.jsonl`.
- Resource evidence: `/home/lucas/bigdata-demo/logs/bench-smoke03/resource-report.stdout.log` và `resource-report.jsonl`.

## Mục tiêu báo cáo đã chốt — 2026-10-01

- Hai phép tải chính: **15 GB/4 giờ** và **30 GB/4 giờ**, chạy lần lượt.
- Không cần campaign riêng 8 giờ ở rate 2/source. `campaign01` đã dừng;
  container nguồn và collector không còn chạy, metadata ghi `cancelled`,
  logs và dữ liệu giữ nguyên. Không báo cáo campaign01 là hoàn thành 8 giờ.
- Rate và ngân sách đĩa phải xác định từ pilot; không lấy smoke làm năng lực đã chứng minh.
- Tổng thời gian nguồn hai phép là 8 giờ; pilot, fixture, drain, đối soát và Gold/rerun tính riêng.
- Fixture lỗi/NULL/late/duplicate và Gold rerun cần thiết để kết luận Q2–Q4.
- Hai mức tải không tự chứng minh recovery, latency đầu-cuối hoặc hiệu quả tuning.
- Mục tiêu cũ 15 GB/3 giờ giữ làm tham chiếu; 15 GB/4 giờ không chứng minh đạt mốc cũ.
- Chưa khởi chạy hai workload mới.

## Guardrails

- Không commit/push tự động.
- Không reset pipeline.
- Không xóa logs/checkpoints/volumes.
- Không khởi chạy tải lớn trước khi có pilot rate và ước lượng đĩa. Ghi đúng lượng/time thực và đối soát; không tuyên bố đạt mục tiêu từ rate đặt.


## Khởi chạy 15 GB — cập nhật trong phiên

- User yêu cầu chạy 15 GB trước, 4 giờ; chưa chạy 30 GB.
- Pilot `pilot-b1-50-20261001`: 150.005 published/delivered, 47.172.629 payload bytes, không PUBACK failure; payload TB 314,47 byte.
- Probe ở mức cần thiết `p15cal-20261001`: 5×705/s trong 90s; 317.255 published nhưng chỉ 22.141 Bridge deliveries, Mosquitto báo outgoing drop cho iot-bridge.
- Probe này FAIL Q1; không đủ điều kiện khởi chạy 4 giờ ở cấu hình ingress cũ. Logs/evidence giữ nguyên, không reset Kafka/checkpoint/database.
- Coder GPT-6.1 xử lý callback idle polling trong Bridge bằng thay đổi nhỏ và test; một coder khác chuyển summary sang counters/unique-count trên đĩa để tránh OOM.
- Chuẩn bị host supervisor có stdout, source/service stdout, cấu hình hiệu lực/hashes, disk guard 20 GB và Bronze byte summary sau nguồn; chưa khởi chạy workload 4 giờ.

## Chuẩn bị workload chính 15 GB/4 giờ — 2026-10-01

- Probe `p15w100-20261001` vẫn FAIL ingress: 317.255 source, 313.400 delivery, Mosquitto drop; không dùng làm kết quả ổn định.
- Probe `p15w200-20261001` GREEN (180s, 5×705/s, faults off): source/Bridge/Bronze/PG đều 634.505, payload 194.772.497 byte, Bronze distinct Kafka provenance 634.505. MQTT không có outgoing drop.
- Cấu hình ingress đã đổi: Bridge idle poll 0,5→0,01s; MQTT inflight 20→100 (probe fail)→200 (probe pass). Queue 5.000, linger 20ms và ACK sau Kafka delivery giữ nguyên. Không gọi đây là cấu hình baseline nguyên bản.
- Unit tests 127/127 PASS; Compose config và diff check PASS. Supervisor abort regression PASS với Docker timeout và nguồn xuất hiện sau lần dừng đầu.
- Workload định chạy `load15-20261001`: 5 nguồn ×705/s, 14.400s, 5 sensor/source, seed42..46, faults off, spike2%. Chưa launch tại bản cập nhật này; phải kiểm chứng unit/source trước ghi RUNNING.
- Host supervisor đã lưu dưới `/home/lucas/bigdata-demo/logs/bench-load15-20261001/host-supervisor.py`; source/service stdout, disk guard, config hashes/image IDs, resource/progress và Bronze summary cuối run.
- Đĩa ~93 GB trống; projection từ probe ~68,9 GB tăng nếu giữ tuyến tính, thêm 4 GB tạm. Disk guard 20 GB dừng nguồn/launcher và đánh dấu aborted; không tuyên bố thành công nếu dừng sớm.
- Không có Gold/rerun/fixture lỗi/latency đầu-cuối hoặc recovery cho workload chính tại thời điểm này. Không launch 30 GB.

## RUNNING — load15-20261001

- Đã launch unit `bigdata-load15-20261001.service` lúc **2026-10-01 22:08:01 +07**; supervisor PID1507271, runner PID1507363. Không launch unit hoặc runner này lần nữa.
- Năm nguồn bắt đầu 22:08:04..08 ngày01/10; duration14.400s/source, rate705/source, faults off, seed42..46. Dự kiến nguồn dừng **02:08 ngày02/10/2026 +07**; drain/summary sau đó tính riêng.
- Log/evidence riêng: `/home/lucas/bigdata-demo/logs/bench-load15-20261001/`; manifest `/home/lucas/bigdata-demo/logs/simulator-load15-20261001-{A..E}.jsonl`; global Bridge/progress/resource giữ nguyên.
- Startup verification sau6min:5nguồn chạy, nguồnA báo705.0evt/s, PUBACKfail0; services không OOM/restart; resource log có đủ5nguồn; stdout observers đang ghi; sleep inhibitor confirmed modeblock.
- Reviewer GPT-6.1 đã khép2High lifecycle sau rewrite.2Medium được xử lý vận hành: runtime drop-in TimeoutStopSec1200 (không restart workload); unit `bigdata-load15-inhibit-watch-20261001.service` theo dõi inhibitorPID1507355 và PIDstarttime, yêu cầu supervisor abort nếu inhibitor thoát.
- Unit running và tự tiếp tục khi kết thúc turn. Không dừng services/sources, không đổi cấu hình giữa phép đo. Disk guard20GB sẽ dừng launcher/source và đánh dấu aborted nếu thiếuđĩa.
- Sau nguồn dừng: runner settle/snapshot/summary; supervisor tự chạy helperBronze và ghi `bronze-summary.json`, `target-observation.json`. Những bước này không thay thế đối soát provenance/ID đầy đủ.
- **Chưa kết luận đạt15GB/4h**. Gold/rerun, fixture lỗi/NULL/late/replay, latency đầu-cuối, fullQ1–Q4 reconciliation còn phải làm sau phép nguồn.30GB chưa chạy.
- Status: `systemctl --user status bigdata-load15-20261001 --no-pager`; evidence state: `bench-load15-20261001/supervisor-state.json`.

## Incident snapshot — 2026-10-02 03:31 +07

- Run is still active; supervisor state remains `running`, five simulator
  containers remain up, and the runner has not produced `summary.json`.
- Host suspend was confirmed by system journal from **2026-10-01 23:46:15
  +07** to **2026-10-02 03:26:41 +07** (3h40m26s). The sleep-inhibitor
  process was alive before and after, but did not prevent this suspend.
- After resume, MQTT disconnected all six clients for timeout and reconnected;
  simulator C has 1,409 PUBACK timeouts and simulator E has 1,411. These are
  concentrated at resume and require source/Bridge/Bronze provenance
  reconciliation; do not yet label them lost or recovered.
- No OOM/restart was observed for MQTT, Bridge, Spark, PostgreSQL, or the five
  simulator containers. Spark and PostgreSQL resumed processing, but one Spark
  duration and a PostgreSQL checkpoint include the 3h40m suspend gap and must
  be excluded or annotated in report latency statistics.
- Current disk free is about 66 GB, above the 20 GB guard but materially lower
  than the pre-run value. Run has not been stopped; preserve it as an
  interruption/recovery attempt, not a clean continuous 15 GB/4h baseline.

## Rerun launched — load15-clean-20261002

- The interrupted run was stopped with supervisor exit 130. Its logs and
  manifests remain under the old DATA_ROOT for incident evidence.
- PostgreSQL rows with the old prefix were deleted and verified zero afterward:
  `processed_events`, `alerts`, `sensor_latest`, and `gold.sensor_hourly`.
  Schema and other prefixes were preserved.
- The clean rerun uses a fresh active root
  `/home/lucas/bigdata-demo-rerun15-20261002` and a fresh Kafka topic
  `sensor_raw_load15_20261002`; old Kafka/Parquet evidence is not read by this
  run. The `.env` file now points to this root/topic.
- New run `load15-clean-20261002` launched under
  `bigdata-load15-clean-20261002.service` at the observed local time around
  03:42 on 2026-10-02. Five sources use 705 events/s, 14,400 seconds,
  sensors=5, faults off, seed 42..46. No target conclusion yet.
- The new supervisor uses an inhibitor covering sleep, idle, lid, power and
  suspend/hibernate keys; a separate watcher monitors its PID. Startup sample
  showed all five sources at ~705/s with PUBACK failures 0 and no Bridge queue
  or Kafka delivery errors. Keep the machine awake and do not close the lid;
  an explicit forced suspend can still bypass user-level inhibitors.


## Kiểm tra cuối — load15-clean-20261002, ngày 2026-10-02

- Supervisor và watcher đã kết thúc bình thường: `Result=success`,
  `ExecMainStatus=0`; `supervisor-state.json` là `collection_finished`.
  Trạng thái inactive/dead của hai unit là đã hoàn tất, không phải lỗi.
- Năm nguồn bắt đầu **03:42:50–54 +07**, kết thúc **07:42:52–56 +07**
  ngày 02/10/2026. Mỗi nguồn duration 14.400 giây; cửa sổ manifest toàn
  nguồn 14.405,067 giây do khởi chạy lệch nhau. Snapshot settle lúc 07:44:04;
  tổng hợp/đo Bronze hoàn tất **07:53:55 +07**.
- Nguồn, Bridge và Bronze đều **50.760.004** bản tin; nguồn/Bridge/Bronze
  đều **16.226.141.856 byte** payload (16,226141856 GB thập phân).
  Bronze distinct Kafka provenance cũng 50.760.004. Đạt điều kiện lượng
  dữ liệu ít nhất 15 GB trong phép có duration 4 giờ mỗi nguồn.
- Rate nguồn thực theo cửa sổ manifest: 3.523,76 sự kiện/s và
  1.126.419 byte/s; PUBACK failure 0. Bridge delivery error và queue drop 0.
- SQL đọc riêng prefix xác nhận PostgreSQL **50.760.004 event_id duy nhất**,
  duplicate_occurrences 0, latest 25 sensor; alerts 878.187 dòng.
  Gold của prefix mới **0 nhóm**: chưa thực hiện batch/rerun Q4.
  API `/progress` là tổng mọi prefix, không dùng tổng API thay số đo riêng run.
- P95 thời gian micro-batch quan sát: q1 422 ms, q2a 1.465 ms, q2b 694 ms,
  q3 1.650,95 ms. Max quan sát lần lượt 1.236/2.905/1.728/3.339 ms;
  không có batch được ghi log vượt trigger 5s. Đây không phải latency đầu-cuối.
  Progress polling có thể bỏ mẫu: sum input rows trong log ít hơn persisted
  count 9.127; không coi riêng sai lệch log này là dữ liệu mất.
- Kafka end và q1 offsets cuối cùng đều 50.760.004, lag cuối 0. Settle query
  ổn định và count PG khớp nhưng chưa thay thế full anti-join theo ID/provenance.
- Rà toàn bộ stdout riêng run không thấy ERROR/FATAL, PUBACK timeout, MQTT
  drop hoặc Spark falling-behind. Không có suspend/OOM trong journal khoảng
  chạy; inhibitor alive ở toàn bộ 1.003 mẫu. Container không restart trong
  khoảng chạy; Kafka restart_count=1 là trước run, started 01/10/2026.
- Resource 390 mẫu có cả 5 simulator (1.935 source-container samples).
  Spark sampled peak RAM ~1,878 GiB; PostgreSQL ~466,3 MiB.
  Disk guard thấp nhất **21.435.465.728 byte**, trên ngưỡng 20 GB;
  lúc kiểm tra còn khoảng **25,4 GB**. Chưa đủ ngân sách đĩa để launch 30 GB.
- Evidence: `/home/lucas/bigdata-demo-rerun15-20261002/logs/bench-load15-clean-20261002/`
  gồm summary, bronze-summary, target-observation và health-check-20261002.json.
  Manifest, Bridge/progress/resource nằm trong logs của DATA_ROOT mới.
- Còn thiếu để kết luận đầy đủ Q1–Q4: đối soát ID/provenance toàn bộ;
  Silver/Quarantine và đáp án latest/alerts; fixture faults/NULL/late/replay;
  Gold theo cửa sổ, đáp án độc lập và rerun; latency quan sát sau commit nếu cần.
  30 GB chưa chạy. Giữ cấu hình MQTT inflight 200/Bridge poll 0,01s đã ghi;
  không gọi là baseline ingress nguyên bản tại commit tham chiếu.
- Old Parquet/Kafka/manifests/logs vẫn còn vật lý dưới root cũ để giữ evidence;
  chỉ serving rows prefix lỗi đã bị xóa, và run mới đọc root/topic/checkpoint mới.
