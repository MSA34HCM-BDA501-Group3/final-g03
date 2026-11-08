# Hướng dẫn chạy và bàn giao branch Data Engineering của Quyền

**Branch:** `feat/quyen-spark-data-engineering`

**Ngày đối chiếu:** 08/11/2026

**Checklist gốc:** [PROPOSAL_V3_AUDIT_CHECKLIST.md](../PROPOSAL_V3_AUDIT_CHECKLIST.md)

## 1. Branch này làm gì?

Branch bổ sung đường xử lý **Spark batch thật** từ bốn bảng dữ liệu thô:

```text
nodes.csv + streets.csv + segments.csv + segment_status.csv
    → kiểm tra GIS và GPS, lưu quarantine
    → khử trùng theo ID từng ping
    → fit lookup vận tốc trên dữ liệu trước cutoff
    → tổng hợp cửa sổ 30 phút bằng Monoid + salting
    → join GIS, suy LOS, tạo đặc trưng giờ ICT
    → Bronze / Silver / Gold Parquet + manifest + bằng chứng thực thi
```

Entrypoint là [`spark_batch.py`](../spark_batch.py). Các hàm dùng chung nằm tại
[`traffic_data/spark_etl.py`](../traffic_data/spark_etl.py).

`train.csv` không phải input sản xuất của đường xử lý mới. `pipeline.py`, `app.py`
và những artifact cũ vẫn thuộc prototype trước đó; chạy batch mới **không tự đổi
dashboard sang đọc dữ liệu mới**. Kafka, streaming, dự báo ML và MongoDB live vẫn
là phần tích hợp tiếp theo của Đạt/Minh.

## 2. Chuẩn bị trên Windows / PowerShell

### 2.1. Kiểm tra môi trường và branch

Mở Docker Desktop ở chế độ Linux containers, sau đó mở PowerShell tại thư mục repo:

```powershell
Set-Location 'C:\Users\Lap2nd\Documents\final-g03'
git branch --show-current
git status --short
docker version
docker compose version
```

Tên branch mong đợi: `feat/quyen-spark-data-engineering`. Nếu đang ở branch khác,
kiểm tra thay đổi chưa commit trước khi chuyển; không reset/xóa dữ liệu để làm sạch.
Branch và source hiện là công việc local; tài liệu này không xác nhận đã push lên remote.

Không cần cài Python hoặc Java trên host khi chạy bằng Docker. Dockerfile mới dùng
Python 3.11, PySpark 4.2.0 và Java 21. **Java 21 khác Java 17 ghi trong proposal**;
Minh cần xác nhận runtime chung trước khi hợp nhất hạ tầng.

Compose đặt giới hạn 6 GB RAM / 4 CPU cho service batch; master mặc định là
`local[2]`. Đây là giới hạn cấu hình, chưa phải bằng chứng toàn hệ thống chạy vừa
16 GB RAM khi Kafka/Mongo/UI cùng hoạt động. Nên dừng các workload Docker không
cần thiết trên máy trước khi chạy.

### 2.2. Kiểm tra input

Cần đủ các file sau; dữ liệu thô bị `.gitignore` nên người khác checkout source
chưa chắc đã có dữ liệu:

```powershell
$requiredInputs = @('nodes.csv', 'streets.csv', 'segments.csv', 'segment_status.csv')
foreach ($inputName in $requiredInputs) {
    if (-not (Test-Path -LiteralPath "raw_data/$inputName")) {
        throw "Thiếu raw_data/$inputName"
    }
}
New-Item -ItemType Directory -Force data | Out-Null
```

Schema CSV phải giữ header gốc. Riêng `segment_status.csv` cần `_id`, `updated_at`,
`segment_id`, `velocity`. `_id` là ID từng ping, không phải ID cửa sổ.

### 2.3. Chọn cutoff để fit lookup

```powershell
$env:TRAFFIC_FIT_CUTOFF = '2020-08-01T00:00:00Z'
```

Ngày trên là **ví dụ đã dùng để kiểm chứng**, chưa phải quyết định chia TRAIN chính
thức của nhóm. `--train-end` là cutoff UTC **loại trừ biên**: chỉ ping có
`event_ts < cutoff` được dùng fit lookup.

Lookup fit xong ở cutoff chỉ được xem là nhân quả khi áp cho thời điểm sau cutoff.
Gold có thể chứa cả cửa sổ trước cutoff để kiểm toán; không vì vậy mà mọi hàng Gold
đã đủ điều kiện dùng huấn luyện không leakage. Đạt cần chọn warmup hoặc lookup theo
từng fold thời gian cho các hàng huấn luyện sớm.

## 3. Build, chạy batch và chạy lại

### 3.1. Kiểm tra Compose và build image

```powershell
docker compose -f compose.quyen.yml config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Cấu hình Compose không hợp lệ' }

docker compose -f compose.quyen.yml build batch
if ($LASTEXITCODE -ne 0) { throw 'Build image thất bại' }
```

Lần build đầu cần mạng để tải base image, Java và PySpark. Những lần sau tận dụng
Docker cache. Khi source thay đổi, build lại trước khi kiểm thử hoặc chạy batch.
Image đóng gói source tại lúc build; Compose không mount source host vào container.

### 3.2. Chạy một snapshot riêng

```powershell
$runName = 'quyen_run_' + (Get-Date -Format 'yyyyMMdd_HHmmss_fff')
$runPath = "data/$runName"

docker compose -f compose.quyen.yml run --rm batch --raw-dir raw_data --output-root $runPath --train-end $env:TRAFFIC_FIT_CUTOFF --master 'local[2]' --partitions 4
if ($LASTEXITCODE -ne 0) { throw "Batch thất bại; kiểm tra log và $runPath" }

Write-Host "Snapshot hoàn thành: $runPath"
```

`raw_data/` được mount chỉ đọc. `data/` được mount để giữ kết quả trên host.
`--partitions 4` đặt số shuffle partitions, **không thay** 8 salt buckets trong
`aggregate_monoid`. Nếu không truyền partitions, mặc định code là 8.

Lệnh ngắn sau cũng chạy được nhưng luôn dùng cùng output `data/quyen_spark_run`:

```powershell
docker compose -f compose.quyen.yml run --rm batch
```

### 3.3. Chạy lại sau thành công hoặc sau lỗi

Mỗi lần phải dùng **output directory chưa tồn tại**. Code từ chối ghi đè snapshot,
kể cả thư mục của lần chạy lỗi. Tạo `$runName` mới rồi chạy lại; không cần xóa snapshot
cũ. Đây là cơ chế tránh ghi đè, không phải cơ chế resume/checkpoint của streaming.

Chỉ xem run là hoàn tất khi container exit code 0, có `manifest.json` và
`task_metrics.json`. Manifest được ghi trước bước trích task metrics nên chỉ thấy
manifest chưa đủ khẳng định toàn bộ lệnh thành công.

## 4. Chạy kiểm thử

### 4.1. Bộ test Spark của Quyền

Đặt biến cutoff như mục 2.3 trước khi gọi Compose, kể cả khi override entrypoint:

```powershell
docker compose -f compose.quyen.yml run --rm --entrypoint python batch -m unittest discover -s tests -p test_quyen_spark.py -v
if ($LASTEXITCODE -ne 0) { throw 'Có test Spark thất bại' }
```

Kết quả đã chạy từ source đóng gói trong image: **5 tests, OK**.

| Test | Nội dung kiểm tra |
|---|---|
| `test_first_failure_and_identity` | Lỗi đầu tiên, invalid GPS, ping khác ID trong cùng cửa sổ, retry và ID xung đột |
| `test_topology_quarantine_and_node_pruning` | Loại GIS lỗi, sửa length có cờ, ledger quarantine và loại node mồ côi |
| `test_monoid_reference_and_corruption` | Salted/unsalted, baseline độc lập, cố ý sửa từng aggregate, mất/thừa/trùng key |
| `test_lookup_train_only_and_tiers` | Ba tầng lookup; thay velocity ở sau cutoff không đổi lookup |
| `test_los_time_and_real_plans` | Ranh giới LOS, velocity 0, đổi ngày ICT, hai broadcast joins, SortMergeJoin đối chứng, Parquet pruning |

### 4.2. Bộ test prototype cũ

11 hàm test trong `tests/test_platform_v3.py` đã được gọi trực tiếp và pass ở lần
kiểm chứng trước. **Không ghi là pytest pass**: image prototype khi đó thiếu pytest.
Image Spark mới chỉ copy bộ test của Quyền và không cài đủ pandas/sklearn để chạy
toàn bộ prototype. Không dùng kết quả 5 tests này để kết luận ML/live serving đã đạt.

## 5. Kiểm tra kết quả và bằng chứng

### 5.1. Cấu trúc snapshot

| Đường dẫn trong snapshot | Nội dung / người sử dụng |
|---|---|
| `bronze_raw_events/` | GPS gốc dạng string, giữ cả retry/lỗi; Minh dùng làm nguồn recon sau này |
| `quarantine/events/` | GPS bị loại, payload gốc và first failing rule |
| `quarantine/segments/` | GIS lỗi; một số length đã sửa vẫn có bản gốc trong ledger này |
| `segments/`, `active_nodes/` | GIS hợp lệ/được sửa, node sau pruning; Đạt dùng dựng graph |
| `preprocessing/velocity_lookup/` | Lookup có `fit_before_utc`; Đạt/Minh cùng đọc một snapshot |
| `silver_cleaned_windows/` | Cửa sổ gồm 5 thành phần Monoid và mean |
| `curated_gold/year_month=*/` | LOS, GIS và đặc trưng giờ ICT; chưa phải ML feature table 23D hoàn chỉnh |
| `window_metrics/batch_id=0/` | Snapshot batch hiện tại; chưa phải chuỗi microbatch tăng dần |
| `manifest.json` | Counts, input hashes, diff, physical node names và thời gian đo |
| `sort_merge_plan.txt`, `broadcast_plan.txt` | Executed plans thực tế |
| `pruning_plan.txt` | Bằng chứng lọc phân vùng và chỉ đọc cột cần thiết |
| `spark_events/`, `task_metrics.json` | Event log thật và task/shuffle metrics trích từ Spark |

Các đường dẫn Parquet là **thư mục**, không phải một file CSV duy nhất. Không đổi tên
chúng thành file hoặc trỏ UI cũ vào thư mục rồi kỳ vọng tự tương thích.

### 5.2. Kiểm tra manifest bằng PowerShell

Sau lệnh chạy mục 3.2, giữ nguyên `$runPath`:

```powershell
$manifestPath = Join-Path $runPath 'manifest.json'
$manifest = Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
$manifest | Select-Object spark_version, input_events, accepted_unique_events, rejected_events, windows, active_nodes
$manifest.independent_baseline
$manifest.monoid_difference
$manifest.join_difference
$manifest.exchange_counts

$accounted = $manifest.accepted_unique_events + $manifest.rejected_events + $manifest.duplicate_deliveries_removed
if ($manifest.input_events -ne $accounted) { throw 'Counts không cân bằng' }
if (-not $manifest.independent_baseline.equivalent) { throw 'Baseline không khớp' }
if (($manifest.monoid_difference.left_only -ne 0) -or ($manifest.monoid_difference.right_only -ne 0)) { throw 'Monoid không khớp' }
if (($manifest.join_difference.left_only -ne 0) -or ($manifest.join_difference.right_only -ne 0)) { throw 'Join không khớp' }

$taskMetrics = Get-Content -LiteralPath (Join-Path $runPath 'task_metrics.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$taskMetrics.stages | Format-Table stage, job_group, successful_tasks, median_duration_ms, max_to_median_duration, shuffle_read_bytes, shuffle_write_bytes
```

### 5.3. Số liệu đã kiểm chứng, không phải số bắt buộc cho mọi input

Bản evidence tại [`outputs/quyen_validation/`](../outputs/quyen_validation/) được
copy từ run `data/quyen_spark_validation_03/`, cutoff `2020-08-01T00:00:00Z`,
Spark 4.2.0, local[2], 4 shuffle partitions:

| Chỉ số | Kết quả |
|---|---:|
| GPS đầu vào | 90.938 |
| GPS hợp lệ duy nhất | 90.787 |
| GPS quarantine | 151, đều `E4_UNKNOWN_SEGMENT` |
| Retry bị loại trong raw dataset này | 0 |
| Cửa sổ hợp lệ | 33.394 |
| Node sau lọc GIS và pruning | 52.181 |
| Bản ghi GIS quarantine theo first failing rule | 580 |
| Sai lệch 5 thành phần Monoid + mean với baseline | 0 |
| Missing / extra / duplicate window keys | 0 / 0 / 0 |
| Hai chiều exceptAll salted ↔ unsalted | 0 / 0 |
| Hai chiều exceptAll baseline join ↔ broadcast join | 0 / 0 |
| BroadcastHashJoin trong optimized plan | 2 |
| Exchange nodes baseline → optimized | 5 → 4 |

33.394 cửa sổ khác 33.441 trong proposal vì GPS thuộc segment bị loại khỏi tập road
hợp lệ được quarantine. Tương tự, active nodes không còn 52.425. `E4_UNKNOWN_SEGMENT`
ở đây bao gồm cả segment có trong raw GIS nhưng bị policy loại; không được diễn giải
thành toàn bộ 151 ping có ID không hề tồn tại trong file gốc.

580 GIS quarantine là số đếm first-failure thực tế, không cộng máy móc 154+427 khi
có giao nhau. Ledger GIS có thể chứa segment đã được sửa length rồi chấp nhận;
không áp công thức GPS accepted+rejected vào ledger GIS.

Một lần đo thấy join khoảng 1,393s → 1,266s và aggregate 0,937s → 0,883s. Chưa đủ
để kết luận speedup ổn định hoặc đã giải quyết skew 12x. Cần benchmark lặp và đảo
thứ tự đo; không dùng timing này làm SLA production.

## 6. Checklist Q01–Q11: mục nào thực sự đạt?

Quy ước: `[x]` là tiêu chí đã có code và bằng chứng/test tương ứng; `[ ]` là còn mở.
Mục cha chỉ được tick khi toàn bộ yêu cầu nghiệm thu của mục đó đã được chứng minh.
Theo cách tính thận trọng này: **6 mục đã đạt phần nghiệm thu riêng của Quyền,
5 mục còn một phần**, không phải toàn proposal đã hoàn thành.

- [ ] **Q01 — Entrypoint Spark từ raw:** đạt đường xử lý, còn đối chiếu reference.
  - [x] Đọc bốn bảng raw; không đọc train.csv; full batch đã chạy thành công.
  - [x] So aggregate với raw fold độc lập, không mất/thừa key trong accepted set.
  - [ ] Có báo cáo đối chiếu key/count với train.csv gốc và phân loại cụ thể 47 cửa sổ chênh lệch sau policy; hiện chưa có.
- [x] **Q02 — Quality funnel có thứ tự:** all-string input, try_cast/try_to_timestamp, GPS domain/FK, GIS key/length/type/level; tests lỗi semantic pass. CSV hỏng cấu trúc fail-fast, không tự chuyển thành quarantine từng dòng.
- [x] **Q03 — Quarantine và accounting:** giữ raw payload/source/ID/timestamp, first failing rule, xử lý identity conflict; counts GPS full run cân bằng. GPS và ledger sửa GIS có semantics khác nhau như mục 5.
- [ ] **Q04 — Lookup TRAIN-only:** core đã có, chờ Đạt chốt cách dùng nhân quả.
  - [x] P85 segment → median(level,type) → prior 80/60/50/40; tier khớp nhánh fill.
  - [x] Test perturb velocity sau cutoff không làm đổi lookup; đã persist lookup.
  - [ ] Đạt chốt warmup/fold, biên thời gian, và xác minh load lại cùng lookup ở pipeline ML/stream; chưa kiểm chứng toàn bộ VAL/TEST perturbation trong pipeline ML tích hợp.
- [x] **Q05 — LOS và ICT:** suy LOS từ ratio, giữ vận tốc 0, test các ngưỡng, UTC 17:00 sang ngày ICT kế tiếp, weekday/sin/cos; không cần vocabulary period đã học.
- [x] **Q06 — Monoid và salting:** Spark hai tầng 8 buckets, dedup ping-level; đầy đủ 5 thành phần và mean khớp baseline, fixture tải không đều/cùng cửa sổ pass. Salt hiện dùng hash ổn định từ raw_event_id, không phải random không seed.
- [x] **Q07 — Join evidence thật:** baseline SortMergeJoin và hai BroadcastHashJoin; hai chiều exceptAll = 0; lưu executed plans, Exchange 5→4 và pruning plan. Đây là so hai DataFrame plans; chưa có đối chứng DataFrame API ↔ truy vấn SQL riêng.
- [ ] **Q08 — Benchmark skew:** đã có matched workload và Spark task/shuffle metrics thật; còn chạy lặp, đảo thứ tự, so cùng stage và kết luận hiệu quả theo phân vùng.
- [x] **Q09 — Baseline đầy đủ:** hàm mới `compare_window_aggregates` kiểm 5 thành phần+mean, missing/extra/duplicate keys; cố ý sửa từng field hoặc key đều bị phát hiện. CLI legacy trong baseline.py vẫn giữ verifier cũ; Spark entrypoint gọi hàm mới.
- [ ] **Q10 — Lakehouse và tái lập:** đã có Bronze/Silver/Gold, month partition, batch_id=0, input hash, pruning nodes thực và pruning plan; còn chứng minh rerun cùng input trên snapshot mới cho business output tương đương và review đầy đủ schema/manifest cùng nhóm. Từ chối output tồn tại không thay thế phép kiểm này.
- [ ] **Q11 — Handoff:** module dùng chung, fixtures trong test và data dictionary đã có; còn xác nhận của Đạt/Minh và test parity khi dùng trong consumer thực tế.

## 7. Liên hệ với 28 nhóm yêu cầu trong audit tổng

Không sửa ngược kết luận lịch sử của bản audit. Bảng dưới ghi tiến bộ của **đường
Spark mới**, không lấy nó làm bằng chứng mọi entrypoint/UI cũ đã đổi theo.

| Nhóm audit | Branch bổ sung được gì? | Giới hạn còn lại |
|---|---|---|
| 02 — System boundary | Tạo batch từ GIS + raw GPS | Producer/stream vẫn chưa dùng đường mới |
| 03 — Quality funnel | Rule có thứ tự và quarantine thật | Policy GIS cần chốt; malformed CSV cấu trúc fail-fast |
| 04 — Imputation | Lookup GPS TRAIN-prefix ba tầng | Warmup/ML folds và consumption parity chờ tích hợp |
| 05,06 — LOS/time | Derive LOS và ICT trong Gold mới | UI và feature pipeline cũ chưa chuyển sang contract này |
| 07,08 — Monoid/baseline | Spark salt8, kiểm đủ tuple và key | Benchmark repeated chưa xong |
| 09 — Catalyst/evidence | Physical plans/exceptAll/pruning thực | Chưa SQL-vs-DataFrame đối chứng riêng; không khẳng định7→3 Exchanges |
| 26 — Lakehouse | Các tầng Parquet và snapshot riêng | Chưa live append/checkpoint/recon |
| 27 — Tests | 5 test Spark mới và 11 regression functions cũ pass | Chưa full Kafka→Spark→Mongo→UI E2E |
| 28 — Deploy | Docker/Compose riêng cho batch đã build/config-check | Chưa stack đầy đủ và đo RAM 16 GB toàn hệ thống |

Những nhóm mining, target+30m, MLlib, causal feature toàn pipeline, streaming,
sketches, two-tier recon, Mongo IXSCAN live và dashboard live vẫn cần Đạt/Minh làm tiếp.

## 8. Những việc Đạt cần xác nhận

Mỗi checkbox dưới đây đang **chờ xác nhận**, không được hiểu là Đạt đã đồng ý.
Đạt có thể tick trực tiếp trong PR và ghi quyết định/đường dẫn evidence bên cạnh.

- [ ] **D-C01 — Cutoff và dữ liệu huấn luyện:** chốt UTC warmup cutoff, TRAIN/VAL/TEST và cách tránh dùng lookup fit ở tương lai cho hàng TRAIN sớm. Đầu ra cần: bảng biên thời gian + chính sách lookup theo fold.
- [ ] **D-C02 — Ground truth:** dùng LOS suy từ ratio hay LOS gốc trong train.csv; nếu giữ cả hai, đặt tên riêng và thống kê độ lệch. Không tái dùng metric 0.6670/0.7568 như kết quả của target/policy mới.
- [ ] **D-C03 — Phạm vi graph:** chọn topology đã lọc road của branch hay toàn bộ 84.633 segments để giữ UC3/cold-start theo proposal. Đầu ra cần: tập segment IDs chính thức và lý do cho số node/edge/segment thay đổi.
- [ ] **D-C04 — Lookup semantics:** xác nhận dùng GPS P85, median(level,type), level prior; `observed_max_velocity` là metadata riêng. Chốt xử lý segment P85=0, hiện code bỏ0 khỏi fit và đi fallback.
- [ ] **D-C05 — Feature schema:** thống nhất dùng UTC window_start làm key và ICT cho date/period_idx/weekday; Gold hiện chưa có graph/cluster/lag/prior/target 23D. Chốt tên cột, kiểu và feature order khi bổ sung.
- [ ] **D-C06 — Target+30m:** label phải join đúng timestamp+30m, không dùng next-observed; purge label vượt cutoff và có kiểm tra leakage. Đây là phần Đạt làm tiếp, chưa được giải quyết bởi batch data engineering.
- [ ] **D-C07 — Mining hygiene:** nhận tên đã NFC/Ð→Đ/whitespace; tự bổ sung lọc thực thể hành chính, FPGrowth/SON, cluster và PageRank theo policy được duyệt.
- [ ] **D-C08 — Golden data:** chọn fixture chung, load lookup persist, tạo prediction/reference output cho Minh; xác nhận cùng input/features ra cùng dự báo ở batch/stream/recon.

Đạt có thể bắt đầu code reader/target/mining trên fixture ngay. Chỉ chốt metric chính
thức sau D-C01–D-C06; chỉ đóng băng artifacts dùng live sau khi có version/hash/schema.

## 9. Những việc Minh cần xác nhận

- [ ] **M-C01 — Runtime:** xác nhận Spark 4.2/Java 21/Python 3.11 và connectors phù hợp; nếu buộc Java 17 theo rubric, điều chỉnh và chạy lại trước khi merge hạ tầng.
- [ ] **M-C02 — Event ID:** raw_event_id ổn định cho từng ping/retry, có namespace nếu nhiều nguồn; giữ hai ping cùng segment/cửa sổ nhưng khác ID. window_id dành riêng cho cửa sổ; không dùng thay ID ping.
- [ ] **M-C03 — Timestamp/schema producer:** gửi updated_at có Z hoặc offset, velocity đúng đơn vị km/h và segment_id đúng kiểu; thêm ingest_ts/arrival metadata phục vụ replay lateness. Những trường arrival này chưa có trong producer hiện tại.
- [ ] **M-C04 — Quality policy:** đồng ý mapping GPS tới topology đã lọc, trong đó 151 GPS hiện bị E4; dùng cùng hàm clean_events cho foreachBatch và thống nhất nơi lưu ledger.
- [ ] **M-C05 — Dedup liên microbatch:** thiết kế state/watermark/Bloom/exact fallback phù hợp; deduplicate_events chỉ dedup trong DataFrame được truyền vào, không tự nhớ ID từ microbatch trước.
- [ ] **M-C06 — Bronze và checkpoint:** raw append giữ 100% delivery trước watermark; dùng checkpointLocation thật và chính sách khôi phục. Snapshot batch và thư mục spark_events không phải WAL streaming.
- [ ] **M-C07 — Lookup cùng version:** chỉ load snapshot đã fit/được duyệt bởi Đạt; không fit lại từng microbatch. Chốt lookup/static store/model version và checksum chung.
- [ ] **M-C08 — Serving key:** chốt dùng window_id hay alias event_id tại Mongo và document schema, unique index/upsert semantics; không tự đồng nhất raw_event_id với serving key.
- [ ] **M-C09 — Recon:** recompute từ raw Bronze cùng quality/lookup policy, heal lag/prior và rescore; khác set accepted events sẽ làm phép so stream/batch vô nghĩa.
- [ ] **M-C10 — UI/read paths:** chuyển UI sang nguồn live/Gold mới có chủ đích; xác nhận schema/period mới thay vì đọc nhầm dataset.csv và JSON prototype.
- [ ] **M-C11 — Integration proof:** chạy fixture chung có duplicate qua hai batch, late-in/out, restart và race provisional/reconciled; lưu progress, counts, hashes, query explain thật.

Minh có thể dựng broker/topic/network và skeleton consumer ngay. Chỉ nghiệm thu
prediction/reconciliation sau khi M-C02–M-C09 và các quyết định dữ liệu của Đạt được chốt.

## 10. Quyết định chung cần ghi lại trước khi merge

| Chủ đề | Người chốt | Nội dung cần ghi vào PR/ADR |
|---|---|---|
| Chênh33.441→33.394 windows và 52.425→52.181 nodes | Quyền + Đạt + Minh | Chấp nhận road filter hay thay policy; có cần giữ topology toàn mạng riêng |
| Warmup/cutoffs và LOS ground truth | Đạt, Quyền review | Biên UTC, label definition, preprocessing snapshot áp dụng cho từng giai đoạn |
| Raw event và serving identities | Minh, Quyền review | ID từng ping, ID từng window, namespaces và Mongo unique key |
| Runtime Java 21 so với proposalJava 17 | Minh, cả nhóm review | Runtime được thống nhất và kiểm chứng build/test |
| Handoff parity | Đạt + Minh, Quyền hỗ trợ | Fixture và expected outputs được version hóa; diff phải đo thực |

Điều kiện merge phần data engineering: tests Spark pass, counts/diffs được review,
policy chênh dữ liệu được ghi rõ và không gắn nhãn hoàn thành cho phần tích hợp chưa chạy.
Các xác nhận trong tài liệu là checklist nội bộ, **chưa có ai được tự động tick thay**.

## 11. Lỗi thường gặp

| Triệu chứng | Cách xử lý |
|---|---|
| Compose báo thiếu TRAFFIC_FIT_CUTOFF | Đặt biến trong cùng PowerShell trước build/run/test/config |
| FileExistsError ở output-root | Chọn snapshot mới; không ghi đè/xóa snapshot đang dùng |
| Không thấy raw CSV trong container | Kiểm tra file trên host và chạy lệnh ở root repo |
| Test không phản ánh code vừa sửa | Build lại image; source không được bind-mount trong Compose |
| Java gateway/hostname lỗi | Kiểm tra hostname localhost và SPARK_LOCAL_IP trong Compose; nếu tự chạy docker tắt mạng phải giữ hai thiết lập này |
| Unknown street/node endpoint | Kiểm tra GIS references; pipeline cố ý fail thay vì join mất dòng âm thầm |
| Số windows khác proposal | Xem rejected_by_rule, input hashes và policy road filter trước khi coi là bug |
| Có manifest nhưng thiếu task_metrics | Kiểm tra exit code/log; bước trích event log có thể chưa hoàn tất |
| Dashboard vẫn hiện số cũ | Batch mới chưa tích hợp app.py; thực hiện M-C10 thay vì sửa số trong JSON |

Tài liệu kỹ thuật trước đó: [QUYEN_DATA_ENGINEERING.md](QUYEN_DATA_ENGINEERING.md).
Tài liệu tiếng Việt này cung cấp hướng dẫn chạy và checklist nghiệm thu chi tiết hơn;
khi có quyết định mới, cập nhật cả contract và tests trước khi tick mục tương ứng.
