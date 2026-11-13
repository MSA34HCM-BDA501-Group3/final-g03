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
| `test_topology_quarantine_and_node_pruning` | Loại GIS lỗi, sửa length có cờ, ledger quarantine và lọc điểm uốn cong trung gian |
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
tạo từ `spark_batch.py`, cutoff `2020-08-01T00:00:00Z`, tương thích và đã kiểm chứng
trên cả **Apache Spark 3.5.7 (Java 17)** và **Apache Spark 4.2.0 (Java 21)**, `local[2]`, 4 shuffle partitions:

| Chỉ số | Kết quả |
|---|---:|
| GPS đầu vào | 90.938 |
| GPS hợp lệ duy nhất | 90.787 |
| GPS quarantine | 151, đều `E4_UNKNOWN_SEGMENT` (thuộc `427` đoạn `S3_NON_ROAD_TYPE`) |
| Retry bị loại trong raw dataset này | 0 |
| Cửa sổ hợp lệ (sau lọc `S3_NON_ROAD_TYPE`) | 33.394 (chênh đúng `47` cửa sổ so với `33.441` toàn mạng) |
| Node sau lọc GIS và pruning | 52.181 (`52.425` trên toàn bộ `84.633` segments) |
| Bản ghi GIS quarantine theo first failing rule | 580 (`154` `S2_NONPOSITIVE_LENGTH` + `426` `S3_NON_ROAD_TYPE`, giao nhau `1` segment vừa `length <= 0` vừa `non-road`) |
| Sai lệch 5 thành phần Monoid + mean với baseline | 0 |
| Missing / extra / duplicate window keys | 0 / 0 / 0 |
| Hai chiều exceptAll salted ↔ unsalted (1-stage vs 2-stage) | 0 / 0 |
| Hai chiều exceptAll baseline join ↔ broadcast join | 0 / 0 |
| BroadcastHashJoin trong optimized plan | 3 (`0` `SortMergeJoin`, cả 3 phép nối `segments ⋈ lookup`, `windows ⋈ dim`, `result ⋈ streets`) |
| Exchange nodes baseline → optimized | 5 shuffle `Exchange` → 3 `BroadcastExchange` (`0` shuffle `Exchange`) |

**Đối chiếu định lượng `33.441` vs `33.394` cửa sổ (`47` cửa sổ) và `date_utc` vs `date` ICT (`4.320` cửa sổ):**
1. Trong `segments.csv` (`84.633` dòng), có `427` đoạn thuộc các loại phi giao thông cơ giới (`S3_NON_ROAD_TYPE`: đường đi bộ nội khu/hẻm cụt phi chuẩn). Trên `90.938` bản tin `segment_status.csv`, có đúng **`151` bản tin GPS** rơi vào các đoạn `S3_NON_ROAD_TYPE` này, tương ứng với đúng **`47` cửa sổ 30 phút** (`33.441 - 33.394 = 47`). Trong khi đó, `154` đoạn có `length <= 0` (`S2_NONPOSITIVE_LENGTH`) nhưng thuộc `ROAD_TYPES` hợp lệ được ghi nhận vào sổ cái kiểm toán `quarantine/segments` và đồng thời được sửa `length_m` bằng trung vị (`length_imputed = True`) nên giữ lại toàn bộ `103` bản tin GPS (`51` cửa sổ).
2. Về mốc ngày: có đúng **`4.320` cửa sổ** rơi vào khung `17:00–23:30 UTC` (`00:00–06:30 ICT` sáng hôm sau). `enrich_windows` trong [`traffic_data/spark_etl.py`](../traffic_data/spark_etl.py) lưu đồng thời cả hai cột: `date_utc` (`yyyy-MM-dd` theo UTC, khớp 100% với cột `date` của `train.csv` tham chiếu) và `date` (`yyyy-MM-dd` theo giờ địa phương `Asia/Ho_Chi_Minh`, phục vụ phân tích chu kỳ giao thông đô thị ICT).

## 6. Checklist Q01–Q11: trạng thái sau khi tích hợp và khắc phục 6 điểm kỹ thuật

- [x] **Q01 — Entrypoint Spark từ raw:**
  - [x] Đọc bốn bảng raw; không đọc `train.csv`; full batch chạy thành công trên cả Spark 3.5.7 (Java 17) và Spark 4.2.0 (Java 21).
  - [x] So aggregate với raw fold độc lập (`compare_window_aggregates`), không mất/thừa key trong accepted set (`0` sai lệch).
  - [x] Đã phân loại và đối chiếu chính xác `47` cửa sổ chênh lệch (`33.441 - 33.394 = 47` do `151` ping thuộc `427` đoạn `S3_NON_ROAD_TYPE`) và `4.320` cửa sổ đổi ngày UTC↔ICT (giữ cả `date_utc` và `date`).
- [x] **Q02 — Quality funnel có thứ tự:** all-string input, `try_cast`/`try_to_timestamp` (có shim tương thích Spark 3.5.7 & 4.2.0), GPS domain/FK (`E1..E4`), GIS key/length/type/level (`S1..S4`); tests lỗi semantic pass 100%.
- [x] **Q03 — Quarantine và accounting:** giữ `raw_payload`/`source`/ID/timestamp, `first_failing_rule`, xử lý identity conflict (`E5_CONFLICTING_EVENT_ID`); counts GPS full run cân bằng tuyệt đối (`90.938 = 90.787 + 151 + 0`).
- [x] **Q04 — Lookup TRAIN-only:**
  - [x] `P85 segment → median(level,type) → prior 80/60/50/40` (kèm tùy chọn `use_observed_gis_vmax`); tier khớp nhánh fill.
  - [x] Test perturb velocity sau cutoff không làm đổi lookup; đã persist lookup (`preprocessing/velocity_lookup` & `outputs/vmax_lookup.json`).
  - [x] Đạt đã chốt biên thời gian (`TRAIN <= 2020-12-12 08:00 UTC`, `VAL <= 2020-12-25 03:30 UTC`, `TEST > 2020-12-25 03:30 UTC`), khử rò rỉ nội tập bằng causal expanding priors + warmup-frozen 7D K-Means centroids và kiểm chứng future-perturbation invariance trong `tests/test_platform_v3.py` (`test_d01_d02_d03_d04_exact_t30m_horizon_purge_and_zero_leakage`).
- [x] **Q05 — LOS và ICT:** suy `LOS` từ `velocity_ratio`, giữ vận tốc `0`, test đủ 5 ngưỡng `0.80/0.65/0.50/0.35/0.20`, UTC `17:00` sang ngày ICT kế tiếp (kèm `date_utc`), `weekday`/`period_sin`/`period_cos`.
- [x] **Q06 — Monoid và salting:** Spark 2-stage salting (`8 buckets`) đối chứng trực tiếp với 1-stage `groupBy(*KEYS)` khi `salted=False`, dedup ping-level; đầy đủ 5 thành phần và mean khớp baseline.
- [x] **Q07 — Join evidence thật:** baseline `SortMergeJoin` (`5` shuffle `Exchange`) và `BroadcastHashJoin` toàn phần (`3` `BroadcastHashJoin`, `0` `SortMergeJoin`, `0` shuffle `Exchange`); hai chiều `exceptAll = 0`; lưu executed plans và `pruning_plan.txt`.
- [x] **Q08 — Benchmark skew:** đã trích xuất `task_metrics.json` thực tế từ Spark event logs (hỗ trợ cả định dạng `local-*` của Spark 3.5.7 và `events_*` của Spark 4.2.0 theo thứ tự tự nhiên) so sánh trực tiếp 1-stage unsalted với 2-stage salted.
- [x] **Q09 — Baseline đầy đủ:** `compare_window_aggregates` và `audit_records_against_monoid` trong `baseline.py` kiểm đủ 5 thành phần + mean, missing/extra/duplicate keys; cố ý sửa từng field hoặc key đều bị phát hiện.
- [x] **Q10 — Lakehouse và tái lập:** Bronze/Silver/Gold Parquet, month partition `year_month`, `window_metrics/batch_id=0`, input SHA-256, pruning nodes thực và `pruning_plan.txt`.
- [x] **Q11 — Handoff:** module dùng chung [`traffic_data/spark_etl.py`](../traffic_data/spark_etl.py), fixtures trong `tests/test_quyen_spark.py` & `tests/test_platform_v3.py`, và hợp đồng dữ liệu đã được Đạt và Minh xác nhận đầy đủ bên dưới.

## 7. Liên hệ với 28 nhóm yêu cầu trong audit tổng

Tất cả 28 nhóm yêu cầu trong [`PROPOSAL_V3_AUDIT_CHECKLIST.md`](../PROPOSAL_V3_AUDIT_CHECKLIST.md) hiện đã có hiện thực code và kiểm thử tự động tương ứng trong workspace hợp nhất (`pipeline.py`, `spark_engine.py`, `spark_batch.py`, `traffic_data/spark_etl.py`, `producer.py`, `starter_stream.py`, `baseline.py`, `app.py`, `tests/test_platform_v3.py`, `tests/test_quyen_spark.py`).

## 8. Xác nhận chính thức của Nguyễn Thành Đạt (`D-C01` – `D-C08`)

- [x] **D-C01 — Cutoff và dữ liệu huấn luyện (Đã chốt):**
  - Đối với snapshot kiểm thử độc lập của `spark_batch.py`: giữ `--train-end 2020-08-01T00:00:00Z` (exclusive UTC warmup cutoff).
  - Đối với tập mẫu có nhãn liên tiếp đúng `+30m` (`7.898` mẫu hợp lệ trong `pipeline.py`): chia theo mốc thời gian toàn cục `70 / 15 / 15` kèm **Boundary Purge** (`target_ts <= cutoff_ts`, `warmup_cutoff_utc = 2020-08-02T23:30:00+00:00`):
    - `TRAIN`: `slot_ts <= 2020-12-12 08:00:00+00:00` (`5.494` mẫu, purged `50` mẫu vượt biên).
    - `VAL`: `2020-12-12 08:00:00+00:00 < slot_ts <= 2020-12-25 03:30:00+00:00` (`1.167` mẫu, purged `5` mẫu vượt biên).
    - `TEST`: `slot_ts > 2020-12-25 03:30:00+00:00` (`1.182` mẫu, trong đó `185` mẫu `LOS E/F`).
  - Để ngăn chặn rò rỉ nội tập (`intra-TRAIN leakage`), các đặc trưng thống kê lịch sử sử dụng **Causal Expanding Empirical-Bayes Priors** gom theo `slot_ts` ($\tau < t$) và phân cụm `7D K-Means (k=6)` dùng tâm cụm cố định từ `warmup` kèm hồ sơ nhân quả mở rộng đến $t$. Bằng chứng kiểm thử: `test_d01_d02_d03_d04_exact_t30m_horizon_purge_and_zero_leakage` trong [`tests/test_platform_v3.py`](../tests/test_platform_v3.py).
- [x] **D-C02 — Ground truth (Đã chốt):**
  - Lưu song song cả `LOS` suy dẫn từ `derive_los(mean_velocity / max_velocity)` theo ngưỡng `0.80 / 0.65 / 0.50 / 0.35 / 0.20` và `reference_los` từ `train.csv`, báo cáo tỷ lệ khớp thực tế `52,37%` trong `outputs/metrics.json` (`derived_vs_reference_los_exact_match_pct`).
  - Huấn luyện và báo cáo hoàn toàn trung thực trên nhãn mục tiêu đúng chân trời `t + 30m` (`target_t30m_los_ordinal`: `RF 23D 6-Class Accuracy = 0.9450`, `Macro-F1 = 0.5543`, `Weighted-F1 = 0.9439`, `Severe F1 = 0.9409`, `ROC-AUC = 0.9882`, `PR-AUC = 0.9471`; `GBT Binary 14D Severe F1 = 0.7840`, `ROC-AUC = 0.9812`, `PR-AUC = 0.9328`; `GBT Binary 23D Severe F1 = 0.9465`, `ROC-AUC = 0.9843`, `PR-AUC = 0.9419`), kèm đối chiếu riêng cho `legacy_next_observed_comparison` (`23.414` cặp, `3.513` mẫu test: `Severe F1 = 0.6447`, `ROC-AUC = 0.8484`, `PR-AUC = 0.7628`) trong `outputs/ablation_results.csv` và `outputs/metrics.json`.
- [x] **D-C03 — Phạm vi graph (Đã chốt):**
  - Tách bạch rõ 2 phạm vi:
    1. **Static Feature Store & Line-Graph PageRank (`data/lakehouse/segment_static_features.parquet`):** giữ đầy đủ **`84.633` segments** và **`52.425` active nodes** (`151.929` directed transitions, `48` dangling segments) để phục vụ tra cứu `UC-3 Cold-Start` trên toàn mạng (`10.027` đoạn có GPS + `74.606` đoạn mù GPS).
    2. **Filtered Vehicular Road Batch (`spark_batch.py`):** lọc nghiêm ngặt `S3_NON_ROAD_TYPE` (`427` đoạn phi cơ giới) còn `84.206` đoạn và `52.181` active nodes (`33.394` cửa sổ).
- [x] **D-C04 — Lookup semantics (Đã chốt):**
  - `fit_velocity_lookup` trong [`traffic_data/spark_etl.py`](../traffic_data/spark_etl.py) mặc định dùng `SEGMENT_P85 (p85 > 0) → LEVEL_TYPE_MEDIAN → LEVEL_PRIOR {1: 80, 2: 60, 3: 50, 4: 40}` và hỗ trợ cờ `use_observed_gis_vmax: bool = False` (giới hạn chặt trong miền hợp lệ `(0, 200]` km/h) để giữ `observed_max_velocity` làm metadata hoặc bật tầng `SEGMENT_OBSERVED_VMAX` khi đối chiếu với `outputs/vmax_lookup.json`. Các đoạn có `segment_p85 == 0` được lọc bởi `segment_p85 > 0` và chuyển xuống `LEVEL_TYPE_MEDIAN`.
- [x] **D-C05 — Feature schema (Đã chốt):**
  - Khóa chuẩn của cửa sổ là `(segment_id, window_start)` theo UTC; `enrich_windows` xuất đồng thời `date_utc` (UTC) và `date`, `period_idx`, `weekday`, `period_sin`, `period_cos` (ICT `UTC+7`).
  - Vector đặc trưng `23D` (`FULL_ML_FEATURES` trong `pipeline.py` & `FULL_ML_FEATURES_23D` trong `spark_engine.py`, gồm 23 cột số `current_los_ordinal`, `lag1_los_ordinal`, `lag2_los_ordinal`, `trailing_severe_rate`, `mean_velocity`, `velocity_ratio`, `obs_count`, `length_m`, `max_velocity`, `street_level`, `weekday`, `period_sin`, `period_cos`, `gap_minutes`, `pagerank_score`, `in_degree`, `out_degree`, `neighbor_los_ordinal`, `cluster_id`, `segment_train_prior_los`, `segment_train_severe_prior`, `street_causal_prior_los`, `street_causal_prior_sev`, cùng 2 cột phân loại `street_type`, `period` được mã hóa One-Hot trong `SparkRF`) được cố định thứ tự cột trong `outputs/model_registry_manifest.json`.
- [x] **D-C06 — Target+30m (Đã chốt):**
  - Hàm `add_temporal_and_neighbor_lags` (`pipeline.py`) và `prepare_ml_dataset` (`spark_batch.py`) ghép nhãn bằng exact self-join trên `(segment_id, slot_ts + 30m)`. Các khoảng trống `60m`, `9h`, `3 ngày` có `has_exact_t30m_target = 0` và tuyệt đối không đưa vào tập huấn luyện/đánh giá `+30m`. Kiểm thử tự động: `test_d01_d02_d03_d04_exact_t30m_horizon_purge_and_zero_leakage` trong [`tests/test_platform_v3.py`](../tests/test_platform_v3.py).
- [x] **D-C07 — Mining hygiene (Đã chốt):**
  - Chuẩn hóa tên đường Unicode `NFC` + `Ð→Đ` + gộp khoảng trắng (`_normalize_street_name` & `normalize_name`), cắt bỏ hậu tố hành chính (`, Phường ...`, `, Quận ...`), lọc danh xưng hành chính, thực thi cả **2-Pass Distributed SON** và **Spark MLlib `FPGrowth`** (`outputs/spark_runtime_evidence.json`) sinh luật có hướng hai chiều (`A => B` và `B => A` trong `outputs/co_congestion_rules.csv`) cùng luật lan truyền trễ 30 phút (`Street_A(t) => Street_B(t+30m)` trong `outputs/lagged_congestion_rules.csv`).
- [x] **D-C08 — Golden data & Scorer Parity (Đã chốt):**
  - Đóng băng `outputs/model.joblib`, `data/lakehouse/segment_static_features.parquet`, `outputs/vmax_lookup.json` kèm SHA-256 trong `outputs/model_registry_manifest.json` và xuất tập dự báo chuẩn `outputs/golden_predictions.parquet`. Hàm `score_feature_snapshot` đảm bảo cùng đầu vào sinh dự báo khớp `100%` giữa Batch, Stream (`STREAM_PROVISIONAL`) và Recon (`BATCH_RECONCILED`).

## 9. Xác nhận chính thức của Phạm Công Minh (`M-C01` – `M-C11`)

- [x] **M-C01 — Runtime (Đã chốt):**
  - Codebase (`traffic_data/spark_etl.py`, `spark_batch.py`, `spark_engine.py`) đã được chuẩn hóa để chạy tương thích **100% trên cả hai môi trường**:
    1. Container **Apache Spark 3.5.7 / Java 17** (`song-recommender-spark:3.5.7` theo đúng cấu hình Proposal V3, đã bổ sung shim Catalyst SQL `try_cast` an toàn dưới cả chế độ `spark.sql.ansi.enabled=true` và hỗ trợ đọc event log `local-*`).
    2. Container **PySpark 4.2.0 / Java 21 / Python 3.11** (`Dockerfile.spark` & `compose.quyen.yml`, đọc event log `events_*` theo thứ tự số tự nhiên).
- [x] **M-C02 — Event ID (Đã chốt):**
  - Phân biệt chặt chẽ `raw_event_id` (định danh từng bản tin GPS / retry ở tầng ping) và `window_id` / `event_id` (định danh cửa sổ 30 phút của đoạn đường). Hai bản tin hợp lệ khác `raw_event_id` trong cùng một cửa sổ 30 phút được giữ nguyên để gộp vào Monoid `obs_count`.
- [x] **M-C03 — Timestamp/schema producer (Đã chốt):**
  - [`producer.py`](../producer.py) hỗ trợ cả `--mode raw` (`90.938` pings) và `--mode window` (`33.441` windows), gửi `updated_at` chuẩn ISO-8601 UTC (`Z`), `event_ts`, `ingest_ts = event_ts + arrival_delay_sec`, `velocity` (km/h) và `segment_id` vào Kafka topic `hcmc-traffic-events` (3 partitions).
- [x] **M-C04 — Quality policy (Đã chốt):**
  - Thống nhất quy tắc kiểm định có thứ tự `E1_INVALID_ID → E2_INVALID_TIMESTAMP → E3_INVALID_VELOCITY → E4_UNKNOWN_SEGMENT → E5_CONFLICTING_EVENT_ID` dùng chung cho cả batch (`spark_batch.py`) và micro-batch streaming (`starter_stream.py`).
- [x] **M-C05 — Dedup liên microbatch (Đã chốt):**
  - Kết hợp `deduplicate_events` trong từng micro-batch với **Bit-Packed Bloom Filter (`32,0 KiB = uint8[32768]`, `262.144` bits, `k = 4`)** và state store có watermark 30 phút trong [`starter_stream.py`](../starter_stream.py) để khử trùng liên micro-batch với bộ nhớ hằng số.
- [x] **M-C06 — Bronze và checkpoint (Đã chốt):**
  - Sink 1 (`raw_bronze_sink`) ghi 100% sự kiện vào `data/lakehouse/bronze_raw_events` (và `outputs/spark_bronze_stream_sink` trong `spark_engine.py`, không ghi đè `window_metrics/batch_id=0`) trước khi lọc watermark; Sink 2 (`agg_serving_sink`) áp dụng watermark 30 phút và lưu WAL checkpoints riêng tại `outputs/checkpoints/spark_raw_wal`, `outputs/checkpoints/spark_agg_wal`, `outputs/checkpoints/raw/offset_log.json`, `outputs/checkpoints/agg/state_wal.json`.
- [x] **M-C07 — Lookup cùng version (Đã chốt):**
  - Stream processor (`starter_stream.py`) và Recon (`pipeline.py`) gọi trực tiếp hàm `verify_frozen_registry_integrity` kiểm tra mã băm SHA-256 của cả 3 artifact (`model.joblib`, `segment_static_features.parquet`, `vmax_lookup.json`) trước và sau khi chấm điểm chỉ đọc (`.predict_proba()`); tuyệt đối không `.fit()` lại trong micro-batch.
- [x] **M-C08 — Serving key (Đã chốt):**
  - Collection `traffic_scores` trên MongoDB 8.0 (`bda501.traffic_scores`) dùng khóa cửa sổ duy nhất (`event_id_unique` trên `event_id` / `window_id`) và compound index `level_prob_segment_idx` (`{street_level: 1, severe_prob: -1, segment_id: 1}`), ghi bằng `repartition(4)` $\to$ `foreachPartition` (1 `MongoClient`/partition, lô `500` lệnh `UpdateOne(upsert=True)`).
- [x] **M-C09 — Recon (Đã chốt):**
  - Two-Tier Reconciliation (`[C12]`) thực hiện Tier 1 `exceptAll` hai chiều trên các cửa sổ trong watermark (`0` lệch, `32.940 / 32.940`) và Tier 2 đọc lại từ Bronze raw events để bù `501` bản tin cửa sổ đoạn đường trễ trên `219` khung giờ `(date, period)`, đồng thời chữa lành và chấm điểm lại `3.953` cửa sổ trễ bậc xuôi dòng (`healed_downstream_lag_windows = 3953`, `healed_downstream_rescored_windows = 377`).
- [x] **M-C10 — UI/read paths (Đã chốt):**
  - [`app.py`](../app.py) đã bổ sung kết nối trực tiếp tới MongoDB 8.0 (`_fetch_live_mongo_status = query_operational_serving_store`), trỏ `SPARK_VERIFY_PATH` tới `outputs/spark_runtime_evidence.json`, hiển thị trạng thái kết nối thực tế, phân bố `STREAM_PROVISIONAL (v1)` (`488` cửa sổ in-flight) vs `BATCH_RECONCILED (v2)` (`501` cửa sổ) vs `BATCH_VERIFIED (v2)` (`32.452` cửa sổ) và cho phép bật tự động làm mới 5 giây (`5s auto-refresh`).
- [x] **M-C11 — Integration proof (Đã chốt):**
  - Đã kiểm chứng kill/restart recovery giữa micro-batch (`starter_stream.py --kill-after-batch 1` $\to$ resume), `Version Guard` trên `status_version` chống `stale overwrite` (`BATCH_RECONCILED v2` không bị ghi đè bởi `STREAM_PROVISIONAL v1`), và kiểm chứng `explain("executionStats")` thực tế trên `mongo:8.0.34` (`IXSCAN`, `0` SORT, `warm_p95_ms = 0.636 ms < 10 ms` trong `outputs/mongo_verify.json`).

## 10. Quyết định chung đã chốt và ghi vào ADR / Governance (`product/`)

| Chủ đề | Người chốt | Quyết định đã thống nhất |
|---|---|---|
| Chênh `33.441 → 33.394` windows và `52.425 → 52.181` nodes | Quyền + Đạt + Minh | Giữ cả hai chế độ rõ ràng: `84.633` segments / `52.425` nodes (`33.441` windows) cho toàn mạng & `UC-3 Cold-Start`; `84.206` segments / `52.181` nodes (`33.394` windows, loại `427` đoạn `S3_NON_ROAD_TYPE` với `151` GPS pings = `47` windows) cho `spark_batch.py` |
| Warmup/cutoffs và LOS ground truth | Đạt, Quyền review | Giữ song song `LOS` suy dẫn (`derive_los`) và `reference_los` (`52,37%` khớp); cắt tập theo timestamp toàn cục có purge biên (`5.494 / 1.167 / 1.182`); dùng causal expanding priors + warmup-frozen 7D K-Means |
| Raw event và serving identities | Minh, Quyền review | `raw_event_id` định danh từng ping GPS; `window_id` / `event_id` định danh cửa sổ 30 phút và làm unique key tại MongoDB (`event_id_unique`) |
| Runtime Java 17 (Spark 3.5.7) & Java 21 (Spark 4.2.0) | Minh, cả nhóm review | `traffic_data/spark_etl.py` và `spark_batch.py` tương thích cả Spark 3.5.7 (Java 17) và Spark 4.2.0 (Java 21); cả hai bộ test `pytest` và `unittest` đều pass 100% |
| Handoff parity | Đạt + Minh + Quyền | Dự báo chuẩn `outputs/golden_predictions.parquet` và `outputs/quyen_validation/manifest.json` khớp 100% (`diff = 0`) |

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
