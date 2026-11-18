# Hướng dẫn chạy và bàn giao branch Data Engineering của Quyền

**Branch:** `feat/quyen-spark-data-engineering`

**Ngày đối chiếu Wave 2:** 18/11/2026

**Tài liệu đối chiếu gốc:** [TECHNICAL_NOTE.md](../TECHNICAL_NOTE.md) · [product/00-proposal/project-proposal.md](../product/00-proposal/project-proposal.md)

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

`train.csv` không phải input sản xuất của đường xử lý `spark_batch.py`. Trên workspace hợp nhất hiện tại, pipeline batch/ML/streaming (`pipeline.py`, `spark_engine.py`, `live_kafka_stream.py`) đã tích hợp đầy đủ Kafka, MongoDB 8.0 và ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)** — chi tiết xem Mục 7–10 bên dưới.

## 2. Chuẩn bị trên Windows / PowerShell

### 2.1. Kiểm tra môi trường và branch

Mở Docker Desktop ở chế độ Linux containers, sau đó mở PowerShell tại thư mục repo:

```powershell
Set-Location 'd:\Dat\FSB\big_data\groupexe-knowledgetest'
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
| Cửa sổ hợp lệ (sau lọc `S3_NON_ROAD_TYPE`) | 33.394 (đã loại đúng `47` cửa sổ phi đường bộ khỏi bảng thô) |
| Node sau lọc GIS và pruning | 52.181 (loại bỏ `525.786` / `90,97%` từ `577.967` nút thô) |
| Bản ghi GIS quarantine theo first failing rule | 580 (`154` `S2_NONPOSITIVE_LENGTH` + `426` `S3_NON_ROAD_TYPE`, giao nhau `1` segment vừa `length <= 0` vừa `non-road`) |
| Sai lệch 5 thành phần Monoid + mean với baseline | 0 |
| Missing / extra / duplicate window keys | 0 / 0 / 0 |
| Hai chiều exceptAll salted ↔ unsalted (1-stage vs 2-stage) | 0 / 0 |
| Hai chiều exceptAll baseline join ↔ broadcast join | 0 / 0 |
| BroadcastHashJoin trong optimized plan | 3 (`0` `SortMergeJoin`, cả 3 phép nối `segments ⋈ lookup`, `windows ⋈ dim`, `result ⋈ streets`) |
| Exchange nodes baseline → optimized | 5 shuffle `Exchange` → 3 `BroadcastExchange` (`0` shuffle `Exchange`) |

**Đối chiếu định lượng `47` cửa sổ phi đường bộ bị loại để còn `33.394` cửa sổ cơ giới và `date_utc` vs `date` ICT (`4.305` cửa sổ trên tập cơ giới):**
1. Trong `segments.csv` (`84.633` dòng), có `427` đoạn thuộc các loại phi giao thông cơ giới (`S3_NON_ROAD_TYPE`: đường đi bộ nội khu/hẻm cụt phi chuẩn). Trên `90.938` bản tin `segment_status.csv`, có đúng **`151` bản tin GPS** rơi vào các đoạn `S3_NON_ROAD_TYPE` này, tương ứng với đúng **`47` cửa sổ 30 phút** bị loại để thu về đúng **`33.394` cửa sổ cơ giới sạch**. Trong khi đó, `154` đoạn có `length <= 0` (`S2_NONPOSITIVE_LENGTH`) nhưng thuộc `ROAD_TYPES` hợp lệ được ghi nhận vào sổ cái kiểm toán `quarantine/segments` và đồng thời được sửa `length_m` bằng trung vị (`length_imputed = True`) nên giữ lại toàn bộ `103` bản tin GPS (`51` cửa sổ).
2. Về mốc ngày: trên `33.394` cửa sổ đường cơ giới sạch có **`4.305` cửa sổ** rơi vào khung `17:00–23:30 UTC` (`00:00–06:30 ICT` sáng hôm sau; `4.320` trên tập chưa lọc 47 cửa sổ phi đường bộ). `enrich_windows` trong [`traffic_data/spark_etl.py`](../traffic_data/spark_etl.py) lưu đồng thời cả hai cột: `date_utc` (`yyyy-MM-dd` theo UTC, khớp 100% với cột `date` của `train.csv` tham chiếu) và `date` (`yyyy-MM-dd` theo giờ địa phương `Asia/Ho_Chi_Minh`, phục vụ phân tích chu kỳ giao thông đô thị ICT).

## 6. Checklist Q01–Q11: trạng thái sau khi tích hợp và khắc phục 6 điểm kỹ thuật

- [x] **Q01 — Entrypoint Spark từ raw:**
  - [x] Đọc bốn bảng raw; không đọc `train.csv`; full batch chạy thành công trên cả Spark 3.5.7 (Java 17) và Spark 4.2.0 (Java 21).
  - [x] So aggregate với raw fold độc lập (`compare_window_aggregates`), không mất/thừa key trong accepted set (`0` sai lệch).
  - [x] Đã phân loại và đối chiếu chính xác `47` cửa sổ phi đường bộ bị loại (do `151` ping thuộc `427` đoạn `S3_NON_ROAD_TYPE`, giữ đúng `33.394` cửa sổ cơ giới sạch) và `4.305` cửa sổ cơ giới đổi ngày UTC↔ICT (giữ cả `date_utc` và `date`).
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

## 7. Liên hệ với các yêu cầu kiểm toán & Hợp nhất Hệ thống

Các yêu cầu kỹ thuật cốt lõi hiện đã được thống nhất xuyên suốt các module trong workspace hợp nhất (`pipeline.py`, `spark_engine.py`, `spark_batch.py`, `traffic_data/spark_etl.py`, `producer.py`, `starter_stream.py`, `live_kafka_stream.py`, `baseline.py`, `toc_backend/`, `toc_frontend/`).

## 8. Xác nhận chính thức của Nguyễn Thành Đạt (`D-C01` – `D-C08`, Leak-Free Verified)

- [x] **D-C01 — Cutoff, lọc burst và dữ liệu huấn luyện (Đã chốt):**
  - Đối với snapshot kiểm thử độc lập của `spark_batch.py`: giữ `--train-end 2020-08-01T00:00:00Z` (exclusive UTC warmup cutoff).
  - Đối với tập mẫu có nhãn liên tiếp đúng `+30m` (`7.885` cặp hợp lệ trong `pipeline.py`): phát hiện `7.192` cửa sổ burst tổng hợp tại `2020-08-02 23:30` và `2020-08-03 00:00` (`3.609` đoạn thô / `3.596` đoạn cơ giới sạch mỗi slot), loại bỏ `3.596` cặp burst khi huấn luyện ML (`TRAIN`: `slot_ts <= 2020-12-12T08:00:00Z`, giữ **`1.885` cặp `TRAIN` hữu cơ**, purged `50` mẫu vượt biên; **`1.167` `VAL`**, purged `5` mẫu vượt biên; **`1.182` `TEST`**, trong đó `185` mẫu `LOS E/F`).
  - Các đặc trưng thống kê lịch sử sử dụng **Causal Expanding Empirical-Bayes Priors** gom theo `slot_ts` ($\tau < t$) và phân cụm Unified `7D K-Means (k=6)` học trên `3.609` đoạn `TRAIN` hữu cơ (`Silhouette = 0.2241, DBI = 1.3284, CH = 806.49`).
- [x] **D-C02 — Ground truth & ML sạch (Đã chốt):**
  - Dùng `derive_urban_los` theo chuẩn **BkTraffic Urban 5 km/h** đạt **`89.19%`** khớp chính xác với `reference_los` của `train.csv` trên `33.394` cửa sổ cơ giới (tách bạch rõ với hàm tiện ích tỷ lệ `derive_los` dùng ngưỡng `0.80 / 0.65 / 0.50 / 0.35 / 0.20`).
  - Huấn luyện sạch không dùng `sample_weight` nhân tạo hay fallback bypass (`class_weight` lũy thừa `0.65`): `RF 23D` thắng cổng `VAL` (`(val_accuracy_6class + val_pr_auc + val_roc_auc)/3 = 0.8882` vs `0.8812` của `DT 23D`; `τ_severe = 0.25`, `τ_change = 0.40`, `τ_onset = 0.25`). Trên `TEST` (`1.182` mẫu): **`ROC-AUC = 0.9894`**, **`PR-AUC = 0.9543`**, **`6-Class Accuracy = 0.9255`**, **`Macro-F1 = 0.4253`** *(do mất cân bằng lớp trung gian `B/C/D/E`)*, **`Weighted-F1 = 0.9297`**, **`Severe F1 = 0.9362`** (`Prec = 0.9215, Rec = 0.9514`), đường chéo `[944, 3, 1, 3, 5, 138]`; vượt `Persistence` khi chuyển pha (`10 / 61 = 16.39%`, `McNemar p = 0.001953`, `b=10, c=0`; hiệu chỉnh `VAL` đạt `14 / 61 = 22.95%`, tổng `90.19%`; `Detect ROC-AUC = 0.8822`, `PR-AUC = 0.4009`). Đối chiếu `legacy_next_observed_comparison` (`4.787` mẫu test): `Severe F1 = 0.6121`, `PR-AUC = 0.7378`, `ROC-AUC = 0.8668`.
- [x] **D-C03 — Phạm vi graph thống nhất (Đã chốt):**
  - Hợp nhất cả `pipeline.py`, `spark_batch.py` và `segment_static_features.parquet` về cùng phạm vi **mạng lưới cơ giới sạch `84.206` đoạn đường** và **`52.181` active nodes** (loại bỏ `427` đoạn `S3_NON_ROAD_TYPE` và `525.786` điểm uốn cong trung gian = `90,97%`).
  - Đồ thị nối đoạn đường **đã loại cung U-turn trên đoạn hai chiều**: **`92.453` cạnh chuyển tiếp**, **`314` đoạn cụt (`dangling nodes`)**, nút cổ chai `#1` là **`21969 — Nguyễn Kiệm (level=2, PR=2.7799)`**. Phủ kín **`100%`** `84.206` đoạn (`9.995` có GPS + `74.211` Cold-Start = `88.13%`).
- [x] **D-C04 — Lookup semantics (Đã chốt):**
  - `fit_velocity_lookup` trong [`traffic_data/spark_etl.py`](../traffic_data/spark_etl.py) mặc định dùng `SEGMENT_P85 (p85 > 0) → LEVEL_TYPE_MEDIAN → LEVEL_PRIOR {1: 80, 2: 60, 3: 50, 4: 40}` và hỗ trợ cờ `use_observed_gis_vmax: bool = False`; `fit_train_vmax_lookup` trong `pipeline.py` học chỉ trên `TRAIN` hữu cơ không rò rỉ (phân bổ cửa sổ: `4.946` Observed + `7.650` P85 + `19.601` Tier 2 + `1.197` Tier 3 = `33.394`; phân bổ đoạn: `9.869 / 472 / 54.259 / 19.606 = 84.206`).
- [x] **D-C05 — Feature schema (Đã chốt):**
  - Khóa chuẩn của cửa sổ là `(segment_id, window_start)` theo UTC; `enrich_windows` xuất đồng thời `date_utc` (UTC) và `date`, `period_idx`, `weekday`, `period_sin`, `period_cos` (ICT `UTC+7`).
  - Vector đặc trưng `23D` cố định thứ tự cột trong `outputs/model_registry_manifest.json` (`data/dataset.csv` gồm `33.394` dòng × `60` cột có `is_synthetic_burst`).
- [x] **D-C06 — Target+30m (Đã chốt):**
  - Hàm `add_temporal_and_neighbor_lags` (`pipeline.py`) và `prepare_ml_dataset` (`spark_batch.py`) ghép nhãn bằng exact self-join trên `(segment_id, slot_ts + 30m)` (`7.885` cặp hợp lệ).
- [x] **D-C07 — Mining hygiene trên TRAIN hữu cơ (Đã chốt):**
  - Chuẩn hóa tên đường `Unicode NFC` (`695 → 641` tên chuẩn, gộp `54` biến thể trên `2.247` cửa sổ), loại danh xưng hành chính (`631` cửa sổ `R3`) và khai phá **chặt trên `TRAIN` hữu cơ + kiểm chứng hold-out**: `12` luật đồng kẹt (`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.2308`) và `14` luật trễ `persistence_controlled = True` (`Lê Quang Định → Trần Quang Khải`, `Lift = 24.0`).
- [x] **D-C08 — Golden data & Scorer Parity (Đã chốt):**
  - Đóng băng `outputs/model.joblib` (`a2f411846508`), `data/lakehouse/segment_static_features.parquet` (`75328ac2d5e3`), `outputs/vmax_lookup.json` (`ee7f692daad8`) và mẫu gọn `sample_static_features.parquet` (`f26ebdf7d396`) kèm SHA-256 trong `outputs/model_registry_manifest.json` (`rf_full_23d_v3`) và xuất `outputs/golden_predictions.parquet`.

## 9. Xác nhận chính thức của Phạm Công Minh (`M-C01` – `M-C11`, Leak-Free Verified)

- [x] **M-C01 — Runtime (Đã chốt):**
  - Codebase (`traffic_data/spark_etl.py`, `spark_batch.py`, `spark_engine.py`) tương thích trên cả hai môi trường:
    1. Container **Apache Spark 3.5.7 / Java 17** (`hcmc-traffic-spark:3.5.7` trong `Dockerfile.spark35` & `docker-compose.yml`, hỗ trợ event log `local-*` và `ANSI try_cast`).
    2. Container **PySpark 4.2.0 / Java 21 / Python 3.11** (`Dockerfile.spark` & `compose.quyen.yml`).
- [x] **M-C02 — Event ID (Đã chốt):**
  - Phân biệt chặt chẽ `raw_event_id` (tầng ping) và `window_id` / `event_id` (cửa sổ 30 phút).
- [x] **M-C03 — Timestamp/schema producer (Đã chốt):**
  - [`producer.py`](../producer.py) hỗ trợ `--mode raw`, `--mode window` và `--mode story` gửi sự kiện ISO-8601 UTC vào Kafka topic `hcmc-traffic-events` (`3` partitions).
- [x] **M-C04 — Quality policy (Đã chốt):**
  - Quy tắc kiểm định có thứ tự `E1..E5` / `R1..R6` thống nhất cho cả batch và micro-batch streaming.
- [x] **M-C05 — Dedup liên microbatch (Đã chốt):**
  - Kết hợp `deduplicate_events` trong micro-batch với **`32.0 KiB` `RotatingBloomFilter` (`2 × 16.0 KiB`, `333` dups, `13` FPs, `0.0389%` thực nghiệm vs `0.0792%` lý thuyết)** và **`1.0 KiB` `64-bit HLL++` (`p=10`, `uint8[1024]`, `9.620,2` vs `9.995`, sai số `3,75%`)** trong [`starter_stream.py`](../starter_stream.py) & [`live_kafka_stream.py`](../live_kafka_stream.py).
- [x] **M-C06 — Bronze, Lakehouse và checkpoint (Đã chốt):**
  - `data/lakehouse/bronze_raw_events/events.parquet` (`90.787` raw pings), `stream_replay_windows/replay.parquet` (`33.727`), `silver_cleaned_windows/windows.parquet` (`33.394`), `curated_gold/year_month=*` (`10` phân vùng đọc sạch bởi cả PyArrow/Pandas và Spark 3.5.7); Spark Streaming Sink 1 (`outputs/spark_bronze_stream_sink`) & Sink 2 watermark thực **`2020-12-01T12:30:00.000Z`**; WAL checkpoint tại `outputs/checkpoints/` (`73.7 KB`, `73,714 B`).
- [x] **M-C07 — Lookup cùng version (Đã chốt):**
  - `verify_frozen_registry_integrity` kiểm tra SHA-256 của cả 3 artifact trước và sau mỗi lần chấm điểm chỉ đọc.
- [x] **M-C08 — Serving key (Đã chốt):**
  - `bda501.traffic_scores` trên MongoDB 8.0.34 dùng khóa duy nhất `event_id_unique`, compound index `level_prob_segment_idx`, ghi qua 4 phân vùng (`69` lô × `500` `UpdateOne(upsert=True)`) kèm atomic `$status_version` pipeline guard.
- [x] **M-C09 — Recon (Đã chốt):**
  - Tier 1 `exceptAll = 0` (`100.0%` trong watermark); Tier 2 bù **`500`** cửa sổ đoạn đường trễ trên **`226`** khung giờ (`98.5% → 100.0%`), chữa lành **`609`** cửa sổ hạ lưu (`healed_downstream_lag_windows`), chấm điểm lại **`384`** cửa sổ (**`213`** đổi xác suất).
- [x] **M-C10 — UI/read paths (Đã chốt):**
  - Hợp nhất toàn bộ trải nghiệm điều hành vào một ứng dụng web duy nhất: **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`, dịch vụ `toc-web` = `toc_backend/main.py` + `toc_frontend/dist/`)**, đọc dữ liệu thực (`32.405` `BATCH_VERIFIED v2` + `500` `BATCH_RECONCILED v2` + `489` `STREAM_PROVISIONAL v1` = `33.394` docs).
- [x] **M-C11 — Integration proof (Đã chốt):**
  - Kill/restart WAL recovery (`--kill-after-batch 1`, `0` duplicates, `90.741` in-WM), atomic `$status_version` guard chặn `25/25` stale `v1`, và `mongo:8.0.34` `explain("executionStats")` xác nhận `IXSCAN` (`warm median = 0.360 ms / p95 = 0.603 ms` vs `COLLSCAN median = 11.289 ms`, nhanh hơn **`31.36x`**, `1.234` cảnh báo trục chính).

## 10. Quyết định chung đã chốt (`product/` & Tài liệu Kỹ thuật)

| Chủ đề | Người chốt | Quyết định đã thống nhất |
|---|---|---|
| Phạm vi đường cơ giới sạch `84.206` & `33.394` windows | Quyền + Đạt + Minh | Toàn bộ `pipeline.py`, `spark_batch.py` và `segment_static_features.parquet` thống nhất trên `84.206` đoạn đường cơ giới (`52.181` active nodes, `90.787` accepted pings, `33.394` windows), loại `427` đoạn `S3_NON_ROAD_TYPE` (`151` pings = `47` windows) |
| `derive_urban_los` vs `derive_los` & Lọc Burst | Đạt, Quyền review | Dùng `derive_urban_los` (`BkTraffic 5 km/h`) khớp **`89.19%`** reference `train.csv` (tách rõ với `derive_los` tỷ lệ); loại `7.192` cửa sổ burst (`3.596` TRAIN pairs) để huấn luyện sạch trên `1.885` `TRAIN` hữu cơ |
| Raw event, `RotatingBloom` & Serving guard | Minh, Quyền review | `raw_event_id` định danh ping, `event_id` định danh cửa sổ; `32 KiB RotatingBloomFilter` + `1 KiB 64-bit HLL++`; atomic `$status_version` pipeline guard trên MongoDB 8.0 |
| Runtime Spark 3.5.7 & Spark 4.2.0 | Minh + Quyền | `hcmc-traffic-spark:3.5.7` (Java 17) cho cụm chính `docker-compose.yml`; `Dockerfile.spark` (PySpark 4.2.0 / Java 21) cho `compose.quyen.yml` |
| Web UI Hợp nhất (`:8000`) | Minh, cả nhóm review | Ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center (`toc-web :8000`)** chạy cùng `kafka`, `mongo`, `spark`, `live-consumer` (`5` dịch vụ mặc định = `11.0 GB RAM`) |

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
| Web TOC vẫn hiện số cũ | Tải lại dịch vụ `toc-web` (`docker compose restart toc-web`) và xác nhận `outputs/` mới nhất đã được sinh |

Tài liệu kỹ thuật trước đó: [QUYEN_DATA_ENGINEERING.md](QUYEN_DATA_ENGINEERING.md).
Tài liệu tiếng Việt này cung cấp hướng dẫn chạy và checklist nghiệm thu chi tiết hơn;
khi có quyết định mới, cập nhật cả contract và tests trước khi tick mục tương ứng.
