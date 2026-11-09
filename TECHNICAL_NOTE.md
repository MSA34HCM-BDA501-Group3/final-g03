# Technical Note & Architectural Specification (V3)

## HCMC Smart Traffic Analytics & Congestion Prediction Platform

**Đơn vị đào tạo:** FSB — FPT University (Master of Software Engineering — `MSE`)  
**Học phần:** Big Data Analytics (`BDA501`)  
**Nền tảng Học thuật & Kỹ thuật:** *Big Data Analytics: A Hands-On Approach* (Bahga & Madisetti, 2019), *Mining of Massive Datasets, 3rd Ed.* (Leskovec, Rajaraman & Ullman, 2020), kết hợp 4 Trụ cột Thực hành Lab `BDA501` (`Pillar I1` Hadoop MapReduce & Commutative Monoid, `Pillar I2` Spark SQL/Catalyst/Skew, `Pillar I3` Spark MLlib Pipelines & Zero-Conflict MLOps, `Pillar S7` Kafka KRaft + Structured Streaming + MongoDB 8.0)  
**Nhóm thực hiện (`Project Team`):**

| STT | Họ và Tên | Mã Học Viên | Vai trò Chịu trách nhiệm Chính (`DRI`) |
|---:|---|---|---|
| 1 | Nguyễn Tạ Minh Quyền | `25ms23280` | `Pillars I1 & I2` (`[C2]`, `[C5]`, `[C9]`, `[C14]`, `[C15]` — `Q01`..`Q11`) |
| 2 | Nguyễn Thành Đạt | `25ms23306` | `MMDS Ch. 5–7, 12 & Pillar I3` (`[C10]`, `[C11]`, `[C13a]`, `[C13b]` — `D01`..`D12`) |
| 3 | Phạm Công Minh | `25ms23310` | `Pillar S7, MMDS Ch. 4 & Serving` (`[C1]`, `[C4]`, `[C6]`–`[C8]`, `[C12]`, `[C16]`, `[C17]` — `M01`..`M14`) |

**Nguồn Dữ liệu Chính (`Primary Dataset`):** Bộ dữ liệu giao thông đô thị TP.HCM 5 bảng liên kết (`nodes.csv`, `streets.csv`, `segments.csv`, `segment_status.csv`, `train.csv` — Kaggle `thanhnguyen2612/traffic-flow-data-in-ho-chi-minh-city-viet-nam`).  
**Hiện vật Bàn giao (`Deliverables`):** Hồ dữ liệu `Medallion Parquet Lakehouse` (`data/dataset.csv`, `data/lakehouse/*.parquet`), các báo cáo kiểm chứng định lượng tại `outputs/` (bao gồm bằng chứng thực thi thật trên `Spark 3.5.7` và `MongoDB 8.0.34`), bộ kiểm thử tự động (`tests/test_platform_v3.py`), hệ thống tài liệu quản trị sản phẩm 11 giai đoạn (`product/00-proposal` .. `product/09-growth`), và giao diện điều hành 5-Tab `Streamlit BI Dashboard` (`app.py`) tại `http://localhost:8501`.

---

## 1. Định Nghĩa Toán Học (`Sub-Problems P1–P4`), Quyết Định Thiết Kế (`G01–G05`) & Trường Hợp Biên (`EC-1–EC-10`)

### 1.1. Bốn Bài Toán Con Liên Kết (`Coupled Sub-Problems P1–P4`)
- **`Sub-Problem P1` — Descriptive Windowing & LOS Derivation (`Pillars I1 & I2`, `Q01–Q11`):** Từ luồng bản tin vận tốc GPS thô $(s_i, v_i, t_{\text{event}})$ trong `segment_status.csv` (`90,938` bản tin) và 3 bảng GIS (`nodes.csv`, `streets.csv`, `segments.csv`), hệ thống độc lập dựng `33,441` cửa sổ 30 phút $w = (s, d, p)$ (không phụ thuộc vào `train.csv`), đồng bộ toàn diện sang giờ địa phương Việt Nam (`ICT = UTC+7`: `ict_date`, `ict_weekday`, `vn_hour`, `ict_period`, $\sin/\cos$), cách ly bản ghi lỗi vào `Quarantine Store` với đúng một `first_failing_rule` duy nhất (`input = dedup_dropped + accepted + rejected`), cộng gộp qua **5-Tuple Commutative Monoid** $\mathbf{m} = (\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt})$ với phần tử đơn vị $\mathbf{e} = (0, 0.0, +\infty, 0.0, 0)$ (kiểm chứng `0` sai lệch trên cả 5 thành phần so với [`baseline.py`](./baseline.py)), điền khuyết $\hat{v}_{\max,s}$ qua phễu phân cấp 3 tầng học độc quyền trên `TRAIN` (`80/60/50/40 km/h`) và suy dẫn nhãn `Level of Service` (`derive_los` theo ngưỡng `0.80 / 0.65 / 0.50 / 0.35 / 0.20`).
- **`Sub-Problem P2` — Diagnostic Graph & Pattern Mining (`MMDS Ch. 5, 6, 7`, `D04–D07`):** Cắt tỉa `525,542 / 577,967` (`90.93%`) đỉnh hình học OSM mồ côi (`EC-7`), dựng **Directed Line Graph** $L(G) = (V_L, E_L)$ (`84,633` đỉnh đoạn đường, `151,929` cạnh chuyển tiếp có hướng, `48` `dangling dead-ends`), tính **PageRank** ($\beta = 0.85$), khai phá luật đồng kẹt xe có hướng 2 chiều (`A => B` và `B => A`) bằng thuật toán **2-Pass SON** và **Spark FPGrowth** trên giỏ hàng đầy đủ không cắt ngọn (`untruncated`) kèm luật lan truyền trễ 30 phút $\text{Street}_A(t) \Rightarrow \text{Street}_B(t+30\text{m})$, và phân cụm các đoạn đường thành $k=6$ chế độ hành vi trên không gian **Train-Only 7D Behavioral Space** (gán cụm từng dòng bằng hồ sơ nhân quả mở rộng đến thời điểm $t$ và tâm cụm cố định từ `warmup` để triệt tiêu hoàn toàn rò rỉ nội tập `TRAIN`), phủ kín cả `10,027` đoạn có GPS lẫn `74,606` (`88.15%`) đoạn vùng mù `Cold-Start` (`EC-6`).
- **`Sub-Problem P3` — Leak-Free Exact `t + 30m` Horizon Forecasting (`Pillar I3 & MMDS Ch. 12`, `D01–D03, D08–D12`):** Học hàm dự báo nhân quả $f_{\boldsymbol{\theta}}: \mathcal{X}_{s,\le t} \mapsto (\widehat{\text{LOS}}_{s,t+30\text{m}} \in \{A..F\}, \hat{p}_{\text{severe},s,t+30\text{m}} \in [0, 1])$ trên vector đặc trưng `23D` với nhãn mục tiêu chính thức được tạo bằng phép nối thời gian chính xác `(segment_id, slot_ts + 30m)` (`7,898` cặp liên tiếp đúng `+30m`; các khoảng trống `60m` hoặc `9h` bị loại trừ hoàn toàn khỏi nhãn `+30m`). Tập dữ liệu có nhãn được chia theo mốc thời gian toàn cục `70% TRAIN / 15% VAL / 15% TEST` và lọc bỏ (`purge`) mọi cửa sổ ở biên có `target_ts > cutoff_ts` (`5,494` TRAIN sau khi purge `50` mẫu biên, `1,167` VAL sau khi purge `5` mẫu biên, `1,182` TEST với `185` mẫu `LOS E/F`). Các biến **Causal Expanding Empirical-Bayes Priors** được gom theo mốc `slot_ts` để bất biến `100%` trước mọi nhiễu loạn tương lai ở $> t$. Huấn luyện độc quyền ở **Phase A** (`[C11]`), chọn siêu tham số và ngưỡng cảnh báo trên tập `VAL` (`Validation Gate`), và đóng băng bất biến 3 artifact (`model.joblib`, `segment_static_features.parquet`, `vmax_lookup.json`).
- **`Sub-Problem P4` — $O(1)$ Stream Sketching, WAL Recovery & Two-Tier Reconciliation (`Pillar S7 & MMDS Ch. 4`, `M01–M14`):** Xử lý luồng trực tuyến (**Phase B**, $t > T_{\text{train}}$) trong ngưỡng `30m Watermark` bằng bộ nhớ hằng số $O(1)$ (**Bit-Packed Bloom Filter `32.0 KiB = uint8[32768]`** khử trùng lặp `1% = 334` bản tin, **HyperLogLog++ `1.0 KiB = uint8[1024]`** đếm số đoạn đường hoạt động, và **Bounded Exponentially Decaying Hotspot Score** $C_s(t) = (1-\alpha)C_s(t-1) + \mathbb{I}(\text{LOS} \in \{E,F\})$ với $\alpha = 0.15$ và cắt tỉa ngưỡng $\epsilon$), phát hành dự báo `STREAM_PROVISIONAL (status_version=1)` qua `score_feature_snapshot`, hỗ trợ hồi phục từ `WAL Checkpoints` sau sự cố (`0` duplicate side effects), và tự động chữa lành `1.5%` (`501`) bản tin đến trễ quá `Watermark` trên `219` khung giờ `(date, period)` cùng `3,953` cửa sổ trễ `lag1` kế tiếp qua **Two-Tier Reconciliation** (**Phase C**, `[C12]`, nâng cấp lên `BATCH_RECONCILED / BATCH_VERIFIED (status_version=2)` có khóa chống ghi đè lùi) vào **MongoDB 8.0.34** (`[C16]`).

### 1.2. Mười Trường Hợp Biên Thực Nghiệm (`EC-1–EC-10`)

| Mã Edge Case | Biểu hiện Thực nghiệm trong Dữ liệu TP.HCM | Cơ chế Phòng vệ Kỹ thuật & Thành phần Đảm nhiệm (`[C1]`–`[C17]`) |
|---|---|---|
| **`EC-1`** | Khuyết `85.21%` (`28,495`) `max_velocity` ở `train.csv` và `88.34%` (`74,762`) ở `segments.csv` | `[C5]` **TRAIN-Only 3-Tier Velocity Imputation** (`TIER_1_SEGMENT_OBSERVED/P85` $\to$ `TIER_2_LEVEL_ROADTYPE_MEDIAN` $\to$ `TIER_3_HIERARCHICAL_LEVEL_PRIOR {1:80, 2:60, 3:50, 4:40}`), lưu thành `outputs/vmax_lookup.json` |
| **`EC-2`** | `154` đoạn đường có `length <= 0m`; `427` đoạn mang nhãn OSM phi giao thông (`house`, `fuel`...); `448` bản tin `v = 0` | `[C5]` **Ordered Quality Funnel** gán đúng một `first_failing_rule` duy nhất vào `[C15]` `quarantine_events.parquet`; điền khuyết `length <= 0` bằng trung vị TRAIN `27.0m` và chặn dưới vận tốc `1.0 km/h` |
| **`EC-3`** | `695` tên đường thô lẫn lộn `Unicode NFD/NFC`, ký tự `Ð/Đ`, tiền tố `Đường ` và nhãn hành chính `Phường/Quận` | `[C5]` **Street Entity Hygiene** chuẩn hóa về `649` thực thể `Unicode NFC` sạch và lọc bỏ `631` cửa sổ mang nhãn `Phường/Quận` (`0` luật tự suy diễn trong `[C10]`) |
| **`EC-4`** | Cột `period` cắt theo giờ `UTC` (lệch `-7h` so với `ICT`); xuất hiện đổi ngày tại `17:00 UTC = 00:00 ICT` | `[C5]` & `[C11]` Hàm `align_utc_to_ict` đồng bộ đầy đủ `ict_date`, `ict_weekday`, `vn_hour`, `ict_period` và mã hóa chu kỳ điều hòa $\sin/\cos(2\pi t_{\text{ICT}} / 48)$ (xử lý an toàn `period` lạ) |
| **`EC-5`** | Độ thưa cực đại: `79.26%` đoạn đường có $\le 2$ quan sát; ma trận 48 khung giờ khuyết `95.07%` | `[C10]` Thay ma trận 48 chiều bằng **7D Behavioral Space** với tâm cụm cố định từ `warmup`; `[C11]` dùng **Causal Expanding Empirical-Bayes Priors** gom theo `slot_ts` (bất biến `100%` với tương lai $> t$) |
| **`EC-6`** | Chuỗi đứt quãng (`66.27%` cặp liên tiếp cách nhau $> 30\text{m}$, chỉ có `7,898` cặp đúng `+30m`) và `88.15%` (`74,606`) đoạn đường `Cold-Start` | `[C11]` Tạo nhãn mục tiêu bằng phép nối `(segment_id, slot_ts + 30m)` và kiểm định `is_contiguous_30m = (prev_gap == 30m)`; `[C13b]` xuất `segment_static_features.parquet` phủ đủ `84,633` đoạn đường và hỗ trợ `find_peer_segments` cho cả `Cold-Start` |
| **`EC-7`** | `525,542 / 577,967` (`90.93%`) đỉnh `nodes.csv` là đỉnh mồ côi bậc 0; `48` đoạn đường có bậc ra bằng `0` (`Dangling Nodes`) | `[C2]`/`[C9]` Cắt tỉa đỉnh mồ côi (giữ `52,425` giao lộ thực); `[C10]` phân phối lại khối lượng xác suất nút cụt $\beta \sum_{d \in \mathcal{D}} r_d^{(k)} / N$ ở mỗi vòng lặp `PageRank` |
| **`EC-8`** | `producer.py` phát `1%` (`334`) bản tin trùng; khung giờ cao điểm lệch tải gấp `687.43x` so với trung vị | `[C7]` Khử trùng lặp bằng **Bit-Packed `32.0 KiB` Bloom Filter** (`uint8[32768]`, $k=4$); `[C9]` cộng gộp qua **5-Tuple Commutative Monoid** kết hợp **2-Stage Key Salting (`8 buckets`)** (`CV = 0.0057`, `peak_slot_salted_skew = 1.023`) |
| **`EC-9`** | `5%` sự kiện trễ (`1,170` trong `Watermark`, `501` trễ quá `Watermark` trên `219` khung giờ) | `[C6]` **Dual-Sink** lưu `100%` sự kiện thô tại `Sink 1`; ở **Phase C**, `[C12]` đối soát tập hợp hai chiều (`tier1_exceptAll_diff_count == 0`), tính lại `501` cửa sổ trễ và chữa lành `3,953` cửa sổ `lag1` kế tiếp (`98.50% -> 100.00%`) |
| **`EC-10`** | Khởi động lại container giữa chừng hoặc tranh chấp ghi đồng thời giữa **Phase B** và **Phase C** trên `MongoDB` | `[C15]` Lưu & hồi phục từ `WAL Checkpoints` (`0` duplicate side effects khi kill/restart); `[C16]` ghi lô `500` bản ghi qua 4 phân vùng vào `MongoDB 8.0.34` kèm khóa phiên bản `status_version` chống ghi đè lùi `BATCH_RECONCILED -> STREAM_PROVISIONAL` |

---

## 2. Kết Quả Thực Nghiệm & Bằng Chứng Kiểm Chứng (`Empirical Results & Runtime Evidence`)

### 2.1. Kiểm Chứng Thực Thi Trên `Apache Spark 3.5.7` (`outputs/spark_runtime_evidence.json`)
- **Pillar I1 (`2-Stage Salted 5-Tuple Monoid` trên Spark 3.5.7):** Cộng gộp `90,938` bản tin GPS thô thành `59,828` nhóm cục bộ (`Stage 1`, 8 salt buckets) và rút gọn về `33,441` cửa sổ (`Stage 2`) $\to$ kiểm chứng hai chiều `exceptAll()` so với luồng không salt đạt **`two_way_exceptAll_diff = 0`**.
- **Pillar I2 (`Catalyst Physical Plan & BroadcastHashJoin`):** Đo trực tiếp cây kế hoạch vật lý (`queryExecution().simpleString()`):
  - Baseline (`spark.sql.autoBroadcastJoinThreshold = -1`): sử dụng `SortMergeJoin` với **`10` bước `Exchange`**.
  - Optimized (`F.broadcast(segments)`, `F.broadcast(streets)`): chuyển hoàn toàn sang `BroadcastHashJoin` với chỉ **`2` bước `Exchange`**, kiểm chứng đối xứng DataFrame DSL vs Spark SQL đạt **`two_way_exceptAll_diff = 0`**.
  - Partitioned Parquet Pruning (`curated_gold/year_month=*`): xác nhận `PartitionFilters: [isnotnull(year_month), (year_month = 2020-08)]` và `ReadSchema` chỉ đọc đúng 4 cột được chiếu.
- **Pillar I3 & S7 (`Spark FPGrowth`, `Spark KMeans`, `Spark PipelineModel` & `Structured Streaming WAL`):**
  - `Spark FPGrowth`: khai phá `335` luật kết hợp trên giỏ hàng `LOS E/F`.
  - `Spark KMeans (k=6)`: hội tụ đủ `6` cụm hành vi.
  - `Spark MLlib PipelineModel` (`Imputer -> StringIndexer(handleInvalid='keep') -> OneHotEncoder -> VectorAssembler -> RandomForestClassifier`): lưu xuống `outputs/spark_pipeline_model`, nạp lại kiểm chứng `100%` khớp dự báo và xử lý an toàn nhãn `street_type` / `period` chưa từng xuất hiện.
  - `Spark Structured Streaming`: khởi tạo `Sink 1` (append Parquet) và `Sink 2` (watermarked stateful aggregation) với `stateStoreSave` (`memoryUsedBytes = 10,944 B`, `walCommit = 148 ms`) và lưu vết `lastProgress`.

### 2.2. Kiểm Chứng Thực Thi Trên `MongoDB 8.0.34` (`outputs/mongo_verify.json`)
- **Idempotent Bulk Upsert Toàn Bộ `33,441` Bản Ghi:** Chia `33,441` cửa sổ dự báo thành `4` phân vùng (`8,131 / 8,485 / 8,222 / 8,603` bản ghi), thực thi `69` lô `Bulk UpdateOne(upsert=True)` (`500` docs/lô) vào `bda501.traffic_scores` trên `MongoDB 8.0.34`. Phát lại (`replay`) phân vùng 0 xác nhận tổng số bản ghi giữ nguyên tuyệt đối ở `33,441` (`idempotent_replay_verified = true`).
- **Kiểm chứng Chỉ mục Phức hợp `IXSCAN` & Phân phối Độ trễ (`UC-2`):**
  - Chỉ mục sử dụng: `level_prob_segment_idx` (`{street_level: 1, severe_prob: -1, segment_id: 1}`).
  - Chuỗi bước thắng cuộc (`winning_stages`): `["LIMIT", "PROJECTION_SIMPLE", "FETCH", "SORT_MERGE", "IXSCAN", "IXSCAN"]`, **`in_memory_sort = false`**, `nReturned = 25`, `totalKeysExamined = 26`, `totalDocsExamined = 25`, `executionTimeMillis = 0 ms`.
  - Độ trễ đo thực tế (25 lần lặp): `cold = 2.111 ms`, `warm_min = 0.339 ms`, **`warm_median = 0.510 ms`**, **`warm_p95 = 0.767 ms`** (`< 10 ms` SLA đạt chuẩn).

### 2.3. Kết Quả Thực Nghiệm `Ablation Study` Dự Báo Đúng Chân Trời `t + 30m` (`70% TRAIN / 15% VAL / 15% TEST`)

Trên tập dữ liệu có cặp liên tiếp đúng `+30 phút` (`7,898` cửa sổ; `5,494` TRAIN sau khi purge `50` mẫu vượt mốc cắt `2020-12-12T08:00:00Z`, `1,167` VAL sau khi purge `5` mẫu vượt mốc cắt `2020-12-25T03:30:00Z`, và `1,182` TEST với `185` mẫu `LOS E/F`):

| Mô hình Dự báo (`Model`) | Loại Bài toán (`Task Type`) | Tập Đặc trưng (`Feature Set`) | 6-Class Accuracy | 6-Class Macro-F1 | Severe E/F Precision | Severe E/F Recall | Severe E/F F1 | Severe ROC-AUC | Severe PR-AUC |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| **Persistence Baseline ($t \to t+30\text{m}$)** | `MULTICLASS_6CLASS_LOS` | Local (`Lag-0`) | **0.9484** | **0.5654** | 0.9365 | **0.9568** | **0.9465** | 0.9867 | 0.9367 |
| **Decision Tree (Local Only)** | `MULTICLASS_6CLASS_LOS` | Local Features (`14D`) | 0.9289 | 0.3439 | 0.9351 | 0.9351 | 0.9351 | 0.9846 | 0.9363 |
| **Decision Tree (+Graph, Priors & K-Means)** | `MULTICLASS_6CLASS_LOS` | Local + Graph + Cluster (`23D`) | 0.9289 | 0.3439 | 0.9351 | 0.9351 | 0.9351 | 0.9831 | 0.9269 |
| **Random Forest (Local Only)** | `MULTICLASS_6CLASS_LOS` | Local Features (`14D`) | 0.9408 | 0.3496 | 0.9362 | 0.9514 | 0.9437 | 0.9884 | **0.9634** |
| **Random Forest (+Graph, Priors & K-Means)** | `MULTICLASS_6CLASS_LOS` | Local + Graph + Cluster (`23D`) | 0.9374 | 0.3435 | 0.9344 | 0.9243 | 0.9293 | **0.9901** | 0.9621 |
| **GBT Binary Alerting (Local Only)** | `BINARY_SEVERE_EF_ONLY` | Local Features (`14D`) | `N/A (Binary)` | `N/A` | **0.9448** | 0.9243 | 0.9344 | 0.9822 | 0.9431 |
| **GBT Binary Alerting (+Graph, Priors & K-Means)** | `BINARY_SEVERE_EF_ONLY` | Local + Graph + Cluster (`23D`) | `N/A (Binary)` | `N/A` | 0.7409 | 0.8811 | 0.8049 | 0.9787 | 0.9281 |

> **Ghi chú Giải trình Khoa học (`G04 / D10`):**
> 1. Trên các cặp quan sát liên tiếp cách nhau đúng `30 phút` ($\Delta t = 30\text{m}$, `7,898` cặp), quán tính trạng thái giao thông rất cao nên cả `Persistence Baseline` (`PR-AUC = 0.9367`) và `Random Forest (+Graph, Priors & K-Means, 23D)` — được chọn tự động qua cổng `VAL` (`val_pr_auc = 0.9168`, `val_roc_auc = 0.9854`, ngưỡng tối ưu `0.40`) — đều đạt độ chính xác vượt trội trên `TEST` (**`ROC-AUC = 0.9901, PR-AUC = 0.9621, Severe F1 = 0.9293`**), nâng `PR-AUC` thêm **`+2.54 điểm phần trăm`** so với `Persistence`.
> 2. Để đối chứng minh bạch với giao thức cũ trong báo cáo đề xuất (vốn dùng `shift(-1)` dự báo *quan sát kế tiếp bất kể khoảng cách thời gian* trên `23,414` cặp với `3,513` mẫu test, trung vị $\Delta t = 9.0\text{h}$), pipeline đồng thời xuất kết quả `legacy_next_observed_comparison`: trên bài toán cách quãng đó, `Random Forest (23D)` đạt `ROC-AUC = 0.8484`, `PR-AUC = 0.7628`, `Severe F1 = 0.6447`.
> 3. Hai mô hình `GBT Binary Alerting` chỉ huấn luyện phân loại nhị phân `LOS E/F` nên các cột `6-Class Accuracy / Macro-F1` được để `null / N/A` trung thực thay vì sao chép từ `Random Forest`.


---

## 3. Tài Liệu Tham Khảo (`References`)

1. Bahga, A., & Madisetti, V. (2019). *Big Data Analytics: A Hands-On Approach*.
2. Leskovec, J., Rajaraman, A., & Ullman, J. D. (2020). *Mining of Massive Datasets* (3rd Ed.). Cambridge University Press.
3. HCMC Traffic Flow Dataset (Kaggle): <https://www.kaggle.com/datasets/thanhnguyen2612/traffic-flow-data-in-ho-chi-minh-city-viet-nam>.
