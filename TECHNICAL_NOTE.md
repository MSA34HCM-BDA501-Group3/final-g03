# Technical Note & Architectural Specification (V3)

## HCMC Smart Traffic Analytics & Congestion Prediction Platform

**Đơn vị đào tạo:** FSB — FPT University (Master of Software Engineering — `MSE`)  
**Học phần:** Big Data Analytics (`BDA501`)  
**Nền tảng Học thuật & Kỹ thuật:** *Big Data Analytics: A Hands-On Approach* (Bahga & Madisetti, 2019), *Mining of Massive Datasets, 3rd Ed.* (Leskovec, Rajaraman & Ullman, 2020), kết hợp 4 Trụ cột Thực hành Lab `BDA501` (`Pillar I1` Hadoop MapReduce & Commutative Monoid, `Pillar I2` Spark SQL/Catalyst/Skew, `Pillar I3` Spark MLlib Pipelines & Zero-Conflict MLOps, `Pillar S7` Kafka 4.1 KRaft + Structured Streaming + MongoDB 8.0)  
**Nhóm thực hiện (`Project Team`):**

| STT | Họ và Tên | Mã Học Viên | Vai trò Chịu trách nhiệm Chính (`DRI`) |
|---:|---|---|---|
| 1 | Nguyễn Tạ Minh Quyền | `25ms23280` | `Pillars I1 & I2` (`[C2]`, `[C5]`, `[C9]`, `[C14]`, `[C15]`) |
| 2 | Nguyễn Thành Đạt | `25ms23306` | `MMDS Ch. 5–7, 12 & Pillar I3` (`[C10]`, `[C11]`, `[C13a]`, `[C13b]`) |
| 3 | Phạm Công Minh | `25ms23310` | `Pillar S7, MMDS Ch. 4 & Serving` (`[C1]`, `[C4]`, `[C6]`–`[C8]`, `[C12]`, `[C16]`, `[C17]`) |

**Nguồn Dữ liệu Chính (`Primary Dataset`):** Bộ dữ liệu giao thông đô thị TP.HCM 5 bảng liên kết (`nodes.csv`, `streets.csv`, `segments.csv`, `segment_status.csv`, `train.csv` — Kaggle `thanhnguyen2612/traffic-flow-data-in-ho-chi-minh-city-viet-nam`).  
**Hiện vật Bàn giao (`Deliverables`):** Hồ dữ liệu `Medallion Parquet Lakehouse` (`data/dataset.csv`, `data/lakehouse/*.parquet`), các báo cáo kiểm chứng định lượng tại `outputs/`, bộ kiểm thử tự động (`tests/test_platform_v3.py`), hệ thống 25 tài liệu quản trị sản phẩm 11 giai đoạn (`product/00-proposal` .. `product/09-growth`), và giao diện điều hành 5-Tab `Streamlit BI Dashboard` (`app.py`) tại `http://localhost:8501`.

---

## 1. Định Nghĩa Toán Học (`Sub-Problems P1–P4`), Kịch Bản Vận Hành (`UC-1–UC-6`) & Trường Hợp Biên (`EC-1–EC-10`)

### 1.1. Bốn Bài Toán Con Liên Kết (`Coupled Sub-Problems P1–P4`)
- **`Sub-Problem P1` — Descriptive Windowing & LOS Derivation (`Pillars I1 & I2`):** Từ luồng bản tin vận tốc GPS bất đồng bộ $(s_i, v_i, t_{\text{event}})$ và đồ thị đường bộ tĩnh, ánh xạ vào các cửa sổ trượt không giao nhau 30 phút $w = (s, d, p)$ theo giờ địa phương Việt Nam (`ICT = UTC+7`), cách ly bản ghi lỗi vào `Quarantine Store` (`EC-1`–`EC-4`), cộng gộp qua **5-Tuple Commutative Monoid** $\mathbf{m} = (\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt})$ với phần tử đơn vị $\mathbf{e} = (0, 0.0, +\infty, 0.0, 0)$ (kiểm chứng `0` sai lệch so với [`baseline.py`](./baseline.py)), điền khuyết $\hat{v}_{\max,s}$ qua phễu phân cấp 3 tầng và suy dẫn nhãn `Level of Service` ($\text{LOS} \in \{A..F\}$).
- **`Sub-Problem P2` — Diagnostic Graph & Pattern Mining (`MMDS Ch. 5, 6, 7`):** Cắt tỉa `525,542 / 577,967` (`90.93%`) đỉnh hình học OSM mồ côi (`EC-7`), dựng **Directed Line Graph** $L(G) = (V_L, E_L)$ (`84,633` đỉnh đoạn đường, `151,929` cạnh chuyển tiếp có hướng, `48` `dangling dead-ends`), tính **PageRank** ($\beta = 0.85$), khai phá `30` luật đồng kẹt xe có hướng $\{\text{Street}_A\} \Rightarrow \{\text{Street}_B\}$ trên giỏ hàng `LOS E/F` đã chuẩn hóa `Unicode NFC` (`EC-3`), và phân hoạch các đoạn đường thành $k=6$ chế độ hành vi trên không gian **Train-Only 7D Behavioral Space** để phủ kín cả `10,027` đoạn có GPS lẫn `74,606` (`88.15%`) đoạn vùng mù `Cold-Start` (`EC-6`).
- **`Sub-Problem P3` — Leak-Free Next-Horizon Forecasting (`Pillar I3 & MMDS Ch. 12`):** Học hàm dự báo nhân quả $f_{\boldsymbol{\theta}}: \mathcal{X}_{s,\le t} \mapsto (\widehat{\text{LOS}}_{s,t+1} \in \{A..F\}, \hat{p}_{\text{severe},s,t+1} \in [0, 1])$ trên vector đặc trưng `23D` (`14` đặc trưng cục bộ/trễ/$\Delta t$ + `9` đặc trưng `Line-Graph PageRank`, `7D K-Means` và **Causal Expanding Empirical-Bayes Priors** $\tau \le t$) dưới giao thức chia tách thời gian nghiêm ngặt `70% TRAIN / 15% VAL / 15% TEST` (`EC-5`). Huấn luyện độc quyền ở **Phase A** (`[C11]`) và đóng băng bất biến vào `[C13a]`.
- **`Sub-Problem P4` — $O(1)$ Stream Sketching & Two-Tier Reconciliation (`Pillar S7 & MMDS Ch. 4`):** Xử lý luồng trực tuyến (**Phase B**, $t > T_{\text{train}}$) trong ngưỡng `30m Watermark` bằng bộ nhớ hằng số $O(1)$ (`32 KB Bloom Filter` khử trùng lặp `1% = 334` bản tin, `1 KB HyperLogLog++` đếm số đoạn đường hoạt động, và `Exponentially Decaying Hotspot Score` $C_s(t) = (1-\alpha)C_s(t-1) + \mathbb{I}(\text{LOS} \in \{E,F\})$ với $\alpha = 0.15$), phát hành dự báo `STREAM_PROVISIONAL` qua `.transform()` chỉ đọc, và tự động chữa lành `1.5%` (`501`) bản tin đến trễ quá `Watermark` trên `319` cửa sổ qua **Two-Tier Reconciliation** (**Phase C**, `[C12]`) lên **MongoDB 8.0** (`[C16]`).

### 1.2. Sáu Kịch Bản Vận Hành (`UC-1–UC-6`) & Mười Trường Hợp Biên Thực Nghiệm (`EC-1–EC-10`)
- **`UC-1` Cảnh báo Điểm nóng Ùn tắc Kéo dài (`TOC Dispatcher`):** Cập nhật đệ quy $C_s(t) = 0.85 \cdot C_s(t-1) + \mathbb{I}(\text{LOS} \in \{E,F\})$ tại `[C7]`, xuất `Top-25` điểm nóng đang kẹt nặng (`outputs/hotspot_alerts.csv`).
- **`UC-2` Dự báo Ùn tắc Khung giờ Tới `< 10ms` (`Commuter App & Signal Ops`):** Truy vấn các trục đường chính (`street_level IN [1, 2]`, `severe_prob >= 0.70`) trên `MongoDB 8.0` (`[C16]`) qua `Compound Index` `{street_level: 1, severe_prob: -1, segment_id: 1}` đạt `IXSCAN` với `0` bước `SORT` trong bộ nhớ.
- **`UC-3` Kiểm toán Điểm mù Nút cổ chai (`Urban Planner — Sở GTVT`):** Xếp hạng `100%` mạng lưới (`84,633` đoạn đường) bằng `Directed Line-Graph PageRank` (`outputs/road_pagerank_top.csv`) và cấp `Cold-Start Prior` tại `[C13b]`.
- **`UC-4` Ngăn chặn Lan truyền Liên tuyến (`District Signal Engineer`):** Khai phá `30` luật lan truyền có hướng (`Lý Chính Thắng => Nguyễn Thông`, `Lift = 28.5833`) sau khi chuẩn hóa `Unicode NFC` (`outputs/co_congestion_rules.csv`).
- **`UC-5` Phân nhóm Chế độ Hành vi & Tìm Đoạn Tương đồng (`Transport Data Scientist`):** Phân cụm $k=6$ trên `7D Behavioral Space` (`Silhouette = 0.3158`, `Davies–Bouldin = 1.1462`) và tra cứu `Top-K` đoạn đường tương đồng.
- **`UC-6` Kiểm toán Quarantine & Đối soát Hai Tầng (`Data / MLOps Engineer`):** Chứng minh `Tier 1` khớp `100.0%` trong `Watermark` (`exceptAll == 0`) và `Tier 2` bù trừ `501` bản tin trễ trên `319` cửa sổ, nâng độ đầy đủ từ `98.50%` lên `100.00%`.

| Mã Edge Case | Biểu hiện Thực nghiệm trong Dữ liệu TP.HCM | Cơ chế Phòng vệ Kỹ thuật & Thành phần Đảm nhiệm (`[C1]`–`[C17]`) |
|---|---|---|
| **`EC-1`** | Khuyết `85.21%` (`28,495`) `max_velocity` ở `train.csv` và `88.34%` (`74,762`) ở `segments.csv` | `[C5]` **3-Tier Hierarchical Velocity Imputation** (`TIER_1_SEGMENT_OBSERVED` $\to$ `TIER_2_LEVEL_ROADTYPE_MEDIAN` $\to$ `TIER_3_HIERARCHICAL_LEVEL_PRIOR`), độ phủ `100%` |
| **`EC-2`** | `154` đoạn đường có `length <= 0m`; `427` đoạn mang nhãn OSM phi giao thông (`house`, `fuel`...); `448` bản tin `v = 0` | `[C5]` **Ordered Quality Funnel** cách ly bản ghi lỗi vào `[C15]` `quarantine_events.parquet`; điền khuyết `length <= 0` bằng trung vị `27.0m` và chặn dưới vận tốc `1.0 km/h` |
| **`EC-3`** | `695` tên đường thô lẫn lộn `Unicode NFD/NFC`, ký tự `Ð/Đ`, tiền tố `Đường ` và nhãn hành chính `Phường/Quận` | `[C5]` **Street Entity Hygiene** chuẩn hóa về `649` thực thể `Unicode NFC` sạch và lọc bỏ `631` cửa sổ mang nhãn `Phường/Quận` (`0` luật tự suy diễn trong `[C10]`) |
| **`EC-4`** | Cột `period` cắt theo giờ `UTC` (lệch `-7h` so với `ICT`); `period_20_30..21_00` (`03:30–04:30 ICT`) trống hoàn toàn | `[C5]` & `[C11]` Giữ khóa ghép `UTC` để khớp `100%` dữ liệu gốc, cộng `+7h` sang `vn_hour` (`ICT`) và mã hóa chu kỳ điều hòa $\sin/\cos(2\pi t / 48)$ |
| **`EC-5`** | Độ thưa cực đại: `79.26%` đoạn đường có $\le 2$ quan sát (`3,096` đoạn chỉ có đúng `1` quan sát); ma trận 48 khung giờ khuyết `95.07%` | `[C10]` Thay ma trận 48 chiều bằng **7D Behavioral Space**; `[C11]` thay trung bình tĩnh gây rò rỉ nhãn bằng **Causal Expanding Empirical-Bayes Priors** ($\tau \le t$, $m_{\text{seg}}=5, m_{\text{street}}=10$) |
| **`EC-6`** | Chuỗi đứt quãng (`66.27%` cặp liên tiếp cách nhau $> 30\text{m}$, trung vị $\Delta t = 9.0\text{h}$) và `88.15%` (`74,606`) đoạn đường `Cold-Start` | `[C11]` Đưa `gap_minutes` và `is_contiguous_30m` vào vector `23D`; `[C13b]` xuất `segment_static_features.parquet` phủ đủ `84,633` đoạn đường qua `PageRank` & `7D K-Means` |
| **`EC-7`** | `525,542 / 577,967` (`90.93%`) đỉnh `nodes.csv` là đỉnh mồ côi bậc 0; `48` đoạn đường có bậc ra bằng `0` (`Dangling Nodes`) | `[C2]`/`[C9]` Cắt tỉa đỉnh mồ côi (giữ `52,425` giao lộ thực); `[C10]` phân phối lại khối lượng xác suất nút cụt $\beta \sum_{d \in \mathcal{D}} r_d^{(k)} / N$ ở mỗi vòng lặp `PageRank` |
| **`EC-8`** | `producer.py` phát `1%` (`334`) bản tin trùng; khung giờ cao điểm lệch tải gấp `7.78x–12x` so với trung vị | `[C7]` Khử trùng lặp bằng `32 KB Bloom Filter` ($k=4$); `[C9]` cộng gộp qua **5-Tuple Commutative Monoid** kết hợp **2-Stage Key Salting (`8 buckets`)** trên `90,938` bản tin (`CV = 0.0057`) |
| **`EC-9`** | `5%` sự kiện trễ (`1,170` trong `Watermark`, `501` trễ quá `Watermark` trên `319` cửa sổ) | `[C6]` **Dual-Sink** lưu `100%` sự kiện thô tại `Sink 1` (`bronze_raw_events/`); ở **Phase C**, `[C12]` tính lại `319` cửa sổ bị ảnh hưởng (`98.50% -> 100.00%`) |
| **`EC-10`** | Khởi động lại container giữa chừng hoặc tranh chấp ghi đồng thời giữa **Phase B** và **Phase C** trên `MongoDB` | `[C15]` Lưu `WAL Checkpoints` (`checkpoints/{raw,agg}`); `[C16]` phân vùng `repartition(4, 'segment_id')` và ghi lô `500` bản ghi bằng `UpdateOne(upsert=True)` trên `Unique Index` `{event_id: 1}` |

---

## 2. Kiến Trúc Giải Pháp Tổng Thể V3 (`Zones A–F`, Components `[C1]`–`[C17]`, `10 Dataflows`)

1. **`Zone A` — External Sources & Downstream Clients (`[C1]`–`[C3]`):** `[C1]` GPS Probe Fleet & Event-Time Replay Producer (`producer.py`, `segment_status.csv`), `[C2]` OSM / GIS Master Topology (`nodes.csv`, `streets.csv`, `segments.csv`), `[C3]` Traffic Ops & Commuter Clients (truy vấn qua cổng `:8501`).
2. **`Zone B` — Ingestion & Ordered Quality Funnel (`[C4]`–`[C5]`):** `[C4]` Apache Kafka 4.1.0 Broker (`KRaft Mode`, 3 partitions băm theo `segment_id`), `[C5]` Ordered Quality Funnel & Entity Hygiene (`try_cast`, `Unicode NFC`, dịch giờ `UTC+7` và `3-Tier Velocity Imputation`).
3. **`Zone C` — Speed Layer (`Phase B: t > T_train`, `[C6]`–`[C8]`):** `[C6]` Watermark (`30m`) & Dual-Sink Stream Router (`Sink 1` append `bronze_raw_events/`, `Sink 2` update `foreachBatch`), `[C7]` Sub-Linear $O(1)$ Stream Sketch Engine (`32 KB Bloom Filter`, `1 KB HyperLogLog++`, `Exponentially Decaying Hotspot Detector` $\alpha = 0.15$), `[C8]` Online Broadcast Feature Join & Provisional ML Scorer (chỉ gọi `.transform()` chỉ đọc trên `[C13a]`/`[C13b]` và gắn nhãn `STREAM_PROVISIONAL`).
4. **`Zone D` — Batch Layer (`Phase A Fit & Phase C Recon`, `[C9]`–`[C12]`):** `[C9]` 5-Tuple Commutative Monoid Combiner, 2-Stage Key Salting (`8 buckets`) & Catalyst `BroadcastHashJoin`, `[C10]` MMDS Multi-Pass Mining Suite (`Directed Line-Graph PageRank`, `SON/FPGrowth`, `Train-Only 7D Behavioral K-Means`), `[C11]` Leak-Free Causal Feature & ML Pipeline Trainer (**thành phần duy nhất gọi `.fit()` ở Phase A** và đối chiếu [`baseline.py`](./baseline.py)), `[C12]` Two-Tier Batch–Stream Reconciliation & Backfill Engine (**Phase C** — kiểm chứng `Tier 1 exceptAll == 0` và bù trừ `501` bản tin trễ trên `319` cửa sổ ở `Tier 2`).
5. **`Zone E` — Zero-Conflict Frozen Registry (`[C13a]`–`[C13b]`):** `[C13a]` Frozen Model Registry (`outputs/model.joblib` & `outputs/model_registry_manifest.json` kèm kiểm chứng bất biến `SHA-256`), `[C13b]` Static Feature Store (`data/lakehouse/segment_static_features.parquet` bao phủ `100%` = `84,633` đoạn đường).
6. **`Zone F` — Polyglot Storage, Fault-Tolerance & BI Serving Layer (`[C14]`–`[C17]`):** `[C14]` Medallion Parquet Lakehouse (`bronze_raw_events/`, `silver_cleaned_windows/`, `curated_gold/year_month=*`), `[C15]` Quarantine, WAL Checkpoints & Audit Store (`quarantine_events.parquet`, `checkpoints/{raw,agg}`, `query_progress.json`, `reconciliation_audit.csv`, `mongo_verify.json`), `[C16]` MongoDB 8.0 Operational Serving Store (`bda501.traffic_scores` với `Compound Index IXSCAN`), `[C17]` Streamlit 5-Tab BI Dashboard (`app.py` trên cổng `:8501`).

---

## 3. Kết Quả Thực Nghiệm & Bằng Chứng Kiểm Chứng (`Empirical Results & Verification`)

### 3.1. Làm Sạch Dữ Liệu, Đồ Thị `Line-Graph PageRank` & Khai Phá Luật Đồng Kẹt Xe
- **Cắt tỉa Đỉnh Mồ côi (`EC-7`):** Loại bỏ `525,542 / 577,967` (`90.93%`) đỉnh OSM bậc 0; giữ lại `52,425` giao lộ thực tế tạo nên `84,633` đoạn đường có hướng và `151,929` cạnh chuyển tiếp trên `Directed Line Graph` (`48` `dangling nodes`).
- **Điểm nghẽn Cấu trúc Hàng đầu (`UC-3`, `Directed Line-Graph PageRank`, $\beta = 0.85$):** `Segment 61781` (Phú Định, $\text{PR} = 3.7515$), `Segment 50017` (Lê Lai, $\text{PR} = 3.4650$), `Segment 62682` (Châu Văn Liêm, $\text{PR} = 2.9246$), `Segment 21969` (Nguyễn Kiệm, $\text{PR} = 2.7313$), `Segment 34723` (Quốc lộ 1, $\text{PR} = 2.6430$).
- **Luật Kết hợp Đồng Kẹt xe (`UC-4`, $\text{LOS} \in \{E, F\}$):**
  - `Lý Chính Thắng => Nguyễn Thông` ($\text{Support} = 0.0233, \text{Confidence} = 1.0000, \text{Lift} = 28.5833$)
  - `Cao Thắng => Cống Quỳnh` ($\text{Support} = 0.0292, \text{Confidence} = 0.8333, \text{Lift} = 15.8796$)
  - `Cộng Hòa => Trường Chinh` ($\text{Support} = 0.0466, \text{Confidence} = 0.8889, \text{Lift} = 12.1956$)
- **Phân cụm `Train-Only 7D K-Means` (`UC-5`, $k = 6$):** Huấn luyện trên `7,549` đoạn đường tập `TRAIN` (**`Silhouette = 0.3158`**, **`Davies–Bouldin = 1.1462`**, **`Calinski–Harabasz = 2,237.50`**) và mở rộng suy diễn ra toàn bộ `84,633` đoạn đường (`10,027` có GPS + `74,606` vùng mù `Cold-Start`).

### 3.2. Tóm Tắt Luồng $O(1)$, Kiểm Chứng `Monoid` & `MongoDB 8.0 IXSCAN`
- **Kiểm chứng `5-Tuple Commutative Monoid` (`Pillar I1`):** Đối chiếu `33,441` cửa sổ với [`baseline.py`](./baseline.py) và `2-Stage Key Salting` (`8 buckets` trên `90,938` bản tin, `stage1 = 59,895` nhóm cục bộ, `CV = 0.0057`) $\to$ **`0` sai lệch về số lượng bản tin**, **`0.0` sai số tuyệt đối cực đại về vận tốc**.
- **Tóm tắt Luồng Bộ nhớ Hằng số (`UC-1`, `[C7]`):** Xử lý `33,775` sự kiện (`334` bản ghi trùng bị chặn `100%` bởi `32 KB Bloom Filter` với `empirical FP rate = 0.006908`; `1 KB HyperLogLog++` ước lượng `9,627.9` so với `10,027` đoạn đường thực tế, sai số tương đối `3.98%`).
- **Đối soát Hai Tầng (`UC-6`, `EC-9`, `[C12]`):** `Tier 1 In-Watermark Match` đạt **`100.0%`** (`0` sai lệch); `Tier 2 Backfill` tính lại đúng **`319` cửa sổ `(date, period)`** chịu ảnh hưởng bởi `501` bản tin đến trễ quá `Watermark` (khớp `100%` tức `501/501` giữa `bronze_raw_events/events.parquet` và `serving_traffic_scores.parquet`), khôi phục độ đầy đủ từ **`98.50%` lên `100.00%`**.
- **Kiểm chứng `MongoDB 8.0 IXSCAN` (`UC-2`, `EC-10`, `[C16]`):** Kiểm chứng trực tiếp trên container `mongo:8.0` (`outputs/mongo_verify.json`): `4` phân vùng ghi, `69` lô `Bulk UpdateOne` (`500` docs/lô), chỉ mục phức hợp `level_prob_segment_idx` đạt `winningStages = ["LIMIT", "FETCH", "IXSCAN"]`, `in_memory_sort = false`, `executionTimeMillis <= 1 ms`.

### 3.3. Kết Quả Thực Nghiệm `Ablation Study` Dự Báo Nhân Quả (`70% TRAIN / 15% VAL / 15% TEST`)

Đánh giá độc lập trên `3,513` cửa sổ kiểm thử theo thời gian (`1,177` mẫu ùn tắc nghiêm trọng `LOS E/F`):

| Mô hình Dự báo (`Model`) | Tập Đặc trưng (`Feature Set`) | 6-Class Accuracy | 6-Class Macro-F1 | Severe E/F Precision | Severe E/F Recall | Severe E/F F1 | Severe ROC-AUC | Severe PR-AUC |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| **Persistence Baseline ($t \to t+30\text{m}$)** | Local (`Lag-0`) | 0.5255 | **0.3458** | 0.6740 | **0.6483** | 0.6609 | 0.8181 | 0.6369 |
| **Decision Tree (Local Only)** | Local Features (`14D`) | 0.5272 | 0.3161 | 0.7392 | 0.5081 | 0.6022 | 0.7807 | 0.6834 |
| **Decision Tree (+Graph, Priors & K-Means)** | Local + Graph + Cluster (`23D`) | 0.5127 | 0.2513 | 0.6182 | 0.5531 | 0.5839 | **0.8090** | **0.6974** |
| **Random Forest (Local Only)** | Local Features (`14D`) | **0.5406** | 0.2877 | 0.7061 | 0.6143 | 0.6570 | **0.8509** | 0.7528 |
| **Random Forest (+Graph, Priors & K-Means)** | Local + Graph + Cluster (`23D`) | 0.5400 | 0.2664 | **0.7135** | **0.6262** | **0.6670** | 0.8458 | **0.7568** |
| **GBT Binary Alerting (Local Only)** | Local Features (`14D`) | 0.5406 | 0.2877 | 0.7308 | 0.4843 | 0.5825 | 0.8332 | 0.7140 |
| **GBT Binary Alerting (+Graph, Priors & K-Means)** | Local + Graph + Cluster (`23D`) | 0.5400 | 0.2664 | **0.7421** | 0.4596 | 0.5677 | 0.8311 | **0.7143** |

---

## 4. Tài Liệu Tham Khảo (`References`)

1. Bahga, A., & Madisetti, V. (2019). *Big Data Analytics: A Hands-On Approach*.
2. Leskovec, J., Rajaraman, A., & Ullman, J. D. (2020). *Mining of Massive Datasets* (3rd Ed.). Cambridge University Press.
3. HCMC Traffic Flow Dataset (Kaggle): <https://www.kaggle.com/datasets/thanhnguyen2612/traffic-flow-data-in-ho-chi-minh-city-viet-nam>.
