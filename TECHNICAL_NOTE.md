# Đặc Tả Kỹ Thuật & Cơ Sở Toán Học Hệ Thống (Leak-Free Ground Truth)

Tài liệu này cung cấp đặc tả toán học, cấu trúc dữ liệu, bằng chứng thực thi phân tán và quy trình tái lập cho **Hệ thống Giám sát Hiện trạng & Dự báo Ùn tắc Giao thông Đô thị TP.HCM** theo cấu trúc 7 chương đồng bộ 100% với `outputs/metrics.json` và Báo cáo Tổng kết Đồ án (`product/00-proposal/project-proposal.md`).

---

## Chương 1. Câu Chuyện Dữ Liệu Thực Tế, Dây Chuyền 4 Bước (`P1–P4`), Tình Huống Sử Dụng (`UC-1–UC-6`) & Ranh Giới Hệ Thống

### 1.1. Từ Bản Tin Vận Tốc GPS Ngoài Đường Đến Dây Chuyền Vận Hành 4 Bước
- **Một dòng dữ liệu gốc ngoài đường:** Tại `07:08:28` sáng giờ Việt Nam ngày `03/08/2020`, xe đi qua đoạn đường Lý Thường Kiệt số `47407` (Quận 10) gửi về vận tốc `4 km/h` (hoặc lúc `11:30` trưa tại Công Trường Quốc Tế số `21064`, xe chạy `12 km/h`).
- **Thang đo mức phục vụ giao thông (`LOS A–F`) — Phân biệt `BkTraffic Urban 5 km/h` và `derive_los` tỷ lệ:**
  - Hàm chính thức **`derive_urban_los(mean_velocity, raw_max_velocity)`** áp dụng ngưỡng chuẩn đô thị TP.HCM `BkTraffic` từng bước `5 km/h` (`≥ 35/30/25/20/15 km/h`, dịch `+5 km/h` khi `raw_max >= 60` và `+10 km/h` khi `raw_max >= 70`), đạt **`89.19%`** tỷ lệ khớp chính xác với nhãn `LOS` tham chiếu của `train.csv` trên `33,394` cửa sổ cơ giới.
  - Hàm **`derive_los(velocity_ratio)`** (`0.80 / 0.65 / 0.50 / 0.35 / 0.20` trên tỷ lệ $r = \bar{v} / \hat{v}_{\max}$) được tách riêng làm tiện ích kiểm định chuẩn hóa theo tỷ lệ tốc độ (`spark_etl.py` / unit checks), không trộn lẫn với phân bố `BkTraffic` (`LOS A–B`: thông thoáng; `C–D`: đông đúc; `E–F`: ùn ứ nặng đến kẹt cứng).
- **Định vị quy mô mẫu chuẩn (`Proof-of-Scale`):** Năm bảng công khai của Kaggle (`~38.50 MB`, bảng `segment_status.csv` nặng `3.58 MB` với `90,938` bản tin GPS) đóng vai trò **mẫu chuẩn kiểm chứng quy mô (`Proof-of-Scale`)** để đối chứng sai số toán học bằng `0.0` trước khi mở rộng ra luồng định vị toàn TP.HCM (`> 1` triệu xe, `8–17` tỷ bản tin/ngày $\approx$ `1.5–3.0 TB/ngày`).
- **Sự liên kết nhân quả của 4 bước vận hành (`P1 → P4`):**
  1. **Bước 1 (`P1` — Chuẩn hóa hiện trạng):** Lọc `427` đoạn phi đường bộ (`84,633 → 84,206` đoạn cơ giới sạch), lọc `90,938 → 90,787` bản tin GPS (`151` phi đường bộ vào quarantine), ước lượng `max_velocity` không rò rỉ chỉ học trên `TRAIN` hữu cơ (`4,946 / 7,650 / 19,601 / 1,197` cửa sổ; `9,869 / 472 / 54,259 / 19,606` đoạn), và gom khung `30 phút UTC` bằng **Pure 5-Tuple Monoid** thành **`33,394` cửa sổ** (khớp `100.0%` `baseline.py`, sai số `0.0 km/h`).
  2. **Bước 2 (`P2` — Lấp vùng mù `88.13%` đường thiếu GPS):** Lọc bỏ `525,786` điểm uốn cong trung gian (`90.97%`), giữ `52,181` nút giao hoạt động; dựng đồ thị nối đoạn đường (đã loại cung U-turn trên đoạn hai chiều) gồm `92,453` cạnh và `314` `dangling nodes`; xếp hạng **`PageRank`** (`#1`: `21969 — Nguyễn Kiệm`, `level=2, PR=2.7799`); khai phá luật kết hợp chỉ trên `TRAIN` hữu cơ (`12` luật đồng kẹt + `14` luật trễ `persistence_controlled=True`); và phân `6` nhóm **Unified `K-Means`** trên `3,609` đoạn `TRAIN` hữu cơ (`Silhouette = 0.2241, DBI = 1.3284, CH = 806.49`) phủ kín **`74,211` đoạn thiếu GPS (`88.13%`)**.
  3. **Bước 3 (`P3` — Lọc burst & Dự báo đúng `+30 phút`):** Phát hiện **`7,192` cửa sổ burst tổng hợp** (`2020-08-02 23:30` & `2020-08-03 00:00`), loại **`3,596` cặp burst** khỏi tập huấn luyện, giữ **`1,885` `TRAIN` hữu cơ / `1,167` `VAL` / `1,182` `TEST`** (`7,885` cặp đúng `+30 phút`); huấn luyện sạch bằng `class_weight` lũy thừa `0.65` (KHÔNG dùng `sample_weight` nhân tạo hay fallback bypass), chọn `Random Forest 23D` trên `VAL` theo tiêu chí `(val_accuracy_6class + val_pr_auc + val_roc_auc) / 3` (**`0.8882`** vs `0.8812` của `DT 23D`).
  4. **Bước 4 (`P4` — Cảnh báo trực tuyến & Tự động bù tin trễ):** Chia hai nhánh trên `Apache Kafka 4.1.0 KRaft`, bộ đếm gọn **`32.0 KiB RotatingBloomFilter` (`0.0389%` FPR) + `1.0 KiB 64-bit HLL++` (`3.75%` sai số)** + `EWMA` ($\alpha=0.15$), `WAL = 73.7 KB` (`73,714 B`), đối soát 2 tầng (`tier1_exceptAll_diff = 0`, hồi phục `500` cửa sổ đoạn đường trễ trên `226` khung giờ, chữa lành `609` cửa sổ hạ lưu, chấm lại `384`, đổi xác suất **`213`**), phục vụ **`MongoDB 8.0.34`** (`warm IXSCAN median = 0.360 ms / p95 = 0.603 ms`, `31.36x` vs `COLLSCAN`) và ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)**.

### 1.2. Sáu Tình Huống Sử Dụng Thực Tế (`UC-1–UC-6`)

| Mã UC | Người Sử dụng | Thông tin Nhận trên Màn hình | Quyết định Xử lý & Bằng chứng Thực nghiệm |
|---|---|---|---|
| **`UC-1`** | Trung tâm Điều hành (`TOC`) | 25 đoạn đường đang kẹt cứng dai dẳng nhất nhiều khung liên tiếp (`EWMA α = 0.15`) | Phát cảnh báo bảng điện tử & cử CSGT giải tỏa (`#1`: Đoạn `47407` Lý Thường Kiệt `= 6.6652`, `#2`: `21263` Trương Định `= 6.6635`) |
| **`UC-2`** | Kỹ sư Tín hiệu Đèn | Trục đường cấp `1–2` có xác suất kẹt cứng `≥ 70%` sau `30 phút` | Kéo dài đèn xanh hướng chính trước khi ùn tắc tràn nút (`1,234` cửa sổ, `IXSCAN warm_median = 0.360 ms`, `p95 = 0.603 ms`) |
| **`UC-3`** | Chuyên viên `Sở GTVT` | Bản đồ xếp hạng nút cổ chai toàn đô thị (`84,206` đoạn cơ giới sạch) | Ưu tiên lắp camera tại vùng thiếu GPS (`74,211` đoạn `88.13%`; `#1 PageRank`: Đoạn `21969` Nguyễn Kiệm `PR = 2.7799`) |
| **`UC-4`** | Ứng dụng Dẫn đường | Cảnh báo cặp đường thường kẹt đồng thời / lan sau `30 phút` (khai phá trên `TRAIN` hữu cơ) | Gợi ý lộ trình vòng từ xa (`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.23`; `Lê Quang Định → Trần Quang Khải`, `Lift = 24.0`, `persistence_controlled=True`) |
| **`UC-5`** | Đội Tuần tra Giao thông | Bản đồ chia `6` nhóm hành vi đường phố (`7D K-Means`) phủ `100%` mạng lưới | Bố trí chốt trực theo nhóm nghẽn mãn tính `Cluster 1` (`3,530` đoạn trong Static Store, `sev_prior = 51.28%`, `Silhouette = 0.2241`) |
| **`UC-6`** | Kỹ sư Dữ liệu | Báo cáo đối soát cuối ngày & tiến độ khôi phục bản tin đến muộn | Bảo đảm kho lịch sử chuẩn `100%` (`tier1_exceptAll_diff = 0`; bù `500` cửa sổ trễ, hàn gắn `609` cửa sổ hạ lưu, chấm lại `384`, đổi xác suất `213`) |

---

## Chương 2. Kiểm Kê Dữ Liệu (`5 Bảng`), `10 Tình Huống Thực Tế` (`EC-1–EC-10`) & Phễu Làm Sạch (`[C5]`)

### 2.1. Thống Kê 5 Bảng Dữ Liệu Thô & Bảo Toàn Quân Số Bản Ghi
- **5 bảng nguồn (`raw_data/`):** `nodes.csv` (`577,967` dòng, `19.44 MB`), `streets.csv` (`5,553` dòng, `0.20 MB`), `segments.csv` (`84,633` dòng, `10.29 MB`), `segment_status.csv` (`90,938` dòng, `3.58 MB`), và `train.csv` (`33,394` dòng cơ giới sạch + `47` dòng phi đường bộ bị loại bởi `R2`, `4.99 MB` — bảng đối chứng tham chiếu).
- **Bảo toàn quân số (`status_accounting_balanced = true`):**
  1. Trên `segments.csv`: phát hiện `154` đoạn chiều dài `≤ 0m` (`R1`, điền trung vị `27.0m`) và `427` đoạn phi đường bộ (`R2`, trong đó `426` nhận `R2` làm lỗi đầu tiên vì `1` đoạn trùng `R1`), tổng **`580` bản ghi GIS** lưu vào `data/lakehouse/quarantine_events.parquet`; mạng đường cơ giới sạch còn **`84,206` đoạn** và **`52,181` giao lộ hoạt động** (tỉa bỏ **`525,786` điểm uốn cong trung gian = `90.97%`**).
  2. Trên `segment_status.csv`: `90,938` bản tin gốc = **`90,787` bản tin cơ giới hợp lệ** + **`151` bản tin rơi vào đoạn phi đường bộ bị cách ly (`R6`)** + `0` bản trùng trong batch. Tổng hợp thành **`33,394` cửa sổ 30 phút UTC** (khớp `100.0%` phần đường cơ giới sau khi trừ đúng `47` cửa sổ phi đường bộ).
  3. Chuẩn hóa tên đường `Unicode NFC` (`R4`) trên `2,247` cửa sổ gộp `695` tên thô về **`641` tên chuẩn** (`54` biến thể gộp; `640` tên khác rỗng trong `dataset.csv`), gắn cờ `631` cửa sổ tên `Phường/Quận` (`R3`), và xử lý lùi/tiến ngày qua nửa đêm `UTC → ICT` cho **`4,305` cửa sổ (`12.89%`)** (`EC-4`).
  4. **Phát hiện Burst tổng hợp (`flag_synthetic_burst_slots`):** Đúng 2 khung giờ `2020-08-02 period_23_30` và `2020-08-03 period_0_00` có mật độ bùng nổ bất thường (`3,609` đoạn thô / `3,596` đoạn cơ giới sạch mỗi slot = **`7,192` cửa sổ burst**).

### 2.2. Công Thức Ước Lượng Tốc Độ Tối Đa Không Rò Rỉ Chỉ Học Từ Tập TRAIN Hữu Cơ
Với `28,448 / 33,394` cửa sổ cơ giới (`85.19%`, hay `85.21%` trên bảng thô) và `74,762 / 84,633` đoạn (`88.34%`) khuyết tốc độ tối đa, hàm `fit_train_vmax_lookup` chỉ học trên các cửa sổ **`TRAIN` hữu cơ** (`slot_ts <= 2020-12-12T08:00:00Z` và `is_synthetic_burst == 0`):
$$\hat{v}_{\max}(s) = \begin{cases}
v_{\max}^{\text{raw}}(s), & \text{Giá trị gốc hợp lệ } (> 0) \text{ — }\mathbf{4{,}946}\text{ cửa sổ }(\mathbf{9{,}869}\text{ đoạn}) \\[4pt]
\text{P85}_{w \in \text{TRAIN}_{\text{org}}(s)}(v_{s,w}), & \text{Phân vị P85 trên TRAIN hữu cơ } (n \ge 5) \text{ — }\mathbf{7{,}650}\text{ cửa sổ }(\mathbf{472}\text{ đoạn}) \\[4pt]
\text{Median}_{u \in \text{TRAIN}_{\text{org}}(\text{level}(s), \text{type}(s))}\big(\hat{v}_{\max}(u)\big), & \text{Trung vị cùng cấp \& loại đường — }\mathbf{19{,}601}\text{ cửa sổ }(\mathbf{54{,}259}\text{ đoạn}) \\[4pt]
\text{Prior}(\text{level}(s)) \in \{1: 80, \; 2: 60, \; 3: 50, \; 4: 40\}\text{ km/h}, & \text{Chuẩn đô thị — }\mathbf{1{,}197}\text{ cửa sổ }(\mathbf{19{,}606}\text{ đoạn})
\end{cases}$$

---

## Chương 3. Kiến Trúc Tổng Thể, Quy Trình Khóa Mô Hình & Thực Thi Phân Tán `Apache Spark 3.5.7`

### 3.1. Bộ 5 Đại Lượng Giao Hoán Kết Hợp, Khóa `SHA-256` & Chia 8 Giỏ Khóa (`[C9]`, `[C13a/b]`)
- **Pure 5-Tuple Commutative Monoid:** Tập trạng thái $\mathcal{M} = \mathbb{Z}_{\ge 0} \times \mathbb{R}_{\ge 0} \times (\mathbb{R}_{\ge 0} \cup \{+\infty\}) \times (\mathbb{R}_{\ge 0} \cup \{-\infty\}) \times \mathbb{Z}_{\ge 0}$ với phần tử đơn vị $\mathbf{e} = (0, 0.0, +\infty, -\infty, 0)$ và phép gộp $\mathbf{m}_1 \oplus \mathbf{m}_2 = (\text{cnt}_1 + \text{cnt}_2, \sum v_1 + \sum v_2, \min(\min v_1, \min v_2), \max(\max v_1, \max v_2), \text{sev}_1 + \text{sev}_2)$.
  - Kiểm chứng `monoid_baseline_verification.json`: `33,394 / 33,394` cửa sổ khớp `100.0%` (`0` missing/extra/duplicate keys, `0` discrepancies ở tất cả 5 thành phần và `mean_velocity`, `max_abs_velocity_diff = 0.0 km/h`, `42,124` severe pings). Trên phân vùng lệch `[1, 2, 3]`, Monoid đạt sai số `0.0` trong khi trung bình của các trung bình lệch **`11.111111 km/h`**.
- **Ba tệp khóa cố định chữ ký `SHA-256` sau `Pha A` (`outputs/model_registry_manifest.json`):**
  - `outputs/model.joblib`: `a2f4118465082aa9f53d4427ac0c0fa9aac52ee9936e94068199b546dd23845e` (`rf_full_23d_v3`)
  - `data/lakehouse/segment_static_features.parquet`: `75328ac2d5e3c3a92a3357d38c7d152fa43435d505acf00faaa1d7ae14d99555` (bản mẫu `sample_static_features.parquet`: `f26ebdf7d396faac6a6112b3323b5f0217fc319ed699e77668a3ecf8ca072dd8`)
  - `outputs/vmax_lookup.json`: `ee7f692daad8dd2db32c43c8d1576ffa37172b2482468b27ca15f92984d46593`
- **Chia `8` giỏ khóa chống lệch tải (`2-Stage Key Salting`):** Từ `90,787` bản tin hợp lệ, Bước 1 gán `salt` $\in \{0..7\}$ tạo `59,655` nhóm cục bộ (`59,714` trong Spark), kéo tỷ số lệch tải khung giờ cao điểm từ **`684.95x`** xuống **`1.0197x`** ở giỏ cao điểm (`salted_partition_skew_ratio = 1.0123x`, `CV = 0.0059`, 8-partition hash skew `1.0846x → 1.0167x`); Bước 2 cộng gộp về đúng `33,394` cửa sổ (`max_abs_diff = 0.0`).

### 3.2. Bằng Chứng Thực Thi Vật Lý Trên `Apache Spark 3.5.7` (`hcmc-traffic-spark:3.5.7`)

| Hạng mục Kiểm chứng Phân tán | Kết quả trên `spark_engine.py` (`outputs/spark_runtime_evidence.json`) | Kết quả trên `spark_batch.py` (`outputs/quyen_validation/manifest.json`) |
|---|---|---|
| **1. Tối ưu hóa ghép bảng (`Catalyst Join`)** | `SortMergeJoin` (`5` Exchange, `2` Shuffle Joins, `166.18 ms`, IQR `23.71 ms`) $\to$ **`BroadcastHashJoin` (`1` Exchange, `2` BHJ, `123.37 ms`, IQR `4.57 ms`, `25.76%` speedup)** | `SortMergeJoin` (`5` Exchange, `0.932s`) $\to$ **`3` nút `BroadcastHashJoin` (`3` BroadcastExchange, `0` shuffle Exchange, `0.666s`)** |
| **2. Monoid Salting (`Spark`)** | Unsalted `33,394` (`97.92 ms`, IQR `2.13 ms`) vs 8-Bucket Salted `59,714 → 33,394` (`185.78 ms`, IQR `3.68 ms`) | 1-stage `0.409s` vs 2-stage salted `0.509s` (reducer stage max/median `1.18` vs `1.25`) |
| **3. Đối chiếu tập hợp `exceptAll()`** | `two_way_exceptAll_diff = 0` (cả Join & Monoid trên `33,394` dòng) | `monoid_difference = 0`, `join_difference = 0`, `independent_baseline.equivalent = true` |
| **4. Cắt tỉa `Parquet` & Streaming Sink 2** | `PartitionFilters: [(year_month = 2020-08)]`, `ReadSchema` chỉ `4` cột; Sink 2 watermark thực **`2020-12-01T12:30:00.000Z`** (`3` batches) | `10` phân vùng tháng `2020-07..2021-04` trên `Spark 3.5.7 (Java 17)` / `PySpark 4.2.0 (Java 21)` |
| **5. Spark MLlib (`TRAIN` hữu cơ)** | `FPGrowth = 32` luật; `KMeans` (`6` cụm trên `3,609` đoạn hữu cơ, `cost = 12,196.7418`); `Spark RF 23D` (`1,885` train hữu cơ / `1,182` test: `test_acc = 0.9416, roc_auc = 0.9903`, reload parity `true`) | Xuất `bronze_raw_events/`, `silver_cleaned_windows/`, `curated_gold/`, `window_metrics/` |

---

## Chương 4. Đồ Thị Giao Lộ (`PageRank`), Cặp Đường Hay Kẹt (`SON / FPGrowth`) & Phân Nhóm `K-Means`

1. **Đồ thị nối đoạn đường `PageRank` không U-turn (`[C10]`):** Lọc bỏ `525,786` điểm uốn cong trung gian (`90.97%`), giữ `52,181` giao lộ hoạt động và **`84,206` đoạn đường cơ giới sạch**. Dựng đồ thị nối đoạn đường **đã loại bỏ cung quay đầu (`U-turn`) trên đoạn đường hai chiều** (`dst_e_node != src_s_node` và `src_seg != dst_seg`), thu được **`92,453` cạnh chuyển tiếp** và **`314` `dangling nodes`**. Sau tối đa `30` vòng lặp ($\beta = 0.85$), top 5 nút cổ chai (`outputs/road_pagerank_top.csv`):
   - **#1 `21969` — Nguyễn Kiệm (`level=2, in=3, out=1, PR=2.7799`)**
   - **#2 `37345` — Cầu vượt Ngã tư Gò Dưa (`level=3, in=2, out=1, PR=2.7196`)**
   - **#3 `34723` — Quốc lộ 1 (`level=1, in=2, out=1, PR=2.6894`)**
   - **#4 `40948` — Quốc lộ 1 (`level=1, in=2, out=1, PR=2.6516`)**
   - **#5 `81815` — Song Hành Xa lộ Hà Nội (`level=4, in=2, out=2, PR=2.6508`)**
2. **Luật kết hợp khai phá chặt trên `TRAIN` hữu cơ + kiểm chứng Hold-out (`[C10]`):**
   - `co_congestion_rules.csv` (**`12` luật đồng kẹt xe**): đứng đầu **`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`** (`9` cửa sổ TRAIN, `support = 0.0292`, `conf = 0.6923 / 0.6429`, **`Lift = 15.2308`**, `holdout_co_windows = 1`); **`Cống Quỳnh ↔ Trần Hưng Đạo`** (`10` cửa sổ, `Lift = 12.2222`, `holdout = 1`); **`Tô Hiến Thành ↔ Lý Thường Kiệt`** (`17` cửa sổ TRAIN, `conf = 0.8947`, `Lift = 3.2421`, **`holdout_co_windows = 20`, `holdout_conf = 0.5405`**); **`Lạc Long Quân ↔ Lý Thường Kiệt`** (`12` cửa sổ TRAIN, `Lift = 3.1059`, `holdout = 8`, `holdout_conf = 0.8889`).
   - `lagged_congestion_rules.csv` (**`14` luật lan truyền trễ `+30m`, tất cả đều `persistence_controlled = True`** tức loại bỏ trường hợp đường $B$ vốn đã kẹt từ $t$): đứng đầu **`Lê Quang Định → Trần Quang Khải`** (`5` cửa sổ, `support = 0.0298`, `conf = 0.7143`, **`Lift = 24.0`**), **`Lê Quang Định → Trần Quốc Toản`** (`Lift = 24.0`), **`Lê Quang Định → Trần Quốc Thảo`** (`Lift = 20.0`), **`Nguyễn Thái Sơn → Trần Quang Khải`** (`Lift = 18.6667`), **`Phan Văn Trị → Cách Mạng Tháng Tám`** (`6` cửa sổ, `Lift = 6.5455`). *Lưu ý trung thực:* Do cỡ mẫu hỗ trợ nhỏ (`5–8` khung) trên dữ liệu GPS thưa, các luật trễ chỉ đóng vai trò giả thuyết cảnh báo thăm dò.
3. **Phân `6` nhóm hành vi Unified `K-Means` trên `TRAIN` hữu cơ phủ kín `74,211` đoạn thiếu GPS (`[C13b]`):** Huấn luyện `StandardScaler + KMeans(k=6)` trên **`3,609` đoạn `TRAIN` hữu cơ** (`Silhouette = 0.2241`, `Davies-Bouldin = 1.3284`, `Calinski-Harabasz = 806.49`, `Inertia = 11,921.05`; kích thước cụm `{0: 846, 1: 924, 2: 708, 3: 136, 4: 840, 5: 155}`), suy diễn phủ **`100%` `84,206` đoạn cơ giới** (`9,995` quan sát + **`74,211` Cold-Start = `88.13%`**) trong `data/lakehouse/segment_static_features.parquet`.

---

## Chương 5. Lọc Burst Tổng Hợp, Dự Báo Đúng `+30 Phút`, Bảng Thử Nghiệm 7 Cấu Hình & Kiểm Định Thống Kê

### 5.1. Loại Bỏ Burst Tổng Hợp, Tỷ Lệ Quá Khứ ($\tau < t$) & Chia Tập Cắt Sạch Mẫu Biên
- **Tỷ lệ kẹt lịch sử nhân quả ($\tau < t$):**
  $$\text{Prior}_s(t) = \frac{\sum_{\tau < t} y_{s,\tau} + m_{\text{seg}} \cdot \bar{y}_{\text{global}}(\tau < t)}{\text{count}_s(\tau < t) + m_{\text{seg}}} \quad (m_{\text{seg}} = 5, \; m_{\text{street}} = 10)$$
- **Chia tập & Lọc Burst (`7,885` cặp đúng `t + 30 phút`):**
  - `warmup_cutoff_utc = 2020-08-02T23:30:00Z` · `train_cutoff_utc = 2020-12-12T08:00:00Z` · `val_cutoff_utc = 2020-12-25T03:30:00Z`.
  - Hai khung burst tổng hợp (`2020-08-02 23:30` và `2020-08-03 00:00`, mỗi khung `3,609` thô / `3,596` đoạn cơ giới sạch $\to$ `7,192` cửa sổ burst) tạo ra **`3,596` cặp burst** trong `TRAIN`. Hệ thống **loại hoàn toàn `3,596` cặp burst này khi huấn luyện ML**, giữ lại **`1,885` cặp `TRAIN` hữu cơ** (`50` mẫu biên bị cắt), **`1,167` `VAL`** (`5` mẫu biên bị cắt), và **`1,182` `TEST`** (`959` `A`, `10` `B`, `15` `C`, `13` `D`, `20` `E`, `165` `F`; `185` mẫu kẹt cứng `E/F`).
  - **Ablation có/không burst (`metrics.json`):** Huấn luyện kèm `3,596` cặp burst đạt `Test Acc = 0.9399, Severe F1 = 0.9344`, so với huấn luyện thuần `1,885` cặp hữu cơ đạt `Test Acc = 0.9255, Severe F1 = 0.9362` — loại burst vừa bảo đảm mô hình không học quy luật nhân tạo từ đợt replay đồng loạt tháng 08/2020 vừa cải thiện `Severe F1`.

### 5.2. Cổng Chọn `VAL` & Bảng Thử Nghiệm 7 Cấu Hình Trên Tập `TEST` (`outputs/ablation_results.csv`)
- **Huấn luyện sạch:** Loại bỏ hoàn toàn `sample_weight` nhân tạo và fallback bypass; chỉ dùng `class_weight` theo công thức cân bằng mềm lũy thừa `0.65`: $w_c = (N / (6 \cdot n_c))^{0.65}$.
- **Cổng chọn mô hình trên `VAL` (`(val_accuracy_6class + val_pr_auc + val_roc_auc) / 3`):** **`Random Forest 23D`** thắng với **`val_composite_score = 0.8882`** (`VAL 6-Class Acc = 0.8166`, `VAL PR-AUC = 0.8912`, `VAL ROC-AUC = 0.9567`, `VAL Severe F1 = 0.8912`, `τ_severe = 0.25`, `τ_change = 0.40`, `τ_onset = 0.25`), vượt `Decision Tree 23D` (`0.8812`) và vượt xa `RF Local Only 14D` (`0.6794`).

| Cấu hình Thử nghiệm (Mô hình chính duy nhất: `Random Forest 23D`) | Số biến | Đúng 6 mức | `Macro-F1` | `Weighted-F1` | `Prec Kẹt` | `Rec Kẹt` | `F1 Kẹt` | `ROC-AUC` | `PR-AUC` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `Persistence Baseline (t -> t+30m)` | `Lag-0` | `0.9484` | `0.5654` | `0.9461` | `0.9365` | `0.9568` | `0.9465` | `0.9867` | `0.9367` |
| `Decision Tree (Local Only)` | `14D` | `0.8697` | `0.4226` | `0.9019` | `0.7106` | `0.9027` | `0.7952` | `0.9767` | `0.9198` |
| `Decision Tree (+Graph, Causal Priors & K-Means)` | `23D` | `0.8790` | `0.3856` | `0.9035` | `0.7479` | `0.9459` | `0.8353` | `0.9853` | `0.9479` |
| `Random Forest (Local Only)` | `14D` | `0.9222` | `0.3976` | `0.9261` | `0.9355` | `0.9405` | `0.9380` | `0.9881` | `0.9554` |
| **`Random Forest (+Graph, Causal Priors & K-Means) [Đã chọn]`** | **`23D`** | **`0.9255`** | **`0.4253`** | **`0.9297`** | **`0.9215`** | **`0.9514`** | **`0.9362`** | **`0.9894`** | **`0.9543`** |
| `GBT Binary Alerting (Local Only)` | `14D` | `N/A` | `N/A` | `N/A` | `0.9267` | `0.9568` | `0.9415` | `0.9751` | `0.9488` |
| `GBT Binary Alerting (+Graph, Causal Priors & K-Means)` | `23D` | `N/A` | `N/A` | `N/A` | `0.9227` | `0.9676` | `0.9446` | `0.9830` | `0.9498` |

- **Bảng đối chiếu 6 lớp trên `TEST` (`1,182` mẫu):** Đường chéo chính **`[944, 3, 1, 3, 5, 138]`** (đủ cả 6 lớp `> 0`). *Hạn chế được ghi nhận trung thực:* `macro_f1 = 0.4253` thấp hơn `weighted_f1 = 0.9297` do sự mất cân bằng tự nhiên rất lớn ở các lớp trung gian `B/C/D/E` (`10`, `15`, `13`, `20` mẫu TEST so với `959` mẫu `A` và `165` mẫu `F`).

### 5.3. Kiểm Định Đổi Trạng Thái, Ý Nghĩa Thống Kê (`Bootstrap` & `McNemar`) & `Masked-GPS Cold-Start`
1. **Nhóm đổi trạng thái (`LOS(t) ≠ LOS(t+30p)`, `61 / 1,182` mẫu TEST = `5.16%`):**
   - `Persistence`: **`0 / 61` (`0.00%`)**, `Detect ROC-AUC = 0.5000`, `PR-AUC = 0.5408`, `Brier = 0.3279`.
   - **`Random Forest (23D)`:** Ngưỡng mặc định đúng **`16.39%` (`10 / 61`, kiểm định McNemar `b=10, c=0`, `p = 0.001953`)**; với ngưỡng hiệu chỉnh trên `VAL` (`τ_change = 0.40`) đạt **`22.95%` (`14 / 61`, độ chính xác tổng `90.19%`)**; xếp đúng hướng lớp mới ở vị trí `#1` đạt **`31.15%` (`19 / 61`)**; **`Detect ROC-AUC = 0.8822`** (`PR-AUC = 0.4009`, vượt mô hình heuristic 2 biến `0.7921` và `DT 23D = 0.8511`); `Severe PR-AUC` trên nhóm đổi trạng thái đạt **`0.5556`** (`GBT 23D = 0.6811`) và `Brier = 0.2811` (so với `Persistence = 0.3279`).
2. **Đường đang thoáng (`A–D`) đột ngột hóa kẹt cứng (`E–F`, `8 / 993` mẫu TEST):** `RF 23D` đạt **`Onset ROC-AUC = 0.8438`** (so với `Persistence = 0.8021`) và **`Onset risk lift = 3.26x`** (xác suất trung bình `0.0940` trên ca sắp kẹt so với `0.0288` trên đường thoáng ổn định), ghi nhận trung thực ở ngưỡng cứng `τ_onset = 0.25` đạt `0 / 8` do cỡ mẫu khởi phát quá hiếm (`8` ca trên `993`).
3. **Paired Bootstrap (`1,000` lần lặp) 95% CIs:** `RF Severe ROC-AUC = [0.9824, 0.9955]`, `RF Severe F1 = [0.9091, 0.9602]`, `PR-AUC diff (RF - Persistence) = [-0.0029, 0.0444]`, `Brier gain = [-0.0064, 0.0012]`.
4. **Kiểm định `Masked-GPS Spatial Cold-Start Proxy` trên `TEST` (`1,182` mẫu, che toàn bộ GPS hiện tại chỉ giữ GIS tĩnh + street prior):** `RF 23D` đạt `6-Class Acc = 58.63% (0.5863)`, `ROC-AUC = 0.7000`, `PR-AUC = 0.5152`, `Brier = 0.1123` (so với đoán đa số toàn cục `LOS A = 0.8113`); trên toàn mạng **`84,206` đoạn**, `RF 23D` phủ **`100.0%`** (bao gồm **`74,211` đoạn Cold-Start = `88.13%`**) so với `11.87%` của `Persistence` (`0.0%` Cold-Start).
5. **Chế độ ngắt quãng `> 30 phút` (`3,605` mẫu TEST, `49.21%` đổi mức):** `RF 23D PR-AUC = 0.6405` vs `Persistence = 0.5732` (`GBT 23D = 0.6175`, `RF Acc = 0.4910`). Đối chiếu phụ `legacy_next_observed` (`4,787` test rows): `Severe F1 = 0.6121`, `PR-AUC = 0.7378`, `ROC-AUC = 0.8668`.

---

## Chương 6. Luồng Trực Tuyến (`Kafka 4.1 KRaft`), Bộ Đếm `33.0 KiB`, Lakehouse, Đối Soát & `MongoDB 8.0`

1. **Bộ đếm gọn `33.0 KiB` & Điểm nóng `EWMA` ($\alpha = 0.15$):**
   - Trong cả xử lý luồng thực tế (`starter_stream.py`, `live_kafka_stream.py`) và mô phỏng batch replay (`33,727` sự kiện = `33,394` gốc + `333` trùng, `1,168` trễ trong watermark, `500` trễ ngoài watermark tại `226` khung giờ): dùng **`32.0 KiB RotatingBloomFilter` (`2 × 16.0 KiB`, `262,144` bits, $k=10$ hàm băm)** chặn **`333 / 333`** tin trùng với **`13` dương tính giả (`0.0389%` thực nghiệm vs `0.0792%` lý thuyết)**; **`1.0 KiB 64-bit HyperLogLog++` (`p=10`, `uint8[1024]`)** ước lượng **`9,620.2 / 9,995`** đoạn hoạt động (**sai số `3.75%`**). Điểm nóng `EWMA` ($\alpha=0.15$, tự xóa khóa `< 10^{-4}`) bắt `#1` Lý Thường Kiệt (`#47407`, `6.6652`), `#2` Trương Định (`#21263`, `6.6635`), `#3` Nguyễn Đình Chiểu (`#22684`, `6.6300`).
2. **Hồi phục sự cố `WAL` (`outputs/query_progress.json`):** `starter_stream.py --kill-after-batch 1` lưu **`WAL = 73,714 B (73.7 KB)`**, tự động bỏ qua `batch 1` đã commit và chạy tiếp đến **`90,741` sự kiện trong watermark** (`90,787` bản ghi Bronze), **`0` bản ghi nhân đôi**.
3. **Hồ dữ liệu `Medallion Lakehouse` (`data/lakehouse/`):**
   - `bronze_raw_events/events.parquet` (**`90,787` raw pings** × `4` cols), `stream_replay_windows/replay.parquet` (**`33,727` replay windows** × `45` cols), `silver_cleaned_windows/windows.parquet` (**`33,394` windows** × `86` cols), `curated_gold/year_month=*` (**`10` phân vùng tháng**, `33,394` dòng × `86` cols) — **đọc sạch bởi cả `PyArrow/Pandas` lẫn `Apache Spark 3.5.7`**.
4. **Đối soát Hai Tầng & Khóa Phiên Bản (`[C12]`, `outputs/query_progress.json`):**
   - **Tầng 1:** **`tier1_exceptAll_diff_count = 0`** (**`100.0%`** khớp tuyệt đối trong watermark).
   - **Tầng 2:** Bù đủ **`500` cửa sổ đoạn đường trễ** (`226` khung giờ, nâng độ đầy đủ **`98.5% → 100.0%`**), hàn gắn **`609` cửa sổ hạ lưu (`healed_downstream_lag_windows`)**, chấm điểm lại **`384` cửa sổ** (**`213` cửa sổ đổi xác suất dự báo**).
   - Phân bố vòng đời cuối cùng (`33,394` docs): **`32,405` `BATCH_VERIFIED (v2)`**, **`500` `BATCH_RECONCILED (v2)`**, **`489` `STREAM_PROVISIONAL (v1)`**.
5. **Kho Phục vụ `MongoDB 8.0.34` (`outputs/mongo_verify.json`) & Hạ tầng Web TOC Duy nhất:**
   - Ghi cập nhật `33,394` docs qua `4` phân vùng (`69` lô `500` docs) với **atomic `$status_version` pipeline guard** (chặn `25/25` bản tin `v1` cũ).
   - Truy vấn `UC-2` (`street_level IN [1,2]`, `severe_prob >= 0.70`, `limit 25`, `1,234` cửa sổ khớp) chạy **`IXSCAN` (`level_prob_segment_idx`, `in_memory_sort = false`, `totalKeysExamined = 26`, `totalDocsExamined = 25`)**: **`warm_median = 0.360 ms`**, **`warm_p95 = 0.603 ms`** (`cold = 0.933 ms`, `min = 0.261 ms`, `max = 0.940 ms`), nhanh gấp **`31.36x`** so với **`COLLSCAN` (`median = 11.289 ms`, `p95 = 12.217 ms`)**.
   - Phục vụ tập trung qua ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`, dịch vụ `toc-web`)** trong cụm Docker **`5` dịch vụ mặc định (`11.0 GB / 16.0 GB RAM`)**.

---

## Chương 7. Phân Công Nhóm & Quy Trình Tái Lập Toàn Hệ Thống

### 7.1. Phân Công Thực Thi Của Ba Thành Viên (GVHD: Đặng Trần Khánh)
- **Nguyễn Tạ Minh Quyền (`25MS23280`):** Bước 1 — Làm sạch dữ liệu, ước lượng tốc độ tối đa từ `TRAIN` & xử lý phân tán trên `Apache Spark 3.5.7` / `PySpark 4.2.0` (`spark_batch.py`, `traffic_data/spark_etl.py`, `baseline.py`, `outputs/quyen_validation/*`).
- **Nguyễn Thành Đạt (`25MS23306`):** Bước 2 & Bước 3 — Đồ thị `PageRank` (loại U-turn), luật trên `TRAIN` hữu cơ, `K-Means` phủ vùng thiếu GPS, lọc burst tổng hợp & mô hình `Random Forest 23D` dự báo `+30 phút` (`pipeline.py`, `spark_engine.py`, `tests/`).
- **Phạm Công Minh (`25MS23310`):** Bước 4 — Luồng `Kafka 4.1.0 KRaft`, `32 KiB RotatingBloomFilter` + `1 KiB 64-bit HLL++`, đối soát bù tin trễ, `MongoDB 8.0.34`, và ứng dụng **Web TOC `ReactJS + FastAPI (:8000)`** (`producer.py`, `starter_stream.py`, `live_kafka_stream.py`, `toc_backend/`, `toc_frontend/`, `docker-compose.yml`).

### 7.2. Quy Trình Tái Lập Toàn Bộ Kết Quả Từ Đầu (`PowerShell / Docker CLI`)
```powershell
# 1. Khởi chạy cụm 5 dịch vụ mặc định (kafka, mongo, spark, toc-web :8000, live-consumer)
docker compose up -d --build

# 2. Thực thi trọn vẹn dây chuyền xử lý 4 bước & kiểm chứng đối chiếu độc lập bộ gộp 5 đại lượng
python pipeline.py
python baseline.py

# 3. Thực thi luồng chịu lỗi WAL Kill/Restart & phát sự kiện Kafka
python producer.py --mode both --bootstrap-servers localhost:9092
python starter_stream.py --kill-after-batch 1

# 4. Chạy bộ kiểm thử tự động & kiểm tra chất lượng mã nguồn Ruff (McCabe complexity < 15)
python -m pytest -v
python -m ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
```
