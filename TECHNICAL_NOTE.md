# Đặc Tả Kỹ Thuật & Cơ Sở Toán Học Hệ Thống

Tài liệu này cung cấp đặc tả toán học, cấu trúc dữ liệu, bằng chứng thực thi phân tán và quy trình tái lập cho **Hệ thống Giám sát Hiện trạng & Dự báo Ùn tắc Giao thông Đô thị TP.HCM** theo cấu trúc 7 chương đồng bộ 100% với Báo cáo Tổng kết Đồ án (`product/00-proposal/project-proposal.md`).

---

## Chương 1. Câu Chuyện Dữ Liệu Thực Tế, Dây Chuyền 4 Bước (`P1–P4`), Tình Huống Sử Dụng (`UC-1–UC-6`) & Ranh Giới Hệ Thống

### 1.1. Từ Bản Tin Vận Tốc GPS Ngoài Đường Đến Dây Chuyền Vận Hành 4 Bước
- **Một dòng dữ liệu gốc ngoài đường:** Tại `07:08:28` sáng giờ Việt Nam ngày `03/08/2020`, xe đi qua đoạn đường Lý Thường Kiệt số `47407` (dài `16m`, Quận 10) gửi về vận tốc `4 km/h` (hoặc lúc `11:30` trưa tại Công Trường Quốc Tế số `21064`, xe chạy `12 km/h`).
- **Thang đo mức phục vụ giao thông (`LOS A–F`):** Dựa trên tỷ lệ vận tốc $r = \bar{v} / \hat{v}_{\max}$, trong đó `LOS A–B` ($r \ge 0.65$) là đường thông thoáng tự do; `LOS C–D` ($0.35 \le r < 0.65$) là đông đúc ổn định; `LOS E–F` ($r < 0.35$, riêng `LOS F` $< 0.20$ hoặc dừng hẳn) là ùn ứ nghiêm trọng đến kẹt cứng.
- **Định vị quy mô mẫu chuẩn (`Proof-of-Scale`):** Năm bảng công khai của Kaggle (`~38.50 MB`, bảng `segment_status.csv` nặng `3.58 MB` với `90,938` bản tin GPS) đóng vai trò **mẫu chuẩn kiểm chứng quy mô (`Proof-of-Scale`)** để đối chứng sai số toán học bằng `0.0` trước khi mở rộng ra luồng định vị toàn TP.HCM (`> 1` triệu xe, `8–17` tỷ bản tin/ngày $\approx$ `1.5–3.0 TB/ngày`).
- **Sự liên kết nhân quả của 4 bước vận hành (`P1 → P4`):**
  1. **Bước 1 (`P1` — Chuẩn hóa hiện trạng):** Làm sạch `90,938` bản tin GPS, ước lượng tốc độ tối đa chỉ học từ `TRAIN`, gom khung `30 phút` bằng bộ `5` đại lượng (`33,441` cửa sổ; `33,394` cửa sổ trên đường cơ giới) và quy đổi mức `LOS A–F`.
  2. **Bước 2 (`P2` — Lấp vùng mù `88.15%` đường thiếu GPS):** Lọc bỏ `525,542` điểm uốn cong trung gian (`90.93%`), tính `PageRank` trên `84,633` đoạn đường một chiều (`151,929` hướng rẽ, `48` đoạn cụt), khai phá `Spark FPGrowth` (`335` luật) & `2-Pass SON`, và phân `6` nhóm `K-Means` phủ kín `74,606` đoạn thiếu GPS — vừa phục vụ Sở GTVT, vừa truyền làm biến đầu vào cho Bước 3.
  3. **Bước 3 (`P3` — Dự báo trước `30 phút`):** Ghép cặp quan sát cách đúng `30 phút` (`7,898` cặp); chia tập có cắt sạch mẫu biên (`5,494 TRAIN / 1,167 VAL / 1,182 TEST`) và huấn luyện mô hình duy nhất `Random Forest (23 biến)` không rò rỉ dữ liệu tương lai.
  4. **Bước 4 (`P4` — Cảnh báo trực tuyến & tự động bù tin trễ):** Chia hai nhánh trên `Apache Kafka 4.1.0 KRaft`, bộ đếm luồng gọn `33.0 KiB` (`32.0 KiB Bloom` + `1.0 KiB HLL++`), bù đủ `501` cửa sổ đến trễ cuối ngày và truy vấn `MongoDB 8.0.34` (`warm_p95 = 0.636 ms`).

### 1.2. Sáu Tình Huống Sử Dụng Thực Tế (`UC-1–UC-6`)

| Mã UC | Người Sử dụng | Thông tin Nhận trên Màn hình | Quyết định Xử lý & Bằng chứng Thực nghiệm |
|---|---|---|---|
| **`UC-1`** | Trung tâm Điều hành (`TOC`) | 25 đoạn đường đang kẹt cứng dai dẳng nhất nhiều khung liên tiếp | Phát cảnh báo bảng điện tử & cử lực lượng giải tỏa (`#1`: Đoạn `47407` Lý Thường Kiệt, điểm `= 6.6652`) |
| **`UC-2`** | Kỹ sư Tín hiệu Đèn | Trục đường cấp `1–2` có xác suất kẹt cứng `≥ 70%` sau `30 phút` | Kéo dài đèn xanh hướng chính trước khi ùn tắc tràn nút (`1,669` cửa sổ, `IXSCAN warm_p95 = 0.636 ms`) |
| **`UC-3`** | Chuyên viên `Sở GTVT` | Bản đồ xếp hạng nút cổ chai toàn đô thị (`84,633` đoạn đường) | Ưu tiên lắp camera tại vùng thiếu GPS (`74,606` đoạn; `#1 PageRank`: Đoạn `61781` Phú Định `3.7515`) |
| **`UC-4`** | Ứng dụng Dẫn đường | Cảnh báo cặp đường thường kẹt đồng thời / giả thuyết lan sau `30 phút` | Gợi ý lộ trình vòng từ xa (`Lý Chính Thắng ↔ Nguyễn Thông`, `Lift = 28.5833x`; `Cộng Hòa ↔ Trường Chinh`, `12.1956x`) |
| **`UC-5`** | Đội Tuần tra Giao thông | Bản đồ chia `6` nhóm hành vi đường phố theo tốc độ & tần suất kẹt | Bố trí chốt trực tập trung vào `2,352` giao lộ nghẽn mãn tính (`K-Means`, `Silhouette = 0.2853`) |
| **`UC-6`** | Kỹ sư Dữ liệu | Báo cáo đối soát cuối ngày & tiến độ khôi phục bản tin đến muộn | Bảo đảm kho lịch sử chuẩn `100%` (đúng hạn khớp `32,940/32,940`; bù `501` cửa sổ trễ, cập nhật `3,953` cửa sổ kế tiếp, chấm lại `377` cửa sổ) |

---

## Chương 2. Kiểm Kê Dữ Liệu (`5 Bảng`), `10 Tình Huống Thực Tế` (`EC-1–EC-10`) & Phễu Làm Sạch (`[C5]`)

### 2.1. Thống Kê 5 Bảng Dữ Liệu Thô & Bảo Toàn Quân Số Bản Ghi
- **5 bảng nguồn (`raw_data/`):** `nodes.csv` (`577,967` dòng, `19.44 MB`), `streets.csv` (`5,553` dòng, `0.20 MB`), `segments.csv` (`84,633` dòng, `10.29 MB`), `segment_status.csv` (`90,938` dòng, `3.58 MB`), và `train.csv` (`33,441` dòng, `4.99 MB` — bảng đối chứng độc lập).
- **Bảo toàn quân số:**
  1. Trên `segments.csv`: phát hiện `154` đoạn có chiều dài `≤ 0m` (`R1`) và `427` đoạn lối đi bộ/nội bộ (`R2`, hợp nhất thành `580` bản ghi cách ly địa lý).
  2. Trên `segment_status.csv`: `90,938` bản tin hợp lệ tạo **`33,441` cửa sổ 30 phút** khớp `100.0%` khóa với `train.csv`. Khi lọc tách đoạn phi cơ giới trong `spark_batch.py`, `151` bản tin lối đi bộ được tách riêng, còn **`90,787` bản tin cơ giới** tạo **`33,394` cửa sổ** (chênh đúng `47` cửa sổ lối đi bộ).
  3. Chuẩn hóa tên đường `Unicode NFC` (`R4`) gộp `695` tên thô về `649` tên chuẩn (`2,071` cửa sổ), gắn cờ `631` cửa sổ tên `Phường/Quận` (`R3`), và đổi giờ Việt Nam qua nửa đêm cho `4,320` cửa sổ (`EC-4`).

### 2.2. Công Thức Ước Lượng Tốc Độ Tối Đa Chỉ Học Từ Tập TRAIN
Với `28,495 / 33,441` cửa sổ (`85.21%`) và `74,762 / 84,633` đoạn (`88.34%`) khuyết tốc độ tối đa, hàm `fit_train_vmax_lookup` chỉ học trên tập `TRAIN` ($\tau \le \text{2020-12-12T08:00:00Z}$):
$$\hat{v}_{\max}(s) = \begin{cases}
v_{\max}^{\text{raw}}(s), & \text{Giá trị gốc hợp lệ } (> 0) \text{ — }\mathbf{4{,}946}\text{ cửa sổ }(\mathbf{9{,}871}\text{ đoạn}) \\[4pt]
\text{P85}_{w \in \text{TRAIN}(s)}(v_{s,w}), & \text{Phân vị P85 trên TRAIN } (n \ge 5) \text{ — }\mathbf{8{,}164}\text{ cửa sổ }(\mathbf{519}\text{ đoạn}) \\[4pt]
\text{Median}_{u \in \text{TRAIN}(\text{level}(s), \text{type}(s))}\big(\hat{v}_{\max}(u)\big), & \text{Trung vị cùng cấp \& loại đường trên TRAIN — }\mathbf{20{,}124}\text{ cửa sổ }(\mathbf{72{,}726}\text{ đoạn}) \\[4pt]
\text{Prior}(\text{level}(s)) \in \{1: 80, \; 2: 60, \; 3: 50, \; 4: 40\}\text{ km/h}, & \text{Chuẩn đô thị — }\mathbf{207}\text{ cửa sổ }(\mathbf{1{,}517}\text{ đoạn})
\end{cases}$$

---

## Chương 3. Kiến Trúc Tổng Thể, Quy Trình Khóa Mô Hình & Thực Thi Phân Tán `Apache Spark 3.5.7`

### 3.1. Bộ 5 Đại Lượng Giao Hoán Kết Hợp, Khóa `SHA-256` & Chia 8 Giỏ Khóa (`[C9]`, `[C13a/b]`)
- **Bộ 5 đại lượng (`5-Tuple Commutative Monoid`):** Tập trạng thái $\mathcal{M} = \mathbb{Z}_{\ge 0} \times \mathbb{R}_{\ge 0} \times (\mathbb{R}_{\ge 0} \cup \{+\infty\}) \times \mathbb{R}_{\ge 0} \times \mathbb{Z}_{\ge 0}$ với phần tử đơn vị $\mathbf{e} = (0, 0.0, +\infty, 0.0, 0)$ và phép gộp $\mathbf{m}_1 \oplus \mathbf{m}_2 = (\text{cnt}_1 + \text{cnt}_2, \sum v_1 + \sum v_2, \min(\min v_1, \min v_2), \max(\max v_1, \max v_2), \text{sev}_1 + \text{sev}_2)$.
- **Ba tệp khóa cố định chữ ký `SHA-256` sau `Pha A`:** `model.joblib` (`86195ec9e1a8...`), `segment_static_features.parquet` (`2877a4dd1102...`), và `vmax_lookup.json` (`d00c68e39a3c...`).
- **Chia `8` giỏ khóa chống lệch tải (`2-Stage Key Salting`):** Bước 1 gán $b = \text{hash}(\text{raw\_event\_id}) \bmod 8$ tạo `59,895` nhóm cục bộ, kéo tỷ số lệch tải giờ cao điểm từ **`687.43x` xuống `1.023x` (`CV = 0.0057`)**; Bước 2 bỏ giỏ gộp về đúng `33,441` cửa sổ.

### 3.2. Bằng Chứng Thực Thi Vật Lý Trên `Apache Spark 3.5.7 (Java 17)`

| Hạng mục Kiểm chứng Phân tán | Kết quả trên `spark_engine.py` (`outputs/spark_runtime_evidence.json`) | Kết quả trên `spark_batch.py` (`outputs/quyen_validation/manifest.json`) |
|---|---|---|
| **1. Tối ưu hóa ghép bảng (`BroadcastHashJoin`)** | Chuyển từ `SortMergeJoin` (`10` bước `Shuffle`, `0.946s`) sang **`BroadcastHashJoin` (`2` bước `Exchange`, `0` `SortMergeJoin`, `0.506s`)** | Chuyển từ `SortMergeJoin` (`5` bước `Shuffle`, `0.932s`) sang **`3` nút `BroadcastHashJoin` (`0` bước `Shuffle`, `0.666s`)** |
| **2. Đối chiếu tập hợp `exceptAll()`** | `dsl_except_sql_count = 0` và `sql_except_dsl_count = 0` (`33,441` dòng) | `baseline_vs_broadcast_diff = 0` và `salted_vs_unsalted_diff = 0` (`33,394` dòng) |
| **3. Cắt tỉa phân vùng & cột `Parquet`** | `PartitionFilters: [year_month = 2020-08]` — chỉ đọc đúng **`7,294` dòng** tháng `2020-08` (`4 / 59` cột) | Xuất `10` phân vùng tháng `2020-07..2021-04` trên `Apache Spark 3.5.7 (Java 17)` *(tương thích chéo `PySpark 4.2.0 / Java 21`)* |
| **4. Đối chiếu độc lập với `baseline.py`** | `discrepancy_count = 0`, `tuple_component_mismatch_count = 0` trên toàn bộ `33,441` cửa sổ (`42,170` bản tin kẹt) | `value_mismatches = 0`, `missing_keys = 0`, `extra_keys = 0`, `duplicate_keys = 0` |

---

## Chương 4. Đồ Thị Giao Lộ (`PageRank`), Cặp Đường Hay Kẹt (`SON / FPGrowth`) & Phân Nhóm `K-Means`

1. **Đồ thị nối đoạn đường `PageRank` (`[C10]`):** Lọc bỏ `525,542` điểm uốn cong trung gian (`90.93%`), dựng đồ thị nối đoạn đường một chiều gồm $N = 84{,}633$ đỉnh đoạn đường, $151{,}929$ hướng rẽ và $48$ đoạn cụt. Sau `25` vòng lặp ($\beta = 0.85$), tổng điểm bảo toàn $\sum_{s=1}^{84{,}633} r_s^{(25)} = 84{,}633.0$. Năm nút cổ chai đứng đầu: **#1 Phú Định (`61781`, `PR = 3.7515`)**, **#2 Lê Lai (`50017`, `3.4650`)**, **#3 Châu Văn Liêm (`62682`, `2.9246`)**, **#4 Nguyễn Kiệm (`21969`, `2.7313`)**, **#5 Quốc lộ 1 (`34723`, `2.6430`)**.
2. **`2-Pass SON`, `Spark FPGrowth` & Lưu ý cỡ mẫu nhỏ:**
   - `Spark FPGrowth` tìm `286` tập phổ biến và `335` luật kết hợp trên giỏ tuyến đường không cắt ngọn (`max_basket_size = 57`); `2-Pass SON` xuất `30` luật đồng kẹt xe 2 chiều (`Lý Chính Thắng ↔ Nguyễn Thông`, `Lift = 28.5833x`; `Cộng Hòa ↔ Trường Chinh`, `Lift = 12.1956x`) và `20` luật kẹt nối tiếp sau `30 phút` (`Nguyễn Ngọc Lộc → Hải Thượng Lãn Ông`, `Lift = 32.9167x`).
   - **Lưu ý trung thực:** Các luật nối tiếp sau `30 phút` mới cùng xuất hiện trong **`5–8` cửa sổ 30 phút** (`2.5–4.0` giờ quan sát trong 10 tháng) do GPS thưa, nên chỉ dùng làm giả thuyết tham khảo kèm đối chiếu trên đồ thị đường bộ.
3. **Phân `6` nhóm hành vi `K-Means` học trên `TRAIN` phủ kín `74,606` đoạn thiếu GPS (`[C13b]`):** Học trên `6,926` đoạn đường `TRAIN` đạt `Silhouette = 0.2853`, `Davies-Bouldin = 1.1181`, cung cấp trực tiếp biến `cluster_id` và `pagerank_score` cho mô hình `Random Forest` ở Chương 5.

---

## Chương 5. Dự Báo Đúng `+30 Phút`, Bảng Thử Nghiệm 7 Cấu Hình & Đối Chứng Trực Diện `Baseline` – `Random Forest`

### 5.1. Tỷ Lệ Kẹt Tích Lũy Quá Khứ & Chia Tập Cắt Sạch Mẫu Biên
- Tỷ lệ kẹt lịch sử của đoạn đường và tuyến đường chỉ cộng dồn trên quá khứ chặt $\tau < t$:
  $$\text{Prior}_s(t) = \frac{\sum_{\tau < t} y_{s,\tau} + m_{\text{seg}} \cdot \bar{y}_{\text{global}}(\tau < t)}{\text{count}_s(\tau < t) + m_{\text{seg}}} \quad (m_{\text{seg}} = 5, \; m_{\text{street}} = 10)$$
- Tập dữ liệu ghép đúng `t + 30 phút` (`7,898` cặp): `TRAIN <= 2020-12-12T08:00:00Z` (`5,494` mẫu, cắt `50` mẫu biên) · `VAL <= 2020-12-25T03:30:00Z` (`1,167` mẫu, cắt `5` mẫu biên) · `TEST` (`1,182` mẫu, gồm `959` `LOS A`, `10` `B`, `15` `C`, `13` `D`, `20` `E`, `165` `F`).

### 5.2. Bảng Thử Nghiệm 7 Cấu Hình Trên Tập `TEST` (`1,182` mẫu — `outputs/ablation_results.csv`)

| Cấu hình Thử nghiệm (Mô hình chính duy nhất: `Random Forest 23 biến`) | Số biến | Đúng 6 mức | `Macro-F1` | `Prec Kẹt` | `Rec Kẹt` | `F1 Kẹt` | `ROC-AUC` | `PR-AUC` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `Baseline (Đoán LOS(t+30p) = LOS(t) hiện tại)` | `1 biến` | `0.9484` | `0.5654` | `0.9365` | `0.9568` | `0.9465` | `0.9867` | `0.9367` |
| `Decision Tree (Chỉ biến cục bộ)` | `14 biến` | `0.9425` | `0.3794` | `0.9362` | `0.9514` | `0.9437` | `0.9856` | `0.9482` |
| `Decision Tree (+PageRank, Tỷ lệ quá khứ & K-Means)` | `23 biến` | `0.9425` | `0.3772` | `0.9362` | `0.9514` | `0.9437` | `0.9844` | `0.9327` |
| `Random Forest (Chỉ biến cục bộ & giờ VN)` | `14 biến` | `0.9425` | `0.5455` | `0.9409` | `0.9459` | `0.9434` | `0.9866` | `0.9630` |
| **`Random Forest (+PageRank, Tỷ lệ quá khứ & K-Means) [Mô hình chính]`** | **`23 biến`** | **`0.9450`** | **`0.5543`** | **`0.9358`** | **`0.9459`** | **`0.9409`** | **`0.9882`** | **`0.9471`** |
| `GBT Nhị phân (Chỉ báo kẹt cứng E/F, 14 biến)` | `14 biến` | `N/A` | `N/A` | `0.6667` | `0.9514` | `0.7840` | `0.9812` | `0.9328` |
| `GBT Nhị phân (+PageRank, Tỷ lệ quá khứ & K-Means)` | `23 biến` | `N/A` | `N/A` | `0.9365` | `0.9568` | `0.9465` | `0.9843` | `0.9419` |

### 5.3. Đối Chứng Trực Diện 1-Đối-1 Giữa `Baseline` Và `Random Forest (23 Biến)` (`outputs/transition_evaluation.csv`)
- **Vì sao trên tổng thể `Baseline` đạt `0.9484`?** Do `81.13%` (`959 / 1,182`) mẫu `TEST` là đường thông thoáng `LOS A` và **`94.84%` (`1,121 / 1,182`) cặp giữ nguyên mức `LOS` cũ sau 30 phút** (`1,121 / 1,182 = 0.9484`).
- **So sánh trực diện không pha trộn mô hình phụ ở 4 tình huống thực tế:**
  1. **Nhóm đổi trạng thái (`LOS(t) ≠ LOS(t+30p)`, `61` mẫu):** `Baseline` đoán sai `100%` (`0/61`, `0.00%`, `ROC-AUC = 0.5000`, `Brier = 0.3279`); `Random Forest (23 biến)` phát hiện sớm tín hiệu đổi pha đạt **`ROC-AUC = 0.7664`** (`Brier = 0.2948`, `PR-AUC = 0.5708` so với `0.5408`), đoán đúng mức mới **`9/61` (`14.75%`)** ở ngưỡng xác suất $\tau = 0.30$ chọn trên tập `VAL` (mặc định thận trọng đạt `2/61 = 3.28%`), và xếp đúng hướng mới vị trí `#1` đạt **`24/61` (`39.34%`)**.
  2. **Nhóm đường đang thoáng (`A–D`) đột ngột hóa kẹt cứng (`E–F`, chỉ có `8 / 993` mẫu):** Ghi nhận trung thực: do cỡ mẫu quá hiếm (`0.81%`), ở ngưỡng cứng mặc định cả hai bắt `0/8` ca (`0.00%`), nhưng xét theo xác suất cảnh báo sớm, `Random Forest (23 biến)` đạt **`ROC-AUC = 0.8136`** và đẩy xác suất rủi ro trung bình trên 8 ca sắp kẹt **cao gấp `9.35x`** đường thoáng ổn định (`3.46%` so với `0.37%`).
  3. **Nhóm tín hiệu GPS ngắt quãng trên `30 phút` (`3,513` mẫu, `47.45%` đổi mức):** `Random Forest (23 biến)` đạt `PR-AUC = 0.7082` vượt xa `Baseline = 0.6369` (đối chiếu chuẩn lịch sử: `PR-AUC = 0.7628`).
  4. **Vùng thiếu cảm biến toàn thành phố (`74,606 / 84,633` đoạn):** `Baseline` đạt độ phủ **`0.0%`**, trong khi `Random Forest (23 biến)` kết hợp kho đặc trưng bản đồ tĩnh phủ kín **`100.0%`**.

---

## Chương 6. Luồng Trực Tuyến (`Kafka 4.1 KRaft`), Bộ Đếm Gọn `33.0 KiB`, Đối Soát Tin Trễ & `MongoDB 8.0`

1. **Khác biệt bổ trợ giữa `Random Forest` (Chương 5) và `Điểm nóng EWMA` (Chương 6):** `Random Forest` dự báo tương lai `t + 30 phút` (*"đoạn đường nào SẮP kẹt?"*), trong khi `Điểm nóng EWMA` ($\alpha = 0.15$, tự xóa khóa $< 10^{-4}$) đo độ lì tích lũy ngay hiện tại (*"đoạn đường nào ĐANG kẹt dai dẳng nhiều giờ chưa thông?"*, bắt số 1 tại Lý Thường Kiệt `= 6.6652`), đi kèm `BitPackedBloomFilter` (`uint8[32768]` = **`32.0 KiB`**, chặn `334/334` tin trùng, `FPR = 0.616%`) và `HyperLogLogPlusPlus` (`uint8[1024]` = **`1.0 KiB`**, ước lượng `9,627.9 / 10,027` đoạn, sai số `3.98%`).
2. **Tự hồi phục sự cố (`WAL`):** `producer.py` (`seed=42`) phát `33,775` sự kiện cửa sổ và `91,847` bản tin thô; `starter_stream.py --kill-after-batch 1` tự hồi phục từ nhật ký `WAL` với **`0` bản ghi bị nhân đôi**.
3. **Đối soát cuối ngày & khóa phiên bản:** Tầng 1 khớp `100.0%` (`32,940 / 32,940`); Tầng 2 bù đủ `501` cửa sổ đến trễ (`98.50% -> 100.00%`), cập nhật lịch sử cho `3,953` cửa sổ kế tiếp và chấm điểm lại `377` cửa sổ; khóa phiên bản `status_version` chặn `25/25` bản tin luồng cũ ghi đè kết quả chuẩn `v2`.
4. **Kho phục vụ `MongoDB 8.0.34` & Giao diện `Streamlit (:8501)`:** Truy vấn trục chính trên chỉ mục kết hợp `level_prob_segment_idx` trả 25 đoạn đầu qua `IXSCAN` (`0` bước sắp xếp RAM), độ trễ **`warm_median = 0.530 ms`**, **`warm_p95 = 0.636 ms`**.

---

## Chương 7. Phân Công Nhóm & Quy Trình Tái Lập Toàn Hệ Thống

### 7.1. Phân Công Thực Thi Của Ba Thành Viên (`100% Hoàn thành` · GVHD: Đặng Trần Khánh)
- **Nguyễn Tạ Minh Quyền (`25MS23280`):** Bước 1 — Làm sạch dữ liệu, ước lượng tốc độ tối đa từ `TRAIN` & xử lý phân tán trên `Apache Spark 3.5.7` (`spark_batch.py`, `traffic_data/spark_etl.py`, `baseline.py`, `outputs/quyen_validation/*`).
- **Nguyễn Thành Đạt (`25MS23306`):** Bước 2 & Bước 3 — Đồ thị `PageRank`, `SON / FPGrowth`, phân nhóm `K-Means` phủ vùng thiếu GPS & mô hình `Random Forest` dự báo `+30 phút` (`pipeline.py`, `spark_engine.py`, `tests/`).
- **Phạm Công Minh (`25MS23310`):** Bước 4 — Luồng `Kafka 4.1.0 KRaft`, bộ đếm `33.0 KiB`, đối soát bù tin trễ, `MongoDB 8.0.34` & giao diện `Streamlit` 5 tab (`producer.py`, `starter_stream.py`, `app.py`, `docker-compose.yml`, `scripts/*`).

### 7.2. Quy Trình Tái Lập Toàn Bộ Kết Quả Từ Đầu (`PowerShell / Docker CLI`)
```powershell
# 1. Khởi chạy 4 dịch vụ container cốt lõi (10.5 GB / 16.0 GB RAM)
docker compose up -d kafka mongo spark app

# 2. Thực thi trọn vẹn dây chuyền xử lý 4 bước & kiểm chứng đối chiếu độc lập bộ gộp 5 đại lượng
python pipeline.py
python baseline.py

# 3. Thực thi luồng chịu lỗi WAL Kill/Restart & phát sự kiện Kafka
python producer.py --mode both --bootstrap-servers localhost:9092
python starter_stream.py --kill-after-batch 1

# 4. Xuất bản Sơ đồ Kiến trúc, Bản đồ TP.HCM, Báo cáo PDF & Bộ Slide Thuyết trình (.pptx + .pdf)
python scripts/render_arch_diagram.py
python scripts/render_visual_assets.py
python scripts/build_proposal_pdf.py
python scripts/build_slides.py

# 5. Chạy kiểm thử tự động toàn diện & kiểm tra chất lượng mã nguồn Ruff
pytest -v
ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
```
