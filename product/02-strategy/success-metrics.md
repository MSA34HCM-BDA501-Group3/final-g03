# Chỉ Số Đo Lường Thành Công & Cam Kết SLA (Success Metrics & KPIs)

## 1. Hệ Thống Chỉ Số Kỹ Thuật & Học Máy (`Engineering & ML KPIs — Wave 2 Verified`)

| Nhóm Chỉ số (`KPI Category`) | Chỉ số Cụ thể (`Metric`) | Mục tiêu Cam kết (`Target SLA`) | Kết quả Thực nghiệm Đạt được (`Verified Actual`) |
| :--- | :--- | :--- | :--- |
| **1. Chất lượng Dữ liệu (`P1`, `[C5]`)** | Lọc đoạn phi đường bộ & khớp khung `30m UTC` | `100.0%` bảo toàn quân số (`0` Silent Drop) | **`90,938 = 90,787` hợp lệ + `151` cách ly; `33,394` cửa sổ cơ giới (`100.0%` khớp `baseline.py`)** |
| | Đối chiếu thang đo `LOS` (`derive_urban_los`) | Khớp chuẩn đô thị TP.HCM (`BkTraffic 5 km/h`) | **`89.19%` exact match** trên `33,394` cửa sổ (tách bạch với `derive_los` tỷ lệ) |
| | Độ phủ nội suy `max_velocity` (`3-Tier`) | `100.0%` (từ `85.21%` khuyết) | **`100.0%` (`33,394` cửa sổ & `84,206` đoạn cơ giới)** |
| **2. Khai phá Đồ thị & Mẫu (`P2`, `[C10]`)** | Độ phủ `Directed Line-Graph PageRank` (bỏ U-turn) | `100.0%` mạng cơ giới (`84,206` đoạn) | **`84,206` đoạn (`92,453` cạnh, `314` dangling, `#1 21969 Nguyễn Kiệm PR=2.7799`)** |
| | Khai phá luật kết hợp trên `TRAIN` hữu cơ | `0%` luật tự suy diễn, tách hold-out | **`12` co-rules (`Lift 15.23`) & `14` lagged rules (`Lift 24.0`, `persistence_controlled=True`)** |
| | Phân cụm `Train-Only 7D K-Means` ($k=6$) | Học thuần trên `TRAIN` hữu cơ không burst | **`Silhouette = 0.2197`, `DBI = 1.3592` (`3,609` đoạn hữu cơ; `6` cụm)** |
| **3. Dự báo Tắc nghẽn (`P3`, `[C11]`)** | `Severe Congestion (LOS E/F) PR-AUC` (`15% TEST`, `t+30m`) | `>= 0.7200` | **`0.9595`** (`1,182` mẫu, sạch không `sample_weight` / không burst) / `0.7441` (legacy `shift(-1)`) |
| | `Severe Congestion (LOS E/F) F1-Score` (`15% TEST`, `t+30m`) | `>= 0.6600` | **`0.9301`** (`Prec = 0.9251, Rec = 0.9351`, 95% CI `[0.9012, 0.9547]`) / `0.6178` (legacy) |
| | `Severe Congestion (LOS E/F) ROC-AUC` & `6-Class` (`TEST`) | `>= 0.8400` | **`ROC-AUC = 0.9900`** (95% CI `[0.9836, 0.9953]`), `Acc = 0.9289`, `Macro-F1 = 0.4475` *(mất cân bằng `B/C/D/E`)* |
| | Vượt `Persistence` khi chuyển trạng thái (`61` mẫu) | Có ý nghĩa thống kê (`p < 0.05`) | **`McNemar p = 0.015625` (`7 vs 0`), `Detect ROC-AUC = 0.9146`, `Onset risk lift = 3.32x`** |
| **4. Xử lý Luồng & Đối soát (`P4`, `[C7]`, `[C12]`)** | Lọc bản ghi trùng (`32.0 KiB Bloom / RotatingBloom`) | `100.0%` (`FPR < 3%`) | **`100.0%` (`333 / 333` trùng, `FPR = 0.6109%`, `32,768` bytes)** |
| | Sai số tương đối `1.0 KiB 64-bit HLL` (`1,024` thanh ghi) | `< 5.0%` | **`3.75%` (`9,620.2` ước lượng vs `9,995` thực tế, `1,024` bytes)** |
| | Khớp trong `Watermark` (`Tier 1`) & Hồi phục `Tier 2` | `tier1_exceptAll_diff = 0`, `100%` completeness | **`0` diff (`100.0%`), bù `500` trễ (`226` slots), hàn gắn `609` hạ lưu, chấm lại `384` (`185` đổi xác suất), `WAL = 73.7 KB`** |
| **5. Spark & Serving (`[C9]`, `[C16]`, `[C17]`)** | `Spark 3.5.7` Catalyst Join & Monoid parity | `BroadcastHashJoin`, `exceptAll = 0` | **`5 → 1` Exchange (`145.24 → 120.45 ms`, `17.07%` speedup), `0` diff; Sink 2 WM `2020-12-01T12:30:00Z`** |
| | `MongoDB 8.0` (`UC-2` Arterial Query) | `IXSCAN`, `0` bước `SORT`, `< 10ms` | **`IXSCAN warm_median = 0.389 ms / p95 = 0.692 ms` vs `COLLSCAN 9.161 ms` (`23.55x`), atomic `$status_version` guard** |
