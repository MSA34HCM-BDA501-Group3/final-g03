# Chỉ Số Đo Lường Thành Công & Cam Kết SLA (Success Metrics & KPIs)

## 1. Hệ Thống Chỉ Số Kỹ Thuật & Học Máy (`Engineering & ML KPIs`)

| Nhóm Chỉ số (`KPI Category`) | Chỉ số Cụ thể (`Metric`) | Mục tiêu Cam kết (`Target SLA`) | Kết quả Thực nghiệm Đạt được (`Verified Actual`) |
| :--- | :--- | :--- | :--- |
| **1. Chất lượng Dữ liệu (`P1`, `[C5]`)** | Tỷ lệ khớp khung 30m `UTC` giữa `segment_status` & `train` | `100.0%` | **`100.0%` (`33,441 / 33,441` cửa sổ)** |
| | Độ phủ nội suy `max_velocity` (`3-Tier Imputation`) | `100.0%` (từ `85.21%` khuyết) | **`100.0%` (`33,441` cửa sổ & `84,633` đoạn)** |
| | Tỷ lệ rơi rớt bản ghi ngầm (`Silent Drop Rate`) | `0.0%` (mọi lỗi phải vào `Quarantine`) | **`0.0%` (có nhật ký `quarantine_summary.csv`)** |
| **2. Khai phá Đồ thị & Mẫu (`P2`, `[C10]`)** | Độ phủ xếp hạng `Directed Line-Graph PageRank` | `100.0%` mạng lưới (`84,633` đoạn) | **`84,633` đoạn (`151,929` cạnh, $\sum PR = 84{,}633$)** |
| | Tỷ lệ luật tự suy diễn bẩn trong `SON / FPGrowth` | `0.0%` (nhờ chuẩn hóa `Unicode NFC`) | **`0.0%` (`30` luật sạch, `Lift` tối đa `28.5833x`)** |
| | Chất lượng phân cụm `Train-Only 7D K-Means` ($k=6$) | `Silhouette > 0.25`, `DBI < 1.30` | **`Silhouette = 0.3158`**, **`DBI = 1.1462`** |
| **3. Dự báo Tắc nghẽn (`P3`, `[C11]`)** | `Severe Congestion (LOS E/F) PR-AUC` trên `15% TEST` | `>= 0.7200` (vượt `Persistence 0.6369`) | **`0.7568` (`+11.99%` so với `Persistence`)** |
| | `Severe Congestion (LOS E/F) F1-Score` trên `15% TEST` | `>= 0.6600` (vượt `Local RF 0.6570`) | **`0.6670` (`Recall = 0.6262`, `Precision = 0.7135`)** |
| | `Severe Congestion (LOS E/F) ROC-AUC` trên `15% TEST` | `>= 0.8400` | **`0.8458`** (`RF +Graph`) / **`0.8509`** (`RF Local`) |
| **4. Xử lý Luồng & Đối soát (`P4`, `[C7]`, `[C12]`)** | Tỷ lệ bắt bản ghi trùng bằng `32 KB Bloom Filter` | `100.0%` (`FPR < 3%`) | **`100.0%` (`334 / 334` trùng, `FPR = 0.6908%`)** |
| | Sai số tương đối `1 KB HyperLogLog++` (`1,024` thanh ghi) | `< 5.0%` | **`3.98%` (`9,627.9` ước lượng vs `10,027` thực tế)** |
| | Khớp thuật toán trong `Watermark` (`Tier 1 exceptAll`) | `100.0%` (`0` dòng lệch) | **`100.0%` (`32,940 / 32,940` sự kiện trong `WM`)** |
| | Độ đầy đủ trước & sau đối soát (`Tier 2 Completeness`) | `98.50% -> 100.0%` | **`98.50%` (`501` trễ) $\to$ `100.0%` (`319` cửa sổ bù)** |
| **5. Phục vụ Truy vấn (`[C16]`, `[C17]`)** | Kế hoạch truy vấn `MongoDB 8.0` (`UC-2` Arterial Query) | `IXSCAN`, `0` bước `SORT`, `< 10ms` | **`IXSCAN` (`in_memory_sort = False`, `< 5ms`)** |
