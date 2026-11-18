# Tiêu Chí Nghiệm Thu Hệ Thống (Acceptance Criteria — Gate 3 Checklist, Wave 2)

## 1. Tiêu Chí Nghiệm Thu Cho 4 Bài Toán Con (`P1`–`P4`)

- **`AC-P1` (`Descriptive Windowing & Quality Funnel`):**
  - [x] Lọc bỏ `427` đoạn phi đường bộ (`84,633 → 84,206` đoạn cơ giới) và tách `151` bản tin GPS rơi ngoài đường bộ (`90,938 = 90,787` hợp lệ + `151` cách ly + `0` dedup; `status_accounting_balanced = true`), tạo **`33,394` cửa sổ 30 phút UTC**.
  - [x] `3-Tier Hierarchical Velocity Imputation` học trên `TRAIN` hữu cơ điền khuyết `100.0%` (`4,946` Observed + `9,376` P85 + `17,693` Tier 2 + `1,379` Tier 3 trên `33,394` cửa sổ; `9,869 + 791 + 51,615 + 21,931 = 84,206` đoạn).
  - [x] `derive_urban_los` (`BkTraffic 5 km/h`) khớp **`89.19%`** tham chiếu `train.csv` (tách bạch rõ với `derive_los` tỷ lệ).
  - [x] Phép gộp **Pure 5-Tuple Monoid** $\mathbf{m} = (\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt})$ khớp **`100.0%`** (`0` sai lệch, `0.0 km/h` diff) so với `baseline.py` trên `33,394` cửa sổ.
- **`AC-P2` (`Diagnostic Graph, Rule & Cluster Mining`):**
  - [x] Tỉa bỏ **`525,786` điểm uốn cong/mồ côi (`90.97%`)**, giữ **`52,181` nút giao hoạt động**; dựng `Directed Line Graph` **bỏ U-turn** gồm **`84,206` đỉnh**, **`92,453` cạnh chuyển tiếp** và **`314` `Dangling Nodes`**; `#1 PageRank`: **`21969 — Nguyễn Kiệm (level=2, PR=2.7799)`**.
  - [x] Khai phá chỉ trên `TRAIN` hữu cơ + kiểm chứng hold-out: **`12` luật đồng kẹt** (`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.2308`) và **`14` luật lan truyền trễ** (`Lê Quang Định → Trần Quang Khải`, `Lift = 24.0`, `persistence_controlled = True`) trên tên đường `Unicode NFC` (`695 → 641` tên, `54` biến thể gộp).
  - [x] Phân cụm `Train-Only 7D Behavioral K-Means` ($k=6$) trên **`3,609` đoạn `TRAIN` hữu cơ** đạt `Silhouette = 0.2197` và `Davies-Bouldin = 1.3592`, phủ kín `84,206` đoạn (`74,211` Cold-Start `88.13%`).
- **`AC-P3` (`Burst-Filtered Clean Predictive ML`):**
  - [x] Phát hiện `7,192` cửa sổ burst tổng hợp (`2020-08-02 23:30` & `2020-08-03 00:00`, `3,609` thô / `3,596` cơ giới mỗi slot) và loại `3,596` cặp burst khi huấn luyện, giữ **`1,885` `TRAIN` hữu cơ (purged `50`) / `1,167` `VAL` (purged `5`) / `1,182` `TEST` (`185` E/F)** trong `7,885` cặp đúng `+30m`.
  - [x] Huấn luyện sạch (`class_weight` lũy thừa `0.65`, không `sample_weight` nhân tạo hay fallback bypass): `RF 23D` thắng cổng `VAL` (`0.8990` vs `0.5343` `14D`; `VAL acc = 0.9023`) và đạt trên `TEST`: **`ROC-AUC = 0.9900`**, **`PR-AUC = 0.9595`**, **`Acc = 0.9289`**, **`Severe F1 = 0.9301`** (`Prec = 0.9251, Rec = 0.9351`), **`Macro-F1 = 0.4475`**, đường chéo `[937, 4, 1, 7, 2, 147]`; vượt `Persistence` khi chuyển trạng thái (`McNemar p = 0.015625`, `7 vs 0`, `Detect ROC-AUC = 0.9146`, `Onset risk lift = 3.32x`) và qua kiểm định `Masked-GPS Spatial Cold-Start Proxy`.
- **`AC-P4` (`Stream Sketching, Lakehouse, Two-Tier Recon & MongoDB Serving`):**
  - [x] `32.0 KiB RotatingBloom` / `BitPackedBloom` chặn `100%` tin trùng (`333/333`, `FPR = 0.6109%`); `1.0 KiB 64-bit HLL` ước lượng `9,620.2 / 9,995` (`3.75%` sai số); `WAL = 73.7 KB` crash recovery `0` duplicate side effects.
  - [x] Lakehouse (`bronze_raw_events` `90,787` pings, `stream_replay_windows` `33,727`, `curated_gold/year_month=*` `10` phân vùng) đọc sạch bởi cả PyArrow/Pandas và Spark 3.5.7; Spark Catalyst `5 → 1` Exchange (`145.24 → 120.45 ms`, `17.07%` speedup, `0 exceptAll diff`) và Streaming Sink 2 watermark thực `2020-12-01T12:30:00Z`.
  - [x] `Tier 1` `tier1_exceptAll_diff_count = 0` (`100.0%`); `Tier 2` bù `500` cửa sổ trễ (`226` khung giờ, hoàn thiện `98.5% → 100.0%`), hàn gắn **`609` cửa sổ hạ lưu**, chấm lại **`384`** (`185` đổi xác suất).
  - [x] `MongoDB 8.0.34` (`33,394` docs) xác nhận atomic **`$status_version` pipeline guard** (`25/25` stale v1 rejected) và `Compound Index IXSCAN` (`warm_median = 0.389 ms / p95 = 0.692 ms` vs `COLLSCAN median = 9.161 ms`, **`23.55x`**), phục vụ **Web TOC `:8000`** & **Streamlit `:8501`**.
