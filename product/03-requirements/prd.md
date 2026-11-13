# Tài Liệu Đặc Tả Yêu Cầu Sản Phẩm (Product Requirements Document — PRD V3)

## 1. Phạm Vi Hệ Thống & Ranh Giới Giao Tiếp (`System Boundary Contract`)

Hệ thống tuân thủ hợp đồng ranh giới nghiêm ngặt gồm **2 Nguồn Dữ liệu Ngoại vi (`External Inputs`)**, **1 Kho Dẫn xuất Nội bộ (`Internal Derived Lakehouse & Feature Registry`)**, và **4 Đầu ra Phân tích & Phục vụ (`System Outputs`)**:
- **External Input 1 (`GIS Master Topology — [C2]`):** `nodes.csv` (`577,967` đỉnh), `streets.csv` (`5,553` tuyến đường), `segments.csv` (`84,633` đoạn đường có hướng).
- **External Input 2 (`GPS Telemetry Stream — [C1], [C4]`):** `segment_status.csv` (`90,938` bản tin GPS thô hợp lệ trong `pipeline.py`; `151` bản tin thuộc `427` đoạn `S3_NON_ROAD_TYPE` bị lọc trong `spark_batch.py`) phát lại theo `Event-Time ISO-8601 UTC` qua topic `hcmc-traffic-events` kèm `1%` bản ghi trùng (`334`) và `5%` độ trễ mạng (`1,170` trong `Watermark`, `501` ngoài `Watermark`).
- **Internal Derived Store (`[C13a/b]`, `[C14]`, `[C15]`):** Bảng cửa sổ lịch sử 30 phút (`33,441` dòng sinh độc lập từ GIS + `segment_status.csv`, với `7,898` cặp có nhãn đúng `+30m`), vector đặc trưng nhân quả `23D`, bảng đặc trưng tĩnh cho toàn bộ `84,633` đoạn đường (`segment_static_features.parquet`), mô hình đã đóng băng (`model.joblib`, `vmax_lookup.json`, `model_registry_manifest.json`), và Data Lakehouse `Medallion Parquet`.
- **System Outputs 1–4:**
  - `Output 1`: Dự báo `6-Class LOS` & xác suất kẹt nặng $\hat{p}_{\text{severe},t+30m}$ trên `MongoDB 8.0` (`STREAM_PROVISIONAL` v1 $\to$ `BATCH_VERIFIED` / `BATCH_RECONCILED` v2).
  - `Output 2`: Bảng xếp hạng điểm nghẽn `Directed Line-Graph PageRank` (`84,633` đoạn), `30` luật đồng kẹt xe `2-Pass SON/FP-Growth` + `20` luật lan truyền trễ `t -> t+30m`, và `6` cụm hành vi `7D K-Means`.
  - `Output 3`: Top-25 cảnh báo điểm nóng suy giảm mũ $C_s(t)$, thống kê `32.0 KiB Bit-Packed Bloom Filter` và `1.0 KiB HyperLogLog++`.
  - `Output 4`: Data Lakehouse `Gold Parquet` phân vùng theo `year_month`, nhật ký `Quarantine`, báo cáo `Two-Tier Reconciliation` và bằng chứng kế hoạch thực thi `BroadcastHashJoin` / `MongoDB IXSCAN`.

---

## 2. Yêu Cầu Chức Năng Theo 6 Kịch Bản Nghiệp Vụ (`Functional Requirements UC-1 – UC-6`)

| Mã Yêu Cầu | Kịch Bản Nghiệp Vụ (`Use Case`) | Thành Phần Tham Gia (`[C1]`–`[C17]`) | Yêu Cầu Xử Lý Chi Tiết |
| :--- | :--- | :--- | :--- |
| **`FR-UC1`** | Cảnh báo Điểm nóng Ùn tắc Kéo dài Thời gian thực | `[C1] -> [C4] -> [C6] -> [C7] -> [C14] -> [C17]` | Cập nhật điểm suy giảm mũ $C_s(t) = (1-\alpha)C_s(t-1) + \mathbb{I}(\text{LOS} \in \{E,F\})$ kèm cắt tỉa dưới ngưỡng $\epsilon = 10^{-4}$, gắn nhãn `CRITICAL (E/F)` khi $C_s(t) \ge 2.5$ và xuất `Top-25 Hotspots`. |
| **`FR-UC2`** | Truy vấn Dự báo Tắc nghẽn `t+30m` Dưới 10ms | `[C8] & [C12] -> [C13a/b] -> [C16] -> [C17]` | Chấm điểm cửa sổ 30 phút bằng `.transform()` chỉ đọc (xác thực `SHA-256` trước và sau khi chấm điểm); ghi vào `MongoDB 8.0` qua `UpdateOne(upsert=True)`; phục vụ truy vấn lọc `street_level IN [1,2]` & `severe_prob >= 0.70` bằng `Compound Index IXSCAN`. |
| **`FR-UC3`** | Kiểm toán Điểm nghẽn Cấu trúc & Vùng thiếu GPS | `[C2] -> [C5] -> [C9] -> [C10] -> [C13b] -> [C17]` | Lọc bỏ `525,542` điểm uốn cong trung gian (`shape points`), dựng đồ thị nối đoạn đường (`84,633` đỉnh, `151,929` cạnh) và chạy 25 vòng lặp `PageRank` ($\beta=0.85$) có chia đều lại xác suất `48` đoạn cụt; phủ điểm không gian cho `74,606` đoạn đường chưa có GPS. |
| **`FR-UC4`** | Dự báo & Ngăn chặn Lan truyền Tắc nghẽn Liên trục | `[C5] -> [C9] -> [C10] -> [C14] -> [C17]` | Chuẩn hóa tên đường về `Unicode NFC`, loại bỏ tiền tố/hậu tố hành chính (`Phường/Quận`), gom `Transaction Baskets` `(date, period)` có `LOS E/F` không cắt ngọn và khai phá `30` `Association Rules` hai chiều $\{\text{Street}_A\} \Rightarrow \{\text{Street}_B\}$ cùng `20` luật trễ `t -> t+30m`. |
| **`FR-UC5`** | Phân cụm Chế độ Giao thông & Tìm Đoạn Tương đồng | `[C10] -> [C13b] -> [C17]` | Huấn luyện `7D Behavioral K-Means` ($k=6$) có `StandardScaler` chỉ trên tập `TRAIN` đã lọc ranh giới (`6,926` đoạn, `Silhouette = 0.2853`, `Davies-Bouldin = 1.1181`), gán cụm cho toàn bộ `84,633` đoạn đường và truy xuất `Top-K` đoạn đường tương đồng nhất (hỗ trợ cả đoạn `Cold-Start`). |
| **`FR-UC6`** | Kiểm toán Cách ly Dữ liệu Lỗi & `Two-Tier Reconciliation` | `[C5] -> [C15]` & `[C6]/[C9] -> [C12] -> [C16] -> [C17]` | Phân loại bản ghi vi phạm theo quy tắc lỗi đầu tiên (`first-failing rule`) vào `Quarantine Store`; kiểm chứng `Tier 1 exceptAll == 0` trong `Watermark` và tự động `backfill` `501` cửa sổ đoạn đường trễ (`219` khung giờ) cùng `377` `downstream windows` ở `Tier 2`. |

---

## 3. Yêu Cầu Phi Chức Năng & Phòng Vệ 10 `Edge Cases` (`Non-Functional & Edge Case Requirements EC-1 – EC-10`)

1. **`NFR-EC1` (`3-Tier Train-Only Velocity Imputation`):** Nội suy `100%` giá trị `max_velocity` bị khuyết (`85.21%` trong `train.csv`, `88.34%` trong `segments.csv`) bằng bảng tra cứu bất biến `outputs/vmax_lookup.json` chỉ khớp trên tập `TRAIN`: `Tier 1 Observed / TRAIN Segment P85` ($\ge 5$ quan sát) $\to$ `Tier 2` trung vị `(street_level, street_type)` trên `TRAIN` $\to$ `Tier 3` prior theo `street_level` (`{1:80, 2:60, 3:50, 4:40}` km/h).
2. **`NFR-EC2` (`Ordered Quality Funnel & Zero-Length Guard`):** Không làm sập tiến trình hoặc rơi rớt ngầm khi gặp `154` đoạn `length <= 0m` hoặc chuỗi số lỗi; cách ly vào `quarantine_events.parquet` theo quy tắc lỗi đầu tiên và gán chiều dài trung vị tập `TRAIN` (`27.0m`).
3. **`NFR-EC3` (`Canonical Unicode NFC Street Hygiene`):** Đồng nhất `100%` chuỗi `NFD/NFC`, `Ð/Đ`, tiền tố `Đường ` và cắt bỏ tiền tố/hậu tố hành chính (`Phường/Quận`) trên `2,071` cửa sổ (`gộp 46 thực thể đường trùng`) để triệt tiêu Association Rules tự suy diễn.
4. **`NFR-EC4` (`UTC-to-ICT Shift & Cyclical Encoding`):** Giữ khóa ghép `UTC` để khớp dữ liệu gốc, đồng thời cộng `+7h` sang giờ `ICT` (xử lý lùi ngày/thứ qua nửa đêm `ict_date`, `ict_weekday`, `is_weekend`) và mã hóa điều hòa $(\sin, \cos)$ cho `48` khung giờ.
5. **`NFR-EC5` (`Anti-Leakage Causal Expanding Priors`):** Tuyệt đối không dùng trung bình tĩnh trên tập `TRAIN`; chỉ tính `Empirical-Bayes Prior` trên cửa sổ quá khứ chặt $\tau < t$ (`shift(1)` trước `cumsum()`, với `35%` đầu tập `TRAIN` làm `Warm-up Anchor`, $m_{\text{seg}}=5, m_{\text{street}}=10$).
6. **`NFR-EC6` (`Gap-Aware Lags & Cold-Start Fallback`):** Đưa `gap_minutes` vào vector đặc trưng `23D` (và `is_contiguous_30m` trong `data/dataset.csv`); hỗ trợ suy diễn cho `88.15%` đoạn đường `Cold-Start` qua `[C13b]`.
7. **`NFR-EC7` (`Shape-Point Filtering & Dangling Mass Redistribution`):** Lọc bỏ `525,542` điểm uốn cong trung gian (`shape points`) từ OSM và chia đều lại xác suất của `48` đoạn cụt ở mỗi vòng lặp `PageRank`.
8. **`NFR-EC8` (`Bit-Packed Bloom Dedup, Commutative Monoid & 2-Stage Key Salting`):** Khử trùng lặp bằng `32.0 KiB Bit-Packed Bloom Filter` (`uint8[32768]`), cộng gộp qua bộ 5 phần tử `Commutative Monoid` và gắn `salt` $\in \{0..7\}$ giảm lệch tải trên khung giờ nóng từ `687.43x` xuống `1.023x` (`salted_partition_skew_ratio = 1.0117`).
9. **`NFR-EC9` (`Dual-Sink Raw Preservation & Post-Watermark Backfill`):** Lưu `100%` sự kiện hợp lệ ở `Sink 1` (`outputs/spark_bronze_stream_sink` / `bronze_raw_events/`) để `Phase C` bù trừ `501` sự kiện trễ quá `Watermark` (`98.50% -> 100.0%`).
10. **`NFR-EC10` (`WAL Checkpoints, Version Guard & Partitioned Idempotent Upsert`):** Lưu trạng thái `WAL` tại `outputs/checkpoints/`, bảo vệ thứ tự phiên bản (`status_version`: v1 `STREAM_PROVISIONAL` không bao giờ ghi đè v2 `BATCH_RECONCILED`) và ghi vào `MongoDB 8.0` theo lô `500` lệnh `UpdateOne(upsert=True)` trên 4 phân vùng băm.
