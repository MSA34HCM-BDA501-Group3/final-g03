# Tài Liệu Đặc Tả Yêu Cầu Sản Phẩm (Product Requirements Document — PRD V3, Wave 2)

## 1. Phạm Vi Hệ Thống & Ranh Giới Giao Tiếp (`System Boundary Contract`)

Hệ thống tuân thủ hợp đồng ranh giới nghiêm ngặt gồm **2 Nguồn Dữ liệu Ngoại vi (`External Inputs`)**, **1 Kho Dẫn xuất Nội bộ (`Internal Derived Lakehouse & Feature Registry`)**, và **4 Đầu ra Phân tích & Phục vụ (`System Outputs`)**:
- **External Input 1 (`GIS Master Topology — [C2]`):** `nodes.csv` (`577,967` đỉnh $\to$ **`52,181` nút giao hoạt động** sau tỉa `525,786` = `90.97%`), `streets.csv` (`5,553` tuyến đường), `segments.csv` (`84,633` đoạn thô $\to$ **`84,206` đoạn cơ giới sạch** sau loại `427` đoạn phi đường bộ).
- **External Input 2 (`GPS Telemetry Stream — [C1], [C4]`):** `segment_status.csv` (`90,938` bản tin thô $\to$ **`90,787` bản tin cơ giới hợp lệ**, cách ly `151` bản tin phi đường bộ) phát lại theo `Event-Time ISO-8601 UTC` qua topic `hcmc-traffic-events` (`33,727` sự kiện cửa sổ = `33,394` gốc + `333` trùng, `1,168` trễ trong `Watermark`, `500` trễ ngoài `Watermark` tại `226` khung giờ).
- **Internal Derived Store (`[C13a/b]`, `[C14]`, `[C15]`):** Bảng cửa sổ `33,394` dòng (`7,885` cặp đúng `+30m`, đã gắn cờ `7,192` cửa sổ burst tổng hợp), vector đặc trưng nhân quả `23D`, bảng tĩnh cho **`84,206` đoạn cơ giới** (`segment_static_features.parquet`), mô hình đã đóng băng (`model.joblib`, `vmax_lookup.json`, `model_registry_manifest.json`), và Data Lakehouse `Medallion Parquet`.
- **System Outputs 1–4:**
  - `Output 1`: Dự báo `6-Class LOS` & xác suất kẹt nặng $\hat{p}_{\text{severe},t+30m}$ trên `MongoDB 8.0` (`32,405` `BATCH_VERIFIED v2` + `500` `BATCH_RECONCILED v2` + `489` `STREAM_PROVISIONAL v1` = `33,394` docs).
  - `Output 2`: Bảng xếp hạng `Line-Graph PageRank` (`84,206` đoạn, `92,453` cạnh bỏ U-turn, `#1 21969 Nguyễn Kiệm PR=2.7799`), `12` co-rules + `14` lagged rules (`persistence_controlled=True`) trên `TRAIN` hữu cơ, và `6` cụm `7D K-Means`.
  - `Output 3`: Top-25 cảnh báo điểm nóng `EWMA` $C_s(t)$, thống kê `32.0 KiB RotatingBloom` (`2 × 16 KiB`) và `1.0 KiB 64-bit HLL`.
  - `Output 4`: Lakehouse (`bronze_raw_events`, `stream_replay_windows`, `silver_cleaned_windows`, `curated_gold/year_month=*`), `Quarantine` (`580` dòng), `WAL` (`73.7 KB`), bằng chứng `Spark 3.5.7` (`5 → 1` Exchange) & `MongoDB 8.0 IXSCAN` (`0.389 / 0.692 ms`), và bộ đôi giao diện **Web TOC `:8000`** + **Streamlit `:8501`**.

---

## 2. Yêu Cầu Chức Năng Theo 6 Kịch Bản Nghiệp Vụ (`Functional Requirements UC-1 – UC-6`)

| Mã Yêu Cầu | Kịch Bản Nghiệp Vụ (`Use Case`) | Thành Phần Tham Gia (`[C1]`–`[C17]`) | Yêu Cầu Xử Lý Chi Tiết |
| :--- | :--- | :--- | :--- |
| **`FR-UC1`** | Cảnh báo Điểm nóng Ùn tắc Kéo dài Thời gian thực | `[C1] -> [C4] -> [C6] -> [C7] -> [C14] -> [C17]` | Cập nhật điểm `EWMA` ($\alpha=0.15$) cắt tỉa dưới $\epsilon = 10^{-4}$, xuất `Top-25 Hotspots` (`#1 47407 Lý Thường Kiệt 6.6652`) lên Web TOC `:8000` & Streamlit `:8501`. |
| **`FR-UC2`** | Truy vấn Dự báo Tắc nghẽn `t+30m` Dưới 1 ms | `[C8] & [C12] -> [C13a/b] -> [C16] -> [C17]` | Chấm điểm bằng `.transform()` chỉ đọc (xác thực `SHA-256`); ghi vào `MongoDB 8.0` kèm **atomic `$status_version` pipeline guard**; truy vấn `street_level IN [1,2]` & `severe_prob >= 0.70` (`1,202` cửa sổ) qua `IXSCAN` (`0.389 / 0.692 ms`). |
| **`FR-UC3`** | Kiểm toán Điểm nghẽn Cấu trúc & Vùng thiếu GPS | `[C2] -> [C5] -> [C9] -> [C10] -> [C13b] -> [C17]` | Tỉa `525,786` (`90.97%`) nút phụ, loại `427` phi đường bộ & cung `U-turn`, chạy `PageRank` trên `84,206` đỉnh (`92,453` cạnh, `314` `dangling nodes`) phủ `74,211` đoạn `Cold-Start` (`88.13%`). |
| **`FR-UC4`** | Dự báo & Ngăn chặn Lan truyền Tắc nghẽn Liên trục | `[C5] -> [C9] -> [C10] -> [C14] -> [C17]` | Chuẩn hóa tên đường `Unicode NFC` (`695 → 641`), loại danh xưng hành chính (`631` `R3`), khai phá **chỉ trên `TRAIN` hữu cơ**: `12` co-rules (`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.23`) & `14` lagged rules (`Lê Quang Định → Trần Quang Khải`, `Lift = 24.0`, `persistence_controlled=True`). |
| **`FR-UC5`** | Phân cụm Chế độ Giao thông & Tìm Đoạn Tương đồng | `[C10] -> [C13b] -> [C17]` | Huấn luyện `7D Behavioral K-Means` ($k=6$) trên **`3,609` đoạn `TRAIN` hữu cơ** (`Silhouette = 0.2197`, `DBI = 1.3592`), gán cụm cho đủ `84,206` đoạn cơ giới và truy xuất `Top-K` đoạn tương đồng. |
| **`FR-UC6`** | Kiểm toán Cách ly Dữ liệu Lỗi & `Two-Tier Reconciliation` | `[C5] -> [C15]` & `[C6]/[C9] -> [C12] -> [C16] -> [C17]` | Lưu `580` bản ghi GIS + `151` ping phi đường bộ vào `Quarantine`; kiểm chứng `tier1_exceptAll_diff = 0`, backfill `500` cửa sổ trễ (`226` khung giờ), hàn gắn **`609` cửa sổ hạ lưu** và chấm lại **`384` cửa sổ** (`185` đổi xác suất). |

---

## 3. Yêu Cầu Phi Chức Năng & Phòng Vệ 10 `Edge Cases` (`Non-Functional & Edge Case Requirements EC-1 – EC-10`)

1. **`NFR-EC1` (`3-Tier Train-Only Velocity Imputation`):** Nội suy `100%` giá trị `max_velocity` bị khuyết bằng `outputs/vmax_lookup.json` chỉ học trên `TRAIN` hữu cơ (`4,946` Observed + `9,376` P85 + `17,693` Tier 2 + `1,379` Tier 3).
2. **`NFR-EC2` (`Ordered Quality Funnel & Zero-Length Guard`):** Cách ly `580` bản ghi GIS (`154` `R1 length <= 0` điền trung vị `27.0m`; `427` `R2` phi đường bộ loại khỏi đồ thị/ETL) và `151` ping phi đường bộ (`90,938 → 90,787`) với `0` mất mát ngầm.
3. **`NFR-EC3` (`Canonical Unicode NFC Street Hygiene`):** Đồng nhất `Unicode NFC`, `Ð/Đ`, tiền tố `Đường ` trên `2,247` cửa sổ (`695 → 641` tên chuẩn, gộp `54` biến thể) và loại `631` bản ghi hành chính khỏi giỏ khai phá luật.
4. **`NFR-EC4` (`UTC-to-ICT Shift & Cyclical Encoding`):** Giữ khóa `UTC` (`33,394` cửa sổ), tính chính xác giờ `ICT` gồm `4,305` cửa sổ qua nửa đêm (`12.89%`) và mã hóa điều hòa $(\sin, \cos)$.
5. **`NFR-EC5` (`Burst Filtering & Anti-Leakage Causal Priors`):** Phát hiện và loại `7,192` cửa sổ burst (`3,596` cặp `TRAIN` burst) khỏi huấn luyện ML (`1,885` `TRAIN` hữu cơ); không dùng `sample_weight` nhân tạo; dùng `Causal Expanding Priors` ($\tau < t$) với $m_{\text{seg}}=5, m_{\text{street}}=10$.
6. **`NFR-EC6` (`Gap-Aware Lags & Cold-Start Fallback`):** Đưa `gap_minutes` vào `23D` (và `is_contiguous_30m` trong `data/dataset.csv`, `60` cột); kiểm chứng `Masked-GPS Spatial Cold-Start Proxy` (`84,206` tổng / `74,211` cold-start `88.13%` được phủ `100%`).
7. **`NFR-EC7` (`Shape-Point Filtering, No U-turn & Dangling Redistribution`):** Tỉa `525,786` (`90.97%`) nút phụ, loại cung U-turn trên đoạn hai chiều (`92,453` cạnh) và phân phối lại xác suất `314` đoạn cụt trong `PageRank`.
8. **`NFR-EC8` (`RotatingBloom, Pure 5-Tuple Monoid & 2-Stage Key Salting`):** `32.0 KiB RotatingBloom` (`2 × 16 KiB`), **Pure 5-Tuple Monoid** (`0` sai lệch `baseline.py`), `8` salt buckets hạ lệch tải khung giờ cao điểm `684.95x → 1.0197x` (`salted_partition_skew_ratio = 1.0123x`, `CV = 0.0059`).
9. **`NFR-EC9` (`Dual-Sink Raw Preservation & Post-Watermark Backfill`):** Lưu `90,787` raw pings vào `bronze_raw_events/events.parquet` và `33,727` replay windows vào `stream_replay_windows/replay.parquet` để `Phase C` bù `500` cửa sổ trễ (`98.5% → 100.0%`).
10. **`NFR-EC10` (`WAL 73.7 KB, Atomic $status_version Guard & Idempotent Upsert`):** Lưu `WAL` (`73,713 B`), bảo vệ phiên bản bằng **atomic `$status_version` pipeline guard** trên `MongoDB 8.0` (`25/25` stale v1 bị từ chối) và `Bulk UpdateOne(upsert=True)` trên `4` phân vùng (`69` lô).
