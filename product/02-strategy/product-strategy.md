# Chiến Lược Sản Phẩm (Product Strategy): HCMC Smart Traffic Big Data Platform

## 1. Định Vị Chiến Lược (`Strategic Positioning`)

Nền tảng được định vị là hệ thống **Enterprise Traffic Lakehouse & Real-Time Stream Intelligence** hợp nhất 4 trụ cột thực hành của học phần **`BDA501`** với 5 chương thuật toán cốt lõi của giáo trình **`MMDS` (`Ch. 4, 5, 6, 7 & 12`)**, vận hành trọn vẹn trong ngân sách phần cứng `12.0 GB / 16.0 GB RAM` (`6` dịch vụ Docker mặc định).

---

## 2. Bốn Trụ Cột Chiến Lược Kỹ Thuật (`Core Strategic Pillars`)

| Trụ cột Chiến lược | Bài toán Giải quyết | Khác biệt Cốt lõi (`Wave 2 Verified`) |
| :--- | :--- | :--- |
| **1. `Zero-Conflict 3-Phase MLOps`** | Chấm dứt xung đột giữa `Batch Layer` và `Speed Layer` khi chia sẻ mô hình học máy và đặc trưng đồ thị. | `Phase A` (`Batch`) độc quyền gọi `.fit()` trên `1,885` cặp `TRAIN` hữu cơ (đã loại `3,596` cặp burst, không dùng `sample_weight` nhân tạo) và đóng băng `[C13a]`/`[C13b]` (`SHA-256`); `Phase B` & `Phase C` chỉ gọi `.transform()` chỉ đọc. |
| **2. `100% Clean Motorized Network Coverage`** | Giải quyết vùng mù cảm biến trên `88.13%` (`74,211 / 84,206`) đoạn cơ giới sạch chưa có GPS. | Lọc `427` đoạn phi đường bộ (`84,633 → 84,206`), tỉa `525,786` (`90.97%`) nút phụ (`52,181` active), dựng `Line-Graph PageRank` **bỏ U-turn** (`92,453` cạnh, `314` `Dangling Nodes`, `#1 21969 Nguyễn Kiệm PR=2.7799`) kết hợp `Train-Only 7D K-Means` (`3,609` đoạn hữu cơ). |
| **3. `Bounded O(1) Stream Sketching`** | Xử lý luồng bất tận với độ trễ mạng di động và bản ghi phát lại mà không làm tràn bộ nhớ `RAM`. | Sử dụng **`32.0 KiB RotatingBloomFilter` (`2 × 16 KiB`)** + **`1.0 KiB 64-bit HyperLogLog` (`p=10`, `uint8[1024]`)** + `Bounded Decaying EWMA` ($\alpha=0.15$, tự xóa khóa `< 10^{-4}`). |
| **4. `Self-Healing Two-Tier Reconciliation`** | Đảm bảo đồng thời tính tức thời và tính đầy đủ `100%` khi có `500` cửa sổ đến trễ quá `Watermark`. | `Tier 1` (`tier1_exceptAll_diff = 0`, khớp `100.0%` trong `Watermark`); `Tier 2` đọc lại `bronze_raw_events` (`90,787` pings) bù `500` cửa sổ trễ (`226` khung giờ), hàn gắn **`609` cửa sổ hạ lưu**, chấm lại **`384`** (`185` đổi xác suất) và bảo vệ bằng atomic **`$status_version` pipeline guard**. |

---

## 3. Đánh Giá Cổng Kiểm Soát `Gate 2 — Solution & Business Feasibility`

- [x] **Khả thi Kỹ thuật (`Technical Feasibility`):** Toàn bộ thuật toán (`Pure 5-Tuple Monoid`, `PageRank` không U-turn, `SON/FPGrowth` trên `TRAIN` hữu cơ, `7D K-Means`, `Clean 23D RF`, `RotatingBloom + 64-bit HLL`) vận hành ổn định trong `12.0 GB / 16.0 GB RAM`.
- [x] **Khả thi Dữ liệu (`Data Feasibility`):** Đã thiết lập cơ chế phòng vệ tự động cho toàn bộ `10 Empirical Edge Cases` (`EC-1`–`EC-10`) và loại trừ đợt burst tổng hợp `7,192` cửa sổ tháng 08/2020 khi huấn luyện ML.
- [x] **Kết luận `Gate 2`:** **PASSED.**
