# Chiến Lược Sản Phẩm (Product Strategy): HCMC Smart Traffic Big Data Platform

## 1. Định Vị Chiến Lược (`Strategic Positioning`)

Nền tảng được định vị là hệ thống **Enterprise Traffic Lakehouse & Real-Time Stream Intelligence** hợp nhất 4 trụ cột thực hành của học phần **`BDA501` (`Labs I1, I2, I3 & Session 7`)** với 5 chương thuật toán cốt lõi của giáo trình **`MMDS` (`Ch. 4, 5, 6, 7 & 12`)**, vận hành trọn vẹn trong ngân sách phần cứng tiêu chuẩn `10.5 GB / 16 GB RAM`.

---

## 2. Bốn Trụ Cột Chiến Lược Kỹ Thuật (`Core Strategic Pillars`)

| Trụ cột Chiến lược | Bài toán Giải quyết | Khác biệt Cốt lõi (`Key Differentiators`) |
| :--- | :--- | :--- |
| **1. `Zero-Conflict 3-Phase MLOps`** | Chấm dứt xung đột giữa `Batch Layer` và `Speed Layer` khi chia sẻ mô hình học máy và đặc trưng đồ thị. | `Phase A` (`Batch` $t \le T_{\text{train}}$) độc quyền gọi `.fit()` và đóng băng `[C13a]`/`[C13b]`; `Phase B` (`Stream`) và `Phase C` (`Reconciliation`) chỉ gọi `.transform()` chỉ đọc. |
| **2. `100% Network Topology Coverage`** | Giải quyết vùng mù cảm biến trên `88.15%` (`74,606 / 84,633`) đoạn đường chưa có tín hiệu GPS. | Kết hợp `Directed Line-Graph PageRank` (`84,633` đỉnh, `151,929` cạnh) với `Static Topological Fallback` và `7D Behavioral K-Means` để suy diễn cho mọi đoạn đường `Cold-Start`. |
| **3. `Bounded O(1) Stream Sketching`** | Xử lý luồng bất tận với độ trễ mạng di động và bản ghi phát lại mà không làm tràn bộ nhớ `RAM`. | Sử dụng `32.0 KiB Bit-Packed Bloom Filter` (`uint8[32768]`, $k=4$), `1.0 KiB HyperLogLog++` (`uint8[1024]`), và `Bounded Decaying Window` $C_s(t)$ (`8 bytes/đoạn`, tự động cắt tỉa dưới ngưỡng $\epsilon = 10^{-4}$). |
| **4. `Self-Healing Two-Tier Reconciliation`** | Đảm bảo đồng thời tính tức thời (`Low Latency`) và tính đầy đủ (`100% Completeness`) khi có `1.5%` sự kiện đến trễ quá `Watermark`. | `Tier 1` chứng minh khớp `100%` trong `Watermark` (`exceptAll == 0`); `Tier 2` tự động tính lại `501` cửa sổ đoạn đường trễ (`219` khung giờ `(date, period)`) từ `bronze_raw_events/`, chấm điểm lại `377` cửa sổ hạ lưu và nâng cấp trạng thái lên `BATCH_RECONCILED` (`status_version = 2`). |

---

## 3. Đánh Giá Cổng Kiểm Soát `Gate 2 — Solution & Business Feasibility`

- [x] **Khả thi Kỹ thuật (`Technical Feasibility`):** Toàn bộ thuật toán (`Monoid Combiner`, `Line-Graph PageRank`, `SON/FPGrowth`, `7D K-Means`, `Causal Tree Ensembles`, `Stream Sketches`) hoạt động ổn định trong hạn mức `16 GB RAM`.
- [x] **Khả thi Dữ liệu (`Data Feasibility`):** Đã thiết lập cơ chế phòng vệ tự động cho toàn bộ `10 Empirical Edge Cases` (`EC-1` đến `EC-10`).
- [x] **Kết luận `Gate 2`:** **PASSED (Đủ điều kiện triển khai `MVP Development` & `QA`).**
