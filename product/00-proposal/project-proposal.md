# Đề Xuất Dự Án (Project Proposal V3): HCMC Smart Traffic Big Data Platform

**Học phần:** Big Data Analytics (`BDA501`) — Master of Software Engineering (`MSE`), Viện Quản trị & Công nghệ FSB (Đại học FPT)  
**Trạng thái Stage-Gate:** **Đạt Gate 1 (`Problem Validation`) & Gate 2 (`Solution & Business Feasibility`)**  
**Đội ngũ phụ trách (`DRI Matrix`):**
- **Nguyễn Tạ Minh Quyền (`25ms23280`):** `DRI` cho `Pillars I1 & I2` (`[C2]` Static GIS Loader, `[C5]` Ordered Quality Funnel, `[C9]` Commutative Monoid Combiner & BroadcastHashJoin, `[C14]` Medallion Lakehouse, `[C15]` Quarantine Store).
- **Nguyễn Thành Đạt (`25ms23306`):** `DRI` cho `MMDS Ch. 5–7, 12 & Pillar I3` (`[C10]` MMDS Mining Suite — `Directed Line-Graph PageRank`, `SON/FPGrowth`, `7D Behavioral K-Means`, `[C11]` Leak-Free Causal ML Fit, `[C13a]`/`[C13b]` Frozen Model & Static Feature Registry).
- **Phạm Công Minh (`25ms23310`):** `DRI` cho `Pillar S7, MMDS Ch. 4 & Serving` (`[C1]` Telemetry Producer, `[C4]` Kafka 4.1 KRaft Broker, `[C6]`–`[C8]` Speed Layer, `[C12]` Two-Tier Reconciliation, `[C16]` MongoDB 8.0 Serving, `[C17]` Streamlit 5-Tab UI).

---

## 1. Bối Cảnh & Định Nghĩa Vấn Đề (`Problem Statement`)

Mạng lưới giao thông đô thị TP. Hồ Chí Minh (`HCMC`) có tính phi tuyến cao, chịu chi phối đồng thời bởi cấu trúc đồ thị đường bộ phức tạp và nhịp sinh hoạt hàng ngày. Khác với các bài toán chuẩn trên cao tốc có cảm biến vòng từ cố định (`fixed-loop detectors`), dữ liệu `Crowdsourced GPS Telemetry` thực tế tại TP.HCM (trên 5 bảng quan hệ từ **Kaggle HCMC Traffic Flow Dataset**) đặt ra 4 thách thức lớn về `Big Data`:

1. **Quy mô Đồ thị & Tính không đồng nhất vật lý (`Graph Scale & Heterogeneity`):** Mạng lưới gồm `577,967` nút tọa độ thô (`52,425` nút giao thực tế sau khi cắt tỉa `90.93%` đỉnh hình học mồ côi `Orphan OSM Vertices`), `84,633` đoạn đường có hướng (`Directed Segments`) nối với nhau qua `151,929` cung chuyển tiếp (`Line-Graph Transitions`) trên `5,553` tuyến đường. Tới `88.34%` đoạn đường bị khuyết thuộc tính `max_velocity` và `154` đoạn có `length <= 0m`.
2. **Độ thưa Không gian – Thời gian Cực đại (`Extreme Spatio-Temporal Sparsity`):** Chỉ có `10,027 / 84,633` đoạn đường (`11.85%`) có tín hiệu GPS trong 10 tháng (`07/2020 – 04/2021`), đồng nghĩa `88.15%` (`74,606` đoạn) rơi vào vùng mù cảm biến (`Cold-Start Blind Spot`). Trung vị mỗi đoạn đường chỉ có `2` cửa sổ quan sát (`79.26%` có $\le 2$ quan sát), và `66.27%` cặp quan sát liên tiếp cách nhau $> 30$ phút (trung vị $\Delta t = 9.0\text{h}$).
3. **Nhiễu Thực thể, Độ trễ Mạng & Lệch Múi giờ (`Veracity & Timezone Drift`):** Luồng `90,938` bản tin vận tốc GPS chứa `~1%` bản ghi trùng lặp (`Duplicates`) và `~5%` bản ghi đến trễ (`3.5%` trong `Watermark` 30 phút, `1.5%` trễ quá `Watermark`). Nhãn `period` gốc bị cắt theo giờ `UTC` (lệch `-7h` so với giờ `ICT`), và tên đường lẫn lộn mã hóa Unicode `NFD/NFC`, ký tự `Ð/Đ`, cùng nhãn hành chính (`Phường`, `Quận`).
4. **Xung đột Mô hình Giữa Batch & Stream (`Batch-Stream MLOps Conflict`):** Nếu `Speed Layer` tự huấn luyện hoặc chuẩn hóa lại trên từng `micro-batch` có độ phủ chỉ `98.50%`, mô hình sẽ bị trôi tham số (`Concept Drift`) và xung đột trực tiếp với mô hình `Batch Layer`.

---

## 2. Bốn Bài Toán Con Toán Học (`Formal Sub-Problems P1–P4`)

| Mã | Tầng Phân Tích (`Analytics Tier`) | Biểu Thức Toán Học & Mục Tiêu | Thành Phần Giải Quyết |
| :--- | :--- | :--- | :--- |
| **`P1`** | **Descriptive Tier:** Tổng hợp Cửa sổ 30 phút & Suy diễn `LOS` | Ánh xạ sự kiện $(s_i, v_i, t_{\text{event},i})$ vào cửa sổ 30 phút `ICT` $w=(s,d,p)$ qua `Commutative Monoid` $\mathbf{m}_{s,w} = (\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt})$, nội suy $\hat{v}_{\max,s}$ 3 tầng và tính $\text{LOS}_{s,w} \in \{A..F\}$ từ $r_{s,w} = \bar{v}_{s,w} / \hat{v}_{\max,s}$. | `[C5]` Quality Funnel, `[C9]` Combiner & BroadcastJoin (`Pillars I1, I2`) |
| **`P2`** | **Diagnostic Tier:** Khai phá Điểm nghẽn, Luật Lan truyền & Phân cụm | (a) `MMDS Ch. 5`: `Directed Line-Graph PageRank` ($\beta=0.85$, $84{,}633$ đỉnh, $151{,}929$ cạnh, bù $48$ `Dangling Nodes`); (b) `MMDS Ch. 6`: `SON / FPGrowth` khai phá luật $\{\text{Street}_A\} \Rightarrow \{\text{Street}_B\}$; (c) `MMDS Ch. 7`: `Train-Only 7D Behavioral K-Means` ($k=6$). | `[C10]` MMDS Mining Suite (`Ch. 5, 6, 7`) |
| **`P3`** | **Predictive Tier:** Dự báo Tắc nghẽn `t + 30m` Không Rò rỉ Tương lai | Học ánh xạ nhân quả $f_{\boldsymbol{\theta}}: \mathcal{X}_{s,\le t} \mapsto (\widehat{\text{LOS}}_{s,t+1}, \hat{p}_{\text{severe},s,t+1})$ với `Causal Expanding Empirical-Bayes Priors` ($\tau \le t$), tối đa hóa `Severe F1-Score` và `PR-AUC` trên tập kiểm thử tương lai (`15% TEST`). | `[C11]` Causal ML Fit, `[C13a]`/`[C13b]` (`Pillar I3`, `Ch. 12`) |
| **`P4`** | **Prescriptive Tier:** Luồng Bộ nhớ $O(1)$ & Đối soát Hai Tầng | Xử lý luồng trực tuyến trong `30m Watermark` bằng `32 KB Bloom Filter`, `1 KB HLL++`, `Decaying Window` $C_s(t)$, phát hành `STREAM_PROVISIONAL`, và tự động bù trừ `1.5%` (`501`) sự kiện trễ ở `Phase C` để nâng độ đầy đủ từ `98.50% -> 100.0%` (`BATCH_RECONCILED`). | `[C6]`–`[C8]` Speed Layer, `[C12]` Recon, `[C16]` MongoDB (`Pillar S7`, `Ch. 4`) |

---

## 3. Kiến Trúc Giải Pháp Đề Xuất (`V3 Solution Architecture`)

Hệ thống được thiết kế theo mô hình **6 Phân vùng (`Zone A`–`Zone F`), 17 Thành phần (`[C1]`–`[C17]`), 10 Luồng dữ liệu (`1`–`10`)** kết hợp **Vòng đời MLOps 3 Pha Không Xung Đột (`Zero-Conflict 3-Phase MLOps Lifecycle`)**:
- **Phase A (`Offline Batch Training`, $t \le T_{\text{train}}$):** `[C9]`–`[C11]` thực thi độc quyền `.fit()` trên tập `70% TRAIN` và đóng băng bất biến `[C13a]` `PipelineModel` cùng `[C13b]` `Static Feature Store` (`84,633` đoạn đường).
- **Phase B (`Online Speed Layer`, $t > T_{\text{train}}$):** `[C6]`–`[C8]` cập nhật `Stream Sketches` $O(1)$ và chỉ gọi `.transform()` chỉ đọc từ `[C13a]`/`[C13b]`, ghi trạng thái `STREAM_PROVISIONAL` vào `[C16]` `MongoDB 8.0`.
- **Phase C (`Post-Cutoff Two-Tier Reconciliation`, $t > T_{\text{cutoff}}$):** `[C12]` kiểm định `exceptAll == 0` trong `Watermark` (`Tier 1`), tính lại `319` cửa sổ bị trễ quá hạn từ `bronze_raw_events/` (`Tier 2`), chữa lành `Lag Store`, gọi lại `.transform()` chỉ đọc và cập nhật `BATCH_RECONCILED` lên `[C16]` `MongoDB 8.0`.
