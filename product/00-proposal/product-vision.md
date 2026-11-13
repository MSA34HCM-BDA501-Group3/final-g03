# Tầm Nhìn Sản Phẩm (Product Vision): HCMC Smart Traffic Big Data Platform

## 1. Tuyên Bố Tầm Nhìn (`Vision Statement`)

Dành cho **Trung tâm Quản lý Điều hành Giao thông Đô thị TP.HCM (`TOC`)**, **Kỹ sư Điều khiển Đèn tín hiệu**, **Chuyên gia Quy hoạch Hạ tầng Sở GTVT** và **Hệ thống Dẫn đường Thông minh**, **HCMC Smart Traffic Big Data Platform** là nền tảng `Unified Lakehouse & Stream Intelligence` giúp chuyển hóa dữ liệu `Crowdsourced GPS Telemetry` thưa và nhiễu thành cảnh báo điểm nóng thời gian thực (`< 5s`), dự báo ùn tắc chân trời `t + 30m` độ trễ thấp (`< 10ms` qua `MongoDB Compound Index IXSCAN`), và chẩn đoán điểm nghẽn cấu trúc trên `100%` mạng lưới (`84,633` đoạn đường, bao gồm cả `88.15%` vùng mù cảm biến).

---

## 2. Giá Trị Cốt Lõi Theo Chuỗi Giá Trị Phân Tích 4 Tầng (`4-Tier Analytics Value Chain`)

1. **Descriptive Value (`Tầng Mô tả — P1`):** Chuẩn hóa `100%` dữ liệu về khung giờ địa phương Việt Nam (`ICT = UTC + 7h`, kể cả quy tắc lùi ngày qua nửa đêm), nội suy phân cấp `3-Tier` định hướng theo `TRAIN` cho `85.21%` giá trị `max_velocity` bị khuyết, và cách ly minh bạch mọi bản ghi hỏng vào `Quarantine Store` (`0` rơi rớt ngầm).
2. **Diagnostic Value (`Tầng Chẩn đoán — P2`):** Định vị chính xác các yết hầu giao thông bằng `Directed Line-Graph PageRank` (`84,633` đỉnh, `151,929` cung chuyển tiếp, `48` `Dangling Nodes`), phát hiện `30` luật đồng kẹt xe có hướng hai chiều bằng `2-Pass SON / FP-Growth` (`Lift` lên tới `28.58x`) cùng `20` luật lan truyền trễ `t -> t+30m`, và phân nhóm `6` chế độ hành vi giao thông bằng `Train-Only 7D Behavioral K-Means` (`Silhouette = 0.2853`, `Davies-Bouldin = 1.1181` trên `6,926` đoạn `TRAIN`).
3. **Predictive Value (`Tầng Dự báo — P3`):** Dự báo trên đúng chân trời `t + 30 phút` (`7,898` cặp có nhãn giám sát, chia `70/15/15` theo mốc thời gian kèm `Target Cutoff Purge`) và triệt tiêu hiện tượng `Intra-Train Target Leakage` nhờ `Causal Expanding Empirical-Bayes Priors` ($\tau < t$), đạt **`Severe F1 = 0.9409`**, **`PR-AUC = 0.9471`**, **`ROC-AUC = 0.9882`**, **`6-Class Accuracy = 0.9450`**, và **`Macro-F1 = 0.5543`** (tất cả 6 lớp `LOS A..F` đều có `Recall > 0`).
4. **Prescriptive & Operational Value (`Tầng Điều phối & Vận hành — P4`):** Xử lý luồng sự kiện với bộ nhớ giới hạn $O(1)$ (`32.0 KiB Bit-Packed Bloom Filter uint8[32768]` + `1.0 KiB HyperLogLog++ uint8[1024]` + `Bounded Decaying Window` có cơ chế cắt tỉa $\epsilon$), kết hợp cơ chế `Two-Tier Batch–Stream Reconciliation` tự động nâng độ đầy đủ dữ liệu từ `98.50%` lên `100.0%` (`501` bản tin trễ quá watermark tại `219` khung giờ và chấm điểm lại `377` cửa sổ hạ lưu).

---

## 3. Ba Cột Mốc Sản Phẩm Cốt Lõi (`Core Product Milestones`)

| Cột mốc (`Milestone`) | Định nghĩa Hoàn thành (`Definition of Done`) | Trạng thái Hiện tại |
| :--- | :--- | :--- |
| **1. `MVP (Minimum Viable Product)`** | Hoàn thiện `Pipeline` 17 thành phần (`[C1]`–`[C17]`), `Medallion Lakehouse` (`Bronze/Silver/Gold Parquet`), kiểm chứng `Ablation Study` không rò rỉ tương lai và `Streamlit 5-Tab UI`. | **Đã hoàn thành & Kiểm chứng (`Verified`)** |
| **2. `Product-Market Fit (PMF) Signals`** | Đáp ứng trọn vẹn `6 Operational Use Cases` (`UC-1` đến `UC-6`) và vượt qua `10 Empirical Edge Cases` (`EC-1` đến `EC-10`) với độ trễ truy vấn `< 10ms` (`IXSCAN`) và độ chính xác đối soát trong `Watermark` đạt `100.0%`. | **Đã đạt tiêu chí `Gate 3 & Gate 4`** |
| **3. `Scalable Operations`** | Đóng gói `Docker Compose` trong ngân sách `10.5 GB / 16 GB RAM`, có `WAL Checkpoints` phục hồi lỗi tự động, `Idempotent Upsert` chống trùng lặp và `Operations Runbook` đầy đủ. | **Sẵn sàng vận hành (`Gate 5 Ready`)** |
