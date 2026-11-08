# Tầm Nhìn Sản Phẩm (Product Vision): HCMC Smart Traffic Big Data Platform

## 1. Tuyên Bố Tầm Nhìn (`Vision Statement`)

Dành cho **Trung tâm Quản lý Điều hành Giao thông Đô thị TP.HCM (`TOC`)**, **Kỹ sư Điều khiển Đèn tín hiệu**, **Chuyên gia Quy hoạch Hạ tầng Sở GTVT** và **Hệ thống Dẫn đường Thông minh**, **HCMC Smart Traffic Big Data Platform** là nền tảng `Unified Lakehouse & Stream Intelligence` giúp chuyển hóa dữ liệu `Crowdsourced GPS Telemetry` thưa và nhiễu thành cảnh báo điểm nóng thời gian thực (`< 5s`), dự báo ùn tắc chân trời `t + 30m` độ trễ thấp (`< 10ms` qua `MongoDB Compound Index IXSCAN`), và chẩn đoán điểm nghẽn cấu trúc trên `100%` mạng lưới (`84,633` đoạn đường, bao gồm cả `88.15%` vùng mù cảm biến).

---

## 2. Giá Trị Cốt Lõi Theo Chuỗi Giá Trị Phân Tích 4 Tầng (`4-Tier Analytics Value Chain`)

1. **Descriptive Value (`Tầng Mô tả — P1`):** Chuẩn hóa `100%` dữ liệu về khung giờ địa phương Việt Nam (`ICT = UTC + 7h`), nội suy phân cấp `3-Tier` cho `85.21%` giá trị `max_velocity` bị khuyết, và cách ly minh bạch mọi bản ghi hỏng vào `Quarantine Store` (`0` rơi rớt ngầm).
2. **Diagnostic Value (`Tầng Chẩn đoán — P2`):** Định vị chính xác các yết hầu giao thông bằng `Directed Line-Graph PageRank` (`84,633` đỉnh, `151,929` cung chuyển tiếp), phát hiện `30` luật lan truyền kẹt xe liên trục bằng `SON / FPGrowth` (`Lift` lên tới `28.58x`), và phân nhóm `6` chế độ hành vi giao thông bằng `Train-Only 7D Behavioral K-Means` (`Silhouette = 0.3158`).
3. **Predictive Value (`Tầng Dự báo — P3`):** Triệt tiêu hoàn toàn hiện tượng `Intra-Train Target Leakage` trên dữ liệu thưa nhờ `Causal Expanding Empirical-Bayes Priors` ($\tau \le t$), nâng `Severe Congestion F1-Score` từ `0.5758` (mô hình rò rỉ) lên **`0.6670`** và đạt **`PR-AUC = 0.7568`**, **`ROC-AUC = 0.8458`**.
4. **Prescriptive & Operational Value (`Tầng Điều phối & Vận hành — P4`):** Xử lý luồng sự kiện với bộ nhớ giới hạn $O(1)$ (`32 KB Bloom Filter` + `1 KB HyperLogLog++` + `Exponentially Decaying Window`), kết hợp cơ chế `Two-Tier Batch–Stream Reconciliation` tự động nâng độ đầy đủ dữ liệu từ `98.50%` lên `100.0%` sau giờ chốt.

---

## 3. Ba Cột Mốc Sản Phẩm Cốt Lõi (`Core Product Milestones`)

| Cột mốc (`Milestone`) | Định nghĩa Hoàn thành (`Definition of Done`) | Trạng thái Hiện tại |
| :--- | :--- | :--- |
| **1. `MVP (Minimum Viable Product)`** | Hoàn thiện `Pipeline` 17 thành phần (`[C1]`–`[C17]`), `Medallion Lakehouse` (`Bronze/Silver/Gold Parquet`), kiểm chứng `Ablation Study` không rò rỉ tương lai và `Streamlit 5-Tab UI`. | **Đã hoàn thành & Kiểm chứng (`Verified`)** |
| **2. `Product-Market Fit (PMF) Signals`** | Đáp ứng trọn vẹn `6 Operational Use Cases` (`UC-1` đến `UC-6`) và vượt qua `10 Empirical Edge Cases` (`EC-1` đến `EC-10`) với độ trễ truy vấn `< 10ms` (`IXSCAN`) và độ chính xác đối soát trong `Watermark` đạt `100.0%`. | **Đã đạt tiêu chí `Gate 3 & Gate 4`** |
| **3. `Scalable Operations`** | Đóng gói `Docker Compose` trong ngân sách `10.5 GB / 16 GB RAM`, có `WAL Checkpoints` phục hồi lỗi tự động, `Idempotent Upsert` chống trùng lặp và `Operations Runbook` đầy đủ. | **Sẵn sàng vận hành (`Gate 5 Ready`)** |
