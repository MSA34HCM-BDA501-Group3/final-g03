# Tầm Nhìn Sản Phẩm (Product Vision): HCMC Smart Traffic Big Data Platform

## 1. Tuyên Bố Tầm Nhìn (`Vision Statement`)

Dành cho **Trung tâm Quản lý Điều hành Giao thông Đô thị TP.HCM (`TOC`)**, **Kỹ sư Điều khiển Đèn tín hiệu**, **Chuyên gia Quy hoạch Hạ tầng Sở GTVT** và **Hệ thống Dẫn đường Thông minh**, **HCMC Smart Traffic Big Data Platform** là nền tảng `Unified Lakehouse & Stream Intelligence` giúp chuyển hóa dữ liệu `Crowdsourced GPS Telemetry` thưa và nhiễu thành cảnh báo điểm nóng thời gian thực (`< 5s`), dự báo ùn tắc chân trời `t + 30m` độ trễ thấp (`< 1 ms` qua `MongoDB Compound Index IXSCAN`), và chẩn đoán điểm nghẽn cấu trúc trên `100%` mạng lưới đường cơ giới (`84,206` đoạn đường sạch từ `84,633` đoạn thô, bao gồm cả `88.13%` vùng mù cảm biến).

---

## 2. Giá Trị Cốt Lõi Theo Chuỗi Giá Trị Phân Tích 4 Tầng (`4-Tier Analytics Value Chain`)

1. **Descriptive Value (`Tầng Mô tả — P1`):** Lọc tách `427` đoạn phi đường bộ (`R2`) và `151` bản tin rơi ngoài đường cơ giới (`90,938 → 90,787` bản tin hợp lệ); chuẩn hóa về khung giờ Việt Nam (`ICT = UTC + 7h`, gồm `4,305` cửa sổ đổi ngày qua nửa đêm), nội suy phân cấp `3-Tier` không rò rỉ định hướng theo `TRAIN` hữu cơ cho `max_velocity` bị khuyết (`4,946 / 7,650 / 19,601 / 1,197` cửa sổ; `9,869 / 472 / 54,259 / 19,606` đoạn), đối chiếu `BkTraffic Urban 5 km/h LOS` (`derive_urban_los`, khớp **`89.19%`** với `train.csv`), và tổng hợp bằng **Pure 5-Tuple Monoid** khớp `100.0%` `baseline.py` (`33,394` cửa sổ, `0` sai lệch).
2. **Diagnostic Value (`Tầng Chẩn đoán — P2`):** Lọc bỏ `525,786` điểm uốn cong trung gian (`90.97%`), định vị điểm nghẽn bằng `Directed Line-Graph PageRank` **không U-turn** (`84,206` đỉnh, `92,453` cạnh chuyển tiếp, `314` `Dangling Nodes`, `#1`: `21969 — Nguyễn Kiệm`, `PR = 2.7799`); khai phá trên `TRAIN` hữu cơ `12` luật đồng kẹt (`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.2308`) và `14` luật lan truyền trễ `persistence_controlled=True` (`Lê Quang Định → Trần Quang Khải`, `Lift = 24.0`); phân nhóm `6` chế độ bằng Unified `Train-Only 7D Behavioral K-Means` (`Silhouette = 0.2241`, `DBI = 1.3284`, `CH = 806.49` trên `3,609` đoạn hữu cơ).
3. **Predictive Value (`Tầng Dự báo — P3`):** Phát hiện và loại bỏ `7,192` cửa sổ burst tổng hợp (`3,596` cặp burst `TRAIN`), giữ `1,885` cặp `TRAIN` hữu cơ / `1,167` `VAL` / `1,182` `TEST` (`7,885` cặp đúng `+30m`). Huấn luyện sạch không dùng `sample_weight` nhân tạo (`class_weight` lũy thừa `0.65`), `RF 23D` thắng cổng `VAL` (`0.8882` vs `0.8812` `DT 23D`) và đạt trên `TEST`: **`ROC-AUC = 0.9894`**, **`PR-AUC = 0.9543`**, **`6-Class Acc = 0.9255`**, **`Severe F1 = 0.9362`**, **`Macro-F1 = 0.4253`** *(phản ánh trung thực mất cân bằng lớp `B/C/D/E`)*, vượt `Persistence` ở pha đổi trạng thái (`10 / 61 = 16.39%`, `McNemar p = 0.001953`, `b=10, c=0`; hiệu chỉnh `VAL` đạt `14 / 61 = 22.95%`, `Detect ROC-AUC = 0.8822`, `Onset risk lift = 3.26x`) và phủ `100%` `74,211` đoạn Cold-Start (`88.13%`, `Masked-GPS Acc = 58.63%`, `ROC-AUC = 0.7000`).
4. **Prescriptive & Operational Value (`Tầng Điều phối & Vận hành — P4`):** Xử lý luồng sự kiện trong bộ nhớ cố định (`32.0 KiB RotatingBloomFilter` `0.0389%` FPR + `1.0 KiB 64-bit HLL++` `3.75%` sai số + `Bounded Decaying EWMA`), `WAL = 73.7 KB`, kết hợp `Two-Tier Batch–Stream Reconciliation` (`tier1_exceptAll_diff = 0`, hoàn thiện `98.5% → 100.0%` bằng bù `500` cửa sổ trễ / `226` khung giờ, hàn gắn `609` cửa sổ hạ lưu, chấm lại `384`, đổi xác suất `213`) và phục vụ trên `MongoDB 8.0` (`warm IXSCAN median = 0.360 ms / p95 = 0.603 ms`, `31.36x` vs `COLLSCAN`) qua ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)**.

---

## 3. Ba Cột Mốc Sản Phẩm Cốt Lõi (`Core Product Milestones`)

| Cột mốc (`Milestone`) | Định nghĩa Hoàn thành (`Definition of Done`) | Trạng thái Hiện tại |
| :--- | :--- | :--- |
| **1. `MVP (Minimum Viable Product)`** | Hoàn thiện `Pipeline` 17 thành phần (`[C1]`–`[C17]`), `Medallion Lakehouse` (`bronze_raw_events`, `stream_replay_windows`, `curated_gold`), mô hình `RF 23D` sạch không burst/không `sample_weight`, và ứng dụng web hợp nhất `toc-web (:8000)`. | **Đã hoàn thành & Kiểm chứng (`Gate 3 PASS`)** |
| **2. `Product-Market Fit (PMF) Signals`** | Đáp ứng trọn vẹn `6 Operational Use Cases` (`UC-1` đến `UC-6`) và `10 Empirical Edge Cases` (`EC-1` đến `EC-10`) với độ trễ `IXSCAN` `< 1 ms` (`0.360 / 0.603 ms`, `31.36x`) và `tier1_exceptAll_diff = 0`. | **Đã đạt tiêu chí `Gate 4`** |
| **3. `Scalable Operations`** | Đóng gói **`5` dịch vụ Docker mặc định** trong ngân sách **`11.0 GB / 16.0 GB RAM`**, có `WAL Checkpoints` (`73.7 KB`), atomic `$status_version` pipeline guard, `Operations Runbook`, full `pytest` & Session 9 audit đồng bộ. | **Đạt vận hành (`Gate 5 PASSED`)** |
