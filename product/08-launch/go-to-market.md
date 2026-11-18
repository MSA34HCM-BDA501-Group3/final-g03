# Chiến Lược Triển Khai & Go-To-Market (Production Platform)

## 1. Giá trị Cốt lõi (Value Proposition) cho Sở Giao thông Vận tải & Trung tâm Điều hành (TOC)
1. **Giám sát Toàn mạng Lưới (`84,206` Đoạn Đường Cơ Giới Sạch):** Kết hợp trực tiếp `9,995` đoạn đường có GPS (`11.87%`) với chỉ số cấu trúc PageRank (`#1: 21969 Nguyễn Kiệm, PR = 2.7799`) và không gian hành vi Unified `K-Means 7D` (`Silhouette = 0.2241`) để lập chỉ mục cho toàn bộ `74,211` đoạn đường Cold-Start (`88.13%`), đồng thời công khai trung thực giới hạn vật lý của proxy không gian (`Masked-GPS Cold-Start Proxy Acc = 58.63%, ROC-AUC = 0.7000, PR-AUC = 0.5152`).
2. **Dự báo Đáng tin Cậy & Sạch Rò rỉ (`RF 23D`):** Loại bỏ hoàn toàn `3,596` cặp synthetic burst khỏi huấn luyện, không dùng `sample_weight` nhân tạo; đạt `TEST ROC-AUC = 0.9894`, `PR-AUC = 0.9543`, `Acc = 0.9255`, `Severe F1 = 0.9362`, `Onset risk lift = 3.26x` và bắt chuyển trạng thái tắc nghẽn vượt trội Persistence (`10 / 61 = 16.39%`, `McNemar p = 0.001953`, VAL-calibrated `14 / 61 = 22.95%`, `Detect ROC-AUC = 0.8822` vs heuristic `0.7921`).
3. **Khai phá Lan truyền Tắc nghẽn Thực chứng:** Cung cấp `12` luật đồng tắc nghẽn (`Nguyễn Thị Minh Khai <-> Cống Quỳnh`, Lift `15.2308`) và `14` luật lan truyền có kiểm soát tính tự duy trì (`persistence_controlled = True`, tiêu biểu `Lê Quang Định -> Trần Quang Khải`, Lift `24.0`).
4. **Hiệu năng Truy vấn & Giao diện Điều hành Hợp nhất:**
   - **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`, dịch vụ `toc-web`):** Ứng dụng web hợp nhất duy nhất gồm 5 tab điều hành & kiểm toán trực quan, bản đồ Leaflet động TP.HCM, 4 kịch bản rush-hour thực tế và phát trực tiếp luồng Kafka.
   - **MongoDB 8.0.34 Sub-millisecond Serving:** Độ trễ `IXSCAN` ấm đạt **`0.360 ms` median / `0.603 ms` p95** (**`31.36x`** so với `COLLSCAN 11.289 ms`) cho `33,394` tài liệu (`1,234` cảnh báo trục chính).

## 2. Lộ trình Triển khai Thí điểm (Phased Adoption)
- **Giai đoạn 1 — Shadow Operation:** Đấu nối nguồn GPS trực tiếp qua Kafka KRaft + `live-consumer` trong cụm `5` dịch vụ mặc định (`11.0 GB RAM`), kiểm tra `tier1_exceptAll_diff = 0` và `atomic $status_version` guard.
- **Giai đoạn 2 — Corridor Dispatch:** Kích hoạt cảnh báo sớm theo `1,234` cảnh báo trục chính và `14` luật lan truyền trễ trên **Web TOC (`http://localhost:8000`)** cho đội điều tiết giao thông.
- **Giai đoạn 3 — Full City Lakehouse Integration:** Nối mở rộng nguồn camera/cảm biến vòng từ bổ sung vào lớp `curated_gold/year_month=*` để thu hẹp tỷ lệ `88.13%` đoạn đường Cold-Start.
