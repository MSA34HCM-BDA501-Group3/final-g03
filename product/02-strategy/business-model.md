# Mô Hình Tạo Giá Trị & Ngân Sách Tài Nguyên (Business Model & FinOps Resource Model)

## 1. Khung Giá Trị Kinh Tế – Xã Hội (`Business Model Canvas & Value Proposition`)

| Thành phần (`BMC Block`) | Nội dung Triển khai cho HCMC Smart Traffic Platform (Leak-Free Ground Truth) |
| :--- | :--- |
| **Phân khúc Khách hàng (`Customer Segments`)** | (1) Trung tâm Điều hành Giao thông Đô thị (`TOC`); (2) Sở Giao thông Vận tải TP.HCM (`Urban Planning`); (3) Hệ thống điều khiển đèn tín hiệu thông minh & ứng dụng dẫn đường/logistics. |
| **Giá trị Đề xuất (`Value Proposition`)** | Phát hiện điểm nóng ùn tắc kéo dài (`EWMA`); dự báo trước đúng `30 phút` xác suất kẹt nặng (`LOS E/F`) với **`ROC-AUC = 0.9894`**, **`PR-AUC = 0.9543`**, **`Severe F1 = 0.9362`**; bao phủ `100%` mạng lưới cơ giới sạch (**`84,206` đoạn đường**, gồm `74,211` đoạn Cold-Start `88.13%`) bằng `Line-Graph PageRank` (bỏ U-turn) & Unified `7D K-Means`. |
| **Kênh Phân phối (`Channels`)** | (1) Ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`, dịch vụ `toc-web`)**; (2) Kho truy vấn vận hành `MongoDB 8.0` (`bda501.traffic_scores`, `IXSCAN warm_median = 0.360 ms / p95 = 0.603 ms`, `31.36x` vs `COLLSCAN`); (3) Data Lakehouse `Medallion Parquet` (`bronze_raw_events`, `stream_replay_windows`, `curated_gold`). |
| **Cấu trúc Chi phí & FinOps (`Cost Structure`)** | Tối ưu hóa chi phí hạ tầng nhờ `Polyglot Storage` và `O(1) Stream Sketching` (`33.0 KiB`): cụm **`5` dịch vụ Docker mặc định** (`kafka`, `mongo`, `spark`, `toc-web`, `live-consumer`) vận hành vừa vặn trong **`11.0 GB RAM`** trên máy chủ đơn `16.0 GB RAM`. |

---

## 2. Phân Bổ Ngân Sách Phần Cứng `16 GB RAM` (`FinOps & Container Resource Budget` — `docker-compose.yml`)

| Dịch vụ Container (`docker-compose.yml`) | Hạn mức Tài nguyên (`RAM & vCPU`) | Vai trò Vận hành & Cơ chế Tiết kiệm Chi phí |
| :--- | :--- | :--- |
| **`kafka` (`apache/kafka:4.1.0`)** | **`1536M (1.5 GB)` · `1 vCPU`** | Chạy chế độ `KRaft` không cần `ZooKeeper`; quản lý topic `3` phân vùng `hcmc-traffic-events`. |
| **`mongo` (`mongo:8.0.34`)** | **`2048M (2.0 GB)` · `2 vCPUs`** | Lưu trữ vận hành `bda501.traffic_scores` (`33,394` docs, `WiredTiger Cache = 1.0 GB`); dùng `Compound Index IXSCAN` (`31.36x` vs `COLLSCAN`). |
| **`spark` (`hcmc-traffic-spark:3.5.7`)** | **`6144M (6.0 GB)` · `4 vCPUs`** | Thực thi `Spark 3.5.7 / Java 17`: `BroadcastHashJoin` (`5 → 1` Exchange, `25.76%` speedup), `2-Stage 8-Bucket Salted Monoid`, MLlib organic (`FPGrowth 32`, `KMeans 3,609`, `RF 1,885`) & Streaming Sink 2. |
| **`toc-web` (`uvicorn toc_backend.main:app`)** | **`1024M (1.0 GB)` · `1 vCPU`** | Ứng dụng web hợp nhất duy nhất **ReactJS + TypeScript + FastAPI + Leaflet** tại `http://localhost:8000` (`toc_backend/main.py` + `toc_frontend/dist/`). |
| **`live-consumer` (`starter_stream.py`)** | **`512M (0.5 GB)` · `1 vCPU`** | Consumer Kafka liên tục (`32 KiB RotatingBloomFilter` + `1 KiB 64-bit HLL++` + Frozen 23D RF) ghi trực tiếp vào `MongoDB 8.0`. |
| **Tổng cộng (`5` Dịch vụ Mặc định)** | **`11.0 GB / 16.0 GB RAM`** | **Dành dư `5.0 GB RAM` headroom cho Hệ điều hành Host (`Windows / WSL2`)**, kèm 2 service profile `pipeline` (`[batch]`) & `stream` (`[stream]`). |
