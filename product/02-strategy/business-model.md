# Mô Hình Tạo Giá Trị & Ngân Sách Tài Nguyên (Business Model & FinOps Resource Model)

## 1. Khung Giá Trị Kinh Tế – Xã Hội (`Business Model Canvas & Value Proposition`)

| Thành phần (`BMC Block`) | Nội dung Triển khai cho HCMC Smart Traffic Platform |
| :--- | :--- |
| **Phân khúc Khách hàng (`Customer Segments`)** | (1) Trung tâm Điều hành Giao thông Đô thị (`TOC`); (2) Sở Giao thông Vận tải TP.HCM (`Urban Planning`); (3) Hệ thống điều khiển đèn tín hiệu thông minh & ứng dụng dẫn đường/logistics. |
| **Giá trị Đề xuất (`Value Proposition`)** | Giảm thời gian phát hiện điểm nóng ùn tắc kéo dài xuống `< 5s`; dự báo trước đúng `30 phút` xác suất kẹt nặng (`LOS E/F`) với `PR-AUC = 0.9471`, `ROC-AUC = 0.9882`, `Severe F1 = 0.9409`; tiết kiệm `> 90%` chi phí đầu tư cảm biến phần cứng nhờ tận dụng `Crowdsourced GPS` kết hợp `Line-Graph PageRank` bao phủ `100%` mạng lưới (`84,633` đoạn đường). |
| **Kênh Phân phối (`Channels`)** | (1) `Streamlit 5-Tab Operational Dashboard` (`:8501`); (2) Kho truy vấn vận hành tốc độ cao `MongoDB 8.0` (`bda501.traffic_scores` qua `Compound Index IXSCAN`); (3) Data Lakehouse `Medallion Parquet Lakehouse` (`data/lakehouse/`). |
| **Cấu trúc Chi phí & FinOps (`Cost Structure`)** | Tối ưu hóa chi phí hạ tầng nhờ kiến trúc `Polyglot Storage` và `O(1) Stream Sketching`: toàn bộ cụm `4` dịch vụ (`Kafka 4.1 KRaft`, `Spark 3.5.7`, `MongoDB 8.0`, `Streamlit UI`) chạy vừa vặn trong **`10.5 GB RAM`** trên một máy chủ đơn hoặc laptop `16 GB RAM`. |

---

## 2. Phân Bổ Ngân Sách Phần Cứng `16 GB RAM` (`FinOps & Container Resource Budget`)

| Dịch vụ Container | Hạn mức Tài nguyên (`RAM & vCPU`) | Vai trò Vận hành & Cơ chế Tiết kiệm Chi phí |
| :--- | :--- | :--- |
| **`kafka` (`apache/kafka:4.1.0`)** | **`1.5 GB RAM` · `1 vCPU`** | Chạy chế độ `KRaft` (loại bỏ hoàn toàn cụm `ZooKeeper` truyền thống, tiết kiệm `1.5 GB RAM`); quản lý topic `3` phân vùng `hcmc-traffic-events`. |
| **`spark` (`Pipeline & Stream Engine`)** | **`6.0 GB RAM` · `4 vCPUs`** | Thực thi hợp nhất `Quality Funnel`, `Commutative Monoid`, `MMDS Mining Suite` (`PageRank`, `FPGrowth`, `K-Means`) và `Causal ML Pipeline`. Lọc bỏ `90.93%` điểm uốn cong trung gian trước khi dựng đồ thị. |
| **`mongo` (`mongo:8.0`)** | **`2.0 GB RAM` · `2 vCPUs`** | Lưu trữ phục vụ truy vấn vận hành (`WiredTiger Cache = 1.0 GB`); dùng `Compound Index IXSCAN` loại bỏ hoàn toàn bước `SORT` tốn bộ nhớ. |
| **`app` (`Streamlit 5-Tab UI :8501`)** | **`1.0 GB RAM` · `1 vCPU`** | Cổng giao diện điều hành trực quan đọc trực tiếp từ `MongoDB 8.0` và các bảng `Parquet / CSV` đã tổng hợp. |
| **Tổng cộng (`Total Allocated`)** | **`10.5 GB / 16.0 GB RAM`** | **Dành dư `5.5 GB RAM` (`34.4%` headroom) cho Hệ điều hành Host (`Windows / WSL2`) để chống hiện tượng `Swap Thrashing` hoặc `OOM Kill`.** |
