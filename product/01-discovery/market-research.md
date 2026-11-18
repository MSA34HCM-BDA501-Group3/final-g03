# Nghiên Cứu Thị Trường & Hiện Trạng Giao Thông Đô Thị TP.HCM (Market Research)

## 1. Quy Mô & Đặc Thù Mạng Lưới Giao Thông TP. Hồ Chí Minh

TP. Hồ Chí Minh là siêu đô thị kinh tế lớn nhất Việt Nam với mật độ phương tiện cá nhân phân bổ dày đặc trên mạng lưới đường bộ có cấu trúc hình học không đồng nhất:
- **Quy mô hạ tầng số hóa (`OSM / GIS Topology`):** Từ `577,967` điểm tọa độ bản đồ và `84,633` đoạn thô thuộc `5,553` tuyến đường (`streets.csv`), sau khi lọc `427` đoạn phi đường bộ (`R2`) và tỉa `525,786` điểm uốn cong/mồ côi (`90.97%`), mạng đường cơ giới sạch gồm **`52,181` giao lộ hoạt động** nối **`84,206` đoạn đường có hướng**.
- **Phân bố độ dài & liên kết chuyển tiếp (`Line-Graph Topology`):** Trung vị chiều dài đoạn đường là **`27.0 mét`**; khi loại bỏ các cung quay đầu `U-turn` trên đoạn hai chiều, đồ thị nối đoạn đường tạo ra **`92,453` cung chuyển tiếp có hướng** và **`314` đoạn đường cụt (`Dangling Nodes`)**.
- **Tỷ lệ thiếu hụt siêu dữ liệu hạ tầng:** `88.34%` đoạn đường trong `segments.csv` (`74,762` đoạn) và `82.23%` tuyến đường trong `streets.csv` (`4,566` tuyến) không có thông tin vận tốc thiết kế tối đa (`max_velocity`), và `88.13%` (`74,211 / 84,206`) đoạn cơ giới chưa có quan sát GPS nào.

---

## 2. Phân Tích Nhu Cầu Thực Tế Theo Nhóm Người Dùng Mục Tiêu (`Target Market Segments`)

| Phân khúc Người dùng | Nỗi đau Hiện tại (`Pain Points`) | Nhu cầu Cốt lõi (`Core Needs`) | Kịch bản Đáp ứng |
| :--- | :--- | :--- | :--- |
| **1. Trung tâm Điều hành Giao thông (`TOC Dispatchers`)** | Bị quá tải bởi cảnh báo giả từ các đoạn chỉ dừng đèn đỏ 1 nhịp; không phân biệt được ùn ứ thoáng qua với kẹt xe kéo dài. | Cần bộ lọc điểm nóng thời gian thực có trọng số suy giảm theo thời gian (`EWMA`) với chi phí bộ nhớ $O(1)$ hiển thị trên **Web TOC `:8000`** & **Streamlit `:8501`**. | **`UC-1`** (`Top-25 Hotspots`, `#1 47407 Lý Thường Kiệt`) |
| **2. Ứng dụng Dẫn đường & Hệ thống Đèn Tín hiệu (`Signal & Routing API`)** | Chỉ biết đường đang tắc sau khi sự cố đã xảy ra; truy vấn `COLLSCAN` (`9.161 ms` median) gây nghẽn ở giờ cao điểm. | Cần dự báo xác suất kẹt nặng $\hat{p}_{\text{severe},t+30m}$ trước `30 phút` với độ trễ `< 1 ms` qua chỉ mục ghép (`Compound Index IXSCAN`). | **`UC-2`** (`warm median = 0.389 ms, p95 = 0.692 ms`, nhanh hơn `23.55x`) |
| **3. Sở GTVT & Chuyên gia Quy hoạch Đô thị (`Urban Planners`)** | `88.13%` (`74,211 / 84,206`) đoạn đường cơ giới không có GPS (`Sensor Blind Spot`), khiến quy hoạch thiếu căn cứ định lượng. | Cần thuật toán đồ thị đánh giá độ trung tâm cấu trúc (`Line-Graph PageRank`, bỏ U-turn) trên `100%` mạng lưới thành phố. | **`UC-3`** (`PageRank` trên `84,206` đoạn; `#1 21969 Nguyễn Kiệm`) |
| **4. Kỹ sư Điều phối Giao thông Liên Quận (`Corridor Engineers`)** | Khi một trục chính kẹt cứng, khó định lượng tuyến nhánh nào có nguy cơ kẹt đồng thời hoặc nối tiếp sau `30 phút`. | Cần tập Association Rules khai phá sạch trên `TRAIN` hữu cơ không nhiễm burst và không lỗi mã hóa tên đường. | **`UC-4`** (`12` co-rules `Lift 15.23` & `14` lagged rules `Lift 24.0`, `persistence_controlled=True`) |
