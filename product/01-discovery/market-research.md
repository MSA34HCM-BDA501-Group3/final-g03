# Nghiên Cứu Thị Trường & Hiện Trạng Giao Thông Đô Thị TP.HCM (Market Research)

## 1. Quy Mô & Đặc Thù Mạng Lưới Giao Thông TP. Hồ Chí Minh

TP. Hồ Chí Minh là siêu đô thị kinh tế lớn nhất Việt Nam với mật độ phương tiện cá nhân (đặc biệt là xe máy và ô tô con) phân bổ dày đặc trên mạng lưới đường bộ có cấu trúc hình học không đồng nhất:
- **Quy mô hạ tầng số hóa (`OSM / GIS Topology`):** Gồm `577,967` điểm tọa độ bản đồ, trong đó có `52,425` nút giao thông thực tế (`Active Intersections`) nối `84,633` đoạn đường có hướng (`Directed Road Segments`) thuộc `5,553` tuyến đường đặt tên (`streets.csv`).
- **Phân bố độ dài đoạn đường siêu mịn (`Granular Segment Length`):** Trung vị chiều dài đoạn đường chỉ đạt **`27.0 mét`**, tạo ra `151,929` cung chuyển tiếp có hướng (`Line-Graph Transitions`) và `48` đoạn đường cụt (`Dangling Nodes`).
- **Tỷ lệ thiếu hụt siêu dữ liệu hạ tầng:** Có tới `88.34%` đoạn đường trong `segments.csv` và `82.23%` tuyến đường trong `streets.csv` không có thông tin vận tốc thiết kế tối đa (`max_velocity`), cùng `427` đoạn đường bị gán nhầm nhãn công trình dân dụng (`house`, `fuel`, `convenience`, `theme_park`).

---

## 2. Phân Tích Nhu Cầu Thực Tế Theo Nhóm Người Dùng Mục Tiêu (`Target Market Segments`)

| Phân khúc Người dùng | Nỗi đau Hiện tại (`Pain Points`) | Nhu cầu Cốt lõi (`Core Needs`) | Kịch bản Đáp ứng |
| :--- | :--- | :--- | :--- |
| **1. Trung tâm Điều hành Giao thông (`TOC Dispatchers`)** | Bị quá tải bởi hàng nghìn cảnh báo giả từ các đoạn đường chỉ dừng chờ đèn đỏ 1 chu kỳ; không phân biệt được ùn ứ thoáng qua với kẹt xe kéo dài. | Cần bộ lọc điểm nóng thời gian thực có trọng số suy giảm theo thời gian (`Exponentially Decaying Score`) với chi phí bộ nhớ $O(1)$. | **`UC-1`** (`Top-25 Hotspots`, cập nhật mỗi `5s`) |
| **2. Ứng dụng Dẫn đường & Hệ thống Đèn Tín hiệu (`Signal & Routing API`)** | Chỉ biết đường đang tắc sau khi sự cố đã xảy ra; truy vấn cơ sở dữ liệu truyền thống phải quét toàn bảng (`COLLSCAN`) gây nghẽn ở giờ cao điểm. | Cần dự báo xác suất kẹt nặng $\hat{p}_{\text{severe},t+1}$ trước `30 phút` với độ trễ truy vấn `< 10ms` qua chỉ mục ghép (`Compound Index IXSCAN`). | **`UC-2`** (`< 10ms IXSCAN`, `0` bước `SORT` trong RAM) |
| **3. Sở GTVT & Chuyên gia Quy hoạch Đô thị (`Urban Planners`)** | `88.15%` (`74,606 / 84,633`) đoạn đường không có người dùng gửi tín hiệu GPS (`Sensor Blind Spot`), khiến quy hoạch dựa trên cảm tính. | Cần thuật toán đồ thị đánh giá độ trung tâm cấu trúc (`Directed Line-Graph PageRank`) trên toàn bộ `100%` mạng lưới thành phố. | **`UC-3`** (`PageRank` trên `84,633` đoạn đường) |
| **4. Kỹ sư Điều phối Giao thông Liên Quận (`Corridor Engineers`)** | Khi một trục chính (như Cộng Hòa hay Lý Chính Thắng) kẹt cứng, không định lượng được tuyến nhánh nào sẽ bị khóa chặt tiếp theo. | Cần tập Association Rules có hướng $\{\text{Street}_A\} \Rightarrow \{\text{Street}_B\}$ có `Confidence` và `Lift` cao, không bị nhiễu bởi lỗi mã hóa tên đường. | **`UC-4`** (`30` luật `SON / FPGrowth`, `Lift` tới `28.58x`) |
