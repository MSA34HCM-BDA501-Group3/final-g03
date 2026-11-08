# Khảo Sát Nhu Cầu Tác Nhân Vận Hành & Chân Dung Người Dùng (Customer Interviews & Personas)

## 1. Chân Dung Các Tác Nhân Cốt Lõi (`Core Operational Personas — [C3]`)

### Persona 1: Điều Phối Viên Trực Ban Trung Tâm Quản Lý Giao Thông (`TOC Dispatcher`)
- **Mục tiêu công việc:** Giám sát tình trạng ùn tắc toàn thành phố theo chu kỳ `5 giây/lần` và phát lệnh điều phối lực lượng CSGT tại các giao lộ có nguy cơ tê liệt.
- **Vấn đề gặp phải:** *"Nếu chỉ nhìn vào vận tốc trung bình của một cửa sổ đơn lẻ, một đoàn xe dừng đèn đỏ trên đường Lê Khôi cũng báo đỏ giống hệt như đoạn Lý Thường Kiệt đã kẹt cứng suốt 2 tiếng đồng hồ."*
- **Yêu cầu chuyển thành tính năng (`UC-1`):** Xây dựng chỉ số điểm nóng suy giảm hàm mũ $C_s(t) = (1 - \alpha) C_s(t-1) + \mathbb{I}(\text{LOS} \in \{E, F\})$ kết hợp ước lượng mật độ đoạn đường hoạt động bằng `1 KB HyperLogLog++`.

### Persona 2: Kỹ Sư Hệ Thống Đèn Tín Hiệu & Cổng API Dẫn Đường (`Signal & Routing System Engineer`)
- **Mục tiêu công việc:** Tự động truy vấn danh sách các đoạn đường trục chính (`street_level` 1 và 2) có xác suất kẹt nặng $\hat{p}_{\text{severe},t+1} \ge 0.70$ ở khung 30 phút kế tiếp để điều chỉnh chu kỳ đèn xanh.
- **Vấn đề gặp phải:** *"Truy vấn lọc theo cấp đường và sắp xếp giảm dần theo xác suất rủi ro phải trả về dưới `10ms` mà không được gây `In-Memory SORT` làm treo cơ sở dữ liệu khi đồng thời có luồng ghi từ Spark."*
- **Yêu cầu chuyển thành tính năng (`UC-2`, `EC-10`):** Thiết kế `Compound Index` `{street_level: 1, severe_prob: -1, segment_id: 1}` trên `MongoDB 8.0` (`[C16]`) đảm bảo kế hoạch thực thi `IXSCAN` tuyệt đối, đồng thời dùng `Partitioned Bulk UpdateOne(upsert=True)` và nhãn trạng thái `STREAM_PROVISIONAL` $\to$ `BATCH_RECONCILED`.

### Persona 3: Chuyên Gia Quy Hoạch Hạ Tầng Giao Thông (`Urban Infrastructure Planner — Sở GTVT`)
- **Mục tiêu công việc:** Đánh giá các điểm nghẽn cấu trúc trên toàn bộ `84,633` đoạn đường của TP.HCM để lập kế hoạch mở rộng cầu, vòng xoay và phân luồng tải trọng.
- **Vấn đề gặp phải:** *"Tới `88.15%` số đoạn đường trên bản đồ chưa từng có bản tin GPS trong tập dữ liệu lịch sử. Làm sao biết đoạn đường nào là nút cổ chai tiềm ẩn và dự báo hành vi của nó khi chưa có cảm biến?"*
- **Yêu cầu chuyển thành tính năng (`UC-3`, `UC-5`, `EC-6`, `EC-7`):** Chạy `Directed Line-Graph PageRank` ($\beta=0.85$) trên toàn bộ `84,633` đỉnh đoạn đường (`151,929` cung chuyển tiếp) và kết hợp với `Train-Only 7D Behavioral K-Means` + `Static Topological Fallback` (`[C13b]`).

### Persona 4: Kỹ Sư Nền Tảng Dữ Liệu & MLOps (`Data Platform & MLOps Engineer`)
- **Mục tiêu công việc:** Đảm bảo chất lượng dữ liệu đầu vào, chống thất thoát bản tin khi mạng trễ hoặc container khởi động lại, và duy trì tính toàn vẹn của mô hình học máy.
- **Vấn đề gặp phải:** *"Dữ liệu thô có tên đường lỗi mã hóa Unicode sinh ra luật tự suy diễn vô nghĩa, `85.21%` thiếu vận tốc tối đa, và `1.5%` sự kiện đến trễ sau khi `Watermark` đã đóng cửa sổ."*
- **Yêu cầu chuyển thành tính năng (`UC-4`, `UC-6`, `EC-1`–`EC-5`, `EC-8`–`EC-10`):** Thiết lập `Ordered Quality Funnel` (`[C5]`), `Quarantine Store` (`[C15]`), `Commutative Monoid Combiner` (`[C9]`), và `Two-Tier Reconciliation Engine` (`[C12]`).
