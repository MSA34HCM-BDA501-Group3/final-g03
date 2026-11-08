# Kế Hoạch Triển Khai & Đưa Sản Phẩm Vào Sử Dụng (Go-To-Market & Adoption Plan)

## 1. Lộ Trình Triển Khai Theo 3 Giai Đoạn (`Phased Rollout Plan`)

| Giai đoạn | Đối tượng Áp dụng | Hạng mục Triển khai Chính | Chỉ số Nghiệm thu |
| :--- | :--- | :--- | :--- |
| **Phase 1: `TOC Shadow Mode`** | Trung tâm Quản lý Điều hành Giao thông Đô thị TP.HCM (`TOC`) | Triển khai `Streamlit 5-Tab UI (:8501)` và bảng cảnh báo `Top-25 Decaying Hotspots` (`UC-1`) chạy song song với màn hình camera giám sát hiện hữu. | Thời gian cập nhật `< 5s`, `0` cảnh báo giả từ nhịp đèn đỏ đơn lẻ. |
| **Phase 2: `Corridor Signal Integration`** | Các trục giao thông trọng điểm (Cộng Hòa – Trường Chinh, Lý Thường Kiệt, Quốc lộ 1A, Xa Lộ Hà Nội) | Kết nối API truy vấn `MongoDB 8.0 IXSCAN` (`UC-2`) và bảng `30` luật đồng kẹt xe `SON/FPGrowth` (`UC-4`) vào hệ thống điều khiển pha đèn liên quận. | Độ trễ truy vấn `< 10ms` (`IXSCAN`), cảnh báo sớm trước `30 phút` với `PR-AUC >= 0.75`. |
| **Phase 3: `City-Wide Urban Planning`** | Sở GTVT TP.HCM & Viện Nghiên cứu Phát triển | Cung cấp kho đặc trưng tĩnh `84,633` đoạn đường (`segment_static_features.parquet`) kèm điểm `Line-Graph PageRank` và `7D Behavioral Clusters` (`UC-3`, `UC-5`). | Độ phủ `100%` mạng lưới đường bộ thành phố (bao gồm `88.15%` vùng mù cảm biến). |
