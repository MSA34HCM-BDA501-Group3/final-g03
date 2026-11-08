# Phân Tích Dữ Liệu Sản Phẩm & Đo Lường Vận Hành (Product Analytics — Phase 10)

## 1. Thống Kê Toàn Cảnh Hồ Dữ Liệu & Mạng Lưới (`Lakehouse & Network Telemetry`)

- **Quy mô Đồ thị Đường bộ (`[C2]`, `[C10]`):**
  - Tổng số nút tọa độ thô: `577,967` $\to$ Nút giao thực tế hoạt động: **`52,425` (`9.07%`)**; Đỉnh hình học OSM mồ côi đã cắt tỉa: **`525,542` (`90.93%`)**.
  - Tổng số đoạn đường có hướng: **`84,633`** (trên `5,553` tuyến đường), tạo thành **`151,929`** cạnh chuyển tiếp `Line-Graph`, **`48`** nút cụt đầu ra (`Dangling Nodes`) và **`49`** nút cụt đầu vào (`Source-Only Nodes`).
  - Tỷ lệ đoạn đường có quan sát GPS lịch sử: **`10,027 / 84,633` (`11.85%`)**; Tỷ lệ vùng mù cảm biến (`Cold-Start Segments` được phủ bởi `[C13b]`): **`74,606 / 84,633` (`88.15%`)**.
- **Phân bố Mức độ Phục vụ (`LOS A–F`) trên `33,441` Cửa sổ 30 Phút:**
  - `LOS A`: `13,278` (`39.71%`) · `LOS B`: `4,700` (`14.05%`) · `LOS C`: `3,839` (`11.48%`)
  - `LOS D`: `3,707` (`11.09%`) · `LOS E`: `3,828` (`11.45%`) · `LOS F`: `4,089` (`12.23%`)
  - Tổng tỷ lệ ùn tắc nghiêm trọng (`Severe Congestion LOS E/F`): **`7,917 / 33,441` (`23.67%`)** toàn lịch sử; tăng lên **`1,177 / 3,513` (`33.50%`)** trên tập kiểm thử cuối kỳ (`Temporal Label Shift`).

---

## 2. Phân Tích Hồ Sơ 6 Cụm Hành Vi Giao Thông (`7D Behavioral K-Means Regimes — [C10]`)

| Mã Cụm (`Cluster`) | Số Đoạn Đường (`TRAIN = 7,549`) | Đặc Trưng Hành Vi Vĩ Mô (`Behavioral Regime Archetype`) | Ứng Dụng Điều Hành |
| :---: | ---: | :--- | :--- |
| **`Cluster 0`** | `920` | Các trục đường nhánh đô thị lưu lượng ổn định, vận tốc trung bình khá, ít biến động đột ngột. | Chu kỳ đèn tín hiệu tiêu chuẩn. |
| **`Cluster 1`** | `1,507` | Các đoạn đường nội đô có mật độ nút giao dày, thường xuyên chuyển trạng thái sang `LOS D/E` giờ cao điểm. | Ưu tiên giám sát lan truyền `SON/FPGrowth` (`UC-4`). |
| **`Cluster 2`** | `1,360` | Các đoạn đường kết nối liên quận có vận tốc thiết kế trung bình – cao (`40–50 km/h`), nhạy cảm với sóng ùn tắc đầu nguồn. | Điều phối làn sóng xanh (`Green Wave`). |
| **`Cluster 3`** | `392` | Nhóm đại lộ / quốc lộ huyết mạch (`Quốc lộ 1`, `Xa Lộ Hà Nội`, `Mai Chí Thọ`) có vận tốc thiết kế cao (`60–80 km/h`). | Truy vấn ưu tiên `UC-2` (`street_level IN [1,2]`). |
| **`Cluster 4`** | `3,127` | Nhóm đường nhánh và hẻm thông thoáng (`LOS A/B` chiếm ưu thế, tỷ số vận tốc $r \ge 0.75$). | Dùng làm đường tránh phân luồng khi trục chính kẹt cứng. |
| **`Cluster 5`** | `243` | Nhóm nút cổ chai cấu trúc có điểm `Line-Graph PageRank` rất cao (`PR > 2.0`), hội tụ nhiều luồng rẽ vào. | Trọng điểm cải tạo hạ tầng Sở GTVT (`UC-3`). |
