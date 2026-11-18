# Lộ Trình Phát Triển & Mở Rộng (Growth Roadmap — Post-Release)

## 1. Vị thế Nền tảng Hiện tại (Leak-Free Production Baseline)
- **Độ phủ Dữ liệu & Hạ tầng:** `84,206` đoạn đường cơ giới sạch (`9,995` đoạn có GPS = `11.87%`; `74,211` đoạn Cold-Start = `88.13%`), `33,394` cửa sổ chuẩn hóa (`90,787` GPS pings), bounded `32.0 KiB RotatingBloomFilter` (`2 x 16 KiB`, `0.0389%` FPR) + `1.0 KiB 64-bit HLL++` (`3.75%`), chạy trên **`5` dịch vụ Docker mặc định (`11.0 / 16.0 GB RAM`)** phục vụ qua ứng dụng web hợp nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)**.
- **Trạng thái Kiểm soát Chất lượng (`Gate 3 PASS`):** Toàn bộ bằng chứng thực nghiệm sạch không rò rỉ, bộ kiểm thử tự động `pytest` (`tests/test_platform_v3.py`, `tests/test_quyen_spark.py`), tài liệu quản trị và hồ sơ kiểm định Session 9 (`evidence/session9/*`) đã được đồng bộ và tái sinh hoàn chỉnh.

## 2. Lộ trình 3 Chân trời Mở rộng
### Chân trời 1 (Ngắn hạn — Vận hành Live & Tối ưu Cảnh báo Trục chính)
1. Duy trì kiểm thử hồi quy liên tục (`CI/CD` cho toàn bộ bộ kiểm thử `pytest` và kiểm định `ruff` với độ phức tạp nhận thức `< 15`) khi mở rộng dữ liệu mới.
2. Tối ưu cảnh báo lan truyền theo thời gian thực trên **Web TOC (`http://localhost:8000`)** kết nối trực tiếp `14` luật trễ (`Lê Quang Định -> Trần Quang Khải`, Lift `24.0`) và `1,234` cảnh báo trục chính (`IXSCAN 0.360 / 0.603 ms`).

### Chân trời 2 (Trung hạn — Thu hẹp Cold-Start `88.13%` & Lớp Trung gian)
1. Tích hợp thêm nguồn dữ liệu GPS xe buýt công cộng và camera giao thông vào `bronze_raw_events/` để nâng tỷ lệ đoạn đường quan sát trực tiếp từ `11.87%` (`9,995` đoạn) lên `> 35%`, qua đó cải thiện độ chính xác không gian ngoài giới hạn hiện tại của `Masked-GPS Cold-Start Proxy` (`Acc = 58.63%, ROC-AUC = 0.7000, PR-AUC = 0.5152`).
2. Bổ sung dữ liệu nhãn các mức trung gian `B/C/D/E` và mẫu khởi phát đột ngột (`Onset 8 / 993`) qua các đợt quan trắc giờ chuyển tiếp để nâng `Macro-F1` vượt ngưỡng `0.4253`.

### Chân trời 3 (Dài hạn — Điều phối Tín hiệu Đèn & Học Tăng cường Mạng lưới)
1. Kết hợp cấu trúc đồ thị `92,453` cạnh chuyển hướng và PageRank (`#1: 21969 Nguyễn Kiệm`) làm đầu vào gợi ý chu kỳ pha đèn thích ứng theo hành lang tuyến.
2. Triển khai cập nhật chu kỳ cho vùng Gold (`curated_gold/year_month=*`) và tái kiểm định định kỳ `Validation Gate` không làm suy thoái cân bằng lớp.
