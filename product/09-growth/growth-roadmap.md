# Lộ Trình Phát Triển & Mở Rộng (Growth Roadmap — Post-Wave 2)

## 1. Vị thế Nền tảng Hiện tại (Wave 2 Baseline)
- **Độ phủ Dữ liệu & Hạ tầng:** `84,206` đoạn đường cơ giới sạch (`9,995` đoạn có GPS = `11.87%`; `74,211` đoạn Cold-Start = `88.13%`), `33,394` cửa sổ chuẩn hóa (`90,787` GPS pings), bounded `32 KiB RotatingBloom` (`2 x 16 KiB`) + `1 KiB 64-bit HyperLogLog`, chạy trên `6` dịch vụ Docker mặc định (`12.0 / 16.0 GB RAM`).
- **Trạng thái Kiểm soát Chất lượng:** Bằng chứng Wave 2 và tài liệu đã đồng bộ hoàn toàn; Gate 3 hiện ở mức **`CONDITIONAL`** chờ track đồng bộ assertion của `tests/test_platform_v3.py` (`23` hàm).

## 2. Lộ trình 3 Chân trời Mở rộng
### Chân trời 1 (Ngắn hạn — Hoàn thiện Gate 3 & Vận hành Live)
1. Đồng bộ toàn bộ assertion trong `tests/test_platform_v3.py` sang ngưỡng Wave 2 (`33,394`, `84,206`, `92,453`, `609`, v.v.).
2. Tối ưu cảnh báo lan truyền theo thời gian thực trên `Web TOC (:8000)` kết nối trực tiếp `14` luật trễ (`Lê Quang Định -> Trần Quang Khải`, Lift `24.0`) và `1,202` cảnh báo trục chính.

### Chân trời 2 (Trung hạn — Thu hẹp Cold-Start `88.13%` & Lớp Trung gian)
1. Tích hợp thêm nguồn dữ liệu GPS xe buýt công cộng và camera giao thông vào `bronze_raw_events/` để nâng tỷ lệ đoạn đường quan sát trực tiếp từ `11.87%` (`9,995` đoạn) lên `> 35%`, qua đó cải thiện độ chính xác không gian ngoài giới hạn hiện tại của `Masked-GPS Cold-Start Proxy` (`Acc = 0.5271, ROC-AUC = 0.6175`).
2. Bổ sung dữ liệu nhãn các mức trung gian `B/C/D/E` qua các đợt quan trắc giờ chuyển tiếp để nâng `Macro-F1` vượt ngưỡng `0.4475`.

### Chân trời 3 (Dài hạn — Điều phối Tín hiệu Đèn & Học Tăng cường Mạng lưới)
1. Kết hợp cấu trúc đồ thị `92,453` cạnh chuyển hướng và PageRank (`#1: 21969 Nguyễn Kiệm`) làm đầu vào gợi ý chu kỳ pha đèn thích ứng theo hành lang tuyến.
2. Triển khai cập nhật chu kỳ cho vùng Gold (`curated_gold/year_month=*`) và tái kiểm định định kỳ `Validation Gate` không làm suy thoái cân bằng lớp.
