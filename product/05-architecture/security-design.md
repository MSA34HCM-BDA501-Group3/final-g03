# Thiết Kế Bảo Mật, Quản Trị Dữ Liệu & Độ Tin Cậy (Security, Governance & Reliability Design)

## 1. Bảo Vệ Toàn Vẹn Nguồn Dữ Liệu & Cô Lập Lỗi (`Data Integrity & Fault Isolation`)

1. **Chế độ Chỉ Đọc Cho Dữ Liệu Gốc (`Read-Only Raw Data Mount`):** Trong `docker-compose.yml`, thư mục `raw_data/` được gắn kết ở chế độ chỉ đọc (`./raw_data:/data/raw:ro`), ngăn chặn tuyệt đối mọi thao tác ghi đè hoặc làm hỏng 5 bảng dữ liệu gốc.
2. **Cách ly Dữ liệu Bẩn Không Rơi Rớt Ngầm (`Zero Silent Drop Quarantine`):** Mọi bản ghi vi phạm quy tắc kiểm định trong `[C5]` `Ordered Quality Funnel` đều được gán nhãn quy tắc vi phạm đầu tiên (`first_failing_rule`) và lưu vào `[C15]` `quarantine/` để kiểm toán.
3. **Chống Data Leakage Học máy (`Anti-Leakage Governance`):**
   - Bộ biến đổi (`SimpleImputer`, `StandardScaler`, `KMeans`, `3-Tier Velocity Imputer`, `Tree Ensembles`) chỉ được phép `.fit()` trên `70% TRAIN` ở `Phase A`.
   - Kiểm tra tự động bằng `pytest` xác nhận `0` chồng lấn thời gian giữa `TRAIN`, `VAL`, `TEST` và xác nhận `[C13a]` có mã băm `SHA-256` không đổi trước và sau khi chạy `Phase B` / `Phase C`.

---

## 2. An Toàn Ghi Đồng Thời & Khôi Phục Thảm Họa (`Concurrency & Crash Recovery`)

1. **`Partitioned Idempotent Upsert` (`EC-10`):** Các tác vụ ghi vào `[C16]` `MongoDB 8.0` băm theo `segment_id` thành `4` phân vùng (`repartition(4)`), mỗi phân vùng đóng gói lô `500` lệnh `UpdateOne({"event_id": eid}, {"$set": doc}, upsert=True)` trên khóa `Unique Index` `{"event_id": 1}`, triệt tiêu hoàn toàn lỗi `E11000 duplicate key error` khi chạy lại job hoặc khi `Phase C` ghi đè `Phase B`.
2. **Nhật ký `Write-Ahead Log (WAL)` (`outputs/checkpoints/`):** Lưu trữ offset luồng và trạng thái cửa sổ tại `outputs/checkpoints/raw/` và `outputs/checkpoints/agg/` cùng `outputs/query_progress.json` để phục hồi tức thời sau khi khởi động lại container.
3. **Tuân thủ Bảo mật Git (`Git Security Hygiene`):** Không lưu khóa bí mật (`secrets`), `.env` hoặc thông tin nhạy cảm trong mã nguồn.
