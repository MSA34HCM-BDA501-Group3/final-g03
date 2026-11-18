# Thiết Kế Bảo Mật, Quản Trị Dữ Liệu & Độ Tin Cậy (Security, Governance & Reliability Design)

## 1. Bảo Vệ Toàn Vẹn Nguồn Dữ Liệu & Cô Lập Lỗi (`Data Integrity & Fault Isolation`)

1. **Chế độ Chỉ Đọc Cho Dữ Liệu Gốc (`Read-Only Raw Data Mount`):** Trong `docker-compose.yml` và `compose.quyen.yml`, thư mục `raw_data/` được gắn kết ở chế độ chỉ đọc (`:ro`), ngăn chặn tuyệt đối mọi thao tác ghi đè hoặc làm hỏng 5 bảng dữ liệu gốc.
2. **Cách ly Dữ liệu Bẩn Không Rơi Rớt Ngầm (`Zero Silent Drop Quarantine`):** Mọi bản ghi vi phạm quy tắc kiểm định trong `[C5]` `Ordered Quality Funnel` đều được gán nhãn quy tắc vi phạm đầu tiên (`first_failing_rule`) và lưu vào `[C15]` (`data/lakehouse/quarantine_events.parquet`, `580` bản ghi GIS + cách ly `151` ping phi đường bộ từ `90,938 → 90,787`) để kiểm toán.
3. **Chống Data Leakage Học máy (`Anti-Leakage Governance`):**
   - Bộ biến đổi (`SimpleImputer`, `StandardScaler`, `KMeans`, `3-Tier Velocity Imputer`, `RandomForest`, `GBT`) chỉ được `.fit()` trên tập `TRAIN` hữu cơ ở `Phase A` (sau khi loại bỏ `3,596` cặp burst tổng hợp, giữ `1,885` cặp `TRAIN` hữu cơ, không dùng `sample_weight` nhân tạo).
   - Bất biến không gian/thời gian bảo đảm `max(train.target_ts) <= train_cutoff`, `max_abs_diff = 0.0` khi đảo trộn tương lai $\tau > t$, và `[C13a/b]` giữ nguyên mã băm `SHA-256` (`model = a2f411846508`, `static_store = 75328ac2d5e3`, `vmax_lookup = ee7f692daad8`, `sample_static = f26ebdf7d396`) trước và sau khi chạy `Phase B` / `Phase C`.

---

## 2. An Toàn Ghi Đồng Thời & Khôi Phục Thảm Họa (`Concurrency & Crash Recovery`)

1. **`Partitioned Idempotent Upsert` & Atomic `$status_version` Pipeline Guard (`EC-10`):** Các tác vụ ghi vào `[C16]` `MongoDB 8.0` băm theo `segment_id` thành `4` phân vùng (`69` lô `500` docs), sử dụng **pipeline update nguyên tử kiểm tra `$status_version`** trên `Unique Index` `{"event_id": 1}`, triệt tiêu lỗi `E11000` và ngăn tuyệt đối mọi bản ghi `STREAM_PROVISIONAL (v1)` đến muộn ghi đè lên `BATCH_RECONCILED / BATCH_VERIFIED (v2)` (đã kiểm chứng chặn `25/25` bản ghi `v1` cũ).
2. **Nhật ký `Write-Ahead Log (WAL)` (`outputs/checkpoints/`):** Lưu trữ offset luồng và trạng thái cửa sổ (`73,714 bytes = 73.7 KB` trong `outputs/query_progress.json`) để phục hồi tức thời sau khi ngắt cưỡng bức (`--kill-after-batch 1`) với `0` hiệu ứng phụ trùng lặp.
3. **Tuân thủ Bảo mật Git (`Git Security Hygiene`):** Không lưu khóa bí mật (`secrets`), `.env` hoặc thông tin nhạy cảm trong mã nguồn; `raw_data/` và `data/` (ngoại trừ `data/demo_sample/`) nằm trong `.gitignore`.

## 3. Ranh Giới Bảo Mật Môi Trường Demo

`docker-compose.yml` là cấu hình proof-of-concept chạy cục bộ gồm `5` dịch vụ mặc định (`11.0 GB RAM`): Kafka và MongoDB hiện chưa bật SASL/TLS hoặc xác thực tài khoản; các cổng host (`8000` cho `toc-web`, `9092` cho `kafka`, `27017` cho `mongo`) được bind chặt vào `127.0.0.1` và các container giao tiếp trong bridge network nội bộ `bda501-net`. Không triển khai cấu hình này ra mạng công cộng khi chưa bật xác thực/ủy quyền, TLS, quản lý secret và audit log.
