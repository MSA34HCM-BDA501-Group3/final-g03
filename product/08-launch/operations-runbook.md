# Sổ Tay Vận Hành Hệ Thống (Operations Runbook — 5-Service Stack)

## 1. Khởi chạy Hạ tầng Mặc định (`5 Services — 11.0 / 16.0 GB RAM`)
```bash
docker compose up -d --build
```
Các dịch vụ mặc định khởi chạy cùng lúc (`+2` profile tùy chọn `batch` cho `pipeline` và `stream` cho `stream`):
- `kafka` (`127.0.0.1:9092`, `1536M`): Kafka `4.1.0` KRaft broker nhận GPS telemetry (`3` partitions).
- `mongo` (`127.0.0.1:27017`, `2048M`): MongoDB `8.0.34` (`33,394` documents, `1,234` arterial alerts, composite `IXSCAN 0.360 / 0.603 ms`, `31.36x` vs `COLLSCAN`).
- `spark` (nội bộ `bda501-net`, `6144M`): PySpark `3.5.7` (`hcmc-traffic-spark:3.5.7`) xử lý batch, MLlib organic & structured streaming.
- `toc-web` (`http://localhost:8000`, `1024M`): Ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center** (`toc_backend/main.py` + `toc_frontend/dist/`).
- `live-consumer` (nội bộ `bda501-net`, `512M`): Worker tiêu thụ luồng trực tiếp với `32.0 KiB RotatingBloomFilter` (`0.0389%` FPR) + `1.0 KiB 64-bit HLL++` (`3.75%`).

## 2. Xử lý Sự cố Vận hành (Incident Playbooks)
### Sự cố 1: Sai lệch Mất mát Dữ liệu do GPS Đến Trễ Quá Watermark (`30m`)
- **Triệu chứng:** Một số khung giờ có độ đầy đủ `98.5%` trước thời điểm chốt do `500` cửa sổ đến trễ quá `Watermark = 30m` trên `226` khung giờ `(date, period)`.
- **Khắc phục:** Chạy quy trình Tier-2 Backfill từ `bronze_raw_events/` (`90,787` pings lưu đủ `100%` tại `Sink 1`). Hệ thống tự động chữa lành `609` cửa sổ `Lag Store` hạ lưu, chấm lại `384` cửa sổ (`213` thay đổi xác suất dự báo) và thực hiện MongoDB bulk upsert với **atomic `$status_version` update pipeline guard** nâng trạng thái lên `BATCH_RECONCILED (v2)` (từ chối `25/25` bản tin `v1` cũ đến muộn).

### Sự cố 2: Cảnh báo Sai lệch Registry hoặc Chữ ký Mô hình (`SHA-256 Drift`)
- **Triệu chứng:** `verify_frozen_registry_integrity()` báo không khớp hash trước khi suy diễn luồng.
- **Khắc phục:** Dừng `live-consumer`, kiểm tra lại các tạo tác đóng băng trong Zone E:
  - `outputs/model.joblib` (`a2f411846508...`)
  - `outputs/vmax_lookup.json` (`ee7f692daad8...`)
  - `data/lakehouse/segment_static_features.parquet` (`75328ac2d5e3...`; bản mẫu `data/demo_sample/sample_static_features.parquet`: `f26ebdf7d396...`)
  - `outputs/model_registry_manifest.json` (`model_version: rf_full_23d_v3`)
  Tuyệt đối không tự gọi `.fit()` trên worker streaming.

### Sự cố 3: Áp lực Bộ nhớ RAM trên Kafka Stream hoặc Spark Join
- **Triệu chứng:** Tải đột biến khi phát lại lượng lớn sự kiện trùng lặp hoặc join bảng tĩnh `84,206` đoạn đường.
- **Kiểm soát:**
  1. Giữ cố định bộ lọc `32.0 KiB RotatingBloomFilter` (`2 x 16 KiB`, `333/333` bản trùng, `13` FPs, `0.0389%` empirical vs `0.0792%` theoretical) + `1.0 KiB 64-bit HLL++` + `WAL` (`73.7 KB`, `73,714 B`).
  2. Spark join phải đi qua kế hoạch `BroadcastHashJoin` (`1 Exchange`, `123.37 ms`, tăng tốc `25.76%` so với `SortMergeJoin 166.18 ms`, `0 exceptAll diff`).

## 3. Kiểm tra Tự động & Đối chiếu Trạng thái (`Gate 3 PASS`)
- Chạy `python -m pytest -v` để xác thực toàn bộ bộ kiểm thử tự động (`tests/test_platform_v3.py`, `tests/test_quyen_spark.py` — `Gate 3 PASS`), đã đồng bộ hoàn toàn với bằng chứng thực nghiệm sạch trên đĩa (`outputs/*.json`, `data/`) và hồ sơ kiểm định Session 9 đã tái sinh (`evidence/session9/*`).
