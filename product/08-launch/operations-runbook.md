# Sổ Tay Vận Hành Hệ Thống (Operations Runbook — Wave 2 Stack)

## 1. Khởi chạy Hạ tầng Mặc định (`6 Services — 12.0 / 16.0 GB RAM`)
```bash
docker compose up -d --build
```
Các dịch vụ mặc định khởi chạy cùng lúc:
- `toc-web` (`http://localhost:8000`): Web TOC Command Center (`FastAPI + React/Vite`).
- `app` (`http://localhost:8501`): Streamlit Operations & Analytics Console.
- `spark`: PySpark `3.5.7` (`hcmc-traffic-spark:3.5.7`) xử lý batch & structured streaming.
- `kafka`: Kafka KRaft broker nhận GPS telemetry.
- `live-consumer`: Worker tiêu thụ luồng trực tiếp với `32 KiB RotatingBloom` + `1 KiB 64-bit HLL`.
- `mongo`: MongoDB `8.0.34` (`33,394` documents, composite `IXSCAN`).

## 2. Xử lý Sự cố Vận hành (Incident Playbooks)
### Sự cố 1: Sai lệch Mất mát Dữ liệu do GPS Đến Trễ Quá Watermark (`30m`)
- **Triệu chứng:** Một số khung giờ có độ đầy đủ `98.5%` trước thời điểm chốt do `500` cửa sổ đến trễ quá `Watermark = 30m` trên `226` khung giờ `(date, period)`.
- **Khắc phục:** Chạy quy trình Tier-2 Backfill từ `bronze_raw_events/` (`90,787` pings lưu đủ `100%` tại `Sink 1`). Hệ thống tự động chữa lành `609` cửa sổ `Lag Store` hạ lưu, chấm lại `384` cửa sổ (`185` thay đổi xác suất/nhãn) và thực hiện MongoDB bulk upsert với **atomic `$status_version` update pipeline guard** nâng trạng thái lên `BATCH_RECONCILED (v2)` (từ chối `25/25` bản tin `v1` cũ đến muộn).

### Sự cố 2: Cảnh báo Sai lệch Registry hoặc Chữ ký Mô hình (`SHA-256 Drift`)
- **Triệu chứng:** `verify_frozen_registry_integrity()` báo không khớp hash trước khi suy diễn luồng.
- **Khắc phục:** Dừng `live-consumer`, kiểm tra lại các tạo tác đóng băng trong Zone E:
  - `outputs/model.joblib` (`a2e12b7c...`)
  - `outputs/vmax_lookup.json` (`41577e2e...`)
  - `data/lakehouse/segment_static_features.parquet` (`71a5565e...`)
  - `outputs/model_registry_manifest.json` (`model_version: rf_full_23d_v3`)
  Tuyệt đối không tự gọi `.fit()` trên worker streaming.

### Sự cố 3: Áp lực Bộ nhớ RAM trên Kafka Stream hoặc Spark Join
- **Triệu chứng:** Tải đột biến khi phát lại lượng lớn sự kiện trùng lặp hoặc join bảng tĩnh `84,206` đoạn đường.
- **Kiểm soát:**
  1. Giữ cố định bộ lọc `32 KiB RotatingBloom` (`2 x 16 KiB`, FPR `0.6109%`, bắt `333/333` bản trùng) + `1 KiB 64-bit HLL` + `WAL` (`73.7 KB`).
  2. Spark join phải đi qua kế hoạch `BroadcastHashJoin` (`1 Exchange`, `120.45 ms`, cải thiện `17.07%` so với `SortMergeJoin 145.24 ms`).

## 3. Lưu ý Kiểm tra Tự động
- Khi kiểm tra bằng chứng nền tảng từ `outputs/*.json` và `data/`, lưu ý `tests/test_platform_v3.py` (`23` hàm) hiện vẫn đang chờ đồng bộ assertion từ Wave 1 sang baseline Wave 2 trên một track chuyên trách.
