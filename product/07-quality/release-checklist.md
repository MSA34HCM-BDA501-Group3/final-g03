# Danh Mục Kiểm Tra Trước Phát Hành (Release Readiness Checklist — Gate 3)

## Đánh Giá Cổng Kiểm Soát `Gate 3 — Release Readiness`

- [x] **1. Hoàn thành Toàn bộ Hiện vật Kiến trúc (`[C1]`–`[C17]`):**
  - Đã tạo đầy đủ hồ dữ liệu `Medallion Parquet Lakehouse` (`bronze_raw_events`, `silver_cleaned_windows`, `curated_gold/year_month=*`, `segment_static_features.parquet`), mô hình đóng băng `outputs/model.joblib` + `outputs/model_registry_manifest.json`, và các báo cáo kiểm chứng trong `outputs/`.
- [x] **2. Vượt qua Toàn bộ Kiểm thử Tự động & Kiểm tra Tĩnh (`Zero Defects`):**
  - `pytest` chạy thành công `100%` các bài kiểm thử đơn vị, tích hợp và trường hợp biên (`P1`–`P4`, `UC-1`–`UC-6`, `EC-1`–`EC-10`).
  - `ruff check` đạt `0` lỗi linting và mọi hàm đều giữ `Cognitive Complexity < 15`.
- [x] **3. Sẵn sàng Giám sát, Phục hồi & Rollback (`Monitoring & Rollback Readiness`):**
  - Có `WAL Checkpoints` (`outputs/checkpoints/`), nhật ký tiến độ luồng (`outputs/query_progress.json`), kiểm toán đối soát (`outputs/reconciliation_audit.csv`), và cơ chế ghi lũy đẳng `Idempotent Upsert` trên `MongoDB 8.0`.
- **Kết luận `Gate 3`:** **PASSED (Đủ điều kiện triển khai `Pilot & Product Launch`).**
