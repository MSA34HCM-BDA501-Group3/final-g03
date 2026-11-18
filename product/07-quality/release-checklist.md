# Danh Mục Kiểm Tra Trước Phát Hành (Release Readiness Checklist — Gate 3)

## Đánh Giá Cổng Kiểm Soát `Gate 3 — Release Readiness`

- [x] **1. Hoàn thành Toàn bộ `Architecture Artifacts` (`[C1]`–`[C17]`, `G01–G05`):**
  - Đã tạo đầy đủ `Medallion Parquet Lakehouse` (`bronze_raw_events`, `silver_cleaned_windows`, `curated_gold/year_month=*` với timestamp chuẩn microsecond `us` tương thích `Spark 3.5.7`, `segment_static_features.parquet`), `Frozen Model Registry` bất biến 3 thành phần (`outputs/model.joblib`, `data/lakehouse/segment_static_features.parquet`, `outputs/vmax_lookup.json` kèm SHA-256 trong `outputs/model_registry_manifest.json`), và các báo cáo kiểm chứng thực tế trong `outputs/`.
- [x] **2. Vượt qua Toàn bộ Kiểm thử Tự động & Kiểm tra Tĩnh (`Zero Defects`, xác nhận chạy lại ngày `2026-11-18`):**
  - `pytest -v` chạy thành công **`27/27` (`100%`)** bài kiểm thử tự động (`22/22` trong `tests/test_platform_v3.py` + `5/5` trong `tests/test_quyen_spark.py` trên container `Spark 3.5.7`), bao phủ kiểm thử đơn vị, tích hợp mạng `bda501-net` (`Spark ↔ MongoDB`), luồng trực tuyến `Kafka → Consumer → MongoDB → UI`, đối chứng `Card B (B1/B2/B3)` giữa `raw_data/` và `data/` (kèm gói mẫu `data/demo_sample/`), nhiễu tương lai (`future perturbation`) và tiêm lỗi (`failure injection`).
  - `ruff check` đạt `0` lỗi linting và mọi hàm đều giữ `Cognitive Complexity < 15` (`max-complexity=14`).
- [x] **3. Sẵn sàng Giám sát, Phục hồi, Hồ sơ Kiểm định Session 9 (`A–I`) & Phân định Nguồn gốc Số liệu:**
  - Hồ sơ `evidence/session9/` (`A_architecture` đến `I_readiness` + `README.md`) tách bạch rõ **kết quả lô lịch sử đã lưu (`2026-11-09..11`)** với **kết quả đã chạy kiểm chứng lại (`2026-11-18`: Live Kafka `200` bản tin → `192` dự báo `v1` trong `MongoDB 8.0`, `B1–B3` & `27/27 pytest`)**.
  - Có `WAL Checkpoints` (`outputs/checkpoints/`), kiểm thử kill/restart micro-batch recovery (`outputs/query_progress.json`), kiểm toán `Two-Tier Reconciliation` (`outputs/reconciliation_audit.csv`), bằng chứng kế hoạch thực thi vật lý `Spark 3.5.7` (`outputs/spark_runtime_evidence.json`, `outputs/join_plan_evidence.json`) và cơ chế `Idempotent Bulk Upsert` kèm `Version Guard` (`v2 >= v1`) trên `MongoDB 8.0.34` (`outputs/mongo_verify.json`).
- [x] **4. Tích hợp Nhánh Spark Data Engineering (`spark_batch.py` & `traffic_data/spark_etl.py`):**
  - Đã tích hợp đầy đủ các mô-đun Spark Batch (`Spark 3.5.7 / Java 17`, `BroadcastHashJoin`, `2-Stage 8-Bucket Salted 5-Tuple Monoid`, bảo toàn `date_utc` song song với `date` ICT, `Cognitive Complexity < 15`) và tài liệu hóa tại [`docs/QUYEN_DATA_ENGINEERING.md`](../../docs/QUYEN_DATA_ENGINEERING.md) & [`docs/HUONG_DAN_BRANCH_QUYEN_VI.md`](../../docs/HUONG_DAN_BRANCH_QUYEN_VI.md).
- **Kết luận `Gate 3`:** **PASSED (Đủ điều kiện triển khai `Pilot & Product Launch`).**


