# Danh Mục Kiểm Tra Trước Phát Hành (Release Readiness Checklist — Gate 3)

> **Tính chất của trạng thái:** Các mục đã đánh dấu bên dưới ghi lại kết quả release trước đây. Audit ngày 2026-11-18 không thể tái chạy do thiếu `raw_data/`, `data/` và runtime Python/Docker; xem [Session 9 readiness audit](../../evidence/session9/I_readiness.md) trước khi dùng trạng thái này làm bằng chứng phát hành hiện tại.

## Đánh Giá Cổng Kiểm Soát `Gate 3 — Release Readiness`

- [x] **1. Hoàn thành Toàn bộ `Architecture Artifacts` (`[C1]`–`[C17]`, `G01–G05`):**
  - Đã tạo đầy đủ `Medallion Parquet Lakehouse` (`bronze_raw_events`, `silver_cleaned_windows`, `curated_gold/year_month=*` với timestamp chuẩn microsecond `us` tương thích `Spark 3.5.7`, `segment_static_features.parquet`), `Frozen Model Registry` bất biến 3 thành phần (`outputs/model.joblib`, `data/lakehouse/segment_static_features.parquet`, `outputs/vmax_lookup.json` kèm SHA-256 trong `outputs/model_registry_manifest.json`), và các báo cáo kiểm chứng thực tế trong `outputs/`.
- [x] **2. Vượt qua Toàn bộ Kiểm thử Tự động & Kiểm tra Tĩnh (`Zero Defects`, `T01–T08`):**
  - `pytest` chạy thành công `100%` các bài kiểm thử đơn vị, tích hợp, nhiễu tương lai (`future perturbation`) và tiêm lỗi (`failure injection`) cho `P1`–`P4`, `UC-1`–`UC-6`, `EC-1`–`EC-10`, `Q01`–`Q11`, `D01`–`D12`, `M01`–`M14`.
  - `ruff check` đạt `0` lỗi linting và mọi hàm đều giữ `Cognitive Complexity < 15`.
- [x] **3. Sẵn sàng Giám sát, Phục hồi & Rollback (`Monitoring & Rollback Readiness`):**
  - Có `WAL Checkpoints` (`outputs/checkpoints/`), kiểm thử kill/restart micro-batch recovery (`outputs/query_progress.json`), kiểm toán `Two-Tier Reconciliation` (`outputs/reconciliation_audit.csv`), bằng chứng kế hoạch thực thi vật lý `Spark 3.5.7` (`outputs/spark_runtime_evidence.json`, `outputs/join_plan_evidence.json`, `outputs/quyen_validation/*`) và cơ chế `Idempotent Bulk Upsert` kèm `Version Guard` (`v2 >= v1`) trên `MongoDB 8.0.34` (`outputs/mongo_verify.json`).
- [x] **4. Tích hợp Nhánh Spark Data Engineering của Quyền (`PR #1` — `spark_batch.py` & `traffic_data/spark_etl.py`):**
  - Đã tích hợp đầy đủ 17 tệp từ `origin/feat/quyen-spark-data-engineering` (`commit 2067fcd`), khắc phục toàn bộ 6 điểm kỹ thuật (tương thích `Spark 3.5.7 / Java 17` & `Spark 4.2.0 / Java 21`, `3` `BroadcastHashJoin` / `0` `SortMergeJoin`, 1-stage vs 2-stage salted Monoid trong `aggregate_monoid`, hỗ trợ đọc event log `local-*` và `events_*`, bảo toàn `date_utc` song song với `date` ICT, và giữ `Cognitive Complexity < 15`), đồng thời chốt đầy đủ các mục xác nhận `D-C01..D-C08` và `M-C01..M-C11` trong `docs/HUONG_DAN_BRANCH_QUYEN_VI.md`.
- **Kết luận lịch sử `Gate 3`:** **PASSED tại lần chạy trước; NOT REVALIDATED trong checkout hiện tại.**


