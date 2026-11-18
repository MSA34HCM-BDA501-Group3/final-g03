# Hướng Dẫn Phát Triển (Development Guidelines)

## 1. Cấu trúc Codebase & Phân tách Trách nhiệm
- `pipeline.py`: Engine Batch, Graph PageRank, Lakehouse (`Bronze -> Silver -> Gold`), Unified K-Means `7D`, Causal Priors, Burst Filtering, Huấn luyện ML (`RF 23D`), Streaming Sketches (`RotatingBloomFilter`, `64-bit HLL++`, `WAL`), Two-Tier Reconciliation và kiểm chứng MongoDB 8.0.
- `spark_engine.py`: Engine Spark `3.5.7` (`hcmc-traffic-spark:3.5.7`) xử lý distributed Monoid, Catalyst join optimization (`SortMergeJoin -> BroadcastHashJoin`), Spark MLlib organic TRAIN (`FPGrowth`, `KMeans`, `RandomForest`) và Structured Streaming Dual-Sink.
- `live_kafka_stream.py`: Producer/Consumer Kafka `live-consumer` cập nhật thời gian thực với `RotatingBloomFilter`, `64-bit HLL++` và `atomic $status_version` guard.
- `toc_backend/` & `toc_frontend/`: Ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`, dịch vụ `toc-web`)** phục vụ bản đồ giao thông Leaflet, kịch bản điều hành, live Kafka replay và kiểm toán trực quan 5 tab.
- `tests/`: Bộ kiểm thử tự động (`tests/test_platform_v3.py`, `tests/test_quyen_spark.py` — `Gate 3 PASS`, đồng bộ hoàn toàn với chuẩn sạch không rò rỉ).

## 2. Quy chuẩn Kỹ thuật Bắt buộc (Non-Negotiable Standards)
1. **Zero-Conflict 3-Phase Contract:** Chỉ `Phase A` (`pipeline.py` trên `TRAIN` hữu cơ) được gọi `.fit()`; `Phase B` (Stream Scoring) và `Phase C` (Reconciliation) chỉ nạp `outputs/model.joblib` (`a2f411846508`), `outputs/vmax_lookup.json` (`ee7f692daad8`), `outputs/model_registry_manifest.json`, và `data/lakehouse/segment_static_features.parquet` (`75328ac2d5e3`) ở chế độ `read-only` sau khi xác thực `SHA-256`.
2. **Sạch Rò rỉ & Sạch Burst:** Nghiêm cấm tính prior tĩnh xuyên tương lai, nghiêm cấm đưa `3,596` cặp synthetic burst (`2020-08-02 23:30` & `2020-08-03 00:00`) vào tập huấn luyện có giám sát, và tuyệt đối không dùng `sample_weight` nhân tạo hay heuristic fallback bypass.
3. **Bounded Streaming State:** Luôn dùng `RotatingBloomFilter` kích thước cố định `32.0 KiB` (`2 x 16 KiB`, `0.0389%` FPR), `64-bit HLL++` `1.0 KiB` (`1,024` registers, `3.75%` sai số) và `WAL` giới hạn kích thước (`73.7 KB`) để tránh rò rỉ bộ nhớ trên luồng vô hạn.
4. **Idempotent Database Writes:** Mọi thao tác upsert vào MongoDB 8.0 (`33,394` docs) phải đi kèm `update_pipeline` kiểm tra nguyên tử `$status_version` để chặn bản tin `v1` đến muộn ghi đè `v2`. Không hardcode thông tin xác thực trong mã nguồn hay tài liệu.
5. **Chất lượng Mã & Độ phức tạp:** Giữ McCabe & Cognitive Complexity `< 15` cho mọi hàm Python mới/sửa; chạy `ruff check` sạch lỗi và `pytest` xanh trước khi kết thúc tác vụ.
6. **Trung thực Số liệu:** Mọi tài liệu (`README.md`, `TECHNICAL_NOTE.md`, `docs/`, `product/`) phải đối chiếu trực tiếp từ `outputs/` và `data/`, không phóng đại chỉ số hay kết quả kiểm thử.
