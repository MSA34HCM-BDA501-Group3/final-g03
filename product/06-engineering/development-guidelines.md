# Hướng Dẫn Phát Triển (Development Guidelines)

## 1. Cấu trúc Codebase & Phân tách Trách nhiệm
- `pipeline.py`: Engine Batch, Graph PageRank, Lakehouse (`Bronze -> Silver -> Gold`), K-Means `7D`, Causal Priors, Burst Filtering, Huấn luyện ML (`RF 23D`), Streaming Sketches (`RotatingBloom`, `64-bit HLL`, `WAL`), Two-Tier Reconciliation và kiểm chứng MongoDB 8.0.
- `spark_engine.py`: Engine Spark `3.5.7` (`hcmc-traffic-spark:3.5.7`) xử lý distributed Monoid, Catalyst join optimization (`SortMergeJoin -> BroadcastHashJoin`), Spark Random Forest và Structured Streaming Dual-Sink.
- `live_kafka_stream.py`: Producer/Consumer Kafka `live-consumer` cập nhật thời gian thực với `RotatingBloom`, `64-bit HyperLogLog` và `atomic $status_version` guard.
- `toc_backend/` & `toc_frontend/`: Web TOC Command Center (`FastAPI :8000` + `React/Vite`) phục vụ bản đồ giao thông, kịch bản điều hành (`story_scenarios.py`) và dữ liệu kiểm toán trực quan.
- `app.py`: Streamlit Operations & Analytics Console (`:8501`).
- `tests/`: Bộ kiểm thử tự động (`tests/test_platform_v3.py`, `tests/test_quyen_spark.py`).

## 2. Quy chuẩn Kỹ thuật Bắt buộc (Non-Negotiable Standards)
1. **Zero-Conflict 3-Phase Contract:** Chỉ `Phase A` (`pipeline.py` trên `TRAIN` hữu cơ) được gọi `.fit()`; `Phase B` (Stream Scoring) và `Phase C` (Reconciliation) chỉ nạp `outputs/model.joblib`, `outputs/vmax_lookup.json`, `outputs/model_registry_manifest.json`, và `data/lakehouse/segment_static_features.parquet` ở chế độ `read-only` sau khi xác thực `SHA-256`.
2. **Sạch Rò rỉ & Sạch Burst:** Nghiêm cấm tính prior tĩnh xuyên tương lai, nghiêm cấm đưa `3,596` cặp synthetic burst (`2020-08-02 23:30` & `2020-08-03 00:00`) vào tập huấn luyện có giám sát, và tuyệt đối không dùng `sample_weight` nhân tạo hay heuristic fallback bypass.
3. **Bounded Streaming State:** Luôn dùng `RotatingBloom` kích thước cố định `32 KiB` (`2 x 16 KiB`), `64-bit HyperLogLog` `1 KiB` (`1,024` registers) và `WAL` giới hạn kích thước để tránh rò rỉ bộ nhớ trên luồng vô hạn.
4. **Idempotent Database Writes:** Mọi thao tác upsert vào MongoDB 8.0 (`33,394` docs) phải đi kèm `update_pipeline` kiểm tra nguyên tử `$status_version` để chặn bản tin `v1` đến muộn ghi đè `v2`. Không hardcode thông tin xác thực trong mã nguồn hay tài liệu.
5. **Chất lượng Mã & Độ phức tạp:** Giữ McCabe & Cognitive Complexity `< 15` cho mọi hàm Python mới/sửa; chạy `ruff check` sạch lỗi trước khi kết thúc tác vụ.
6. **Trung thực Số liệu:** Mọi tài liệu (`README.md`, `TECHNICAL_NOTE.md`, `docs/`, `product/`) phải đối chiếu trực tiếp từ `outputs/` và `data/`, không phóng đại chỉ số hay kết quả kiểm thử chưa đồng bộ.
