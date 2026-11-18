# Đặc Tả Prototype & Bộ Đôi Giao Diện Điều Hành (`Web TOC :8000` & `Streamlit :8501`)

## 1. Cấu Hình Triển Khai (`Runtime & Binding Specification`)

- **Hai cổng giao diện vận hành song song (`[C17]`):**
  1. **Web TOC Thế hệ mới (`ReactJS + TypeScript + FastAPI + Leaflet` trên `:8000`):** Dịch vụ `toc-web` (`uvicorn toc_backend.main:app --port 8000`, frontend `toc_frontend/dist`), điều khiển Đồng hồ TOC 4 kịch bản thực tế và kích hoạt live `🎬 Phát Story qua Kafka` / `⚡ Stress-Test 200 tin`.
  2. **Dashboard Kỹ thuật `Streamlit 5-Tab` trên `:8501`:** Dịch vụ `app` (`app.py`), phục vụ phân tích định lượng sâu & kiểm chứng đối soát.
- **Nguồn dữ liệu kết nối (`Connected Artifacts`):**
  1. `data/dataset.csv` (`33,394` dòng × `60` cột) & `data/lakehouse/curated_gold/year_month=*` (`10` phân vùng tháng đọc sạch trên PyArrow/Pandas & Spark 3.5.7); kèm fallback mẫu nhanh `data/demo_sample/` trên checkout mới.
  2. `data/lakehouse/segment_static_features.parquet` (`84,206` đoạn cơ giới sạch × `23` cột: `9,995` đoạn có GPS + `74,211` đoạn `Cold-Start` = `88.13%`).
  3. `outputs/model.joblib` (`rf_full_23d_v3`) & `outputs/model_registry_manifest.json` (`[C13a]` Frozen Model Registry).
  4. `outputs/metrics.json`, `outputs/ablation_results.csv`, `outputs/transition_evaluation.csv`, `outputs/road_pagerank_top.csv`, `outputs/co_congestion_rules.csv`, `outputs/lagged_congestion_rules.csv`, `outputs/hotspot_alerts.csv`, `outputs/reconciliation_audit.csv`, `outputs/quarantine_summary.csv`, `outputs/mongo_verify.json`, `outputs/join_plan_evidence.json`, `outputs/spark_runtime_evidence.json`, `outputs/query_progress.json`, `outputs/monoid_baseline_verification.json`, `outputs/live_kafka_telemetry.json`.

## 2. Cơ Chế Tương Tác Thời Gian Thực (`Interactive Capabilities`)

- **Phóng nét đứt dự báo `+30 phút` & Live Kafka Replay (`toc-web :8000`):** Hiển thị từng khung `[t-30m, t) ➔ Chốt tại t ➔ Dự báo [t, t+30p)` trên bản đồ Leaflet TP.HCM, đồng thời cập nhật telemetry từ `Kafka 4.1` → `live-consumer` (`32 KiB RotatingBloom` + `1 KiB 64-bit HLL`) → `MongoDB 8.0`.
- **Dự báo Đơn điểm & `Cold-Start` (`app.py :8501`):** Gọi trực tiếp `artifact["ml_imputer"].transform()` và `.predict_proba()` chỉ đọc từ `[C13a]` để đối chiếu `Persistence Baseline`, `Decision Tree`, `Random Forest (23D)` và `GBT Binary Alerting`.
- **Truy xuất Đoạn đường Tương đồng (`Within-Cluster Peer Segment Retrieval`):** Tính khoảng cách Euclidean chuẩn hóa trong cùng `cluster_id` để gợi ý `Top-K` đoạn đường có chế độ giao thông giống nhất trên toàn mạng `84,206` đoạn.
- **Bộ lọc Luật Lan truyền & Kiểm toán Đối soát:** Lọc nhanh `12` luật đồng kẹt + `14` luật trễ (`persistence_controlled = True`) và theo dõi vòng đời `STREAM_PROVISIONAL (v1)` (`489`), `BATCH_RECONCILED (v2)` (`500`) và `BATCH_VERIFIED (v2)` (`32,405`).
