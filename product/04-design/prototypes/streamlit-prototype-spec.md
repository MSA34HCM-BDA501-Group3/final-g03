# Đặc Tả Prototype Tương Tác Streamlit (Streamlit Prototype Specification)

## 1. Cấu Hình Triển Khai (`Runtime & Binding Specification`)

- **Điểm vào ứng dụng (`Entrypoint`):** `app.py` (chạy trên cổng `:8501`).
- **Nguồn dữ liệu kết nối (`Connected Artifacts`):**
  1. `data/dataset.csv` & `data/lakehouse/curated_gold/` (`33,441` cửa sổ 30 phút đã làm sạch).
  2. `data/lakehouse/segment_static_features.parquet` (`84,633` đoạn đường toàn thành phố, hỗ trợ tra cứu cả `10,027` đoạn có GPS và `74,606` đoạn `Cold-Start`).
  3. `outputs/model.joblib` & `outputs/model_registry_manifest.json` (`[C13a]` Frozen Model Registry).
  4. `outputs/metrics.json`, `outputs/ablation_results.csv`, `outputs/road_pagerank_top.csv`, `outputs/co_congestion_rules.csv`, `outputs/hotspot_alerts.csv`, `outputs/reconciliation_audit.csv`, `outputs/quarantine_summary.csv`, `outputs/mongo_verify.json`, `outputs/join_plan_evidence.json`, `outputs/monoid_baseline_verification.json`.

## 2. Cơ Chế Tương Tác Thời Gian Thực (`Interactive Capabilities`)

- **Dự báo Đơn điểm & `Cold-Start` (`Single-Window & Cold-Start Inference`):** Gọi trực tiếp `artifact["ml_imputer"].transform()` và `.predict_proba()` chỉ đọc từ `[C13a]` để so sánh dự báo của `Persistence Baseline`, `Decision Tree`, `Random Forest (+Graph)` và `GBT Binary Alerting`.
- **Truy xuất Đoạn đường Tương đồng (`Within-Cluster Peer Segment Retrieval`):** Tính khoảng cách Euclidean chuẩn hóa trong không gian đặc trưng của cùng `cluster_id` để gợi ý `Top-K` đoạn đường có chế độ giao thông giống nhất.
- **Bộ lọc Luật Lan truyền & Kiểm toán Đối soát (`Interactive Rule & Recon Filtering`):** Cho phép lọc nhanh các luật đồng kẹt xe theo ngưỡng `Lift` / `Confidence` và kiểm tra từng cửa sổ `RECOMPUTED_AND_MERGED` vs. `VERIFIED_EXACT`.
