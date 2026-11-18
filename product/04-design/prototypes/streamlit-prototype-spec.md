# Đặc Tả Giao Diện Web Điều Hành Sản Xuất (`ReactJS + FastAPI TOC Command Center — http://localhost:8000`)

> **Ghi chú chuyển đổi kiến trúc UI:** Tên tệp `streamlit-prototype-spec.md` được giữ nguyên để bảo toàn các liên kết Markdown hiện có trong hệ thống tài liệu quản trị. Bản mẫu Streamlit thử nghiệm ban đầu đã được **ngừng sử dụng hoàn toàn (retired)** và thay thế bằng một ứng dụng web hợp nhất duy nhất ở cấp độ sản xuất: **ReactJS + TypeScript + FastAPI TOC Command Center (`http://localhost:8000`)** phục vụ bởi dịch vụ `toc-web` (`toc_backend/main.py` + `toc_frontend/dist/`).

## 1. Cấu Hình Triển Khai (`Runtime & Binding Specification — [C17]`)

- **Ứng dụng Web Hợp nhất Duy nhất (`toc-web` tại `http://localhost:8000`):**
  - **Backend API (`toc_backend/main.py`):** `FastAPI` chạy trên `uvicorn toc_backend.main:app --host 0.0.0.0 --port 8000` (giới hạn `1024M` RAM trong cụm `5` dịch vụ mặc định `11.0 GB RAM` của `docker-compose.yml`), vừa phục vụ bộ REST API `/api/*` vừa phục vụ gói tĩnh SPA `toc_frontend/dist/`.
  - **Frontend SPA (`toc_frontend/src/` → `toc_frontend/dist/`):** `ReactJS + TypeScript + Vite + Leaflet`, tổ chức thành thanh đồng hồ `TopClockBar` và **5 tab chuyên biệt**:
    1. `TabCommandCenter.tsx` (`01 · Điều Hành & Dự Báo +30p`) + `InteractiveTrafficMap.tsx` + `ForecastTimelineChart.tsx`
    2. `TabLiveKafka.tsx` (`02 · Live Kafka & Sketches 33 KiB`)
    3. `TabGraphRules.tsx` (`03 · PageRank & Luật Đồng Kẹt`)
    4. `TabClustersColdStart.tsx` (`04 · 6 Cụm K-Means & Cold-Start`)
    5. `TabInfraAudit.tsx` (`05 · Spark 3.5.7, Ablation & Đối Soát`)
- **Nguồn dữ liệu kết nối (`Connected Artifacts` — Leak-Free Ground Truth):**
  1. `data/dataset.csv` (`33,394` dòng × `60` cột) & `data/lakehouse/curated_gold/year_month=*` (`10` phân vùng tháng đọc sạch trên PyArrow/Pandas & Spark 3.5.7); kèm fallback mẫu nhanh `data/demo_sample/` (`sample_static_features.parquet`, SHA-256 `f26ebdf7d396`) trên checkout mới.
  2. `data/lakehouse/segment_static_features.parquet` (`84,206` đoạn cơ giới sạch × `23` cột: `9,995` đoạn có GPS + `74,211` đoạn `Cold-Start` = `88.13%`; SHA-256 `75328ac2d5e3`).
  3. `outputs/model.joblib` (`rf_full_23d_v3`, SHA-256 `a2f411846508`), `outputs/vmax_lookup.json` (`ee7f692daad8`) & `outputs/model_registry_manifest.json` (`[C13a]` Frozen Model Registry).
  4. `outputs/metrics.json`, `outputs/ablation_results.csv`, `outputs/transition_evaluation.csv`, `outputs/road_pagerank_top.csv`, `outputs/co_congestion_rules.csv`, `outputs/lagged_congestion_rules.csv`, `outputs/hotspot_alerts.csv`, `outputs/reconciliation_audit.csv`, `outputs/quarantine_summary.csv`, `outputs/mongo_verify.json`, `outputs/join_plan_evidence.json`, `outputs/spark_runtime_evidence.json`, `outputs/query_progress.json`, `outputs/monoid_baseline_verification.json`, `outputs/live_kafka_telemetry.json`.

## 2. Cơ Chế Tương Tác Thời Gian Thực (`Interactive Capabilities`)

- **Phóng nét đứt dự báo `+30 phút` & Live Kafka Replay (`POST /api/stream/start`, `GET /api/frame`, `GET /api/segment/{segment_id}/timeline`):** Hiển thị từng khung `[t-30m, t) ➔ Chốt tại t ➔ Dự báo [t, t+30p)` trên bản đồ Leaflet TP.HCM với 4 kịch bản rush-hour thực tế (`noon_transition`, `cong_truong_quoc_te`, `morning_rush_val`, `mega_city_rush`) và 6 nhóm cảnh báo phân loại, đồng thời cập nhật telemetry trực tiếp từ `Kafka 4.1` → `live-consumer` (`32 KiB RotatingBloomFilter` `0.0389%` FPR + `1 KiB 64-bit HLL++` `3.75%`) → `MongoDB 8.0`.
- **Đối chiếu Đơn điểm & Bảng Thử Nghiệm (`GET /api/segment/{segment_id}/timeline`, `GET /api/infrastructure-audit`):** Gọi trực tiếp mô hình đóng băng chỉ đọc từ `[C13a]` để đối chiếu `Persistence Baseline`, `Decision Tree`, `Random Forest (23D)` (`ROC-AUC = 0.9894`, `PR-AUC = 0.9543`, `Severe F1 = 0.9362`) và `GBT Binary Alerting`, kèm kiểm định đổi trạng thái (`10 / 61`, `McNemar p = 0.001953`; `VAL-calibrated 14 / 61`) và `Masked-GPS Cold-Start` (`Acc = 58.63%`, `ROC-AUC = 0.7000`).
- **Truy xuất Đoạn đường Tương đồng (`GET /api/clusters-coldstart`):** Tính khoảng cách Euclidean chuẩn hóa trong cùng `cluster_id` (`Silhouette = 0.2241`, `DBI = 1.3284`, `CH = 806.49`) để gợi ý `Top-K` đoạn đường tương đồng trên toàn mạng `84,206` đoạn (`74,211` `Cold-Start`).
- **Bộ lọc Luật Lan truyền & Kiểm toán Đối soát (`GET /api/graph-rules`, `GET /api/infrastructure-audit`):** Lọc nhanh `Top-50 PageRank` (`#1 21969 Nguyễn Kiệm PR = 2.7799`), `12` luật đồng kẹt + `14` luật trễ (`persistence_controlled = True`), theo dõi vòng đời `STREAM_PROVISIONAL (v1)` (`489`), `BATCH_RECONCILED (v2)` (`500`) và `BATCH_VERIFIED (v2)` (`32,405`), cùng độ trễ `MongoDB 8.0 IXSCAN` (`0.360 / 0.603 ms`, `31.36x` vs `COLLSCAN`).
