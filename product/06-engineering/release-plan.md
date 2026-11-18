# Kế Hoạch Phát Hành (Release Plan — Production Baseline)

## 1. Mục tiêu Phát hành
Đóng gói và vận hành hệ thống **HCMC Smart Traffic Lakehouse & Real-Time Prediction Platform** với đầy đủ dữ liệu thực nghiệm sạch không rò rỉ đã kiểm toán trên đĩa (`33,394 x 60` cửa sổ chuẩn hóa, `84,206` đoạn đường cơ giới sạch, `90,787` GPS pings hợp lệ), hạ tầng Spark `3.5.7`, MongoDB `8.0.34`, Kafka KRaft và ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)**.

## 2. Danh mục Tạo tác & Dịch vụ Phát hành (Release Inventory)
### A. Dữ liệu & Lakehouse (`data/`)
- `data/dataset.csv`: `33,394 x 60` cột chuẩn hóa (UTC/ICT, leak-free Vmax imputation `4,946/7,650/19,601/1,197`, causal priors, Unified K-Means).
- `data/lakehouse/bronze_raw_events/`: `90,787 x 4` GPS pings sạch (sau cách ly `151` non-road pings).
- `data/lakehouse/stream_replay_windows/`: `33,727 x 45` bản ghi phát lại luồng (gồm `333` bản ghi trùng lặp phục vụ kiểm thử `RotatingBloomFilter`).
- `data/lakehouse/silver_cleaned_windows/`: `33,394 x 86` cửa sổ sạch sau Monoid & watermark audit.
- `data/lakehouse/curated_gold/year_month=*/`: `10` phân vùng tháng (`2020-08` có `7,268` cửa sổ, đọc sạch bởi cả PyArrow/Pandas lẫn Spark).
- `data/lakehouse/segment_static_features.parquet`: `84,206 x 23` (`9,995` observed + `74,211` cold-start `88.13%`, SHA-256 `75328ac2d5e3`; bản mẫu gọn `f26ebdf7d396`).

### B. Mô hình & Bằng chứng Kiểm toán (`outputs/`)
- Mô hình đóng băng `rf_full_23d_v3`: `outputs/model.joblib` (`a2f411846508...`), `outputs/vmax_lookup.json` (`ee7f692daad8...`), `outputs/model_registry_manifest.json`.
- Kiểm chứng thuật toán & hạ tầng: `outputs/metrics.json`, `outputs/ablation_results.csv`, `outputs/transition_evaluation.csv`, `outputs/road_pagerank_top.csv`, `outputs/co_congestion_rules.csv`, `outputs/lagged_congestion_rules.csv`, `outputs/monoid_baseline_verification.json`, `outputs/join_plan_evidence.json`, `outputs/spark_runtime_evidence.json`, `outputs/query_progress.json`, `outputs/mongo_verify.json`, `outputs/live_kafka_telemetry.json`.

### C. Hạ tầng Container (`5` dịch vụ mặc định — `11.0 / 16.0 GB RAM` + `batch`/`stream` profiles)
| Service | Vai trò | Cổng / Giới hạn RAM |
|---|---|---|
| `kafka` | Kafka `4.1.0` KRaft broker nhận GPS telemetry | `127.0.0.1:9092` · `1536M` |
| `mongo` | MongoDB `8.0.34` serving store (`33,394` docs, `1,234` arterial alerts, atomic `$status_version`) | `127.0.0.1:27017` · `2048M` |
| `spark` | PySpark `3.5.7` (`hcmc-traffic-spark:3.5.7`) batch, MLlib organic & structured streaming | Nội bộ `bda501-net` · `6144M` |
| `toc-web` | Ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center** (`toc_backend/main.py` + `toc_frontend/dist/`) | `127.0.0.1:8000` · `1024M` |
| `live-consumer` | Tiêu thụ Kafka liên tục, `32 KiB RotatingBloomFilter` + `1 KiB 64-bit HLL++`, ghi MongoDB | Background worker · `512M` |

### D. Tài liệu & Bộ Trình bày Chính thức
- `README.md`, `TECHNICAL_NOTE.md`, `docs/*`, `product/**` (đồng bộ 100% chuẩn sạch không rò rỉ).
- `HCMC_Smart_Traffic_Final_Report.pdf`, `HCMC_Smart_Traffic_Final_Presentation.pptx` (`17` slides), `HCMC_Smart_Traffic_Final_Slides.pdf`, `system_architecture.png`, và hồ sơ kiểm định Session 9 đã tái sinh (`SESSION9_TECHNICAL_AUDIT_REPORT.md`, `evidence/session9/*`).

## 3. Lộ trình Kiểm định trước Chốt Phát hành (Release Gates)
1. **Batch & Spark Parity (`PASS`):** `Pure 5-tuple Monoid` khớp `100.0%` (`0` sai lệch, `0.0 km/h`), Spark Catalyst join tối ưu `5 -> 1 Exchange` (`166.18 ms -> 123.37 ms`, tăng tốc `25.76%`, `0 exceptAll diff`); Monoid `97.92 ms` vs salted `185.78 ms`; Spark MLlib organic TRAIN (`FPGrowth 32`, `KMeans 3,609`, `RF 1,885` đạt `test_acc = 0.9416, roc_auc = 0.9903`).
2. **ML Integrity (`PASS`):** `RF 23D` huấn luyện sạch (`1,885` cặp TRAIN hữu cơ, loại `3,596` cặp burst, không dùng `sample_weight` nhân tạo), thắng `VAL` (`0.8882` vs `DT 23D = 0.8812`), `TEST ROC-AUC = 0.9894`, `PR-AUC = 0.9543`, `Acc = 0.9255`, `Weighted-F1 = 0.9297`, `Severe F1 = 0.9362`, `Macro-F1 = 0.4253`.
3. **Test Suite & Session 9 Audit Alignment (`Gate 3 PASS`):** Toàn bộ bộ kiểm thử tự động `pytest` (`tests/test_platform_v3.py` và `tests/test_quyen_spark.py`) và hồ sơ kiểm định Session 9 (`evidence/session9/`) đã được đồng bộ và tái sinh hoàn chỉnh khớp chuẩn thực nghiệm sạch không rò rỉ và ứng dụng web hợp nhất `:8000`.
