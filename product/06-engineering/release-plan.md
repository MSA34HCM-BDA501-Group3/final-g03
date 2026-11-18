# Kế Hoạch Phát Hành (Release Plan — Wave 2 Production Baseline)

## 1. Mục tiêu Phát hành
Đóng gói và vận hành hệ thống **HCMC Smart Traffic Lakehouse & Real-Time Prediction Platform** với đầy đủ dữ liệu Wave 2 đã kiểm toán trên đĩa (`33,394 x 60` cửa sổ chuẩn hóa, `84,206` đoạn đường cơ giới sạch, `90,787` GPS pings hợp lệ), hạ tầng Spark `3.5.7`, MongoDB `8.0.34`, Kafka KRaft và giao diện kép `Web TOC (:8000)` + `Streamlit Ops (:8501)`.

## 2. Danh mục Tạo tác & Dịch vụ Phát hành (Release Inventory)
### A. Dữ liệu & Lakehouse (`data/`)
- `data/dataset.csv`: `33,394 x 60` cột chuẩn hóa (UTC/ICT, Vmax imputation, causal priors, cluster static/dynamic features).
- `data/lakehouse/bronze_raw_events/`: `90,787 x 4` GPS pings sạch (sau cách ly `151` non-road pings).
- `data/lakehouse/stream_replay_windows/`: `33,727 x 45` bản ghi phát lại luồng (gồm `333` bản ghi trùng lặp phục vụ kiểm thử Bloom Filter).
- `data/lakehouse/silver_cleaned_windows/`: `33,394 x 86` cửa sổ sạch sau Monoid & watermark audit.
- `data/lakehouse/curated_gold/year_month=*/`: `10` phân vùng tháng (`2020-08` có `7,268` cửa sổ, đọc sạch bởi cả PyArrow/Pandas lẫn Spark).
- `data/lakehouse/segment_static_features.parquet`: `84,206 x 23` (`9,995` observed + `74,211` cold-start `88.13%`).

### B. Mô hình & Bằng chứng Kiểm toán (`outputs/`)
- Mô hình đóng băng `rf_full_23d_v3`: `outputs/model.joblib` (`a2e12b7c...`), `outputs/vmax_lookup.json` (`41577e2e...`), `outputs/model_registry_manifest.json`.
- Kiểm chứng thuật toán & hạ tầng: `outputs/metrics.json`, `outputs/ablation_results.csv`, `outputs/transition_evaluation.csv`, `outputs/road_pagerank_top.csv`, `outputs/co_congestion_rules.csv`, `outputs/lagged_congestion_rules.csv`, `outputs/monoid_baseline_verification.json`, `outputs/join_plan_evidence.json`, `outputs/spark_runtime_evidence.json`, `outputs/query_progress.json`, `outputs/mongo_verify.json`, `outputs/live_kafka_telemetry.json`.

### C. Hạ tầng Container (6 dịch vụ mặc định — `12.0 / 16.0 GB RAM`)
| Service | Vai trò | Cổng / Ghi chú |
|---|---|---|
| `kafka` | Kafka KRaft broker nhận GPS telemetry | Nội bộ Docker / `9092` |
| `mongo` | MongoDB 8.0.34 serving store (`33,394` docs, atomic `$status_version`) | `27017` |
| `spark` | PySpark `3.5.7` (`hcmc-traffic-spark:3.5.7`) batch & structured streaming | `8080` / `4040` |
| `live-consumer` | Tiêu thụ Kafka, `32 KiB RotatingBloom` + `1 KiB 64-bit HLL`, ghi MongoDB | Background stream worker |
| `toc-web` | Web TOC Command Center (`FastAPI + React/Vite`) | `:8000` |
| `app` | Streamlit Operations & Analytics Console | `:8501` |

### D. Tài liệu & Bộ Trình bày Chính thức
- `README.md`, `TECHNICAL_NOTE.md`, `docs/*`, `product/**` (đồng bộ 100% số liệu Wave 2).
- `HCMC_Smart_Traffic_Final_Report.pdf`, `HCMC_Smart_Traffic_Final_Presentation.pptx` (`17` slides), `HCMC_Smart_Traffic_Final_Slides.pdf`, `system_architecture.png`.

## 3. Lộ trình Kiểm định trước Chốt Phát hành (Release Gates)
1. **Batch & Spark Parity:** `Pure 5-tuple Monoid` khớp `100.0%` (`0` sai lệch, `0.0 km/h`), Spark Catalyst join tối ưu `5 -> 1 Exchange` (`145.24 ms -> 120.45 ms`, giảm `17.07%`, `0 exceptAll diff`).
2. **ML Integrity:** `RF 23D` huấn luyện sạch (`1,885` cặp TRAIN hữu cơ, loại `3,596` cặp burst, không dùng `sample_weight` nhân tạo), thắng `VAL` (`0.8990` vs `0.5343`), `TEST ROC-AUC = 0.9900`, `PR-AUC = 0.9595`, `Acc = 0.9289`, `Severe F1 = 0.9301`, `Macro-F1 = 0.4475`.
3. **Test Suite Alignment (Đang mở — Gate 3 `CONDITIONAL`):** `tests/test_platform_v3.py` (`23` hàm) và `tests/test_quyen_spark.py` (`5` phương thức) hiện tồn tại trong repo; riêng `tests/test_platform_v3.py` vẫn giữ một số ngưỡng Wave 1 cũ (`33,441` dòng, `46` NFC entities...) nên cần được track chuyên trách đồng bộ sang ngưỡng Wave 2 (`33,394`, `84,206`, ...) trước khi nâng Gate 3 từ `CONDITIONAL` lên `PASSED`.
