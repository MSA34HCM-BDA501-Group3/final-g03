# Kế Hoạch Phát Hành & Đóng Gói (Release Plan — V3.0)

## 1. Danh Mục `Release Artifacts` (`Release Artifacts Manifest`)

| Nhóm `Artifacts` | Đường Dẫn Tệp (`File Path`) | Mô Tả Nội Dung |
| :--- | :--- | :--- |
| **1. Mã nguồn Thực thi (`Core Source Code`)** | `pipeline.py`, `app.py`, `producer.py`, `starter_stream.py`, `spark_engine.py`, `spark_batch.py`, `baseline.py`, `traffic_data/` | Trình điều phối `Zones A–F` (`[C1]`–`[C17]`), giao diện `Streamlit 5-Tab UI`, trình phát luồng `Event-Time Replay`, trình xử lý luồng `WAL Recovery` (`starter_stream.py`), trình thực thi `PySpark 3.5.7` (`spark_engine.py`) và trình đối chiếu độc lập `Pillar I1`. |
| **2. `Medallion Parquet Lakehouse` (`[C14]`)** | `data/dataset.csv`<br>`data/lakehouse/bronze_raw_events/events.parquet`<br>`data/lakehouse/silver_cleaned_windows/windows.parquet`<br>`data/lakehouse/curated_gold/year_month=*`<br>`data/lakehouse/segment_static_features.parquet`<br>`data/lakehouse/serving_traffic_scores.parquet`<br>`data/lakehouse/quarantine_events.parquet` | Bảng `Medallion Gold` (`33,441` dòng $\times$ `59` cột), dữ liệu `Bronze/Silver/Gold Parquet` phân vùng theo tháng (`10` phân vùng), `Static Feature Store` `84,633` đoạn đường (`[C13b]`), `Serving Store` snapshot và `Quarantine Store`. |
| **3. `Frozen Model Registry` & Báo cáo Phân tích (`[C13a]`, `[C15]`)** | `outputs/model.joblib`<br>`outputs/vmax_lookup.json`<br>`outputs/model_registry_manifest.json`<br>`outputs/spark_pipeline_model/`<br>`outputs/metrics.json`<br>`outputs/spark_runtime_evidence.json`<br>`outputs/query_progress.json`<br>`outputs/ablation_results.csv`<br>`outputs/road_pagerank_top.csv`<br>`outputs/co_congestion_rules.csv`<br>`outputs/lagged_congestion_rules.csv`<br>`outputs/hotspot_alerts.csv`<br>`outputs/reconciliation_audit.csv`<br>`outputs/quarantine_summary.csv`<br>`outputs/mongo_verify.json`<br>`outputs/join_plan_evidence.json`<br>`outputs/monoid_baseline_verification.json` | Đầy đủ các `Frozen Artifacts` (`SHA-256` verified), báo cáo `PageRank`, `2-Pass SON/FP-Growth`, `7D K-Means`, `Ablation Study`, `Quarantine`, `Two-Tier Reconciliation`, `BroadcastHashJoin`, `Spark 3.5.7 Runtime Evidence` và `MongoDB 8.0 IXSCAN`. |
| **4. Tài liệu Học thuật & Quản trị (`Docs & Governance`)** | `HCMC_Smart_Traffic_Final_Project_Proposal_V3.pdf`<br>`HCMC_Smart_Traffic_Proposal_Slides_V3.pdf`<br>`HCMC_Smart_Traffic_Proposal_Presentation_V3.pptx`<br>`README.md`, `TECHNICAL_NOTE.md`, `PROPOSAL_V3_AUDIT_CHECKLIST.md`, `product/README.md`, `docs/` | Bản PDF Đề xuất & Báo cáo Kỹ thuật V3 (7 Chương), Bộ 14 Slide thuyết trình kèm Báo cáo Tiến độ & Stage-Gate Audit (`.pdf` & `.pptx`), Tài liệu kỹ thuật, Danh mục kiểm toán V3 (`28/28` nhóm, `50/50` tasks) và thư mục quản trị sản phẩm `product/` (`11 Phases, 5 Gates, 8 Workstreams`). |

---

## 2. Quy Trình Khởi Chạy Chuẩn (`Standard Release Execution`)

```powershell
# 1. Chạy toàn bộ Pipeline V3 ([C1]-[C16]) & xuất Lakehouse/Artifacts + kiểm chứng Spark/Mongo/Kafka
python pipeline.py

# 2. Chạy kiểm chứng luồng chịu lỗi WAL Checkpoint Kill/Restart & Frozen Registry SHA-256
python starter_stream.py --kill-after-batch 1

# 3. Chạy kiểm chứng độc lập Pillar I1 (Monoid vs Baseline)
python baseline.py

# 4. Chạy bộ kiểm thử tự động toàn diện (19 bài kiểm thử trên test_platform_v3.py & test_quyen_spark.py)
pytest -v

# 5. Kiểm tra chất lượng mã nguồn & độ phức tạp hàm (< 15)
ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"

# 6. Khởi chạy hoặc cập nhật dịch vụ Streamlit UI (:8501)
docker compose up -d app
```
