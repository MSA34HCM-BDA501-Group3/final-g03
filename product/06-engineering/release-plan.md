# Kế Hoạch Phát Hành & Đóng Gói (Release Plan — V3.0)

## 1. Danh Mục Hiện Vật Phát Hành (`Release Artifacts Manifest`)

| Nhóm Hiện Vật | Đường Dẫn Tệp (`File Path`) | Mô Tả Nội Dung |
| :--- | :--- | :--- |
| **1. Mã nguồn Thực thi (`Core Source Code`)** | `pipeline.py`, `app.py`, `producer.py`, `baseline.py` | Trình điều phối `Zones A–F` (`[C1]`–`[C17]`), giao diện `Streamlit 5-Tab UI`, trình phát luồng `Event-Time Replay` và trình đối chiếu độc lập `Pillar I1`. |
| **2. Hồ dữ liệu `Medallion Lakehouse` (`[C14]`)** | `data/dataset.csv`<br>`data/lakehouse/bronze_raw_events/`<br>`data/lakehouse/silver_cleaned_windows/`<br>`data/lakehouse/curated_gold/year_month=*`<br>`data/lakehouse/segment_static_features.parquet` | Bảng `Medallion Gold` (`33,441` dòng $\times$ `42` cột), dữ liệu `Bronze/Silver/Gold Parquet` phân vùng theo tháng, và kho đặc trưng tĩnh `84,633` đoạn đường (`[C13b]`). |
| **3. Kho Mô hình & Báo cáo Phân tích (`[C13a]`, `[C15]`)** | `outputs/model.joblib`<br>`outputs/model_registry_manifest.json`<br>`outputs/metrics.json`<br>`outputs/ablation_results.csv`<br>`outputs/road_pagerank_top.csv`<br>`outputs/co_congestion_rules.csv`<br>`outputs/hotspot_alerts.csv`<br>`outputs/reconciliation_audit.csv`<br>`outputs/quarantine_summary.csv`<br>`outputs/mongo_verify.json`<br>`outputs/join_plan_evidence.json`<br>`outputs/monoid_baseline_verification.json` | Đầy đủ các hiện vật mô hình đã đóng băng, báo cáo `PageRank`, `FPGrowth`, `7D K-Means`, `Ablation Study`, `Quarantine`, `Two-Tier Reconciliation`, `BroadcastHashJoin` và `MongoDB 8.0 IXSCAN`. |
| **4. Tài liệu Học thuật & Thuyết trình (`Docs & Deck`)** | `HCMC_Smart_Traffic_Final_Project_Proposal_V3.pdf`<br>`HCMC_Smart_Traffic_Proposal_Slides_V3.pdf`<br>`HCMC_Smart_Traffic_Proposal_Presentation_V3.pptx`<br>`README.md`, `TECHNICAL_NOTE.md`, `product/` | Bản PDF Đề xuất Đồ án V3, Bộ 12 Slide thuyết trình (`.pdf` & `.pptx`), Tài liệu kỹ thuật và thư mục quản trị sản phẩm `product/`. |

---

## 2. Quy Trình Khởi Chạy Chuẩn (`Standard Release Execution`)

```powershell
# 1. Chạy toàn bộ Pipeline V3 ([C1]-[C16]) & xuất Lakehouse/Artifacts
python pipeline.py

# 2. Chạy kiểm chứng độc lập Pillar I1 (Monoid vs Baseline)
python baseline.py

# 3. Chạy bộ kiểm thử tự động toàn diện (P1-P4, UC-1..UC-6, EC-1..EC-10)
pytest -q

# 4. Khởi chạy hoặc cập nhật dịch vụ Streamlit UI (:8501)
docker compose up -d app
```
