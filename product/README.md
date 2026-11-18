# HCMC Smart Traffic Analytics & Congestion Prediction Platform — Product Governance Index

Tài liệu này là **Master Product Governance Index** theo khung quản trị sản phẩm toàn diện (**11 Giai đoạn / Phases 0–10**, **5 Cổng kiểm soát / Gates 1–5**, **8 Luồng công việc song song / Workstreams W1–W8**) cho hệ thống **HCMC Smart Traffic Analytics & Congestion Prediction Platform (`Zones A–F`, Components `[C1]`–`[C17]`, Leak-Free Ground Truth)**.

---

## 1. Cấu Trúc Thư Mục Thực Tế Của `product/` (Khớp 100% Tập Tin Hiện Hữu)

```text
product/
├── README.md
├── 00-proposal/
│   ├── product-vision.md
│   └── project-proposal.md
├── 01-discovery/
│   ├── competitor-analysis.md
│   ├── customer-interviews.md
│   ├── market-research.md
│   └── validation-report.md
├── 02-strategy/
│   ├── business-model.md
│   ├── product-strategy.md
│   └── success-metrics.md
├── 03-requirements/
│   ├── acceptance-criteria.md
│   ├── prd.md
│   ├── prioritized-backlog.md
│   └── user-stories.md
├── 04-design/
│   ├── prototypes/streamlit-prototype-spec.md
│   ├── ux-research/operator-workflows.md
│   └── wireframes/dashboard-5tabs-wireframe.md
├── 05-architecture/
│   ├── security-design.md
│   ├── system-design.md
│   ├── api-specifications/serving-and-stream-contracts.md
│   └── architecture-decisions/
│       ├── adr-001-zero-conflict-mlops.md
│       ├── adr-002-two-tier-reconciliation.md
│       └── adr-003-causal-priors-7d-kmeans.md
├── 06-engineering/
│   ├── development-guidelines.md
│   ├── release-plan.md
│   └── sprint-plans/sprint-plan-v3.md
├── 07-quality/
│   ├── release-checklist.md
│   ├── test-strategy.md
│   └── test-cases/test-matrix-p1-p4-uc1-uc6-ec1-ec10.md
├── 08-launch/
│   ├── go-to-market.md
│   ├── operations-runbook.md
│   └── pilot-report.md
└── 09-growth/
    ├── experiment-log.md
    ├── growth-roadmap.md
    └── product-analytics.md
```

> *Lưu ý cấu trúc thực tế:* Đánh giá sẵn sàng phát hành (`Gate 3 PASS`) được duy trì tại [`07-quality/release-checklist.md`](07-quality/release-checklist.md), đồng bộ hoàn toàn cùng [`../SESSION9_TECHNICAL_AUDIT_REPORT.md`](../SESSION9_TECHNICAL_AUDIT_REPORT.md) và hồ sơ kiểm định Session 9 đã tái sinh tại [`../evidence/session9/I_readiness.md`](../evidence/session9/I_readiness.md). Tệp [`04-design/prototypes/streamlit-prototype-spec.md`](04-design/prototypes/streamlit-prototype-spec.md) giữ nguyên đường dẫn lịch sử để không làm gãy liên kết nhưng chứa đặc tả sản xuất chính thức của ứng dụng web hợp nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)**.

---

## 2. Bản đồ 11 Giai đoạn (Phases 0–10) & Kết Quả Thực Nghiệm Sạch Không Rò Rỉ

| Phase | Tên Giai đoạn | Tài liệu Chính trong `product/` & Repo | Trạng thái | Kết quả / Số liệu Thực chứng (Leak-Free Ground Truth) |
| :--- | :--- | :--- | :---: | :--- |
| **Phase 0** | **Idea & Proposal** | [`00-proposal/product-vision.md`](00-proposal/product-vision.md)<br>[`00-proposal/project-proposal.md`](00-proposal/project-proposal.md) | ✅ Hoàn tất | Tầm nhìn & Báo cáo Kỹ thuật 7 Chương: dự báo `t+30m` trên **`84,206` đoạn cơ giới sạch** (`84,633` thô) & phủ vùng mù Cold-Start **`88.13%` (`74,211` đoạn)**. |
| **Phase 1** | **Discovery & Validation** | [`01-discovery/competitor-analysis.md`](01-discovery/competitor-analysis.md)<br>[`01-discovery/customer-interviews.md`](01-discovery/customer-interviews.md)<br>[`01-discovery/market-research.md`](01-discovery/market-research.md)<br>[`01-discovery/validation-report.md`](01-discovery/validation-report.md) | ✅ Hoàn tất | Kiểm định 5 bảng gốc Kaggle (`577,967` nodes $\to$ `52,181` active, `84,206` clean motorized segments, `90,787/90,938` accepted pings, `33,394` khung UTC 30m, `89.19%` BkTraffic urban LOS exact match). |
| **Phase 2** | **Strategy & Business Case** | [`02-strategy/business-model.md`](02-strategy/business-model.md)<br>[`02-strategy/product-strategy.md`](02-strategy/product-strategy.md)<br>[`02-strategy/success-metrics.md`](02-strategy/success-metrics.md) | ✅ Hoàn tất | Chiến lược Hybrid Lakehouse (`bronze_raw_events`, `stream_replay_windows`, `curated_gold`), `33.0 KiB` Sketches và cụm **`5` dịch vụ Docker mặc định (`11.0 / 16.0 GB RAM`)**. |
| **Phase 3** | **Requirements (PRD)** | [`03-requirements/prd.md`](03-requirements/prd.md)<br>[`03-requirements/acceptance-criteria.md`](03-requirements/acceptance-criteria.md)<br>[`03-requirements/prioritized-backlog.md`](03-requirements/prioritized-backlog.md)<br>[`03-requirements/user-stories.md`](03-requirements/user-stories.md) | ✅ Hoàn tất | Đặc tả 6 Use Cases (`UC-1`..`UC-6`), 10 Edge Cases (`EC-1`..`EC-10`) và SLA phi chức năng. |
| **Phase 4** | **UX/UI Design** | [`04-design/prototypes/streamlit-prototype-spec.md`](04-design/prototypes/streamlit-prototype-spec.md)<br>[`04-design/ux-research/operator-workflows.md`](04-design/ux-research/operator-workflows.md)<br>[`04-design/wireframes/dashboard-5tabs-wireframe.md`](04-design/wireframes/dashboard-5tabs-wireframe.md) | ✅ Hoàn tất | Thiết kế ứng dụng web 5 tab hợp nhất duy nhất: **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)** (`toc-web`) hiển thị vòng đời `STREAM_PROVISIONAL (v1)` / `BATCH_RECONCILED (v2)`. |
| **Phase 5** | **System Architecture** | [`05-architecture/system-design.md`](05-architecture/system-design.md)<br>[`05-architecture/security-design.md`](05-architecture/security-design.md)<br>[`05-architecture/api-specifications/serving-and-stream-contracts.md`](05-architecture/api-specifications/serving-and-stream-contracts.md)<br>[`05-architecture/architecture-decisions/adr-001..003.md`](05-architecture/architecture-decisions/adr-001-zero-conflict-mlops.md) | ✅ Hoàn tất | Kiến trúc 6 phân vùng (`Zones A–F`, `[C1]–[C17]`), 3 ADRs cốt lõi: Zero-Conflict MLOps, Two-Tier Reconciliation & Organic Train-Only 7D K-Means. |
| **Phase 6** | **Engineering & Implementation** | [`06-engineering/development-guidelines.md`](06-engineering/development-guidelines.md)<br>[`06-engineering/release-plan.md`](06-engineering/release-plan.md)<br>[`06-engineering/sprint-plans/sprint-plan-v3.md`](06-engineering/sprint-plans/sprint-plan-v3.md) | ✅ Hoàn tất | Clean Architecture, Cognitive Complexity `< 15`, `Spark 3.5.7 (hcmc-traffic-spark:3.5.7)` + `MongoDB 8.0.34` + `Kafka 4.1.0` + **ReactJS/FastAPI (`:8000`)**. |
| **Phase 7** | **Quality Assurance & Testing** | [`07-quality/release-checklist.md`](07-quality/release-checklist.md)<br>[`07-quality/test-strategy.md`](07-quality/test-strategy.md)<br>[`07-quality/test-cases/test-matrix-p1-p4-uc1-uc6-ec1-ec10.md`](07-quality/test-cases/test-matrix-p1-p4-uc1-uc6-ec1-ec10.md) | ✅ Hoàn tất | **`Gate 3 PASS`:** Toàn bộ bộ kiểm thử tự động `pytest` (`tests/test_platform_v3.py`, `tests/test_quyen_spark.py`), bằng chứng `outputs/` và hồ sơ kiểm định Session 9 (`evidence/session9/`) đã đồng bộ 100% với chuẩn sạch không rò rỉ. |
| **Phase 8** | **Launch, Pilot & Operations** | [`08-launch/go-to-market.md`](08-launch/go-to-market.md)<br>[`08-launch/operations-runbook.md`](08-launch/operations-runbook.md)<br>[`08-launch/pilot-report.md`](08-launch/pilot-report.md) | ✅ Hoàn tất | Runbook vận hành `5` dịch vụ Docker mặc định (`kafka`, `mongo`, `spark`, `toc-web :8000`, `live-consumer`, `11.0 GB RAM`), kiểm chứng chịu lỗi WAL (`73.7 KB`, `0` duplicates). |
| **Phase 9** | **Growth & Analytics** | [`09-growth/experiment-log.md`](09-growth/experiment-log.md)<br>[`09-growth/growth-roadmap.md`](09-growth/growth-roadmap.md)<br>[`09-growth/product-analytics.md`](09-growth/product-analytics.md) | ✅ Hoàn tất | Lọc burst (`7,192` windows / `3,596` TRAIN pairs), huấn luyện sạch `RF 23D` (`ROC-AUC = 0.9894`, `PR-AUC = 0.9543`, `Acc = 0.9255`, `Severe F1 = 0.9362`, `Macro-F1 = 0.4253` do mất cân bằng lớp `B/C/D`), McNemar `p = 0.001953` (`10 / 61`). |
| **Phase 10** | **Continuous Improvement & Governance** | [`../TECHNICAL_NOTE.md`](../TECHNICAL_NOTE.md)<br>[`07-quality/release-checklist.md`](07-quality/release-checklist.md)<br>[`../SESSION9_TECHNICAL_AUDIT_REPORT.md`](../SESSION9_TECHNICAL_AUDIT_REPORT.md) · [`../evidence/session9/I_readiness.md`](../evidence/session9/I_readiness.md) | ✅ Hoàn tất | Đồng bộ 100% giữa số liệu thực nghiệm sạch trên đĩa (`outputs/`), bộ kiểm thử `pytest`, tài liệu quản trị `product/` và hồ sơ kiểm định Session 9 tái sinh (`evidence/session9/`). |

---

## 3. Trạng thái 5 Cổng Kiểm soát (Gates 1–5)

| Cổng Kiểm soát | Điểm chuyển giao | Tiêu chí Đánh giá (Exit Criteria) | Bằng chứng Xác thực | Kết luận |
| :--- | :--- | :--- | :--- | :---: |
| **Gate 1: Problem Validation** | Phase 1 → Phase 2 | Kiểm kê 5 bảng gốc (`577,967` nodes, `84,633` raw $\to$ `84,206` motorized segments, `90,787` accepted pings, `33,394` windows), 10 Edge Cases (`EC-1..EC-10`) & vùng mù `88.13%`. | `outputs/file_inventory.csv`, `outputs/quarantine_summary.csv`, `01-discovery/validation-report.md` | **PASSED** |
| **Gate 2: Solution & Business Feasibility** | Phase 3/4/5 → Phase 6 | Kiến trúc 17 thành phần (`[C1]–[C17]`), 3 ADRs khóa chặt Zero-Conflict MLOps, Two-Tier Reconciliation & Organic Train-Only Priors/K-Means. | `03-requirements/prd.md`, `04-design/`, `05-architecture/architecture-decisions/adr-001..003.md` | **PASSED** |
| **Gate 3: Release Readiness** | Phase 6/7 → Phase 8 | Pipeline Batch/Stream/Spark/Mongo không Data Leakage, bỏ `sample_weight` nhân tạo & lọc burst `7,192` windows, Lakehouse Parquet đọc sạch trên cả PyArrow lẫn Spark 3.5.7, toàn bộ `pytest` và hồ sơ Session 9 đã đồng bộ sạch. | `07-quality/release-checklist.md`, `outputs/metrics.json`, `outputs/model_registry_manifest.json`, `evidence/session9/I_readiness.md` | **PASS** |
| **Gate 4: Product-Market Fit Signals** | Phase 8 → Phase 9 | Spark 3.5.7 (`BHJ 5→1 Exchange, 166.18→123.37 ms, 25.76% speedup`, Sink 2 watermark `2020-12-01T12:30:00Z`), MongoDB 8.0 (`33,394` docs, `1,234` cảnh báo trục chính, `IXSCAN warm_median=0.360 ms / p95=0.603 ms`, `31.36x`), Live Kafka + **Web TOC `:8000`**. | `outputs/spark_runtime_evidence.json`, `outputs/mongo_verify.json`, `outputs/live_kafka_telemetry.json` | **PASSED** |
| **Gate 5: Scale Readiness** | Phase 9 → Phase 10 | Đối soát 2 tầng đạt `100.0%` (`tier1_exceptAll_diff = 0`, bù `500` cửa sổ trễ / `226` khung giờ, hàn gắn `609` cửa sổ hạ lưu, chấm lại `384`, đổi xác suất `213`), `WAL = 73.7 KB`. | `outputs/reconciliation_audit.csv`, `outputs/query_progress.json`, `09-growth/experiment-log.md` | **PASSED** |

---

## 4. Ma trận 8 Luồng Công việc Song song (W1–W8)

| Workstream | Lĩnh vực Phụ trách | Thành phần Kiến trúc (`[C1]–[C17]`) | Tài liệu & Mã nguồn Đại diện |
| :--- | :--- | :--- | :--- |
| **W1: Product & Strategy** | Định vị bài toán, chiến lược sản phẩm, KPIs kinh doanh & vận hành đô thị | Toàn hệ thống (`Zones A–F`) | `00-proposal/`, `02-strategy/`, `03-requirements/prd.md` |
| **W2: UX/UI & Visualization** | Ứng dụng web hợp nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)** | `[C17]` Unified Web TOC Gateway | `04-design/`, `toc_backend/`, `toc_frontend/` |
| **W3: Data Engineering & Ingestion** | Tiếp nhận GIS/GPS, lọc `427` phi đường bộ, phễu `R1–R6`, Monoid (`0` lệch `baseline.py`), Medallion Lakehouse | `[C1]–[C5]`, `[C9]`, `[C14]`, `[C15]` | `producer.py`, `baseline.py`, `spark_batch.py`, `pipeline.py` |
| **W4: Streaming & Real-Time Processing** | Dual-Sink Watermark (`30m`), `32 KiB RotatingBloomFilter` (`0.0389%` FPR), `1 KiB 64-bit HLL++`, `EWMA`, `WAL 73.7 KB` | `[C6]`, `[C7]`, `[C8]`, `[C12]` | `starter_stream.py`, `live_kafka_stream.py`, `spark_engine.py`, `outputs/query_progress.json` |
| **W5: Graph Mining & Pattern Discovery** | Tỉa `525,786` (`90.97%`) nút trung gian, `Line-Graph PageRank` (`84,206` đoạn, `92,453` cạnh bỏ U-turn, `314` dangling), `12` co-rules & `14` lagged rules trên `TRAIN` hữu cơ | `[C10]` Graph & Pattern Engine | `pipeline.py`, `outputs/road_pagerank_top.csv`, `outputs/co_congestion_rules.csv`, `outputs/lagged_congestion_rules.csv` |
| **W6: Machine Learning & MLOps** | Lọc burst `7,192` windows, Unified Train-Only 7D K-Means (`3,609` hữu cơ, `Silhouette = 0.2241`), Causal Priors ($\tau < t$), Clean 23D RF (`class_weight` exp `0.65`), Frozen Registry SHA-256 (`a2f411846508`) | `[C11]`, `[C13a]`, `[C13b]` | `pipeline.py`, `spark_engine.py`, `outputs/model_registry_manifest.json` |
| **W7: Infrastructure, Serving & DevOps** | MongoDB 8.0 (`33,394` docs, atomic `$status_version`, `IXSCAN 0.360/0.603 ms`, `31.36x`), Docker Compose `5` dịch vụ mặc định (`11.0 GB RAM`: `kafka`, `mongo`, `spark 3.5.7`, `toc-web :8000`, `live-consumer`) | `[C16]` MongoDB 8.0 Serving Store | `Dockerfile`, `Dockerfile.spark35`, `docker-compose.yml`, `08-launch/operations-runbook.md` |
| **W8: Quality Assurance & Governance** | Kiểm toán số liệu sạch, bộ kiểm thử tự động `pytest` (`Gate 3 PASS`), tiêm lỗi & hồ sơ Session 9 đã tái sinh | Toàn bộ `[C1]–[C17]` | `07-quality/release-checklist.md`, `07-quality/test-strategy.md`, `tests/`, `evidence/session9/` |
