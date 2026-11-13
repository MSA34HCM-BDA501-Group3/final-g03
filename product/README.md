# HCMC Smart Traffic Analytics & Congestion Prediction Platform (V3) — Product Governance Index (`/workflow`)

Tài liệu này là **Master Product Governance Index** tuân thủ khung quản trị sản phẩm toàn diện `/workflow` (**11 Giai đoạn / Phases 0–10**, **5 Cổng kiểm soát / Stage-Gates 1–5**, **8 Luồng công việc song song / Workstreams W1–W8**) kết hợp **Stage-Gate**, **Dual-Track Agile**, **DevOps** và **Data-Driven Product Management** cho hệ thống **HCMC Smart Traffic Analytics & Congestion Prediction Platform (Architecture V3, Zones A–F, Components `[C1]`–`[C17]`)**.

---

## 1. Bản đồ 11 Giai đoạn (Phases 0–10) & Danh mục Tài liệu `product/`

| Phase | Tên Giai đoạn | Thư mục / Artifacts trong `product/` & Repo | Trạng thái | Kết quả / Số liệu Thực chứng (V3 Verified) |
| :--- | :--- | :--- | :---: | :--- |
| **Phase 0** | **Idea & Proposal** | [`00-proposal/product-vision.md`](00-proposal/product-vision.md)<br>[`00-proposal/project-proposal.md`](00-proposal/project-proposal.md) | ✅ Hoàn tất | Tầm nhìn sản phẩm & Báo cáo Tổng kết Đồ án + Đặc tả Kỹ thuật (7 Chương), bài toán dự báo `t+30m` trên `84,633` đoạn đường TP.HCM & phủ vùng mù Cold-Start `88.15%`. |
| **Phase 1** | **Discovery & Validation** | [`01-discovery/competitor-analysis.md`](01-discovery/competitor-analysis.md)<br>[`01-discovery/validation-report.md`](01-discovery/validation-report.md) | ✅ Hoàn tất | Kiểm định 5 bảng gốc Kaggle (`577,967` nodes, `84,633` segments, `90,938` GPS pings, `33,441` khung 30 phút). |
| **Phase 2** | **Strategy & Business Case** | [`02-strategy/business-model.md`](02-strategy/business-model.md)<br>[`02-strategy/product-strategy.md`](02-strategy/product-strategy.md) | ✅ Hoàn tất | Chiến lược Hybrid Batch–Streaming Lakehouse, ROI tối ưu RAM (`33.0 KiB`) và truy vấn `< 10ms`. |
| **Phase 3** | **Requirements (PRD)** | [`03-requirements/prd.md`](03-requirements/prd.md) | ✅ Hoàn tất | Đặc tả 6 Use Cases (`UC-1`..`UC-6`), 10 Edge Cases (`EC-1`..`EC-10`) và SLA phi chức năng. |
| **Phase 4** | **UX/UI Design** | [`04-design/wireframes/dashboard-5tabs-wireframe.md`](04-design/wireframes/dashboard-5tabs-wireframe.md) | ✅ Hoàn tất | Thiết kế 5 Tabs Streamlit Dashboard (`[C17]`) có điều khiển lọc tương tác, hiển thị vòng đời `STREAM_PROVISIONAL` & `BATCH_RECONCILED`. |
| **Phase 5** | **System Architecture** | [`05-architecture/architecture-decisions/adr-001-zero-conflict-mlops.md`](05-architecture/architecture-decisions/adr-001-zero-conflict-mlops.md)<br>[`05-architecture/architecture-decisions/adr-002-two-tier-reconciliation.md`](05-architecture/architecture-decisions/adr-002-two-tier-reconciliation.md)<br>[`05-architecture/architecture-decisions/adr-003-causal-priors-7d-kmeans.md`](05-architecture/architecture-decisions/adr-003-causal-priors-7d-kmeans.md) | ✅ Hoàn tất | Kiến trúc 6 phân vùng (`Zones A–F`, `[C1]–[C17]`), 3 ADRs cốt lõi: Zero-Conflict MLOps, Two-Tier Reconciliation & Train-Only 7D K-Means. |
| **Phase 6** | **Engineering & Implementation** | [`06-engineering/development-guidelines.md`](06-engineering/development-guidelines.md)<br>[`06-engineering/release-plan.md`](06-engineering/release-plan.md) | ✅ Hoàn tất | Clean Architecture, Cognitive Complexity `< 15` (`max-complexity=14`), 0 lỗi Ruff, PySpark 3.5.7 + MongoDB 8.0 + Kafka 4.1. |
| **Phase 7** | **Quality Assurance & Testing** | [`07-quality/test-strategy.md`](07-quality/test-strategy.md) | ✅ Hoàn tất | Bộ kiểm thử tự động `pytest` (`22 passed`: `17` trong `tests/test_platform_v3.py`, `5` trong `tests/test_quyen_spark.py`) bao phủ 4 trụ cột, kiểm định chuyển pha & Failure Injection. |
| **Phase 8** | **Launch, Pilot & Operations** | [`08-launch/operations-runbook.md`](08-launch/operations-runbook.md)<br>[`08-launch/pilot-report.md`](08-launch/pilot-report.md) | ✅ Hoàn tất | Runbook vận hành Docker Compose, kiểm chứng chịu lỗi WAL Checkpoint Kill/Restart (`0` duplicate side-effects). |
| **Phase 9** | **Growth & Analytics** | [`09-growth/experiment-log.md`](09-growth/experiment-log.md)<br>[`09-growth/product-analytics.md`](09-growth/product-analytics.md) | ✅ Hoàn tất | Ablation Study 7 mô hình (Validation Gate chọn `Random Forest 23D`: `ROC-AUC = 0.9882`, `PR-AUC = 0.9471`, `Macro-F1 = 0.5543`) & giải mã nghịch lý `Persistence Baseline`. |
| **Phase 10** | **Continuous Improvement & Governance** | [`../PROPOSAL_V3_AUDIT_CHECKLIST.md`](../PROPOSAL_V3_AUDIT_CHECKLIST.md)<br>[`../TECHNICAL_NOTE.md`](../TECHNICAL_NOTE.md) | ✅ Hoàn tất | Đối soát 100% các mục kiểm toán (`28/28` nhóm, `50/50` tasks: `G01–G05`, `Q01–Q11`, `D01–D12`, `M01–M14`, `T01–T08`) với bằng chứng runtime thực. |

---

## 2. Trạng thái 5 Cổng Kiểm soát (Stage-Gates 1–5)

| Stage-Gate | Điểm chuyển giao | Tiêu chí Đánh giá (Exit Criteria) | Bằng chứng Xác thực | Kết luận |
| :--- | :--- | :--- | :--- | :---: |
| **Gate 1: Problem Validation** | Phase 1 → Phase 2 | Dữ liệu thực tế từ 5 bảng gốc được kiểm kê đầy đủ; xác định rõ 10 Edge Cases (`EC-1`..`EC-10`) và tỷ lệ vùng mù cảm biến (`88.15%`). | `outputs/file_inventory.csv`, `outputs/quarantine_summary.csv`, `01-discovery/validation-report.md` | **PASSED** |
| **Gate 2: Solution & Business Feasibility** | Phase 3/4/5 → Phase 6 | Kiến trúc 17 thành phần (`[C1]–[C17]`), 3 ADRs khoá chặt hợp đồng Zero-Conflict MLOps, Two-Tier Reconciliation và Train-Only Priors. | `03-requirements/prd.md`, `04-design/`, `05-architecture/architecture-decisions/adr-001..003.md` | **PASSED** |
| **Gate 3: Release Readiness** | Phase 6/7 → Phase 8 | Toàn bộ pipeline Batch/Stream/Spark/Mongo thực thi không Data Leakage (`max(train.target_ts) <= train_cutoff`), `22/22` `pytest` PASS, độ phức tạp hàm `< 15`, 0 lỗi lint. | `pipeline.py`, `spark_engine.py`, `starter_stream.py`, `outputs/model_registry_manifest.json` | **PASSED** |
| **Gate 4: Product-Market Fit Signals** | Phase 8 → Phase 9 | Vận hành ổn định trên Spark 3.5.7, MongoDB 8.0.34 (`IXSCAN`, `0` `SORT`, `warm_p95 = 0.636 ms`), Kafka 4.1.0 (`3,000` bản tin) và Streamlit 5-Tab UI (`:8501`). | `outputs/spark_runtime_evidence.json`, `outputs/mongo_verify.json`, `outputs/query_progress.json` | **PASSED** |
| **Gate 5: Scale Readiness** | Phase 9 → Phase 10 | Đối soát 2 tầng đạt `100.0%` (`exceptAll = 0` trong watermark, hồi phục `100.0%` sau khi bù `501` bản tin trễ, cập nhật đặc trưng lịch sử trễ cho `3,953` cửa sổ kế tiếp & chấm điểm lại `377` cửa sổ hạ lưu). | `outputs/reconciliation_audit.csv`, `09-growth/experiment-log.md`, `PROPOSAL_V3_AUDIT_CHECKLIST.md` | **PASSED** |

---

## 3. Ma trận 8 Luồng Công việc Song song (Workstreams W1–W8)

| Workstream | Lĩnh vực Phụ trách | Thành phần Kiến trúc (`[C1]–[C17]`) | Tài liệu & Mã nguồn Đại diện |
| :--- | :--- | :--- | :--- |
| **W1: Product & Strategy** | Định vị bài toán, chiến lược sản phẩm, KPIs kinh doanh & vận hành đô thị | Toàn hệ thống (`Zones A–F`) | `00-proposal/`, `02-strategy/`, `03-requirements/prd.md` |
| **W2: UX/UI & Visualization** | Thiết kế trải nghiệm 5 Tabs Dashboard, hiển thị vòng đời dữ liệu & Cold-Start | `[C17]` Streamlit 5-Tab Dashboard | `04-design/wireframes/dashboard-5tabs-wireframe.md`, `app.py` |
| **W3: Data Engineering & Ingestion** | Tiếp nhận GIS/GPS, kiểm định chất lượng có thứ tự (`R1–R6`), đối chiếu Monoid, Medallion Lakehouse | `[C1]–[C5]`, `[C9]`, `[C14]`, `[C15]` | `producer.py`, `baseline.py`, `spark_batch.py`, `pipeline.py` |
| **W4: Streaming & Real-Time Processing** | Dual-Sink Watermark (30m), Bit-Packed Bloom (`32 KiB`), HLL++ (`1 KiB`), Decaying Windows | `[C6]`, `[C7]`, `[C8]`, `[C12]` | `starter_stream.py`, `spark_engine.py`, `outputs/query_progress.json` |
| **W5: Graph Mining & Pattern Discovery** | Lọc điểm uốn cong trung gian (`525,542` `shape points`), `Directed Line-Graph PageRank` (`84,633` đoạn), `2-Pass SON / FP-Growth` | `[C10]` Graph & Pattern Engine | `pipeline.py`, `outputs/road_pagerank_top.csv`, `outputs/co_congestion_rules.csv` |
| **W6: Machine Learning & MLOps** | Train-Only 7D K-Means, Causal Priors, 23D Random Forest/GBT, Validation Gate & Frozen Registry | `[C11]`, `[C13a]`, `[C13b]` | `pipeline.py`, `spark_engine.py`, `outputs/model_registry_manifest.json` |
| **W7: Infrastructure, Serving & DevOps** | Lưu trữ vận hành MongoDB 8.0 (`IXSCAN`), Docker Compose, Kafka 4.1 & Spark 3.5.7 | `[C16]` MongoDB 8.0 Serving Store | `Dockerfile`, `docker-compose.yml`, `08-launch/operations-runbook.md` |
| **W8: Quality Assurance & Governance** | Kiểm thử đơn vị, tích hợp, tiêm lỗi (Failure Injection), kiểm toán tính trung thực (Honesty Audit) | Toàn bộ `[C1]–[C17]` | `07-quality/test-strategy.md`, `tests/test_platform_v3.py`, `tests/test_quyen_spark.py` |
