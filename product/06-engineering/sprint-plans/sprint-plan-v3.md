# Kế Hoạch Sprint & Điều Phối 8 Workstreams (Sprint Plan & Workstream DRI Matrix)

## 1. Ma Trận Phân Công 8 Workstreams (`DRI Matrix — Skill Workflow Section 3`)

| Workstream | Vai trò Chính (`Role`) | Người Chịu Trách Nhiệm Cuối Cùng (`DRI`) | Deliverables Chính |
| :--- | :--- | :--- | :--- |
| **1. `Product Management`** | `PM / PO` | **Nguyễn Thành Đạt (`25ms23306`)** | `Proposal V3`, `Vision`, `Roadmap`, `Stage-Gate Reviews 1–5` |
| **2. `Business & Research`** | `BA / Data Analyst` | **Nguyễn Tạ Minh Quyền (`25ms23280`)** | `5-Table Deepdive EDA`, `UC-1..UC-6`, `EC-1..EC-10`, `PRD` |
| **3. `Product Design`** | `UX/UI Designer` | **Phạm Công Minh (`25ms23310`)** | `5-Tab Wireframes`, `14-Slide Deck V3`, `Architecture Diagram V4` |
| **4. `Engineering`** | `Tech Lead / Data Eng` | **Nguyễn Tạ Minh Quyền (`25ms23280`)** | `[C2]` GIS Loader, `[C5]` Quality Funnel, `[C9]` Monoid & Salting, `[C14]`/`[C15]` Lakehouse & Quarantine, `spark_batch.py`, `traffic_data/spark_etl.py`, `baseline.py` |
| **5. `Data & AI`** | `Data Scientist / ML Eng` | **Nguyễn Thành Đạt (`25ms23306`)** | `[C10]` `PageRank`/`FPGrowth`/`7D K-Means`, `[C11]` Causal ML Fit, `[C13a]`/`[C13b]` Frozen Registry, xác nhận `D-C01..D-C08` |
| **6. `Quality & Security`** | `QA / Verification Eng` | **Nguyễn Thành Đạt & Nguyễn Tạ Minh Quyền** | `tests/test_platform_v3.py` (`15` tests), `tests/test_quyen_spark.py` (`5` tests), `Ruff Linting`, `Reconciliation & IXSCAN Verification` |
| **7. `Infrastructure`** | `DevOps / SRE` | **Phạm Công Minh (`25ms23310`)** | `Dockerfile`, `Dockerfile.spark`, `docker-compose.yml`, `compose.quyen.yml`, `[C1]` `producer.py`, `starter_stream.py`, `[C16]` `MongoDB 8.0`, `[C17]` `Streamlit :8501`, xác nhận `M-C01..M-C11` |
| **8. `Business Growth`** | `Adoption & Analytics` | **Phạm Công Minh (`25ms23310`)** | `Pilot Report`, `Operations Runbook`, `Experiment Log`, `Growth Roadmap` |

---

## 2. Lịch Trình 3 Sprints Phát Triển (`Dual-Track Agile Sprints`)

- **Sprint 1 (`Data Engineering & Quality Funnel — Pillars I1, I2`):** Hoàn thành kiểm kê 5 bảng, lọc `525,542` điểm uốn cong trung gian (`shape points`, `[C2]`), `Ordered Quality Funnel` `[C5]` + `[C15]` `Quarantine`, nội suy `3-Tier max_velocity`, tổng hợp `Commutative Monoid` (`[C9]`), lưu trữ `Medallion Parquet Lakehouse` (`[C14]`), tích hợp `spark_batch.py` + `traffic_data/spark_etl.py` (`PR #1`, `3` `BroadcastHashJoin` / `0` `SortMergeJoin`), và kiểm chứng `baseline.py`.
- **Sprint 2 (`MMDS Mining & Zero-Conflict Causal ML — MMDS Ch. 5–7, 12 & Pillar I3`):** Hoàn thành `Directed Line-Graph PageRank` (`84,633` đoạn), `30` luật `SON/FPGrowth` + `20` luật trễ `+30m`, `Train-Only 7D Behavioral K-Means` ($k=6$), `Causal Expanding Empirical-Bayes Priors` ($\tau < t$), huấn luyện `Phase A` và đóng băng `[C13a]`/`[C13b]` (đã chốt `D-C01..D-C08`).
- **Sprint 3 (`Speed Layer, Two-Tier Reconciliation, MongoDB IXSCAN & 5-Tab UI — Pillar S7, Ch. 4`):** Hoàn thành `producer.py` (`[C1]`), `32KB Bloom Filter` + `1KB HLL++` + `Decaying Hotspots` (`[C7]`), chấm điểm `Phase B` (`[C8]`), đối soát `Phase C` (`[C12]`, hàn gắn `3,953` cửa sổ trễ bậc & chấm điểm lại `377` cửa sổ), phục vụ `MongoDB 8.0 IXSCAN` (`[C16]`), giao diện `app.py` (`[C17]`) và bộ kiểm thử `pytest` `20/20` (đã chốt `M-C01..M-C11`).
