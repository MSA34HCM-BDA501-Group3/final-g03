# HCMC Smart Traffic Analytics & Congestion Prediction Platform (V3)

## 1. Tổng quan Đề tài & Kiến trúc Giải pháp V3 (Zones A–F, Components `[C1]`–`[C17]`)

Dự án xây dựng nền tảng phân tích dữ liệu lớn và dự báo ùn tắc giao thông đô thị TP. Hồ Chí Minh (**HCMC Smart Traffic Big Data Platform — V3**) trên bộ dữ liệu thực tế 5 bảng liên kết (**Kaggle HCMC Traffic Flow Dataset**), bám sát khung lý thuyết của hai giáo trình cốt lõi và hệ thống 4 bài Lab thực hành của học phần **BDA501**:
- **Big Data Systems:** *Big Data Analytics: A Hands-On Approach* (Arshdeep Bahga & Vijay Madisetti, 2019 — 4-Tier Analytics Value Chain, Gamma/Delta Pattern, Medallion Lakehouse, Load Leveling, NoSQL CAP).
- **Massive Dataset Mining:** *Mining of Massive Datasets, 3rd Ed.* (Jure Leskovec, Anand Rajaraman & Jeffrey D. Ullman, 2020 — Stream Sketching Ch. 4, Link Analysis PageRank Ch. 5, Frequent Itemsets SON/FP-Growth Ch. 6, Clustering Ch. 7, Decision Trees & Ensembles Ch. 12).
- **4 Trụ cột Lab BDA501:**
  - **Pillar I1 (`Hadoop MapReduce`):** Gộp cửa sổ 30 phút bằng **5-Tuple Commutative Monoid Combiner** $(\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt})$ tại `[C9]`, đối chứng độc lập `0` sai lệch với bộ kiểm chuẩn đơn nút [`baseline.py`](./baseline.py).
  - **Pillar I2 (`Spark SQL, Catalyst & Skew`):** Phễu kiểm định chất lượng (**Ordered Quality Funnel**) tại `[C5]` đẩy bản ghi lỗi sang `[C15]` `quarantine_events.parquet`; tối ưu hóa `BroadcastHashJoin` cho bảng nhỏ và **2-Stage Key Salting (`8 buckets`)** chống lệch phân vùng (`Data Skew`) trên `90,938` bản tin vận tốc.
  - **Pillar I3 (`Spark MLlib & Zero-Conflict MLOps`):** Quy trình **Zero-Conflict 3-Phase MLOps Lifecycle** phân tách tuyệt đối quyền huấn luyện (`.fit()` duy nhất tại **Phase A** bởi `[C11]` trên tập `TRAIN` $\to$ đóng băng `[C13a]` `model.joblib` và `[C13b]` `segment_static_features.parquet`) với quyền suy luận chỉ đọc (`.transform()` tại **Phase B Stream `[C8]`** và **Phase C Reconciliation `[C12]`**).
  - **Pillar S7 (`Kafka + Spark Structured Streaming + MongoDB 8.0`):** Bộ phát sự kiện [`producer.py`](./producer.py) (`[C1]`) định tuyến theo `hash(segment_id) % 3` vào `[C4]`, xử lý luồng kép (`Dual-Sink` `[C6]`) với **Bloom Filter (`32 KB`)**, **HyperLogLog++ (`1 KB`)**, **Exponentially Decaying Windows ($\alpha=0.15$)** tại `[C7]`, và ghi idempotent **Bulk `UpdateOne(upsert=True)`** vào `[C16]` **MongoDB 8.0** kèm kiểm chứng chỉ mục phức hợp **`IXSCAN`**.

Đặc tả toán học `P1–P4`, `UC-1–UC-6`, `EC-1–EC-10`, kiến trúc 6 phân vùng (`Zones A–F`, `[C1]`–`[C17]`, `10 Dataflows`) và kết quả thực nghiệm chi tiết xem tại **[TECHNICAL_NOTE.md](./TECHNICAL_NOTE.md)** và cây tài liệu quản trị sản phẩm **[product/](./product/)**.

---

## 2. Cấu trúc 5 Bảng Dữ liệu Đầu vào (`raw_data/`)

| Tệp nguồn | Số bản ghi | Số cột | Dung lượng | Vai trò trong Kiến trúc V3 (`System Boundary`) |
|---|---:|---:|---:|---|
| `nodes.csv` | 577,967 | 3 | 19.44 MB | `[C2]` Tọa độ `(long, lat)`; cắt tỉa `525,542` đỉnh hình học OSM mồ côi (`EC-7`), giữ `52,425` giao lộ thực |
| `streets.csv` | 5,553 | 5 | 0.20 MB | `[C2]` Danh mục đường phố (`name`, `level` 1–4, `type`, `max_velocity`) dùng cho `BroadcastHashJoin` |
| `segments.csv` | 84,633 | 11 | 10.29 MB | `[C2]` Đồ thị cạnh có hướng (`s_node_id -> e_node_id`, `151,929` cạnh chuyển tiếp Line-Graph, `48` nút cụt) |
| `segment_status.csv` | 90,938 | 4 | 3.58 MB | `[C1]` Bản tin vận tốc GPS thực tế (`velocity`, `updated_at`) dùng cho gộp Monoid 30 phút & luồng Replay |
| `train.csv` | 33,441 | 18 | 4.99 MB | `Derived View` (khớp `100.0%` với gộp 30m UTC từ `segment_status.csv`); nhãn `LOS` (`A`–`F`) trên `10,027` đoạn đường |

---

## 3. Cây Tài liệu Quản trị Sản phẩm (`product/` — 11 Phases, 5 Stage-Gates)

Toàn bộ tài liệu vòng đời sản phẩm theo chuẩn `workflow` được tổ chức tại thư mục [`product/`](./product/):
- [`00-proposal/`](./product/00-proposal/): [`project-proposal.md`](./product/00-proposal/project-proposal.md), [`product-vision.md`](./product/00-proposal/product-vision.md) (**Gate 1 — Concept Gate**)
- [`01-discovery/`](./product/01-discovery/): [`market-research.md`](./product/01-discovery/market-research.md), [`competitor-analysis.md`](./product/01-discovery/competitor-analysis.md), [`customer-interviews.md`](./product/01-discovery/customer-interviews.md), [`validation-report.md`](./product/01-discovery/validation-report.md) (**Gate 2 — Validation Gate**)
- [`02-strategy/`](./product/02-strategy/): [`product-strategy.md`](./product/02-strategy/product-strategy.md), [`business-model.md`](./product/02-strategy/business-model.md), [`success-metrics.md`](./product/02-strategy/success-metrics.md)
- [`03-requirements/`](./product/03-requirements/): [`prd.md`](./product/03-requirements/prd.md), [`user-stories.md`](./product/03-requirements/user-stories.md), [`acceptance-criteria.md`](./product/03-requirements/acceptance-criteria.md), [`prioritized-backlog.md`](./product/03-requirements/prioritized-backlog.md) (**Gate 3 — Commitment Gate**)
- [`04-design/`](./product/04-design/): [`ux-research/operator-workflows.md`](./product/04-design/ux-research/operator-workflows.md), [`wireframes/dashboard-5tabs-wireframe.md`](./product/04-design/wireframes/dashboard-5tabs-wireframe.md), [`prototypes/streamlit-prototype-spec.md`](./product/04-design/prototypes/streamlit-prototype-spec.md)
- [`05-architecture/`](./product/05-architecture/): [`system-design.md`](./product/05-architecture/system-design.md), [`security-design.md`](./product/05-architecture/security-design.md), [`api-specifications/serving-and-stream-contracts.md`](./product/05-architecture/api-specifications/serving-and-stream-contracts.md), ADRs [`adr-001`](./product/05-architecture/architecture-decisions/adr-001-zero-conflict-mlops.md), [`adr-002`](./product/05-architecture/architecture-decisions/adr-002-two-tier-reconciliation.md), [`adr-003`](./product/05-architecture/architecture-decisions/adr-003-causal-priors-7d-kmeans.md)
- [`06-engineering/`](./product/06-engineering/): [`development-guidelines.md`](./product/06-engineering/development-guidelines.md), [`release-plan.md`](./product/06-engineering/release-plan.md), [`sprint-plans/sprint-plan-v3.md`](./product/06-engineering/sprint-plans/sprint-plan-v3.md)
- [`07-quality/`](./product/07-quality/): [`test-strategy.md`](./product/07-quality/test-strategy.md), [`release-checklist.md`](./product/07-quality/release-checklist.md), [`test-cases/test-matrix-p1-p4-uc1-uc6-ec1-ec10.md`](./product/07-quality/test-cases/test-matrix-p1-p4-uc1-uc6-ec1-ec10.md) (**Gate 4 — Launch Gate**)
- [`08-launch/`](./product/08-launch/): [`pilot-report.md`](./product/08-launch/pilot-report.md), [`go-to-market.md`](./product/08-launch/go-to-market.md), [`operations-runbook.md`](./product/08-launch/operations-runbook.md)
- [`09-growth/`](./product/09-growth/): [`product-analytics.md`](./product/09-growth/product-analytics.md), [`experiment-log.md`](./product/09-growth/experiment-log.md), [`growth-roadmap.md`](./product/09-growth/growth-roadmap.md) (**Gate 5 — Scale Gate**)

---

## 4. Hướng dẫn Chạy Dự án & Kiểm thử Tự động

### 4.1. Chạy trực tiếp trên Host Python (Windows PowerShell)

```powershell
python -m pip install -r requirements.txt
python pipeline.py
python baseline.py
python producer.py
pytest tests/test_platform_v3.py -v
ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
streamlit run app.py
```

Sau khi khởi chạy, mở trình duyệt tại <http://localhost:8501>.

### 4.2. Chạy bằng Docker Compose

```powershell
docker compose run --build --rm pipeline
docker compose up --build -d app
```

Mở <http://localhost:8501>. Tầng dữ liệu nguồn `raw_data/` được mount ở chế độ chỉ đọc (`:ro`), còn kết quả sinh ra được lưu bền vững tại `data/` và `outputs/`.

---

## 5. Danh mục Tệp Đầu ra Chính (`data/` & `outputs/`)

| Tệp kết quả | Thành phần Kiến trúc | Nội dung |
|---|---|---|
| `data/dataset.csv` | `[C14]` Medallion Gold | Bảng dữ liệu hợp nhất (`33,441` dòng $\times$ `43` cột) đã làm sạch, gắn PageRank, K-Means và biến nhân quả |
| `data/lakehouse/bronze_raw_events/events.parquet` | `[C14]` Bronze Layer | Luồng sự kiện GPS Replay (`33,775` bản tin gồm `1%` trùng lặp & `5%` sự kiện trễ) |
| `data/lakehouse/silver_cleaned_windows/windows.parquet` | `[C14]` Silver Layer | Cửa sổ 30 phút đã chuẩn hóa Unicode `NFC`, sửa lệch `UTC+7` và nội suy vận tốc 3 tầng |
| `data/lakehouse/curated_gold/year_month=*` | `[C14]` Gold Partitioned | `10` phân vùng Parquet theo tháng (`2020-07` đến `2021-04`) |
| `data/lakehouse/segment_static_features.parquet` | `[C13b]` Static Feature Store | Hồ sơ tĩnh cho toàn bộ `84,633` đoạn đường (`10,027` có GPS + `74,606` vùng mù `Cold-Start`) |
| `data/lakehouse/quarantine_events.parquet` | `[C15]` Quarantine Store | Bản ghi vi phạm quy tắc chất lượng (`154` đoạn đường `length <= 0` + `427` nhãn OSM phi giao thông) |
| `data/lakehouse/serving_traffic_scores.parquet` | `[C16]` Serving Mirror | Toàn bộ `33,441` bản ghi dự báo kèm trạng thái vòng đời `STREAM_PROVISIONAL` / `BATCH_VERIFIED` / `BATCH_RECONCILED` |
| `outputs/quarantine_summary.csv` | `[C5]` & `[C15]` | Thống kê phễu lọc chất lượng 6 quy tắc kiểm định (`EC-1` đến `EC-4`) |
| `outputs/join_plan_evidence.json` | `[C9]` Pillar I1 & I2 | Bằng chứng thực thi `BroadcastHashJoin`, `2-Stage Key Salting (8 buckets)` trên `90,938` bản tin và cắt tỉa `525,542` đỉnh mồ côi |
| `outputs/monoid_baseline_verification.json` | `[C9]` Pillar I1 | Kiểm chứng đối chiếu Monoid 5-tuple khớp tuyệt đối (`0` sai lệch) so với [`baseline.py`](./baseline.py) |
| `outputs/model.joblib` & `model_registry_manifest.json` | `[C13a]` Frozen Registry | Mô hình đóng băng từ Phase A (`[C11]`) kèm mã băm bất biến `SHA-256` kiểm chứng Zero-Conflict MLOps |
| `outputs/mongo_verify.json` | `[C16]` MongoDB 8.0 | Kết quả ghi Bulk `UpdateOne` 4 phân vùng và bằng chứng `explain("executionStats")` đạt `IXSCAN` |
| `outputs/road_pagerank_top.csv` | `[C10]` MMDS Ch. 5 | Top 50 nút cổ chai cấu trúc theo Directed Line-Graph PageRank ($\beta = 0.85$) |
| `outputs/co_congestion_rules.csv` | `[C10]` MMDS Ch. 6 | Top 30 luật đồng kẹt xe dây chuyền ($\text{LOS} \in \{E, F\}$) xếp theo Lift & Confidence |
| `outputs/hotspot_alerts.csv` | `[C7]` MMDS Ch. 4.7 | Top 25 điểm nóng ùn tắc kéo dài tính bằng cửa sổ suy giảm mũ ($\alpha = 0.15$) |
| `outputs/reconciliation_audit.csv` | `[C12]` Two-Tier Recon | Bảng kiểm toán đối soát Batch–Streaming (`100%` in-watermark match, bù `319` cửa sổ trễ nâng `98.5% -> 100%` completeness) |
| `outputs/ablation_results.csv` | `[C11]` MMDS Ch. 12 | Bảng đối chứng 7 cấu hình ML giữa `Local Features (14D)` và `Local + Graph + Cluster (23D)` |
| `outputs/roc_curve.csv`, `outputs/roc_curve.png` | `[C11]` Evaluation | Tọa độ FPR/TPR và biểu đồ đường cong ROC dự báo ùn tắc nghiêm trọng (`LOS E/F`) |
| `outputs/checkpoints/` & `query_progress.json` | `[C6]` & `[C15]` | WAL offsets (`raw/offset_log.json`), state store log (`agg/state_wal.json`) và telemetry luồng |
| `outputs/metrics.json` | `[C17]` UI Telemetry | Tổng hợp toàn bộ chỉ số vận hành của 17 thành phần `[C1]`–`[C17]` |
