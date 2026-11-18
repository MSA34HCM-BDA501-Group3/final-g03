# BDA501 Session 9 Lab Technical Audit Report — HCMC Smart Traffic Platform (Card B)
- **Team:** Nguyễn Tạ Minh Quyền (`25MS23280`) · Nguyễn Thành Đạt (`25MS23306`) · Phạm Công Minh (`25MS23310`)
- **Instructor:** **Đặng Trần Khánh** (`FSB · FPT University | MSE AI | BDA501 – Big Data Analytics`)
- **Completed Word Manuals:** `BDA501_Session9_Lab_Manual.docx`, `BDA501_Session9_Lab_Manual_v2.docx`, `BDA501_Session9_Lab_Manual_Complete.docx`
- **Session 9 Summary Slide Deck:** `HCMC_Smart_Traffic_Session9_Readiness_Slide.pptx` (`evidence/session9/I_session9_summary_slide.png`)

---

# Deliverable A — End-to-End Integration Truth Audit (`A_architecture`)

- **Data Period / Sample:** HCMC Road Network (`84,633` raw -> `84,206` clean motorized segments, `577,967` raw -> `52,181` active intersections, `5,553` streets) + Empirical GPS Probes (`90,938` raw -> `90,787` accepted pings -> `33,394` 30m windows across `2020-07-03` to `2021-04-22`).
- **Diagram File:** `evidence/session9/A_architecture.png`
- **Status Classification:**
  - **`[I] Implemented` (Measured & Verified in Repository / Docker Compose):**
    1. **Zone A1 & A2 (`producer.py` + `Kafka 4.1.0 KRaft`):** Reads raw CSV/Parquet telemetry and replays event-time-stamped JSON messages into 3 Kafka partitions (`topic = hcmc-traffic-events`, partitioned by `segment_id`).
    2. **Zone B1 & B2 (`pipeline.py`, `traffic_data/spark_etl.py`, `starter_stream.py`):** Ordered Quality Funnel (`ANSI try_cast`, NFC `695 -> 641` streets, `0 Silent Drops`), Dead-Letter Quarantine (`580` bad segment records, `151` rejected pings), and WAL Checkpoints (`73,713 B`).
    3. **Zone C (`starter_stream.py` & `spark_engine.py`):** Dual-Sink Stream Router (`Sink 1` appends 100% raw events; `Sink 2` enforces `30m Event-Time Watermark`), `33.0 KiB` bounded sketches (`32.0 KiB` Bloom, `1.0 KiB` HLL++, bounded `EWMA` $\alpha=0.15$), and read-only `.predict_proba()`.
    4. **Zone D (`spark_batch.py`, `traffic_data/spark_etl.py`, `pipeline.py`):** Spark 3.5.7 Batch ETL (`groupexe-knowledgetest-spark-1`) with `2-Stage 8-Bucket Key Salting + 5-Tuple Monoid`, `BroadcastHashJoin` (`5 -> 1` Exchanges), orphan node pruning (`577,967 -> 52,181` active nodes), `Line-Graph PageRank` (`92,453` edges, `314` dangling), `FPGrowth` (`335` rules), and `7D K-Means` (`k=6`).
    5. **Zone E (`outputs/model.joblib`, `outputs/vmax_lookup.json`, `data/lakehouse/segment_static_features.parquet`):** Phase A Frozen Registry locked with `SHA-256` manifest (`outputs/model_registry_manifest.json`).
    6. **Zone F1, F2, F3 (`data/lakehouse/curated_gold`, `MongoDB 8.0.34`, `app.py`, `toc_backend`):** Columnar Parquet partitioned by `year_month`, MongoDB `bda501.traffic_scores` (`33,394` docs, `ESR IXSCAN` live median `0.870 ms`, saved p95 `0.692 ms`), Streamlit Dashboard (`:8501`), and React+FastAPI TOC UI (`:8000`).
  - **`[P] Proposed / Design-Only`:**
    1. Live telco/bus/ride-hailing city-wide streaming feed (`8–17` billion pings/day, `1.5–3.0 TB/day`).
    2. Multi-node cloud Kubernetes (`EKS/GKE SparkOperator`) / `YARN` auto-scaling & `S3 Delta Lake`.
    3. Closed-loop hardware actuation into HCMC `SCATS` traffic signal controllers.


---

# Deliverable B — Controlled Performance & Scalability Experiment (`B_benchmark`)

- **Command & Runtime:** Live 3-way controlled benchmark executed in `groupexe-knowledgetest-spark-1` (Apache Spark `3.5.7`, `local[4]`, `8` shuffle partitions, `AQE=false`, `2` warmup + `5` interleaved runs; source=`live_container`). Cross-checked with `outputs/spark_runtime_evidence.json`.
- **Dataset & Period:** `90,938` raw -> `90,787` accepted GPS pings joined with `84,206` clean motorized segments and `5,553` streets across `2020-07-03` to `2021-04-22` (`33,394` 30-minute windows, `38.50 MB`).
- **Controlled Factors Changed:**
  1. **Join Strategy:** `SortMergeJoin` (`autoBroadcastJoinThreshold = -1`) vs `BroadcastHashJoin` on static dimension tables `segments` & `streets`.
  2. **Aggregation Skew Mitigation:** `Unsalted GroupBy` vs `2-Stage 8-Bucket Key Salting + 5-Tuple Commutative Monoid`.

## Live Measured 3-Way Results (`evidence/session9/B_benchmark.csv` & `B_live_spark_benchmark.json`)
| Run | Configuration | Input / Partitions | Median Runtime (IQR) | Exchange Stages | Peak-to-Median Skew | Parity (`exceptAll`) |
|---|---|---|---:|---:|---:|---:|
| **Baseline** | `SortMergeJoin` + Unsalted GroupBy | `90,787` pings / `8` parts | `179.73 ms` (`IQR 48.83`) | `5` | `684.95x` | `0` rows diff |
| **Variant 1** | `BroadcastHashJoin` + Unsalted Monoid | `90,787` pings / `8` parts | `142.36 ms` (`-20.79%`, `IQR 4.35`) | `1` (`-80%`) | `684.95x` | `0` rows diff |
| **Variant 2** | `BroadcastHashJoin` + `8-Bucket Salted Monoid` | `90,787` -> `59,714` salted -> `33,394` windows | `219.01 ms` (`+76.65 ms` vs V1, `IQR 14.05`) | `2` | `1.0197x` (`CV=0.0059`) | `0` rows diff |

- **Wave 2 Isolated `outputs/spark_runtime_evidence.json` Baseline Check:** Catalyst Join `SortMergeJoin 145.24 ms -> BroadcastHashJoin 120.45 ms` (`17.07%` speedup, `5 -> 1` Exchange, `exceptAll=0`); Isolated Monoid unsalted `147.11 ms` vs 8-bucket salted `222.60 ms` (`exceptAll=0`).

## Required Three-Part Interpretation
1. **What changed, and why did the result move in that direction?**
   - Switching from `SortMergeJoin` to `BroadcastHashJoin` pinned `segments` (`84,206` rows) and `streets` (`5,553` rows) in executor memory, removing `4` network/sort `Exchange` stages (`5 -> 1`) and reducing median runtime by **`20.79%` (`179.73 ms -> 142.36 ms`)**.
   - Adding `2-Stage 8-Bucket Key Salting` split hot arterial keys across `8` `xxhash64` salt buckets (`59,714` Stage-1 partial groups -> `33,394` windows). On the `38.50 MB` sample, the extra partial-reduce shuffle adds `+76.65 ms` overhead vs V1 while collapsing peak-slot skew from **`684.95x` down to `1.0197x`** with `0.0` numerical error.
2. **At what point would your chosen configuration stop scaling well?**
   - `BroadcastHashJoin` fails with executor/driver `OutOfMemoryError` if broadcast tables exceed `spark.sql.autoBroadcastJoinThreshold` (`> 100–256 MB`). Fixed `8-bucket` salting caps concurrency at `8` cores per hot key, requiring adaptive `32–64` buckets or AQE `optimizeSkewedJoin` at `10^7+` pings/slot.
3. **What limitation prevents this experiment from proving production-scale performance?**
   - Runs inside a single-host Docker container (`groupexe-knowledgetest-spark-1`) over `38.50 MB` where shuffles traverse local disk/loopback rather than multi-rack cluster networking at `1.5–3.0 TB/day`.


---

# Deliverable C — Spark Query-Plan & Shuffle Audit (`C_query_plan`)

- **Evidence File:** `evidence/session9/C_query_plan.txt` & `groupexe-knowledgetest-spark-1` (Apache Spark `3.5.7`).
- **Inspected Operation:** `5-Tuple Monoid` + `SortMergeJoin`/`BroadcastHashJoin` enrichment over `33,394` windows (`84,206` segments, `5,553` streets) and `curated_gold` Parquet read (`year_month = '2020-08'`).

| Audit Check | Verified Evidence from Live `executedPlan()` |
|---|---|
| **1. Inspected query/operation** | `_build_unsalted_monoid` / `_build_salted_monoid(buckets=8)` joined with `segments_clean` & `streets_clean`, plus `spark.read.parquet('data/lakehouse/curated_gold').filter("year_month = '2020-08'").select('segment_id', 'date', 'period', 'LOS')`. |
| **2. `Exchange` / shuffle visibility** | Baseline has **5 `Exchange`** nodes (1 `HashAggregate` + 4 join shuffles before `Sort`). Variant 1 (`BroadcastHashJoin`) has **only 1 `Exchange`** (inside `HashAggregate`); Variant 2 (`8-Bucket Salting`) has **2 `Exchange`** stages (Stage-1 partial + Stage-2 reduce). |
| **3. Join strategy used** | **`BroadcastHashJoin [segment_id], Inner, BuildRight`** and **`BroadcastHashJoin [street_id], LeftOuter, BuildRight`**, eliminating both `SortMergeJoin` + `Sort`. |
| **4. What determines partitioning** | `spark.sql.shuffle.partitions = 8` in memory, `xxhash64(..., salt)` in Stage-1 salted aggregation, and `partitionBy('year_month')` (`10` monthly directories) on disk. |
| **5. `cache/persist` assessment** | Selective `persist()` helps ONLY on reused inputs (`segments_clean`, `streets_clean`, `status_valid`, `silver_cleaned_windows`); caching single-pass frames wastes heap RAM. |
| **6. `BroadcastHashJoin` condition** | Verified valid: `segments` (`84,206` rows) & `streets` (`5,553` rows) fit far below `10 MB` threshold, cutting Exchanges `5 -> 1` (`-80%`). |
| **7. Parquet layout & pruning** | Physical Plan confirms `PartitionFilters: [isnotnull(year_month), (year_month = 2020-08)]` (skips `9/10` months) and `ReadSchema` reading 4 columns via `ColumnarToRow`. |


---

# Deliverable D — Data-Processing Validation (`D_data_validation`)

| validation_item                              | method_used                                                                                                                                                                                 | evidence_result                                                                                                                                                                       | pass     |
|:---------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------|
| Schema / data types                          | All-string ingestion + ordered ANSI try_cast (R0..R6) in pipeline.py & spark_etl.py                                                                                                         | 90,938 raw pings & 84,633 segments validated; 0 uncaught cast exceptions; dirty fixture tests pass 100%.                                                                              | [x] PASS |
| Null / invalid values                        | Quality Funnel (R1 len<=0: 154; R2 non-road: 427) + Train-Only 3-Tier Vmax Imputation (85.21% missing)                                                                                      | 100% of 33,394 windows imputed (4,946 GIS + 9,376 Tier-1 P85 + 17,693 Tier-2 Median + 1,379 Tier-3 Prior).                                                                            | [x] PASS |
| Duplicate handling                           | SHA-256 raw_event_id batch dedup + 32.0 KiB Bloom Filter (k=4) + Mongo $status_version                                                                                                      | Batch isolates conflicting keys; Stream catches 333/333 injected duplicates (FPR = 0.611%).                                                                                           | [x] PASS |
| Row-count reconciliation across key steps    | Strict conservation: input_total == accepted + dedup_dropped + rejected (0 Silent Drops)                                                                                                    | 90,938 -> 90,787 accepted (151 rejected) -> 33,394 windows; Stream 33,727 = 33,394 + 333 dups; Tier-2 recon = 100.0%.                                                                 | [x] PASS |
| Join correctness / unmatched keys            | Referential integrity vs 84,206 clean segments + exceptAll parity                                                                                                                           | 0 unmatched segment_ids in Gold (33,394/33,394); two_way_exceptAll_diff = 0 across SMJ, BHJ & 8-Salt Monoid.                                                                          | [x] PASS |
| Independent sample check / known answer      | Pandas baseline.py comparison + traces of segment_id=47407 (_id=15281, Lý Thường Kiệt, 2020-08-03 07:08 ICT, v=4 km/h) & segment_id=21260 (_id=77975, Nguyễn Bỉnh Khiêm, 07:59 ICT, 4 km/h) | Monoid vs baseline: 0 discrepancies across 33,394 windows (max_abs_diff=0.0); #47407 (1,835 raw pings, 07:00 ICT window LOS C) & #21260 (4/50=0.08 -> LOS F, P(E/F)=0.7712) verified. | [x] PASS |
| Curated output can be read back reproducibly | SHA-256 Frozen Registry check + Parquet read-back & Spark ML PipelineModel reload                                                                                                           | curated_gold (33,394 rows) & static_features (84,206 rows) read back with 0 diff; spark_pipeline_reload_parity = True.                                                                | [x] PASS |


---

# Deliverable E — ML & Large-Scale Analytics Validation (`E_model_validation`)

| question                                                                                     | answer_evidence                                                                                                                                                                                                                                                                                                                     |
|:---------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| What is the task: clustering, classification, regression, frequent-pattern mining, or other? | Primary: Supervised 6-class ordinal classification (LOS A..F at exact t+30m) & binary severe warning (LOS E/F). Supporting unsupervised: Line-Graph PageRank (92,453 edges), Unified 7D K-Means (k=6 on 3,609 organic TRAIN segs), & FPGrowth (335 rules).                                                                          |
| What are the features, and which ones are available BEFORE the target/outcome occurs?        | 23D FULL_ML_FEATURES at origin t: 14 LOCAL_ML_FEATURES (<= t, ICT clock & Train Vmax) + 9 GRAPH_CLUSTER_FEATURES (PageRank, in/out degree, neighbor LOS <= t, 7D K-Means cluster_id, Train/past-only priors tau < t). Future-Shuffle yields max_abs_diff = 0.0.                                                                     |
| What is the split/validation method? Why is it appropriate?                                  | Chronological 70/15/15 split on 7,885 exact +30m pairs; excluded 3,596 synthetic burst TRAIN pairs -> 1,885 organic TRAIN / 1,167 VAL / 1,182 TEST (purged 50 train + 5 val boundary rows).                                                                                                                                         |
| What metric(s) are used? Why do they match the task?                                         | VAL gate: RF 23D wins (0.8990 vs 0.5343 14D, VAL Acc=0.9023). TEST (1,182 pairs): ROC-AUC=0.9900, PR-AUC=0.9595, Acc=0.9289, Macro-F1=0.4475, Severe F1=0.9301 (Prec=0.9251, Rec=0.9351), 6-class diag=[937, 4, 1, 7, 2, 147]. Spark RF 23D TEST: Acc=0.9399, ROC-AUC=0.9924. Transition Detect ROC-AUC=0.9146, McNemar p=0.015625. |
| What is the baseline or comparison?                                                          | 7-model ablation on TEST (1,182 pairs): Naive Persistence Baseline, Decision Tree (14D/23D), Random Forest (14D/23D), Binary GBT (14D/23D) in outputs/ablation_results.csv & outputs/transition_evaluation.csv.                                                                                                                     |
| What result would indicate the model is not useful?                                          | 0% accuracy on the 61 true state changes like Persistence (94.84% overall from 94.84% inertia, 0/61 transitions, 0.0% coverage on 74,211 Cold-Start segs).                                                                                                                                                                          |
| What is the largest leakage / bias / evaluation risk?                                        | (1) 30m temporal inertia (94.84%); (2) Rare non-severe->E/F onsets (8 TEST cases in 993 origins: Onset ROC-AUC=0.8539, 3.32x lift, VAL tau=0.20 catches 0/8); (3) Spatial GPS bias (11.87% observed segments).                                                                                                                      |

## 7-Model Ablation (`outputs/ablation_results.csv`)

| model                                                          | task_type             | feature_set                   |   accuracy_6class |   macro_f1_6class |   weighted_f1_6class |   severe_ef_precision |   severe_ef_recall |   severe_ef_f1 |   severe_roc_auc |   severe_pr_auc |
|:---------------------------------------------------------------|:----------------------|:------------------------------|------------------:|------------------:|---------------------:|----------------------:|-------------------:|---------------:|-----------------:|----------------:|
| Persistence Baseline (t -> t+30m)                              | MULTICLASS_6CLASS_LOS | Local (Lag-0)                 |            0.9484 |            0.5654 |               0.9461 |                0.9365 |             0.9568 |         0.9465 |           0.9867 |          0.9367 |
| Decision Tree (Local Only)                                     | MULTICLASS_6CLASS_LOS | Local Features (14D)          |            0.8832 |            0.3767 |               0.9058 |                0.9467 |             0.8649 |         0.904  |           0.9833 |          0.9296 |
| Decision Tree (+Graph PageRank, Causal Priors & K-Means)       | MULTICLASS_6CLASS_LOS | Local + Graph + Cluster (23D) |            0.8748 |            0.3817 |               0.9006 |                0.9459 |             0.7568 |         0.8408 |           0.9799 |          0.9263 |
| Random Forest (Local Only)                                     | MULTICLASS_6CLASS_LOS | Local Features (14D)          |            0.9239 |            0.4372 |               0.929  |                0.9429 |             0.8919 |         0.9167 |           0.9898 |          0.9557 |
| Random Forest (+Graph PageRank, Causal Priors & K-Means)       | MULTICLASS_6CLASS_LOS | Local + Graph + Cluster (23D) |            0.9289 |            0.4475 |               0.9304 |                0.9251 |             0.9351 |         0.9301 |           0.99   |          0.9595 |
| GBT Binary Alerting (Local Only)                               | BINARY_SEVERE_EF_ONLY | Local Features (14D)          |          nan      |          nan      |             nan      |                0.9362 |             0.9514 |         0.9437 |           0.9849 |          0.9534 |
| GBT Binary Alerting (+Graph PageRank, Causal Priors & K-Means) | BINARY_SEVERE_EF_ONLY | Local + Graph + Cluster (23D) |          nan      |          nan      |             nan      |                0.9259 |             0.9459 |         0.9358 |           0.9822 |          0.9454 |

## Transition Regime Evaluation (`outputs/transition_evaluation.csv`)

| regime                                                                          |   sample_count |   persistence_6class_acc |   rf_23d_6class_acc |   dt_23d_6class_acc |   persistence_severe_pr_auc |   rf_23d_severe_pr_auc |   gbt_23d_severe_pr_auc |   persistence_brier_error |   rf_23d_brier_error |
|:--------------------------------------------------------------------------------|---------------:|-------------------------:|--------------------:|--------------------:|----------------------------:|-----------------------:|------------------------:|--------------------------:|---------------------:|
| 1. Toàn tập kiểm thử TEST (94.84% giữ nguyên mức cũ, 81.13% mức A)              |           1182 |                   0.9484 |              0.9289 |              0.8748 |                      0.9367 |                 0.9595 |                  0.9454 |                    0.0169 |               0.021  |
| 2. Nhóm đổi trạng thái LOS(t) != LOS(t+30p) — Ngưỡng mặc định                   |             61 |                   0      |              0.1148 |              0.1475 |                      0.5408 |                 0.6223 |                  0.6261 |                    0.3279 |               0.2872 |
| 3. Nhóm đổi trạng thái LOS(t) != LOS(t+30p) — Ngưỡng hiệu chỉnh VAL (P >= 0.40) |             61 |                   0      |              0.1967 |              0.1475 |                      0.5408 |                 0.6223 |                  0.6261 |                    0.3279 |               0.2872 |
| 4. Cặp tín hiệu GPS ngắt quãng > 30 phút (49.21% đổi mức LOS)                   |           3605 |                   0.5079 |              0.489  |            nan      |                      0.5732 |                 0.6293 |                  0.6239 |                  nan      |             nan      |


---

# Deliverable F — NoSQL Serving & Streaming Audit (`F_serving_stream`)

## 5.1 MongoDB 8.0 Serving Store (`bda501.traffic_scores`)
- **Exact Document Written:** `33,394` JSON documents (`event_id`, `segment_id`, `street_name`, `street_level`, `date`, `period`, `current_los`, `predicted_next_los`, `severe_prob`, `pagerank_score`, `cluster_id`, `lifecycle_status`, `status_version`, `model_version`).
- **Primary Read-Back Query:** Top-K arterial early-warning lookup for `TOC` operators:
  `db.traffic_scores.find({"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}}).sort([("severe_prob", -1), ("segment_id", 1)]).limit(25)`
- **Key & Index Design:** Unique key `event_id` + ESR Compound Index `level_prob_segment_idx` `{"street_level": 1, "severe_prob": -1, "segment_id": 1}`.
- **Measured Read/Write Proof (`groupexe-knowledgetest-mongo-1` & `outputs/mongo_verify.json`):** `33,394` docs upserted across `4` partitions (`69` bulk batches), idempotent replay = `33,394`, atomic `$status_version` pipeline guard rejected `25/25` stale v1 retries. `explain('executionStats')` confirms `IXSCAN` (`in_memory_sort=False`, `nReturned=25`, `totalKeysExamined=26`, `totalDocsExamined=25`): live interleaved (5 warmup + 25 runs) `IXSCAN median = 0.870 ms` (`p95 = 1.230 ms`) vs `COLLSCAN = 9.500 ms` (`10.92x`); saved `mongo_verify.json` `warm_median = 0.389 ms`, `warm_p95 = 0.692 ms` vs `COLLSCAN = 9.161 ms` (**`23.55x` speedup**).

## 5.2 Streaming, Spark Structured Streaming & Live Kafka Monitor
- **Historical Replay Nature:** Historical HCMC GPS telemetry (`90,938` pings / `33,394` 30m windows, `seed=42`) replayed through **`Apache Kafka 4.1.0 KRaft`** (`hcmc-traffic-events`, 3 partitions).
- **Spark 3.5.7 Structured Streaming (`outputs/spark_runtime_evidence.json`):** Sink 2 watermark `2020-12-01T12:30:00.000Z`, `3` batches committed, `watermark_advanced = True`.
- **Live Kafka Monitor (`outputs/live_kafka_telemetry.json`):** `6,200` messages consumed (`443` batches), `3,064` duplicates filtered by `32.0 KiB` Bloom, `4` late >30m held for Tier-2, `3,132` `STREAM_PROVISIONAL (v1)` scored (`HLL++ = 3,038.5` vs `2,903` exact).
- **Two-Tier Reconciliation & WAL Recovery (`outputs/query_progress.json`):** `tier1_exceptAll_diff_count = 0` (`100.0%` in-WM match); `500` out-of-WM late windows + `609` downstream lag windows healed to `100.0%` (`384` rescored, Bloom `32 KiB` `0.611%` FPR, HLL++ `1 KiB` `3.75%` err, WAL `73,713 B`).


---

# Deliverable G — Cloud/Cluster Deployment, Reproducibility & Limitations (`G_deployment`)

## 6.1 Deployable Spark Architecture (`docker-compose.yml` -> Cloud Kubernetes / YARN)
| Deployment Element | Implemented Local Cluster (`[I]`) & Proposed Cloud Scale-Out (`[P]`) |
|---|---|
| **Cluster Manager** | `[I]` Docker Compose topology (`kafka`, `mongo`, `spark`, `app`, `toc-web`, `live-consumer`). `[P]` Managed Cloud Spark on Kubernetes (`EKS/GKE SparkOperator`) or `EMR YARN`. |
| **Driver Placement** | `[I]` Co-located in `groupexe-knowledgetest-spark-1` (`4 GB` heap, `4` cores). `[P]` Dedicated cluster-mode driver pod (`4 vCPU`, `8 GB RAM`) isolated from UI traffic. |
| **Executor Placement & Scaling** | `[I]` `4` executor threads (`spark.sql.shuffle.partitions = 8`). `[P]` Dynamic Allocation (`2..16` pods) scaling on Kafka lag & `8-64` salt buckets. |
| **Storage Location & Access** | `[I]` Bind-mounted Columnar Parquet (`data/lakehouse/curated_gold`, `partitionBy('year_month')`). `[P]` Object storage (`S3 / GCS / MinIO`) with `Delta Lake / Iceberg`. |
| **Fault Tolerance & Checkpoints** | `[I]` Dual-Sink WAL (`73,713 B` + Spark WAL) + MongoDB `33,394` docs with atomic `$status_version` guard + SHA-256 Lock. |
| **Observability** | `[I]` JSON audit logs (`metrics.json`, `spark_runtime_evidence.json`, `mongo_verify.json`, `query_progress.json`) surfaced in `:8501` & `:8000`. |
| **Security & Least Privilege** | `[I]` `.gitignore` blocks `.env`/`*.pem`/`*.key`; read-only SHA-256 verification blocks online model tampering. |
| **Cost / Performance Trade-Off** | Trading `+76.65 ms` 2-stage salting overhead and `33.0 KiB` sketches (`3.75%` HLL++ error) to eliminate skew (`684.95x -> 1.0197x`). |

## 6.2 Reproducibility Smoke Test (7/7 Verified `[x]`)
- `[x]` **Software versions recorded:** Python `3.13`, PySpark `3.5.7`, Kafka `4.1.0`, MongoDB `8.0.34`, scikit-learn (`Dockerfile`, `Dockerfile.spark35`, `requirements.txt`).
- `[x]` **Dependencies/configuration recorded:** `requirements.txt`, `docker-compose.yml`.
- `[x]` **Exact run commands available:** `python pipeline.py`, `python spark_batch.py`, `python starter_stream.py`, `pytest -v`.
- `[x]` **Deterministic seed set:** `RANDOM_SEED = 42` across `pipeline.py`, `producer.py`, `starter_stream.py`, `spark_engine.py`.
- `[x]` **README starts from raw/input data:** `raw_data/` CSVs (`90,938` pings, `84,633` segments) + committed `data/demo_sample/`.
- `[x]` **Key output reproduced without hidden steps:** `35/35` platform tests + `5/5` Spark container tests (`40/40` total).
- `[x]` **No real credentials/private data embedded:** Verified via `.gitignore` and `git status`.


---

# Deliverable H — Project-Specific Challenge Card B: HCMC Smart Traffic (`H_project_card`)

## 1. Responses to All 7 Technical Challenge Prompts (Data & Code Verified)
1. **Forecast as-of rule (`10:00 -> 10:30 ICT`):** All `23` features in `FULL_ML_FEATURES` (`pipeline.py`) use observations strictly `<= 10:00 ICT`. Zero `(10:00, 10:30]` observations are read (`Future-Shuffle max_abs_diff = 0.0`, `55` boundary rows purged, `3,596` synthetic burst TRAIN pairs excluded).
2. **Predictive vs. Causal Terminology Correction:** Explicitly renamed `'Causal ML'` to **`Predictive ML with Strictly Past-Only Temporal Priors (\tau < t)`** (observational forecasting, not Do-calculus).
3. **Sparse GPS Probes (`11.87%` observed vs `88.13%` Cold-Start):** `9,995 / 84,206` clean segments have GPS pings; `74,211` (`88.13%`) have `0` pings. Zero pings means no probe observed, never zero traffic.
4. **Graph Validity (`Line-Graph PageRank` vs Observed Speed/LOS):** In Mini-Test `B2`, `segment_id = 21969` (`Nguyễn Kiệm`, `primary`, `PageRank = 2.7799`, `in=3, out=1`) averages **`45.41 km/h` (`LOS A`)** across `24` windows, proving PageRank measures structural funnel vulnerability, not congestion.
5. **Spatial K-Means (`7D Behavioral & Morphological Road Archetypes`):** `7D K-Means` includes zero lat/lon coordinates and groups structurally similar roads across non-adjacent districts.
6. **Streaming / Sketch Accuracy (`Bloom 0.611% FPR`, `HLL++ 3.75% Error`):** `HLL++ (1.0 KiB)` estimates `9,620.2` vs `9,995` exact (`3.75%` err) for global KPI only; `Bloom (32.0 KiB)` catches `333/333` injected duplicates, and Tier-2 Reconciliation restores `100.0%` completeness.
7. **Batch–Speed Reconciliation & Audit Versions:** Serving documents carry `phase_b_status` (`v1`) and `lifecycle_status` (`BATCH_VERIFIED` `v2`); MongoDB `$status_version` guard rejected `25/25` stale `v1` retries.

## 2. Mini-Test B1 — Point-in-Time Feature Matrix (Excerpt)

| forecast_origin_ict   | target_time_ict      |   feature_idx | feature_name         | feature_group     | source_window_rule                             | source_timestamp_ict            | availability_timestamp_ict        | empirical_sample_value   | raw_vs_data_crosscheck                                                                  | uses_target_interval_t_to_t30m   | pass_fail   |
|:----------------------|:---------------------|--------------:|:---------------------|:------------------|:-----------------------------------------------|:--------------------------------|:----------------------------------|:-------------------------|:----------------------------------------------------------------------------------------|:---------------------------------|:------------|
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |             1 | current_los_ordinal  | Local & Lag (14D) | Window [t-30m, t] derived LOS ordinal          | 2020-07-03 10:00 ICT            | 2020-07-03 10:00 ICT              | N/A                      | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |             2 | lag1_los_ordinal     | Local & Lag (14D) | Prior observed window [t-60m, t-30m] LOS       | 2020-07-03 09:30 ICT            | 2020-07-03 09:30 ICT              | 1.0000                   | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |             3 | lag2_los_ordinal     | Local & Lag (14D) | Two-step prior window [t-90m, t-60m] LOS       | 2020-07-03 09:00 ICT            | 2020-07-03 09:00 ICT              | 1.0000                   | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |             4 | trailing_severe_rate | Local & Lag (14D) | Trailing severe ratio <= t                     | 2020-07-03 10:00 ICT            | 2020-07-03 10:00 ICT              | 0.0000                   | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |             5 | mean_velocity        | Local & Lag (14D) | Window [t-30m, t] mean GPS speed (km/h)        | 2020-07-03 10:00 ICT            | 2020-07-03 10:00 ICT              | 32.5000                  | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |             6 | velocity_ratio       | Local & Lag (14D) | mean_velocity(t) / Train-Only 3-Tier Vmax      | 2020-07-03 10:00 ICT            | 2020-07-03 10:00 ICT              | 1.3000                   | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |             7 | obs_count            | Local & Lag (14D) | GPS probe ping count in window [t-30m, t]      | 2020-07-03 10:00 ICT            | 2020-07-03 10:00 ICT              | 2.0000                   | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |             8 | length_m             | Local & Lag (14D) | Static OSM segment length (meters)             | TRAIN Split (<= 2020-12-12 UTC) | Frozen before inference (Phase A) | 16.0000                  | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |             9 | max_velocity         | Local & Lag (14D) | Train-Only 3-Tier Vmax lookup (km/h)           | TRAIN Split (<= 2020-12-12 UTC) | Frozen before inference (Phase A) | 25.0000                  | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |            10 | street_level         | Local & Lag (14D) | Static OSM road hierarchy level (1..4)         | TRAIN Split (<= 2020-12-12 UTC) | Frozen before inference (Phase A) | 4.0000                   | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |            11 | weekday              | Local & Lag (14D) | Deterministic ICT (UTC+7) day of week at t     | 2020-07-03 10:00 ICT            | 2020-07-03 10:00 ICT              | 0.0000                   | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |
| 2020-07-03 10:00 ICT  | 2020-07-03 10:30 ICT |            12 | period_sin           | Local & Lag (14D) | Deterministic ICT clock sin(2*pi*slot/48) at t | 2020-07-03 10:00 ICT            | 2020-07-03 10:00 ICT              | 0.9914                   | VERIFIED (raw_data/segment_status.csv <-> data/dataset.csv) [seg=47407, raw_pings=1835] | NO (0 target-window leakage)     | PASS        |

## 3. Mini-Test B2 — Graph & Zone Falsification Results

| test_category                                    | entity_id        | name_or_scope                                                                                    |   pagerank_or_size |   observed_windows |   mean_velocity_kmh |   max_velocity_kmh |   velocity_ratio | empirical_los_or_span                                              | raw_vs_data_crosscheck                               | revised_interpretation                                                                                                                                                                                                         |
|:-------------------------------------------------|:-----------------|:-------------------------------------------------------------------------------------------------|-------------------:|-------------------:|--------------------:|-------------------:|-----------------:|:-------------------------------------------------------------------|:-----------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| B2_PART1_HIGH_PAGERANK_UNCONGESTED_FALSIFICATION | segment_id=21969 | Nguyễn Kiệm (primary)                                                                            |             2.7799 |                 24 |               45.41 |               50   |           0.9082 | LOS A (Free-Flow, severe_prior=0.087)                              | VERIFIED (raw_data: len=10m, raw_pings=24)           | High Line-Graph PageRank reflects structural funnel topology, NOT realized congestion; wide multi-lane arterial absorbs inflow at LOS A.                                                                                       |
| B2_PART1_HIGH_PAGERANK_UNCONGESTED_FALSIFICATION | segment_id=15011 | Phan Đăng Lưu (primary)                                                                          |             2.6159 |                 17 |               33    |               49   |           0.6735 | LOS A (Free-Flow, severe_prior=0.057)                              | VERIFIED (raw_data: len=4m, raw_pings=24)            | High Line-Graph PageRank reflects structural funnel topology, NOT realized congestion; wide multi-lane arterial absorbs inflow at LOS A.                                                                                       |
| B2_PART1_HIGH_PAGERANK_UNCONGESTED_FALSIFICATION | segment_id=4438  | Phan Đình Giót (primary)                                                                         |             2.1957 |                 11 |               33    |               33.8 |           0.9778 | LOS A (Free-Flow, severe_prior=0.205)                              | VERIFIED (raw_data: len=48m, raw_pings=14)           | High Line-Graph PageRank reflects structural funnel topology, NOT realized congestion; wide multi-lane arterial absorbs inflow at LOS A.                                                                                       |
| B2_PART1_HIGH_PAGERANK_CONGESTED_COMPARISON      | segment_id=63449 | Lý Thường Kiệt (primary)                                                                         |             1.5809 |                 30 |               15.14 |               50   |           0.3029 | LOS E/F (Bottleneck, severe_prior=0.581)                           | VERIFIED (raw_data: len=78m, raw_pings=78)           | When high topological PageRank coincides with constrained capacity & high empirical severe prior, genuine recurring bottlenecks emerge.                                                                                        |
| B2_PART1_HIGH_PAGERANK_CONGESTED_COMPARISON      | segment_id=34019 | Trần Quốc Thảo (secondary)                                                                       |             1.5156 |                 11 |                7.25 |               80.5 |           0.1    | LOS E/F (Bottleneck, severe_prior=0.745)                           | VERIFIED (raw_data: len=42m, raw_pings=13)           | When high topological PageRank coincides with constrained capacity & high empirical severe prior, genuine recurring bottlenecks emerge.                                                                                        |
| B2_PART1_HIGH_PAGERANK_CONGESTED_COMPARISON      | segment_id=63450 | Lý Thường Kiệt (primary)                                                                         |             1.4962 |                 23 |                8.54 |               50   |           0.1707 | LOS E/F (Bottleneck, severe_prior=0.673)                           | VERIFIED (raw_data: len=45m, raw_pings=83)           | When high topological PageRank coincides with constrained capacity & high empirical severe prior, genuine recurring bottlenecks emerge.                                                                                        |
| B2_PART2_KMEANS_DISCONNECTED_ROADS_AUDIT         | cluster_id=0     | Quốc lộ 1 (1149), Nguyễn Văn Linh (440), Phạm Văn Đồng (286), Xa Lộ Hà Nội (191)                 |          3701      |                540 |               56.28 |               69.4 |           0.8101 | Cold-Start=3161/3701 segs across multiple non-adjacent districts   | VERIFIED (raw_data/segments.csv <-> static_features) | 7D K-Means uses [mean_los_ordinal, severe_rate, mean_velocity, velocity_ratio, length_m, max_velocity, pagerank_score] without lat/lon; clusters are Behavioral & Morphological Road Archetypes, NOT contiguous spatial zones. |
| B2_PART2_KMEANS_DISCONNECTED_ROADS_AUDIT         | cluster_id=1     | Xã Vĩnh Lộc B (1343), Huyện Bình Chánh (1200), Xã Xuân Thới Sơn (734), Xã Xuân Thới Thượng (696) |         37215      |               2713 |               33.73 |               41.3 |           0.8205 | Cold-Start=34502/37215 segs across multiple non-adjacent districts | VERIFIED (raw_data/segments.csv <-> static_features) | 7D K-Means uses [mean_los_ordinal, severe_rate, mean_velocity, velocity_ratio, length_m, max_velocity, pagerank_score] without lat/lon; clusters are Behavioral & Morphological Road Archetypes, NOT contiguous spatial zones. |
| B2_PART2_KMEANS_DISCONNECTED_ROADS_AUDIT         | cluster_id=2     | Võ Văn Kiệt (371), Âu Cơ (200), Nguyễn Xiển (186), Mã Lò (163)                                   |          5372      |               1483 |               24.87 |               47.8 |           0.5229 | Cold-Start=3889/5372 segs across multiple non-adjacent districts   | VERIFIED (raw_data/segments.csv <-> static_features) | 7D K-Means uses [mean_los_ordinal, severe_rate, mean_velocity, velocity_ratio, length_m, max_velocity, pagerank_score] without lat/lon; clusters are Behavioral & Morphological Road Archetypes, NOT contiguous spatial zones. |
| B2_PART2_KMEANS_DISCONNECTED_ROADS_AUDIT         | cluster_id=3     | Nguyễn Văn Linh (110), Quốc lộ 1 (78), Võ Văn Kiệt (69), Xã Tân Nhựt (57)                        |          2890      |                305 |               40.93 |               49.4 |           0.8296 | Cold-Start=2585/2890 segs across multiple non-adjacent districts   | VERIFIED (raw_data/segments.csv <-> static_features) | 7D K-Means uses [mean_los_ordinal, severe_rate, mean_velocity, velocity_ratio, length_m, max_velocity, pagerank_score] without lat/lon; clusters are Behavioral & Morphological Road Archetypes, NOT contiguous spatial zones. |
| B2_PART2_KMEANS_DISCONNECTED_ROADS_AUDIT         | cluster_id=4     | Võ Văn Kiệt (455), Võ Văn Vân (436), Quốc lộ 1 (434), Phạm Thế Hiển (414)                        |         32376      |               4342 |               43.13 |               50.1 |           0.8591 | Cold-Start=28034/32376 segs across multiple non-adjacent districts | VERIFIED (raw_data/segments.csv <-> static_features) | 7D K-Means uses [mean_los_ordinal, severe_rate, mean_velocity, velocity_ratio, length_m, max_velocity, pagerank_score] without lat/lon; clusters are Behavioral & Morphological Road Archetypes, NOT contiguous spatial zones. |
| B2_PART2_KMEANS_DISCONNECTED_ROADS_AUDIT         | cluster_id=5     | Quốc lộ 1 (233), Võ Văn Kiệt (105), Xa Lộ Hà Nội (80), Điện Biên Phủ (66)                        |          2652      |                612 |               38.54 |               49.7 |           0.7783 | Cold-Start=2040/2652 segs across multiple non-adjacent districts   | VERIFIED (raw_data/segments.csv <-> static_features) | 7D K-Means uses [mean_los_ordinal, severe_rate, mean_velocity, velocity_ratio, length_m, max_velocity, pagerank_score] without lat/lon; clusters are Behavioral & Morphological Road Archetypes, NOT contiguous spatial zones. |

## 4. Mini-Test B3 — Missingness & Sketch Accuracy vs Exact SQL

| test_slice                                             | metric_type                                      |   exact_spark_sql_value |   sketch_estimated_value |   relative_error_or_fpr_pct | memory_footprint                           | raw_vs_data_crosscheck                                            | operational_alert_impact                                                                                                                            |
|:-------------------------------------------------------|:-------------------------------------------------|------------------------:|-------------------------:|----------------------------:|:-------------------------------------------|:------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------|
| HLL++ Cardinality Sample (N=100)                       | COUNT(DISTINCT segment_id)                       |                     100 |                   103.01 |                       3.012 | 1.0 KiB (1,024 uint8 registers)            | VERIFIED (raw_data distinct=10,027/84,633 <-> clean=9,995/84,206) | 0 false road alerts: HLL++ only reports city-wide active-segment coverage KPI on UI; per-segment alerts use exact segment_id + RF .predict_proba(). |
| HLL++ Cardinality Sample (N=500)                       | COUNT(DISTINCT segment_id)                       |                     500 |                   507.21 |                       1.442 | 1.0 KiB (1,024 uint8 registers)            | VERIFIED (raw_data distinct=10,027/84,633 <-> clean=9,995/84,206) | 0 false road alerts: HLL++ only reports city-wide active-segment coverage KPI on UI; per-segment alerts use exact segment_id + RF .predict_proba(). |
| HLL++ Cardinality Sample (N=1000)                      | COUNT(DISTINCT segment_id)                       |                    1000 |                  1004.37 |                       0.437 | 1.0 KiB (1,024 uint8 registers)            | VERIFIED (raw_data distinct=10,027/84,633 <-> clean=9,995/84,206) | 0 false road alerts: HLL++ only reports city-wide active-segment coverage KPI on UI; per-segment alerts use exact segment_id + RF .predict_proba(). |
| HLL++ Cardinality Sample (N=2500)                      | COUNT(DISTINCT segment_id)                       |                    2500 |                  2572.93 |                       2.917 | 1.0 KiB (1,024 uint8 registers)            | VERIFIED (raw_data distinct=10,027/84,633 <-> clean=9,995/84,206) | 0 false road alerts: HLL++ only reports city-wide active-segment coverage KPI on UI; per-segment alerts use exact segment_id + RF .predict_proba(). |
| HLL++ Cardinality Sample (N=5000)                      | COUNT(DISTINCT segment_id)                       |                    5000 |                  4907.68 |                       1.846 | 1.0 KiB (1,024 uint8 registers)            | VERIFIED (raw_data distinct=10,027/84,633 <-> clean=9,995/84,206) | 0 false road alerts: HLL++ only reports city-wide active-segment coverage KPI on UI; per-segment alerts use exact segment_id + RF .predict_proba(). |
| HLL++ Cardinality Sample (N=9995)                      | COUNT(DISTINCT segment_id)                       |                    9995 |                  9620.23 |                       3.75  | 1.0 KiB (1,024 uint8 registers)            | VERIFIED (raw_data distinct=10,027/84,633 <-> clean=9,995/84,206) | 0 false road alerts: HLL++ only reports city-wide active-segment coverage KPI on UI; per-segment alerts use exact segment_id + RF .predict_proba(). |
| Live Kafka->Mongo Stream (6,200 msgs)                  | Live Stream Active Segments & Bloom Dedup        |                    2903 |                  3038.5  |                       4.668 | 33.0 KiB (32K Bloom + 1K HLL++)            | VERIFIED (LIVE Kafka 4.1 -> MongoDB 8.0)                          | 3,132 v1 docs scored; 3,064 duplicates caught by Bloom; 4 late held for Tier-2.                                                                     |
| Bloom Filter Stream Dedup (33,727 events)              | Duplicate Event Detection (333 injected dups)    |                     333 |                   333    |                       0.611 | 32.0 KiB (262,144 bit-packed array, k=4)   | VERIFIED (raw_data distinct=10,027/84,633 <-> clean=9,995/84,206) | 100% (333/333) true duplicates caught; 0.611% FPR mitigated by nightly Tier-2 Batch Reconciliation (100.0% completeness).                           |
| Network Spatial Coverage Audit (84,206 clean segments) | Observed vs Zero-Observation Cold-Start Segments |                   84206 |                  9995    |                      88.13  | Static Feature Store (84,206 rows Parquet) | VERIFIED (raw_data distinct=10,027/84,633 <-> clean=9,995/84,206) | 88.13% (74,211 segments) have 0 GPS pings; badged [COLD-START ARCHETYPE PRIOR] on UI.                                                               |

## 5. One-Minute Defense Answer
> **Question:** *What does the system know at the exact forecast time on road segments with no GPS?*
> **Evidence-Based Answer (60s):** At forecast origin $t$ on any of the `74,211` (`88.13%`) zero-GPS segments, the system has **zero live vehicle velocity pings** and never fabricates synthetic readings. It uses three pre-computed, leak-free signals from `segment_static_features.parquet`: (1) **Road geometry & hierarchy** (`length_m`, `street_level`, `street_type`, Train Tier-2/3 `max_velocity`), (2) **Topological centrality** (`Line-Graph PageRank`, `in/out degree` on `52,181` active intersections), and (3) **Behavioral cluster & past-only street priors** (`cluster_id` + `street_causal_prior_sev` fitted on `TRAIN` $\tau < t$) plus ICT harmonics — badged **`[COLD-START STRUCTURAL PRIOR]`** on the UI.


---

# Deliverable I — Session 9 Readiness Self-Assessment & Session 10 Handoff (`I_readiness`)

## 1. Session 10 Readiness Self-Assessment Rubric (`95 / 100`)
| Criterion | Weight | Self-Score | Evidence & Action Completed in Session 9 |
|---|---:|---:|---|
| **Accuracy & Correctness** | 25 | **24 / 25** | `35/35` platform tests + `5/5` Spark tests (`40/40` total); `exceptAll = 0`; `Future-Shuffle = 0.0`; Card B (`B1, B2, B3`) cross-checked across `raw_data/` <-> `data/`. |
| **Creativity & Problem-Solving** | 15 | **14 / 15** | `2-Stage 8-Bucket Salted 5-Tuple Monoid` (`684.95x -> 1.0197x`), `Train-Only 3-Tier Vmax`, `33.0 KiB` bounded sketches, `Two-Tier Reconciliation`. |
| **Completeness of Product** | 25 | **24 / 25** | End-to-end `Zones A–F` + live Kafka -> Consumer -> MongoDB -> Streamlit `:8501` & React+FastAPI `:8000` + `BDA501_Session9_Lab_Manual*.docx` + `evidence/session9/` pack. |
| **Presentation & Expression** | 10 | **9 / 10** | `[I]` vs `[P]` badges added; `'Predictive ML (Past-Only Priors tau < t)'` & `'7D Behavioral & Morphological Road Archetypes'`; separated Saved Historical Run vs live `2026-11-18` Docker reruns. |
| **Application of Theory** | 20 | **19 / 20** | Connects commutative monoids, Spark Catalyst `BroadcastHashJoin` (`5 -> 1` Exchanges), Parquet pruning, MongoDB `ESR IXSCAN` (`0.692 ms` p95), and `30m Watermark`. |
| **Timeliness & Requirements** | 5 | **5 / 5** | All Session 9 & Session 10 deliverables synchronized. |
| **Total Self-Assessed Readiness** | **100** | **95 / 100** | Ready for Session 10 Final Defense (40%). |

## 2. Explicit Evidence Provenance: Saved Historical Run (`Wave 2`) vs Live Container Reruns (`2026-11-18`)
| Evidence Scope | Provenance Classification | Artifact Source | Key Verified Numbers |
|---|---|---|---|
| **Full Batch, 7-Model Ablation & Wave 2 Spark Verification** | **Saved Historical Run (`Wave 2`)** | `outputs/metrics.json`, `outputs/spark_runtime_evidence.json`, `outputs/ablation_results.csv` | `90,938` -> `90,787` pings -> `33,394` windows; Catalyst Join `145.24 -> 120.45 ms` (`17.07%`); RF 23D TEST `ROC-AUC=0.9900`, `PR-AUC=0.9595`, `Acc=0.9289`. |
| **Live 3-Way Spark 3.5.7 Controlled Benchmark** | **Fresh Live Docker Execution (`2026-11-18`)** | `evidence/session9/B_live_spark_benchmark.json` (`groupexe-knowledgetest-spark-1`) | `2w + 5r` interleaved: `Baseline 179.73 ms` (`5 Exch`) -> `V1 142.36 ms` (`1 Exch`, `-20.79%`) -> `V2 219.01 ms` (`2 Exch`, `exceptAll=0`). |
| **MongoDB 8.0 Operational Serving Store (`33,394` docs)** | **Saved Historical Run + Live `2026-11-18` Interleaved Benchmark** | `outputs/mongo_verify.json` + `groupexe-knowledgetest-mongo-1` | `33,394` docs, `25/25` stale v1 rejected; live IXSCAN median `0.870 ms` vs COLLSCAN `9.500 ms` (`10.92x`); saved p95 `0.692 ms` (`23.55x`). |
| **Card B (`B1, B2, B3`) & Live Kafka Monitor** | **Fresh Re-Execution (`2026-11-18`)** | `evidence/session9/H_B*.csv`, `outputs/live_kafka_telemetry.json` | `#21969 Nguyễn Kiệm` `PR=2.7799` (`45.41 km/h LOS A`); Cold-Start `74,211/84,206` (`88.13%`); `6,200` Kafka msgs consumed. |
| **Automated Test & Lint Gate (`pytest` + `Spark` + `Ruff`)** | **Fresh Re-Execution (`2026-11-18`)** | `tests/test_platform_v3.py` (`35/35`) + `tests/test_quyen_spark.py` (`5/5`) | **`40/40` tests**, `0` Ruff errors (`max-complexity <= 14`). |

## 3. Five Required Lines on the Readiness Summary Slide (PPTX)
1. **Architecture truth:** Separated `[I] Implemented` from `[P] Proposed` and standardized 'Predictive ML (Past-Only Priors tau < t)' & '7D Behavioral & Morphological Road Archetypes'.
2. **Performance:** Spark 3.5.7 (`groupexe-knowledgetest-spark-1`) BHJ cuts Exchange `5 -> 1` (`179.73 -> 142.36 ms`, `-20.79%`); 8-Bucket Salting collapses slot skew `684.95x -> 1.0197x` (`exceptAll=0`).
3. **Validation:** `0` Silent Drops across `90,938` pings; `Future-Shuffle diff = 0.0`; RF 23D TEST `ROC-AUC=0.9900`, `PR-AUC=0.9595`, Transition Detect `ROC-AUC=0.9146` (`McNemar p=0.015625`).
4. **Operations:** MongoDB 8.0 IXSCAN (`0.870 ms` live / `0.692 ms` saved p95), `30m Watermark` (`2020-12-01T12:30:00.000Z`), `33 KiB` Sketches (Bloom `0.611%` FPR, HLL++ `3.75%` err), WAL (`73,713 B`), Tier-2 heals `609` lag windows to `100%`.
5. **Limitation:** `88.13%` (`74,211` segments) Cold-Start 0 GPS & rare sudden onsets (`8` TEST cases, VAL `tau=0.20` catches `0/8`).


---

