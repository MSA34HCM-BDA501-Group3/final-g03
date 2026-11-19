# Deliverable I — Session 9 Readiness Self-Assessment & Session 10 Handoff (`I_readiness`)

## 1. Session 10 Readiness Self-Assessment Rubric (`95 / 100`)
| Criterion | Weight | Self-Score | Evidence & Action Completed in Session 9 |
|---|---:|---:|---|
| **Accuracy & Correctness** | 25 | **24 / 25** | `39/39` platform tests + `5/5` Spark tests (`44/44` total); `exceptAll = 0`; `Future-Shuffle = 0.0`; Card B (`B1, B2, B3`) cross-checked across `raw_data/` <-> `data/`. |
| **Creativity & Problem-Solving** | 15 | **14 / 15** | `2-Stage 8-Bucket Salted 5-Tuple Monoid` (`684.95x -> 1.0197x`), `Train-Only 3-Tier Vmax`, `33.0 KiB` bounded sketches, `Two-Tier Reconciliation`. |
| **Completeness of Product** | 25 | **24 / 25** | End-to-end `Zones A–F` + 5-service Docker Compose (`11.0 GB RAM`) + live Kafka -> Consumer -> MongoDB -> Unified **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)** + `BDA501_Session9_Lab_Manual*.docx` + `evidence/session9/` pack. |
| **Presentation & Expression** | 10 | **9 / 10** | `[I]` vs `[P]` badges added; `'Predictive ML (Past-Only Priors tau < t)'` & `'7D Behavioral & Morphological Road Archetypes'`; separated Saved Canonical Run vs live `2026-11-18` Docker reruns. |
| **Application of Theory** | 20 | **19 / 20** | Connects commutative monoids, Spark Catalyst `BroadcastHashJoin` (`5 -> 1` Exchanges), Parquet pruning, MongoDB `ESR IXSCAN` (`0.360 ms` median / `0.603 ms` p95), and `30m Watermark`. |
| **Timeliness & Requirements** | 5 | **5 / 5** | All Session 9 & Session 10 deliverables synchronized. |
| **Total Self-Assessed Readiness** | **100** | **95 / 100** | Ready for Session 10 Final Defense (40%). |

## 2. Explicit Evidence Provenance: Canonical Leak-Free Run vs Live Container Reruns (`2026-11-18`)
| Evidence Scope | Provenance Classification | Artifact Source | Key Verified Numbers |
|---|---|---|---|
| **Full Batch, 7-Model Ablation & Spark MLlib Verification** | **Canonical Leak-Free Run** | `outputs/metrics.json`, `outputs/spark_runtime_evidence.json`, `outputs/model_registry_manifest.json` | `90,938` -> `90,787` pings -> `33,394` windows; Catalyst Join `166.18 -> 123.37 ms` (`25.76%`); RF 23D TEST `ROC-AUC=0.9894`, `PR-AUC=0.9543`, `Acc=0.9255`; Spark MLlib `FPGrowth=32`, `KMeans=3,609`, `RF=1,885` (`test_acc=0.9416`, `roc_auc=0.9903`); SHA-256 `model=a2f411846508`, `static_store=75328ac2d5e3`, `vmax_lookup=ee7f692daad8`. |
| **Live 3-Way Spark 3.5.7 Controlled Benchmark** | **Fresh Live Docker Execution (`2026-11-18`)** | `evidence/session9/B_live_spark_benchmark.json` (`groupexe-knowledgetest-spark-1`) | `2w + 5r` interleaved: `Baseline 213.66 ms` (`5 Exch`) -> `V1 187.07 ms` (`1 Exch`, `-12.45%`) -> `V2 274.99 ms` (`2 Exch`, `exceptAll=0`). |
| **MongoDB 8.0 Operational Serving Store (`33,394` docs)** | **Canonical + Live `2026-11-18` Interleaved Benchmark** | `outputs/mongo_verify.json` + `groupexe-knowledgetest-mongo-1` | `33,394` docs (`1,234` arterial severe alerts), `25/25` stale v1 rejected; live IXSCAN median `0.940 ms` vs COLLSCAN `8.200 ms` (`8.72x`); saved median `0.360 ms` / p95 `0.603 ms` vs `11.289 ms` (`31.36x`). |
| **Card B (`B1, B2, B3`) & Live Kafka Monitor** | **Fresh Re-Execution (`2026-11-18`)** | `evidence/session9/H_B*.csv`, `outputs/live_kafka_telemetry.json` | `#21969 Nguyễn Kiệm` `PR=2.7799` (`45.41 km/h LOS A`); Cold-Start `74,211/84,206` (`88.13%`); `6,000` Kafka msgs consumed. |
| **Automated Test & Lint Gate (`pytest` + `Spark` + `Ruff`)** | **Fresh Re-Execution (`2026-11-18`)** | `tests/test_platform_v3.py` (`39/39`) + `tests/test_quyen_spark.py` (`5/5`) | **`44/44` tests**, `0` Ruff errors (`max-complexity <= 14`). |

## 3. Five Required Lines on the Readiness Summary Slide (PPTX)
1. **Architecture truth:** Separated `[I] Implemented` (5 Docker services = `11.0 GB RAM`, unified ReactJS + FastAPI TOC `http://localhost:8000`) from `[P] Proposed`, and standardized 'Predictive ML (Past-Only Priors tau < t)' & '7D Behavioral & Morphological Road Archetypes'.
2. **Performance:** Spark 3.5.7 (`groupexe-knowledgetest-spark-1`) BHJ cuts Exchange `5 -> 1` (`213.66 -> 187.07 ms`, `-12.45%`); 8-Bucket Salting collapses slot skew `684.95x -> 1.0197x` (`exceptAll=0`).
3. **Validation:** `0` Silent Drops across `90,938` pings; `Future-Shuffle diff = 0.0`; RF 23D TEST `ROC-AUC=0.9894`, `PR-AUC=0.9543`, Transition Detect `ROC-AUC=0.8822` (`McNemar p=0.001953`).
4. **Operations:** MongoDB 8.0 IXSCAN (`0.940 ms` live / `0.360 ms` saved median, `0.603 ms` p95), `30m Watermark` (`2020-12-01T12:30:00.000Z`), `33.0 KiB` Sketches (RotatingBloom `0.0389%` FPR / `13` FPs, HLL++ `3.75%` err), WAL (`73,714 B`), Tier-2 heals `609` lag windows (`98.5% -> 100.0%`).
5. **Limitation:** `88.13%` (`74,211` segments) Cold-Start 0 GPS & rare sudden onsets (`8` TEST cases, VAL `tau=0.25` catches `0/8`).
