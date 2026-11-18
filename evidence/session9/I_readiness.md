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
