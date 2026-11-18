# Deliverable I — Session 9 Readiness Self-Assessment & Session 10 Handoff (`I_readiness`)

## 1. Session 10 Readiness Self-Assessment Rubric (`95 / 100`)
| Criterion | Weight | Self-Score | Evidence & Action Completed in Session 9 |
|---|---:|---:|---|
| **Accuracy & Correctness** | 25 | **24 / 25** | `19/19` platform tests + `4/4` Spark tests pass; `exceptAll diff = 0`; `Future-Shuffle diff = 0.0`; Card B (`B1, B2, B3`) verified on real data. |
| **Creativity & Problem-Solving** | 15 | **14 / 15** | `2-Stage 8-Bucket Salted 5-Tuple Monoid` (`687.43x -> 1.023x` skew), `Train-Only 3-Tier Vmax`, `33.0 KiB` bounded stream sketches, `Two-Tier Reconciliation`. |
| **Completeness of Product** | 25 | **24 / 25** | End-to-end `Zones A–F` integrated (`Kafka 4.1 -> Spark 3.5.7 -> Parquet Lakehouse + MongoDB 8.0 -> 5-Tab Streamlit :8501`) + full `evidence/session9/` pack. |
| **Presentation & Expression** | 10 | **9 / 10** | `[I]` vs `[P]` badges added; renamed `'Causal ML'` -> `'Predictive ML (Past-Only Priors tau < t)'` & `'Spatial Zones'` -> `'7D Behavioral Clusters'`. |
| **Application of Theory** | 20 | **19 / 20** | Connects commutative monoids, Spark Catalyst `BroadcastHashJoin` (`10 -> 2 Exchanges`), Parquet pruning, MongoDB `ESR IXSCAN` (`0.636 ms`), and `30m Watermark`. |
| **Timeliness & Requirements** | 5 | **5 / 5** | All Session 9 & Session 10 deliverables, scripts, PDFs, PPTX decks, and `evidence/session9/` completed and synchronized. |
| **Total Self-Assessed Readiness** | **100** | **95 / 100** | Ready for Session 10 Final Assessment (40%). |

## 2. Top Three Corrections Completed During Session 9 Audit
1. **Priority 1 (Architecture Truth — `[I]` vs `[P]` Separation):** Added explicit `[I] Implemented` vs `[P] Proposed/Design-only` badges (`evidence/session9/A_architecture.png`) to distinguish our measured 4-container Docker implementation (`38.50 MB` dataset, `90,938` pings) from proposed city-wide live telco ingestion (`8–17B` pings/day) and `SCATS` hardware actuation. *(Owner: Nguyễn Tạ Minh Quyền — `[x] Done`)*
2. **Priority 2 (Card B Terminology & Graph/Zone Falsification — `B1 & B2`):** Clarified `'Causal ML'` as **`Predictive ML with Strictly Past-Only Priors (\tau < t)`** (avoiding unverified interventional claims) and proved via Mini-Test `B2` that `Line-Graph PageRank` (`#21969 Nguyễn Kiệm`, `45.41 km/h LOS A`) measures *topological funneling vulnerability* while `7D K-Means` produces *non-contiguous Behavioral & Morphological Road Archetypes*. *(Owner: Nguyễn Thành Đạt — `[x] Done`)*
3. **Priority 3 (Card B Sketch Error Impact & Rare Onset Transparency — `B3`):** Quantified `HLL++` error (`3.98%`, `9,628 vs 10,027`) and `Bloom` FPR (`0.616%`), proving `0` impact on per-segment road alerts, and reported rare `E->F` onset trade-offs (`2/61` at $\tau=0.30$ vs `9/61` at $p \ge 0.15$). *(Owner: Phạm Công Minh — `[x] Done`)*

## 3. Five Required Lines on the "Session 9 Technical Readiness Summary" Slide (`HCMC_Smart_Traffic_Session9_Readiness_Slide.pptx`)
1. **Architecture truth:** Clearly separated `[I] Implemented` (`Kafka -> Spark Batch/Speed -> Parquet/MongoDB -> Streamlit :8501`) from `[P] Proposed` (city-wide `8–17B` live probes, cloud K8s, `SCATS` actuation) and refined `'Predictive ML (Past-Only Priors)'` & `'7D Behavioral Clusters'`.
2. **Performance:** Controlled Spark `3.5.7` experiment: `BroadcastHashJoin` cuts `Exchange` stages `10 -> 2` (`-25.7%` runtime: `1,418.6 -> 1,054.2 ms`); `8-Bucket Salted Monoid` collapses skew `687.43x -> 1.023x` (`+19.2 ms` overhead, `0.0` error).
3. **Validation:** Data conservation `90,938` pings (`0 Silent Drops`, `exceptAll = 0`); ML `Future-Shuffle diff = 0.0` (`B1`), `RF 23D` beats `Persistence` on state transitions (`50.51% vs 0.00%` new-state accuracy, `ROC-AUC = 0.7829`).
4. **Operations:** Deterministic event-time Kafka replay (`30m Watermark` + `Dual-Sink`), `WAL` crash recovery (`0` duplicates), `Tier-2 Reconciliation` (`100%` completeness), `MongoDB 8.0 ESR IXSCAN` (`0.636 ms`, `9.35x` speedup).
5. **Limitation:** `88.15%` (`74,606` segments) have `0` GPS pings (relying on `7D Behavioral Cluster` priors, badged `[COLD-START]` on UI) and rare `E->F` sudden onset sample size (`11` cases in `TEST`).
