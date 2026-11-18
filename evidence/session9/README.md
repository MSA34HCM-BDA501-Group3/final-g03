# Session 9 Technical Audit & Readiness Evidence Pack (`evidence/session9/`)

This directory contains the complete **`A_architecture` through `I_readiness`** audit deliverables for **Card B — HCMC Smart Traffic Platform** (`BDA501 — Big Data Analytics`), generated reproducibly via `python scripts/run_session9_audit.py`.

## Evidence Provenance Policy (Saved Historical Outputs vs Fresh Re-Executions)
- **Saved Historical Benchmark Outputs (`2026-11-09..11`):** Full `38.50 MB` (`90,938` pings -> `33,441` windows) Spark 3.5.7 controlled join/salting benchmark (`B_benchmark.*`, `C_query_plan.*`), 7-model ML ablation (`E_model_validation.*`), and `33,441`-document MongoDB 8.0 `IXSCAN` latency benchmark (`outputs/mongo_verify.json`). Reload parity (`verify_frozen_registry_integrity` & scoring on `TEST`) is re-verified automatically on every `pytest` run.
- **Fresh Re-Executed Verifications (`2026-11-18`):**
  1. **Live End-to-End Kafka -> Consumer -> MongoDB 8.0 -> Streamlit `:8501` Run:** `200` messages published via `producer.py` -> consumed by `starter_stream.py --from-kafka` (`live_kafka_stream.py`) -> `4` Bloom duplicates filtered, `4` `>30m` late events held for Tier-2, `192` `STREAM_PROVISIONAL (v1)` predictions upserted into `MongoDB 8.0` (`outputs/live_kafka_telemetry.json`, recorded in `F_serving_stream.json` & `H_B3_missingness_sketches.csv`).
  2. **Card B (`B1, B2, B3`) `raw_data/` <-> `data/` Cross-Verification:** Direct cross-check between `raw_data/{segments,segment_status}.csv` and `data/{dataset.csv,lakehouse/segment_static_features.parquet}`, with automatic fallback to committed `data/demo_sample/` (`238 KB`) on clean Git checkouts.
  3. **Automated Quality Gate (`2026-11-18`):** `22/22` tests in `tests/test_platform_v3.py` + `5/5` live Spark 3.5.7 container tests in `tests/test_quyen_spark.py` (**`27/27` passed, `0` Ruff errors**).

## File Manifest
- `A_architecture.png`, `A_architecture.md` — End-to-end architecture truth diagram with `[I] Implemented` vs `[P] Proposed` badges.
- `B_benchmark.csv`, `B_benchmark.png`, `B_benchmark.md` — Spark 3.5.7 controlled experiment (`SortMergeJoin` vs `BroadcastHashJoin` vs `8-Bucket Salted 5-Tuple Monoid`).
- `C_query_plan.txt`, `C_query_plan.md` — Spark 3.5.7 physical plans (`EXPLAIN FORMATTED`) & Parquet partition/schema pruning proof.
- `D_data_validation.csv`, `D_data_validation.md` — 7-check data conservation & `raw_data <-> data` trace (`#47407` *Lý Thường Kiệt* & `#21260` *Nguyễn Bỉnh Khiêm*).
- `E_model_validation.csv`, `E_model_validation.md` — 7-model ablation, `Future-Shuffle` leakage check, and 61-case Transition Regime evaluation.
- `F_serving_stream.json`, `F_serving_stream.md` — MongoDB 8.0 `ESR IXSCAN` (`0.636 ms` p95) + `2026-11-18` Live 200-event Kafka -> MongoDB stream audit.
- `G_deployment.md` — Docker Compose (`10.5 / 16 GB RAM`) cluster configuration, Cloud K8s scale-out design, and 7/7 reproducibility smoke test.
- `H_project_card.csv`, `H_project_card.png`, `H_project_card.md`, `H_B1_point_in_time_matrix.csv`, `H_B2_graph_zone_falsification.csv`, `H_B3_missingness_sketches.csv` — Card B 7 challenge prompts, executable Mini-Tests `B1/B2/B3` with `raw_data <-> data` cross-check, and 60s defense.
- `I_readiness.md`, `I_session9_summary_slide.png` — `95/100` readiness rubric, historical-vs-rerun provenance table, and Session 9 Summary Slide.
