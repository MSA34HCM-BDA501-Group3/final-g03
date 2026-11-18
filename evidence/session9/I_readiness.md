# I · Session 9 / Session 10 readiness audit

**Audit date:** 2026-11-18
**Overall status: PARTIAL — do not treat the attached manual's 95/100 self-score or 16/16 completeness as independently reverified from this checkout.**

The codebase and stored outputs show substantial work, but the assessment standard requires evidence ready in the submission package and reproducible commands. This checkout lacks `raw_data/` and `data/`; `evidence/session9/` and the Card B B1/B2/B3 CSVs were also absent before this audit. Python, pytest, Ruff, and Docker are unavailable in the current environment, so no build, test suite, or fresh benchmark could be run. The data/evidence limitation is blocking a verified release-readiness claim.

## Final Technical Completeness Check (16 items)

| # | Assessment item | Current status | Evidence / action |
|---:|---|---|---|
| 1 | Problem and Big Data justification | Available | Report, proposal, and dataset inventory exist. |
| 2 | End-to-end architecture | Available with caveat | Implemented/proposed boundary is drawn in `outputs/s78_production_readiness_architecture.png`; [A](A_architecture.md) links it. |
| 3 | Project-related MapReduce | Available as stored evidence | Monoid baseline reports zero component/key differences; see `outputs/monoid_baseline_verification.json`. |
| 4 | PySpark DataFrames / Spark SQL | Available as saved evidence | Spark ETL code and runtime/physical-plan outputs exist. |
| 5 | Curated Parquet / columnar output | Blocked for rerun | `data/lakehouse/` is absent; restore inputs and run the pipeline. |
| 6 | At least two analytical queries | Available as saved plans | Join and Parquet pruning plans exist under `outputs/quyen_validation/`; see [C](C_query_plan.md). |
| 7 | Performance/scalability experiment | Partial | Machine-readable monoid/join evidence exists; the original benchmark package/chart and raw input are absent. See [B](B_benchmark.md). |
| 8 | Spark ML / large-scale analytics | Available as saved outputs, not revalidated | Model, metrics, ablation, and transition CSVs exist; the dataset is missing. See [E](E_model_validation.md). |
| 9 | NoSQL serving | Available as saved output | `outputs/mongo_verify.json` records write/read-back and IXSCAN. No live MongoDB was checked during this audit. |
| 10 | Streaming consideration | Available as saved replay evidence | Kafka delivery and WAL reports exist; this is historical replay, not a live city-wide feed. |
| 11 | Cloud/cluster deployment design | Partial | Local Compose PoC exists; cloud scale-out is design-only. Compose is unauthenticated and must remain local-only. |
| 12 | Validation | Blocked for current rerun | Test code exists, but no Python/pytest runtime and no source dataset are available here. |
| 13 | Reproducibility | Improved, still blocked | Added [data setup instructions](../../docs/DATA_SETUP.md) and corrected Compose Spark build/network configuration; data download and full test/build still need to be done. |
| 14 | Final report, README, code package | Available with caveat | Main report and final slides exist. README links to nonexistent proposal PDFs were corrected. |
| 15 | Team contribution statement | Available | Contributions are listed in the README and project report. |
| 16 | Demo evidence | Partial | [Static UI demo](../../ui-demo.html) opens without dependencies; operational Streamlit UI needs generated `data/dataset.csv`. |

## Red-flag review

- **Technology truthfulness:** architecture labels implemented Zones A–F separately from proposed Zone G.
- **Real-time claim:** describe the streaming result as a controlled replay of archived data. It does not establish real-time, city-wide freshness.
- **Leakage and evaluation:** exact slot pairing and chronological evaluation are documented; wall-clock feature availability remains an open limitation. The onset result (0/8 cases at the selected threshold) must be disclosed.
- **NoSQL justification:** the query pattern, index, and stored latency evidence are present.
- **Controlled benchmark:** some recorded numbers are present, but the original full benchmark capture is not; it must be regenerated.
- **Clean-checkout reproducibility:** currently fails because source data are intentionally ignored and not downloaded. The new setup guide explains this, but a fresh data-ready run is still required.

## Required actions before claiming “ready”

1. Download the permitted source files into `raw_data/` following [`DATA_SETUP.md`](../../docs/DATA_SETUP.md); keep them out of Git unless licensing permits redistribution.
2. Build and start the Compose stack, then run `pipeline.py`, `baseline.py`, `pytest`, Ruff, and the Spark job. Archive command output and environment versions.
3. Re-run and retain the original three-variant benchmark logs/chart; verify the exact values in `B_benchmark.csv`.
4. Add the referenced Card B B1/B2/B3 CSVs or change the manual to point to a reproducible test/artifact that exists.
5. Create a Session 9 summary slide and record a new readiness score only after all blockers are closed.
6. Before production, add authenticated service access, TLS/SASL/ACLs, managed secrets, monitoring, and an operational availability/freshness SLO.

The score of 95/100 in the attached manual is a historical team self-assessment, not a grade and not validated by this repository snapshot. Re-score after the actions above; do not convert the current partial audit into a numeric score without the official Session 10 rubric and a fresh run.
