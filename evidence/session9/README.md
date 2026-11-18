# Session 9 Technical Audit & Readiness Evidence Pack (`evidence/session9/`)

Complete **`A_architecture` through `I_readiness`** audit deliverables for **Card B — HCMC Smart Traffic Platform** (`BDA501 — Big Data Analytics`), generated via `python scripts/run_session9_audit.py`.

## Live Container Verification & Dynamic Ground Truth
- **Spark 3.5.7 (`groupexe-knowledgetest-spark-1`):** `2` warmup + `5` interleaved runs -> `Baseline 179.73 ms` (`5` Exch) -> `Variant 1 142.36 ms` (`1` Exch, `-20.79%`) -> `Variant 2 219.01 ms` (`2 Exch`, `exceptAll=0`).
- **MongoDB 8.0 (`groupexe-knowledgetest-mongo-1`):** `33,394` docs, live interleaved IXSCAN `0.870 ms` vs COLLSCAN `9.500 ms` (`10.92x`), saved p95 `0.692 ms` (`23.55x`), `25` stale v1 rejected.
- **Card B (`B1, B2, B3`):** `# 21969 Nguyễn Kiệm` (`PR=2.7799`), `9,995` observed vs `74,211` Cold-Start (`88.13%`), HLL++ `3.75%` error, Bloom `0.611%` FPR.
