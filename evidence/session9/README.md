# Session 9 Technical Audit & Readiness Evidence Pack (`evidence/session9/`)

Complete **`A_architecture` through `I_readiness`** audit deliverables for **Card B — HCMC Smart Traffic Platform** (`BDA501 — Big Data Analytics`), generated via `python scripts/run_session9_audit.py`.

## Live Container Verification & Dynamic Ground Truth
- **Spark 3.5.7 (`groupexe-knowledgetest-spark-1`):** `2` warmup + `5` interleaved runs -> `Baseline 205.36 ms` (`5` Exch) -> `Variant 1 164.10 ms` (`1` Exch, `-20.09%`) -> `Variant 2 212.79 ms` (`2 Exch`, `exceptAll=0`).
- **MongoDB 8.0 (`groupexe-knowledgetest-mongo-1`):** `33,394` docs (`1,234` arterial severe alerts), live interleaved IXSCAN `0.940 ms` vs COLLSCAN `11.390 ms` (`12.12x`), saved median `0.360 ms` / p95 `0.603 ms` (`31.36x`), `25` stale v1 rejected.
- **Card B (`B1, B2, B3`):** `#21969 Nguyễn Kiệm` (`PR=2.7799`), `9,995` observed vs `74,211` Cold-Start (`88.13%`), HLL++ `3.75%` error, RotatingBloom `0.0389%` empirical vs `0.0792%` theoretical (`13` FPs).
