# Deliverable B — Controlled Performance & Scalability Experiment (`B_benchmark`)

- **Command Executed:** `pytest tests/test_quyen_spark.py -v` & `python spark_batch.py` (Apache Spark `3.5.7`, `local[4]`, `10.5 / 16 GB RAM` Docker container budget).
- **Dataset & Period:** `90,938` raw GPS pings (`segment_status.csv`) joined with `84,633` road segments (`segments.csv`) and `5,553` streets (`streets.csv`) across `2020-07` to `2021-04` (`33,441` 30-minute windows).
- **Controlled Factor Changed:**
  1. **Join Strategy** (`SortMergeJoin` with `autoBroadcastJoinThreshold = -1` vs `BroadcastHashJoin` on static dimension tables `segments` & `streets`).
  2. **Aggregation Skew Mitigation** (`Unsalted GroupBy` vs `2-Stage 8-Bucket Key Salting + 5-Tuple Commutative Monoid`).

## Measured Results Summary (`evidence/session9/B_benchmark.csv`)
| Run | Configuration | Input / Partitions | Runtime | Exchange Stages | Peak-to-Median Skew | Parity (`exceptAll`) |
|---|---|---|---:|---:|---:|---:|
| **Baseline** | `SortMergeJoin` + Unsalted GroupBy | `90,938` pings / `8` partitions | `1,418.60 ms` | `10` | `687.43x` | `0` rows diff |
| **Variant 1** | `BroadcastHashJoin` + Unsalted Monoid | `90,938` pings / `8` partitions | `1,054.17 ms` | `2` | `687.43x` | `0` rows diff |
| **Variant 2** | `BroadcastHashJoin` + `8-Bucket Salted Monoid` | `90,938` pings -> `59,828` salted -> `33,441` windows | `1,073.37 ms` | `2` | `1.023x` (`CV=0.0057`) | `0` rows diff |

## Required Three-Part Interpretation
1. **What changed, and why did the result move in that direction?**
   - Switching from `SortMergeJoin` (Baseline) to `BroadcastHashJoin` (Variant 1) pinned the small static dimension tables (`segments.csv` `84,633` rows `~6.8 MB` and `streets.csv` `5,553` rows `~0.4 MB`) in executor memory, eliminating `8` full network `Exchange` shuffle/sort stages (`10 -> 2` stages) and reducing execution time by **`25.7%` (`1,418.60 ms -> 1,054.17 ms`)**.
   - Adding `2-Stage 8-Bucket Key Salting` with the `5-Tuple Monoid (count, sum_v, min_v, max_v, severe_count)` (Variant 2) split hot rush-hour arterial keys across `8` deterministic `xxhash64` salt buckets (`59,828` Stage-1 partial groups before reducing to `33,441` final windows). On the `38.50 MB` sample, the extra partial-reduce pass added a tiny **`+19.20 ms` (`+1.8%`)** overhead (`1,054.17 ms -> 1,073.37 ms`), while collapsing peak-to-median partition skew from **`687.43x` down to `1.023x`** (`salt_bucket_cv = 0.0057`) with **`0.0` numerical discrepancy**.
2. **At what point would your chosen configuration stop scaling well?**
   - `BroadcastHashJoin` stops scaling well if the broadcasted dimension table grows beyond `spark.sql.autoBroadcastJoinThreshold` (typically `100–256 MB` per executor, e.g., if `segments` included high-resolution multi-year dynamic polygon histories exceeding executor heap memory), which would trigger driver/executor `OutOfMemoryError`.
   - Fixed `8-bucket` Key Salting stops scaling well when a single ultra-hot arterial segment receives `> 10^7` pings per 30-minute window on a `100+` core cluster (where `8` buckets can only utilize at most `8` cores concurrently), requiring adaptive skew salting (`buckets = 32..64`) or Spark AQE `optimizeSkewedJoin`.
3. **What limitation prevents this experiment from proving production-scale performance?**
   - This benchmark ran on a **single-host `local[4]` / 4-container Docker environment (`10.5 / 16 GB RAM`)** over a **`38.50 MB` (`90,938` pings) Proof-of-Scale dataset** where shuffles occur over local loopback/disk rather than across a physical multi-node network fabric with `1.5–3.0 TB/day` (`8–17` billion pings/day) and multi-rack disk/NIC contention.
