# Deliverable B — Controlled Performance & Scalability Experiment (`B_benchmark`)

- **Command & Runtime:** Live 3-way controlled benchmark executed in `groupexe-knowledgetest-spark-1` (Apache Spark `3.5.7`, `local[4]`, `8` shuffle partitions, `AQE=false`, `2` warmup + `5` interleaved runs; source=`live_container`). Cross-checked with `outputs/spark_runtime_evidence.json`.
- **Dataset & Period:** `90,938` raw -> `90,787` accepted GPS pings joined with `84,206` clean motorized segments and `5,553` streets across `2020-07-03` to `2021-04-22` (`33,394` 30-minute windows, `38.50 MB`).
- **Controlled Factors Changed:**
  1. **Join Strategy:** `SortMergeJoin` (`autoBroadcastJoinThreshold = -1`) vs `BroadcastHashJoin` on static dimension tables `segments` & `streets`.
  2. **Aggregation Skew Mitigation:** `Unsalted GroupBy` vs `2-Stage 8-Bucket Key Salting + 5-Tuple Commutative Monoid`.

## Live Measured 3-Way Results (`evidence/session9/B_benchmark.csv` & `B_live_spark_benchmark.json`)
| Run | Configuration | Input / Partitions | Median Runtime (IQR) | Exchange Stages | Peak-to-Median Skew | Parity (`exceptAll`) |
|---|---|---|---:|---:|---:|---:|
| **Baseline** | `SortMergeJoin` + Unsalted GroupBy | `90,787` pings / `8` parts | `205.36 ms` (`IQR 17.53`) | `5` | `684.95x` | `0` rows diff |
| **Variant 1** | `BroadcastHashJoin` + Unsalted Monoid | `90,787` pings / `8` parts | `164.10 ms` (`-20.09%`, `IQR 31.02`) | `1` (`-80%`) | `684.95x` | `0` rows diff |
| **Variant 2** | `BroadcastHashJoin` + `8-Bucket Salted Monoid` | `90,787` -> `59,714` salted -> `33,394` windows | `212.79 ms` (`+48.69 ms` vs V1, `IQR 35.53`) | `2` | `1.0197x` (`CV=0.0059`) | `0` rows diff |

- **Wave 2 Isolated `outputs/spark_runtime_evidence.json` Baseline Check:** Catalyst Join `SortMergeJoin 166.18 ms -> BroadcastHashJoin 123.37 ms` (`25.76%` speedup, `5 -> 1` Exchange, `exceptAll=0`); Isolated Monoid unsalted `97.92 ms` vs 8-bucket salted `185.78 ms` (`exceptAll=0`).

## Required Three-Part Interpretation
1. **What changed, and why did the result move in that direction?**
   - Switching from `SortMergeJoin` to `BroadcastHashJoin` pinned `segments` (`84,206` rows) and `streets` (`5,553` rows) in executor memory, removing `4` network/sort `Exchange` stages (`5 -> 1`) and reducing median runtime by **`20.09%` (`205.36 ms -> 164.10 ms`)**.
   - Adding `2-Stage 8-Bucket Key Salting` split hot arterial keys across `8` `xxhash64` salt buckets (`59,714` Stage-1 partial groups -> `33,394` windows). On the `38.50 MB` sample, the extra partial-reduce shuffle adds `+48.69 ms` overhead vs V1 while collapsing peak-slot skew from **`684.95x` down to `1.0197x`** with `0.0` numerical error.
2. **At what point would your chosen configuration stop scaling well?**
   - `BroadcastHashJoin` fails with executor/driver `OutOfMemoryError` if broadcast tables exceed `spark.sql.autoBroadcastJoinThreshold` (`> 100–256 MB`). Fixed `8-bucket` salting caps concurrency at `8` cores per hot key, requiring adaptive `32–64` buckets or AQE `optimizeSkewedJoin` at `10^7+` pings/slot.
3. **What limitation prevents this experiment from proving production-scale performance?**
   - Runs inside a single-host Docker container (`groupexe-knowledgetest-spark-1`) over `38.50 MB` where shuffles traverse local disk/loopback rather than multi-rack cluster networking at `1.5–3.0 TB/day`.
