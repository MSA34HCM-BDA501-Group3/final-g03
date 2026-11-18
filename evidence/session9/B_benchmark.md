# B · Controlled Spark benchmark

The benchmark changes join strategy and then salting strategy on the same recorded workload: 90,938 pings, 33,441 output windows, eight shuffle partitions, Spark 3.5.7, in a single-host Docker proof-of-concept. The numbers below are recorded results, not a new run performed during this audit.

| Variant | Runtime | Exchanges | Peak/median skew | Exact output difference |
|---|---:|---:|---:|---:|
| SortMergeJoin + unsalted aggregation | 1,418.60 ms | 10 | 687.43× | 0.0 |
| BroadcastHashJoin + unsalted Monoid | 1,054.17 ms | 2 | 687.43× | 0.0 |
| BroadcastHashJoin + 8-bucket salted Monoid | 1,073.37 ms | 2 | 1.023× | 0.0 |

Interpretation: the recorded join-plan change reduced exchanges from 10 to 2 and reduced runtime by 25.7%. Salting reduced the peak-slot skew substantially but added 19.20 ms (about 1.82%) on this sample; it should be justified as skew control, not as a runtime win at this scale. The full machine-recorded comparisons are in [`spark_runtime_evidence.json`](../../outputs/spark_runtime_evidence.json), [`join_plan_evidence.json`](../../outputs/join_plan_evidence.json), and [`metrics.json`](../../outputs/metrics.json).

The 1,418.60 ms SortMergeJoin timing and the original chart/logs are only reported in the attached completed manual; that source capture was not found in the repository. The local input data are also absent, so the benchmark must be rerun before claiming it was reproduced from a clean checkout. See [`B_benchmark.csv`](B_benchmark.csv).
