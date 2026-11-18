# C · Spark physical-plan and data-layout evidence

Stored Spark 3.5.7 evidence reports the following comparison on the recorded 90,938-ping / 33,441-window input:

- Baseline plan: SortMergeJoin stages and 10 Catalyst Exchange nodes.
- Optimized plan: BroadcastHashJoin for the segment/street dimensions and 2 Exchange nodes.
- Two-way output comparison: `exceptAll` difference is zero.
- Parquet filter on `year_month = 2020-08` shows a partition filter and a projected read schema containing only `segment_id`, `date`, `period`, and `LOS`.

Plan material available in the checkout:

- [`sort_merge_plan.txt`](../../outputs/quyen_validation/sort_merge_plan.txt)
- [`broadcast_plan.txt`](../../outputs/quyen_validation/broadcast_plan.txt)
- [`pruning_plan.txt`](../../outputs/quyen_validation/pruning_plan.txt)
- [`spark_runtime_evidence.json`](../../outputs/spark_runtime_evidence.json)
- [`join_plan_evidence.json`](../../outputs/join_plan_evidence.json)

These are saved run outputs, not a fresh `EXPLAIN` from this audit. Regenerate them with the data-ready Spark batch procedure in [`DATA_SETUP.md`](../../docs/DATA_SETUP.md).
