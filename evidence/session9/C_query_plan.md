# Deliverable C — Spark Query-Plan & Shuffle Audit (`C_query_plan`)

- **Evidence File:** `evidence/session9/C_query_plan.txt` & `groupexe-knowledgetest-spark-1` (Apache Spark `3.5.7`).
- **Inspected Operation:** `5-Tuple Monoid` + `SortMergeJoin`/`BroadcastHashJoin` enrichment over `33,394` windows (`84,206` segments, `5,553` streets) and `curated_gold` Parquet read (`year_month = '2020-08'`).

| Audit Check | Verified Evidence from Live `executedPlan()` |
|---|---|
| **1. Inspected query/operation** | `_build_unsalted_monoid` / `_build_salted_monoid(buckets=8)` joined with `segments_clean` & `streets_clean`, plus `spark.read.parquet('data/lakehouse/curated_gold').filter("year_month = '2020-08'").select('segment_id', 'date', 'period', 'LOS')`. |
| **2. `Exchange` / shuffle visibility** | Baseline has **5 `Exchange`** nodes (1 `HashAggregate` + 4 join shuffles before `Sort`). Variant 1 (`BroadcastHashJoin`) has **only 1 `Exchange`** (inside `HashAggregate`); Variant 2 (`8-Bucket Salting`) has **2 `Exchange`** stages (Stage-1 partial + Stage-2 reduce). |
| **3. Join strategy used** | **`BroadcastHashJoin [segment_id], Inner, BuildRight`** and **`BroadcastHashJoin [street_id], LeftOuter, BuildRight`**, eliminating both `SortMergeJoin` + `Sort`. |
| **4. What determines partitioning** | `spark.sql.shuffle.partitions = 8` in memory, `xxhash64(..., salt)` in Stage-1 salted aggregation, and `partitionBy('year_month')` (`10` monthly directories) on disk. |
| **5. `cache/persist` assessment** | Selective `persist()` helps ONLY on reused inputs (`segments_clean`, `streets_clean`, `status_valid`, `silver_cleaned_windows`); caching single-pass frames wastes heap RAM. |
| **6. `BroadcastHashJoin` condition** | Verified valid: `segments` (`84,206` rows) & `streets` (`5,553` rows) fit far below `10 MB` threshold, cutting Exchanges `5 -> 1` (`-80%`). |
| **7. Parquet layout & pruning** | Physical Plan confirms `PartitionFilters: [isnotnull(year_month), (year_month = 2020-08)]` (skips `9/10` months) and `ReadSchema` reading 4 columns via `ColumnarToRow`. |
