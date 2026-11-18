"""Structured Q&A, table updates, and rich in-document evidence for Session 9 Word Manual."""

from __future__ import annotations


def get_docx_table_updates() -> dict[int, dict[int, dict[int, str]]]:
    """Return exact cell updates for all fillable tables (Tables 01..36) of Session 9 Manual."""
    return {
        1: {
            1: {
                1: (
                    "FSB · FPT University | MSE AI | BDA501 – Big Data Analytics (Fall 2026) | Instructor: "
                    "Đặng Trần Khánh"
                )
            },
            2: {1: "HCMC Smart Traffic Big Data Analytics & 30-Minute Congestion Prediction Platform (Card B)"},
            3: {1: "Nguyễn Tạ Minh Quyền (25MS23280) · Nguyễn Thành Đạt (25MS23306) · Phạm Công Minh (25MS23310)"},
            4: {
                1: (
                    "HCMC Road Network & Probe GPS Dataset (5 CSVs, 38.50 MB: 577,967 nodes, 5,553 streets, "
                    "84,633 segments, 90,938 GPS pings -> 33,441 30m windows)"
                )
            },
        },
        2: {
            1: {
                1: (
                    "Trace the project from source/ingestion through storage, Spark SQL/analytics, ML, NoSQL "
                    "serving/output.\n"
                    "Completed trace: raw ping 77975, Nguyễn Bỉnh Khiêm segment 21260 (Section 2.1)."
                )
            },
            2: {
                1: (
                    "Inspect partitions, shuffle, file layout, caching/broadcast choices and query plans; "
                    "run one controlled experiment.\n"
                    "↳ [COMPLETED]: Ran 3-way Spark 3.5.7 controlled experiment (SMJ vs BHJ vs 8-Bucket "
                    "Salted Monoid) & EXPLAIN FORMATTED audit (evidence/session9/B_benchmark.{csv,png,md} & "
                    "C_query_plan.{txt,md})."
                )
            },
            3: {
                1: (
                    "Validate important processing and analytics/model results using independent evidence or "
                    "baselines.\n"
                    "↳ [COMPLETED]: Verified 7 data conservation checks (0 Silent Drops, exceptAll=0) & "
                    "7-model ML ablation + 61-case Transition Regime evaluation "
                    "(evidence/session9/D_data_validation.{csv,md} & E_model_validation.{csv,md})."
                )
            },
            4: {
                1: (
                    "Defend streaming consideration, cloud/cluster design, reproducibility, failure handling "
                    "and technical limitations.\n"
                    "↳ [COMPLETED]: Audited MongoDB 8.0 ESR IXSCAN (0.636 ms p95), 30m Watermark + 33 KiB "
                    "Sketches + WAL crash recovery, and 4-container Docker cluster "
                    "(evidence/session9/F_serving_stream.{json,md} & G_deployment.md)."
                )
            },
            5: {
                1: (
                    "Check all required technical deliverables and prepare evidence for Session 10 Q&A.\n"
                    "↳ [COMPLETED]: Completed Card B (context + 7 prompts + Mini-Tests B1, B2, B3 + 1m "
                    "defense), 16/16 completeness items, 9/9 Red-Flag checks, 95/100 self-score, and Session "
                    "9 Summary Slide."
                )
            },
        },
        4: {
            1: {
                1: (
                    "Raw ping _id=77975, segment 21260 (Nguyễn Bỉnh Khiêm), 2020-12-29 00:59:35 UTC / "
                    "07:59:35 ICT, 4 km/h."
                ),
                2: "[I] Raw GPS CSV for this trace; Kafka broker replay exercised separately.",
                3: "raw_data/segment_status.csv; outputs/replay_stream_summary.json (broker check: 3,000 messages).",
            },
            2: {
                1: (
                    "Bronze raw append log (data/lakehouse/bronze_raw_events) + quarantine_events.parquet "
                    "(580 unique bad segments)"
                ),
                2: "[I] Parquet storage; WAL recovery exercised in the separate local replay.",
                3: "outputs/spark_runtime_evidence.json (spark_sink1_output_path, 0 Silent Drops)",
            },
            3: {
                1: "Window [07:30, 08:00) ICT, one observation, Vmax 40 km/h, speed ratio 0.10 → LOS F.",
                2: (
                    "[I] Ordered Quality Funnel (ANSI try_cast, NFC 695->649, UTC+7 rollover) + Train-Only "
                    "3-Tier Vmax (pipeline.py & traffic_data/spark_etl.py)"
                ),
                3: "data/dataset.csv, event_id 1e3aa19fb29455452105f2b2.",
            },
            4: {
                1: (
                    "data/lakehouse/curated_gold (33,441 windows partitioned by year_month=2020-07..2021-04) "
                    "+ segment_static_features.parquet (84,633 rows)"
                ),
                2: "[I] Apache Spark 3.5.7 Columnar Parquet Writer (Snappy/Zstd, partitionBy('year_month'))",
                3: "evidence/session9/C_query_plan.txt (PartitionFilters & ReadSchema pruning verified)",
            },
            5: {
                1: (
                    "Aggregated 30m window tuple (obs_count, sum_v, min_v, max_v, severe_cnt) enriched with "
                    "segments & streets"
                ),
                2: "[I] Spark SQL / DataFrame 2-Stage 8-Bucket Key Salting + 5-Tuple Monoid + BroadcastHashJoin",
                3: "outputs/join_plan_evidence.json (10 → 2 shuffle Exchanges, two-way exceptAll = 0).",
            },
            6: {
                1: (
                    "23D Random Forest predicts LOS F for the next slot, labeled 08:00 ICT. Observed target "
                    "LOS F. Saved serving P(E/F) = 0.9971."
                ),
                2: (
                    "[I] Phase A Frozen Registry: Random Forest (23D, 250 trees) + Line-Graph PageRank + 7D "
                    "K-Means (pipeline.py & spark_batch.py)"
                ),
                3: (
                    "data/lakehouse/serving_traffic_scores.parquet and data/dataset.csv (same event_id); "
                    "outputs/ablation_results.csv."
                ),
            },
            7: {
                1: "Serving event_id 1e3aa19fb29455452105f2b2, BATCH_VERIFIED (status_version = 2).",
                2: (
                    "[I] MongoDB 8.0.34 (bda501.traffic_scores) with ESR Compound Index {street_level:1, "
                    "severe_prob:-1, segment_id:1}"
                ),
                3: (
                    "Serving Parquet gives this row. outputs/mongo_verify.json verifies collection read-back "
                    "and warm query p95 = 0.636 ms."
                ),
            },
            8: {
                1: (
                    "Separate historical replay: 32,940 of 33,441 unique windows in watermark; 501 late "
                    "windows backfilled."
                ),
                2: "[I] Kafka delivery check and local replay, 30-minute watermark, WAL and sketches.",
                3: "outputs/replay_stream_summary.json and outputs/query_progress.json.",
            },
            9: {
                1: (
                    "Streamlit presents scores from MongoDB and can read the saved serving Parquet snapshot "
                    "as fallback."
                ),
                2: "[I] Streamlit Dashboard (app.py on localhost:8501) querying MongoDB 8.0 & Lakehouse Parquet",
                3: "app.py (query_operational_serving_store).",
            },
        },
        5: {
            1: {
                1: (
                    "Does BroadcastHashJoin + 2-Stage 8-Bucket Salted 5-Tuple Monoid reduce shuffle stages "
                    "and partition skew without changing aggregation results?"
                )
            },
            2: {
                1: (
                    "Apache Spark 3.5.7 (local[4] / Docker Compose 10.5/16 GB RAM budget on Windows 11 host, "
                    "Python 3.13, seed=42)"
                )
            },
            3: {
                1: (
                    "90,938 raw GPS pings (segment_status.csv) + 84,633 segments + 5,553 streets -> 33,441 "
                    "30m windows (38.50 MB)"
                )
            },
            4: {
                1: (
                    "(1) Join strategy (SortMergeJoin vs BroadcastHashJoin) and (2) Aggregation key salting "
                    "(Unsalted vs 8-Bucket Salted Monoid)"
                )
            },
            5: {
                1: (
                    "Baseline: SortMergeJoin (spark.sql.autoBroadcastJoinThreshold = -1) + Unsalted GroupBy "
                    "(8 shuffle partitions)"
                )
            },
            6: {
                1: (
                    "Variant 1: BroadcastHashJoin + Unsalted Monoid | Variant 2: BroadcastHashJoin + 2-Stage "
                    "8-Bucket Salted 5-Tuple Monoid"
                )
            },
            7: {
                1: (
                    "Execution runtime (ms), Catalyst Exchange shuffle stage count, Peak-to-Median partition "
                    "skew ratio, two-way exceptAll diff"
                )
            },
            8: {
                1: (
                    "BroadcastHashJoin should cut join shuffles and lower runtime; 8-Bucket Salting should "
                    "collapse peak-slot skew (~687x -> ~1x) with minor 2-stage overhead and 0 diff."
                )
            },
        },
        6: {
            1: {
                1: "SortMergeJoin (autoBroadcast=-1) + Unsalted GroupBy",
                2: "90,938 pings -> 33,441 windows (8 partitions)",
                3: "1,418.60 ms",
                4: "10 Exchanges | 687.43x skew | diff=0",
                5: "4 SortMergeJoins shuffle both fact & dim tables; severe hot-key skew on peak arterial windows.",
            },
            2: {
                1: "BroadcastHashJoin (segments, streets) + Unsalted Monoid",
                2: "90,938 pings -> 33,441 windows (8 partitions)",
                3: "1,054.17 ms (-25.7%)",
                4: "2 Exchanges (-80%) | 687.43x skew | diff=0",
                5: (
                    "Eliminates all 8 join Exchange stages; fastest on 38.5 MB sample, but peak-slot key "
                    "skew remains 687.43x."
                ),
            },
            3: {
                1: "BroadcastHashJoin + 2-Stage 8-Bucket Salted 5-Tuple Monoid",
                2: "90,938 pings -> 59,828 salted -> 33,441 windows",
                3: "1,073.37 ms (+19.2 ms vs V1)",
                4: "2 Exchanges | 1.023x skew (CV=0.0057) | diff=0",
                5: (
                    "Collapses peak partition skew 687.43x -> 1.023x with exact monoid equivalence "
                    "(max_abs_diff = 0.0)."
                ),
            },
        },
        7: {
            1: {
                1: (
                    "aggregate_monoid(salted=True, buckets=8) -> enrich_windows(broadcast=True) -> "
                    "curated_gold Parquet read with filter(year_month='2020-08') "
                    "(evidence/session9/C_query_plan.txt)."
                )
            },
            2: {
                1: (
                    "Baseline has 10 Exchange hashpartitioning nodes (before SortMergeJoin & HashAggregate). "
                    "Optimized plan has ONLY 2 Exchange nodes (inside HashAggregate for 5-Tuple Monoid)."
                )
            },
            3: {
                1: (
                    "BroadcastHashJoin (BuildRight) on segment_id (84,633 rows) and street_id (5,553 rows), "
                    "replacing 4 SortMergeJoin + Sort stages."
                )
            },
            4: {
                1: (
                    "spark.sql.shuffle.partitions = 8 and hashpartitioning(segment_id, window_start, salt, "
                    "8) in memory; partitionBy('year_month') (10 directories) on disk."
                )
            },
            5: {
                1: (
                    "Selective persist(MEMORY_AND_DISK) helps ONLY on silver_cleaned_windows & train_events "
                    "(reused by Vmax, PageRank, FPGrowth, K-Means, RF). Caching one-pass DataFrames wastes "
                    "RAM."
                )
            },
            6: {
                1: (
                    "Yes: segments (~6.8 MB) and streets (~0.4 MB) are < 10 MB threshold, cutting Exchange "
                    "stages 10 -> 2 (-80%). Valid while static dimensions stay < 100 MB per executor."
                )
            },
            7: {
                1: (
                    "Yes: Physical Plan confirms PartitionFilters: [isnotnull(year_month), (year_month = "
                    "2020-08)] (skips 9/10 months) and ReadSchema pruning (reads 4/26 columns via "
                    "ColumnarToRow)."
                )
            },
        },
        9: {
            1: {
                1: (
                    "All-string ingestion + ordered ANSI try_cast (R0_MISSING_OR_MALFORMED_FIELD) in "
                    "pipeline.py & traffic_data/spark_etl.py"
                ),
                2: "90,938 pings & 84,633 segments validated; 0 uncaught cast crashes; dirty fixtures pass 100%.",
                3: "[x] PASS",
            },
            2: {
                1: (
                    "Ordered Quality Funnel (R1 length<=0: 154; R2 non-road: 427 -> 580 unique segs) + "
                    "Train-Only 3-Tier Vmax Imputation (EC-1: 85.21% missing)"
                ),
                2: (
                    "100% of 33,441 windows imputed (4,946 GIS + 8,164 Tier-1 P85 + 20,124 Tier-2 Median + "
                    "207 Tier-3 Prior)."
                ),
                3: "[x] PASS",
            },
            3: {
                1: (
                    "SHA-256 raw_event_id exact dedup in Batch + 32.0 KiB bit-packed Bloom Filter (k=4) + "
                    "MongoDB status_version guard in Stream"
                ),
                2: (
                    "Batch isolates conflicting keys; Stream catches 334/334 injected duplicates (empirical "
                    "FPR = 0.616%)."
                ),
                3: "[x] PASS",
            },
            4: {
                1: "Strict conservation law: input_total == accepted + dedup_dropped + quarantined (0 Silent Drops)",
                2: (
                    "90,938 pings -> 33,441 windows; Stream 33,775 = 33,441 unique + 334 dups; Tier-2 recon "
                    "= 33,441/33,441 (100.0%)."
                ),
                3: "[x] PASS",
            },
            5: {
                1: (
                    "Referential integrity check vs 84,633 valid_segment_ids + two-way DataFrame exceptAll "
                    "parity (SMJ vs BHJ)"
                ),
                2: "0 unmatched segment_ids in Gold (33,441/33,441); two_way_exceptAll_diff = 0 rows.",
                3: "[x] PASS",
            },
            6: {
                1: "Independent aggregate baseline and trace of segment 21260, raw ping 77975.",
                2: (
                    "Aggregate parity is recorded in outputs/monoid_baseline_verification.json. The traced "
                    "window has speed ratio 4/40 = 0.10 and LOS F (data/dataset.csv)."
                ),
                3: "[x] PASS",
            },
            7: {
                1: (
                    "SHA-256 manifest check (verify_frozen_registry_integrity) + Parquet & Spark "
                    "PipelineModel reload parity"
                ),
                2: (
                    "curated_gold (33,441) & segment_static_features (84,633) read back with 0 diff; "
                    "reload_parity = True."
                ),
                3: "[x] PASS",
            },
        },
        10: {
            1: {
                1: (
                    "Supervised 6-class ordinal classification (LOS A..F at exact t+30m) & binary severe "
                    "congestion warning (LOS E/F at t+30m), supported by Line-Graph PageRank, 7D K-Means "
                    "(k=6), and SON/FPGrowth (335 rules)."
                )
            },
            2: {
                1: (
                    "23 features (14 local/calendar/road and 9 graph, cluster and history). See Card B1 for "
                    "the distinction between a slot's start label, its completion time and actual "
                    "availability."
                )
            },
            3: {
                1: (
                    "Chronological split on 7,898 exact pairs: 5,494 TRAIN, 1,167 VAL and 1,182 TEST. Purged "
                    "55 target pairs crossing split boundaries (50 TRAIN, 5 VAL)."
                )
            },
            4: {
                1: (
                    "Verified on 1,182 TEST pairs: six-class accuracy 94.50%, macro-F1 0.5543; severe "
                    "ROC-AUC 0.9882 and AP/PR-AUC 0.9471. For detecting any LOS change across TEST, ROC-AUC "
                    "0.7664 and AP 0.3007. On the 61 true changes, next-state accuracy is 2/61 (3.28%) by "
                    "default, 9/61 (14.75%) at the VAL-selected 0.30 change threshold, and 24/61 (39.34%) "
                    "when a change is assumed known and current LOS is excluded."
                )
            },
            5: {
                1: (
                    "7-Model Ablation on TEST (1,182 pairs): Naive Persistence Baseline, Decision Tree (14D "
                    "vs 23D), Random Forest (14D vs 23D), and Binary GBT (14D vs 23D) in "
                    "outputs/ablation_results.csv."
                )
            },
            6: {
                1: (
                    "If accuracy on state-change windows (LOS(t) != LOS(t+30m)) were 0% like Naive "
                    "Persistence (which gets 94.84% overall accuracy due to 30m inertia but 0.00% on all 61 "
                    "transitions and 0.0% Cold-Start coverage)."
                )
            },
            7: {
                1: (
                    "94.84% of TEST pairs retain their LOS. There are 61 true changes and 8 "
                    "non-severe-to-E/F onsets. At the VAL-selected onset threshold 0.15, onset recall is "
                    "0/8. GPS covers 11.85% of road segments in the sample."
                )
            },
        },
        12: {
            1: {
                0: (
                    "RF (23D) improves severe-event ranking versus persistence (AP 0.9471 vs 0.9367). "
                    "Overall six-class accuracy is 94.50% vs 94.84%. On true changes, VAL-calibrated "
                    "accuracy is 14.75%; conditional new-state ranking is 39.34%."
                ),
                1: (
                    "Re-scored outputs/model.joblib on the 1,182 TEST pairs in data/dataset.csv. "
                    "Cross-checked outputs/ablation_results.csv, outputs/transition_evaluation.csv and "
                    "outputs/metrics.json (transition_analysis)."
                ),
                2: (
                    "Only eight new severe onsets occur in TEST; the selected onset threshold detects zero. "
                    "The evaluation uses observed slot pairs and leaves unverified unobserved roads or "
                    "wall-clock issuance."
                ),
            },
            2: {
                0: (
                    "2-Stage 8-Bucket Salted 5-Tuple Monoid + BroadcastHashJoin eliminates join shuffles (10 "
                    "-> 2 Exchanges) and collapses peak-slot skew (687.43x -> 1.023x) with 0.0 error."
                ),
                1: (
                    "outputs/spark_runtime_evidence.json & evidence/session9/B_benchmark.csv (Spark 3.5.7 "
                    "physical plans & two-way exceptAll diff = 0)."
                ),
                2: (
                    "Does NOT prove multi-rack network throughput at 1.5-3.0 TB/day city-wide scale, as "
                    "measurements ran on a single-host 4-container Docker setup over a 38.50 MB "
                    "Proof-of-Scale sample."
                ),
            },
            3: {
                0: (
                    "Static Feature Store (Line-Graph PageRank + 7D K-Means Behavioral Clusters) enables "
                    "100% network coverage across all 84,633 segments including 74,606 Cold-Start roads."
                ),
                1: (
                    "data/lakehouse/segment_static_features.parquet (84,633 rows) & Mini-Test B2/B3 "
                    "(evidence/session9/H_project_card.md)."
                ),
                2: (
                    "Does NOT prove real-time sensor ground-truth accuracy on the 74,606 unobserved segments "
                    "(Cold-Start outputs are structural/behavioral priors, not live GPS measurements)."
                ),
            },
        },
        13: {
            1: {
                1: (
                    "33,441 JSON documents in bda501.traffic_scores: {event_id, segment_id, street_name, "
                    "street_level, date, period, current_los, predicted_next_los, severe_prob, "
                    "pagerank_score, cluster_id, lifecycle_status, status_version, model_version}."
                )
            },
            2: {
                1: (
                    "Top-K arterial early-warning query: db.traffic_scores.find({'street_level': {'$in': [1, "
                    "2]}, 'severe_prob': {'$gte': 0.70}}).sort([('severe_prob', -1), ('segment_id', "
                    "1)]).limit(25)"
                )
            },
            3: {
                1: (
                    "Unique document key: event_id (SHA-256 of segment_id|date|period); Partition key: "
                    "segment_id (4 Spark write partitions); ESR Compound Index: {street_level: 1, "
                    "severe_prob: -1, segment_id: 1}."
                )
            },
            4: {
                1: (
                    "TOC operators query point lookups by event_id and sorted range scans of high-risk "
                    "arterials. The ESR index satisfies Equality (street_level), Sort (severe_prob DESC), "
                    "and tie-break in B-Tree IXSCAN without in-memory SORT."
                )
            },
            5: {
                1: (
                    "AP/Eventual Consistency between Speed Layer v1 (STREAM_PROVISIONAL, 98.5% in-watermark) "
                    "and Batch Tier-2 v2 (BATCH_VERIFIED/RECONCILED, 100.0%), guarded by status_version so "
                    "stale v1 replays never overwrite v2."
                )
            },
            6: {
                1: (
                    "outputs/mongo_verify.json: 33,441 docs upserted in 69 bulk batches; idempotent replay = "
                    "33,441; explain('executionStats') proves IXSCAN (nReturned=25, keysExamined=26, "
                    "docsExamined=25, p50=0.530 ms, p95=0.636 ms, in_memory_sort=False)."
                )
            },
        },
        15: {
            1: {
                1: (
                    "Deterministic event-time replay of static historical GPS dataset (90,938 pings / 33,441 "
                    "windows, seed=42) via producer.py into Apache Kafka 4.1.0 KRaft (NOT claimed as a live "
                    "telco feed)."
                )
            },
            2: {
                1: (
                    "Topic: hcmc-traffic-events (3 partitions keyed by segment_id). Schema: {event_id, "
                    "raw_event_id, segment_id, event_ts, arrival_delay_seconds, velocity, max_velocity, "
                    "lateness_bucket, is_duplicate}."
                )
            },
            3: {
                1: (
                    "producer.py -> Kafka (hcmc-traffic-events) -> Dual-Sink Router (Sink 1: "
                    "bronze_raw_events Parquet; Sink 2: 30m Watermark -> 33 KiB Sketches -> Read-only RF "
                    "Scorer) -> MongoDB 8.0 & Streamlit :8501."
                )
            },
            4: {
                1: (
                    "outputs/checkpoints/{raw/offset_log.json, agg/state_wal.json}. On simulated container "
                    "kill after batch 1, restart skips committed_batch_ids and restores Bloom/HLL/EWMA state "
                    "with 0 duplicates."
                )
            },
            5: {
                1: (
                    "Event time (event_ts) is authoritative because urban mobile GPS packets experience "
                    "variable tunnel/cellular delays; grouping by processing time would corrupt 30m "
                    "rush-hour windows."
                )
            },
            6: {
                1: (
                    "Yes (3.5% = 1,170 in-watermark late <=30m; 1.5% = 501 out-of-watermark >30m). Sink 2 "
                    "accepts <=30m (98.5% completeness); Sink 1 captures 100% raw events for nightly Tier-2 "
                    "Batch Reconciliation -> 100.0%."
                )
            },
            7: {
                1: (
                    "3-layer defense: (1) 32.0 KiB bit-packed Bloom Filter (k=4) in Sink 2 catches 334/334 "
                    "injected duplicates (0.616% FPR); (2) WAL committed_batch_ids check; (3) MongoDB "
                    "UpdateOne(upsert=True) + status_version guard (400 stale v1 rejected)."
                )
            },
            8: {
                1: (
                    "Needed & justified: 30m Tumbling Window + 30m Watermark bounds state RAM, while 33.0 "
                    "KiB sketches (32K Bloom + 1K HLL++ + bounded EWMA alpha=0.15, epsilon=1e-4) replace "
                    "unbounded O(N) sets."
                )
            },
            9: {
                1: (
                    "TOC operators require < 10 ms dashboard query response and immediate provisional alerts "
                    "upon window closure so signal engineers have a full +30m lead time before gridlock "
                    "forms."
                )
            },
        },
        17: {
            1: {
                1: (
                    "[I] Docker Compose 4-container cluster (kafka, spark, mongodb, streamlit) capped at "
                    "10.5 / 16 GB RAM. [P] Cloud Kubernetes (EKS/GKE SparkOperator) or YARN for city-wide "
                    "scale."
                )
            },
            2: {
                1: (
                    "[I] Co-located in spark container (2.5 GB RAM, 2 cores). [P] Dedicated cluster-mode "
                    "driver pod (4 vCPU, 8 GB RAM) isolated from UI/ingestion nodes."
                )
            },
            3: {
                1: (
                    "[I] 4 executor threads, spark.sql.shuffle.partitions = 8. [P] Dynamic Allocation (2..16 "
                    "executor pods, 4 vCPU / 16 GB RAM each) scaling on Kafka lag & 8-64 salt buckets."
                )
            },
            4: {
                1: (
                    "[I] Bind-mounted Columnar Parquet Lakehouse (data/lakehouse/curated_gold, "
                    "partitionBy('year_month')). [P] S3/GCS/MinIO object store with Delta Lake / Iceberg "
                    "ACID logs."
                )
            },
            5: {
                1: (
                    "[I] Dual-Sink WAL checkpoints (offset_log.json & state_wal.json) + MongoDB idempotent "
                    "upsert with status_version ordering guard + Frozen Registry SHA-256 lock."
                )
            },
            6: {
                1: (
                    "[I] Structured JSON audit logs (spark_runtime_evidence.json, query_progress.json, "
                    "mongo_verify.json) surfaced live in Streamlit Tab 5."
                )
            },
            7: {
                1: (
                    "[I] .gitignore blocks .env/*.pem/*.key/*secret*; read-only SHA-256 verification "
                    "(verify_frozen_registry_integrity) prevents online model mutation."
                )
            },
            8: {
                1: (
                    "Trading +19.2 ms 2-stage salting overhead and 33.0 KiB approximate sketches (3.98% "
                    "HLL++ error) to eliminate OOM skew (687.43x -> 1.023x) within a 10.5 GB RAM budget."
                )
            },
        },
        18: {
            1: {
                1: "[x]",
                2: (
                    "Python 3.13, PySpark 3.5.7, Kafka 4.1.0, MongoDB 8.0.34, scikit-learn recorded in "
                    "Dockerfile, requirements.txt & spark_runtime_evidence.json."
                ),
            },
            2: {
                1: "[x]",
                2: (
                    "Pinned in requirements.txt, Dockerfile, and docker-compose.yml (explicit container "
                    "mem_limit=10.5 GB total)."
                ),
            },
            3: {
                1: "[x]",
                2: (
                    "Documented in README.md: python pipeline.py, python spark_batch.py, python "
                    "starter_stream.py, pytest -v."
                ),
            },
            4: {
                1: "[x]",
                2: (
                    "RANDOM_SEED = 42 fixed across pipeline.py, producer.py, starter_stream.py, "
                    "spark_engine.py, and all tests."
                ),
            },
            5: {
                1: "[x]",
                2: (
                    "README.md & pipeline.py run end-to-end starting directly from "
                    "raw_data/{nodes,streets,segments,segment_status,train}.csv."
                ),
            },
            6: {
                1: "[x]",
                2: (
                    "Verified by 22/22 tests in tests/test_platform_v3.py and 5/5 Spark runtime tests in "
                    "tests/test_quyen_spark.py (27/27 total)."
                ),
            },
            7: {
                1: "[x]",
                2: "Verified via .gitignore and git status audit (0 .env, keys, or private credentials in repository).",
            },
        },
        19: {
            1: {
                0: "Sparse Spatial GPS Coverage (88.15% Cold-Start)",
                1: "Only 10,027 / 84,633 segments have empirical GPS pings; 74,606 segments have 0 observations.",
                2: (
                    "Cold-Start segments cannot validate against live ground-truth speed and rely on 7D "
                    "Behavioral Cluster priors (Mini-Test B2 & B3)."
                ),
                3: (
                    "Explicitly badge [COLD-START STRUCTURAL PRIOR] on Tab 4 UI; integrate bus/ride-hailing "
                    "probes in Phase 2."
                ),
            },
            2: {
                0: "30-Minute Inertia & Rare Sudden E->F Onset Sample Size",
                1: (
                    "94.84% of consecutive 30m windows keep the same LOS; TEST has only 61 transitions (8 "
                    "non-severe -> E/F sudden onsets)."
                ),
                2: (
                    "On 61 true changes: default 2/61 and VAL-calibrated 9/61. The conditional 24/61 assumes "
                    "a change is known. The VAL-selected onset threshold detects 0 of 8 non-severe-to-E/F "
                    "onsets."
                ),
                3: (
                    "Expose interactive threshold slider tau on Tab 2 UI; add rainfall/incident exogenous "
                    "feeds in future work."
                ),
            },
            3: {
                0: "Static Train-Only 3-Tier Vmax Lookup (60.79% Imputed)",
                1: (
                    "85.21% of windows lack GIS max_velocity; 20,331 / 33,441 windows use Tier-2/Tier-3 "
                    "road-class priors."
                ),
                2: (
                    "Cannot dynamically reflect temporary roadworks or localized flooding that lower "
                    "effective free-flow speed."
                ),
                3: "Enforce Train-Only fit to prevent leakage; periodically refit Vmax lookup per season.",
            },
            4: {
                0: "Single-Host Docker PoC vs Multi-Node Production Cluster",
                1: "All 4 containers share 10.5 / 16 GB RAM on one machine over a 38.50 MB Proof-of-Scale dataset.",
                2: "Local disk/loopback shuffle does not measure multi-rack network saturation at 8-17B pings/day.",
                3: (
                    "Label city-wide cloud K8s & SCATS actuation as [P] Proposed; use ring-buffer Bloom "
                    "rotation for multi-day streams."
                ),
            },
        },
        23: {
            1: {
                2: (
                    "B1: The 46-row matrix is a feature-rule illustration. A 10:00 wall-clock issue uses a "
                    "closed input window; features with later arrivals or revisions are unavailable. "
                    "Repository checks cover exact t+30 slot pairing and future perturbation of tested "
                    "features. 55 boundary pairs were purged. See answer to prompt 1 for the slot-label "
                    "distinction."
                )
            },
            2: {
                2: (
                    "VERIFIED (evidence/session9/H_B2_graph_zone_falsification.csv): High-PageRank "
                    "segment_id=21969 (Nguyễn Kiệm, PR=2.7313, in=3, out=1) averages 45.41 km/h (LOS A, "
                    "v_ratio=0.9082) across 24 windows -> PR is topological vulnerability, not realized "
                    "congestion. 7D K-Means spans non-adjacent districts -> renamed '7D Behavioral & "
                    "Morphological Road Archetypes'."
                )
            },
            3: {
                2: (
                    "B3: 10,027 observed segments; 74,606 cold-start. HLL estimates 9,627.89 (3.98% error) "
                    "for the aggregate KPI. Bloom detects 334/334 injected duplicates (0.616% false-positive "
                    "rate); valid events may be suppressed in the provisional branch."
                )
            },
        },
        30: {
            1: {
                1: "[x] Ready",
                2: (
                    "38.50 MB Proof-of-Scale (90,938 pings, 84,633 segs) -> 8-17B pings/day city-wide "
                    "justification (Slide 02, 05 & A_architecture.md)"
                ),
            },
            2: {
                1: "[x] Ready",
                2: (
                    "evidence/session9/A_architecture.png with explicit [I] Implemented vs [P] Proposed "
                    "badges & solid/dashed arrows"
                ),
            },
            3: {
                1: "[x] Ready",
                2: (
                    "5-Tuple Commutative Monoid (cnt, sum_v, min_v, max_v, severe_cnt) + 2-Stage 8-Bucket "
                    "Salting (max_abs_diff = 0.0)"
                ),
            },
            4: {
                1: "[x] Ready",
                2: (
                    "traffic_data/spark_etl.py & spark_batch.py: ANSI try_cast, NFC, UTC+7, 3-Tier Vmax, "
                    "BroadcastHashJoin, Monoid"
                ),
            },
            5: {
                1: "[x] Ready",
                2: (
                    "data/lakehouse/curated_gold partitioned by year_month (10 dirs) + "
                    "segment_static_features.parquet (84,633 rows)"
                ),
            },
            6: {
                1: "[x] Ready",
                2: (
                    "evidence/session9/C_query_plan.txt: Enrich BroadcastJoin + Salted Monoid + Parquet "
                    "Partition/Schema Pruning plans"
                ),
            },
            7: {
                1: "[x] Ready",
                2: (
                    "evidence/session9/B_benchmark.csv & .png: SMJ vs BHJ (10 -> 2 Exchanges, -25.7% time) & "
                    "8-Bucket Salting (687x -> 1.023x skew)"
                ),
            },
            8: {
                1: "[x] Ready",
                2: (
                    "7-Model Ablation (outputs/ablation_results.csv) + Transition Regime Eval "
                    "(outputs/transition_evaluation.csv) + Spark ML PipelineModel"
                ),
            },
            9: {
                1: "[x] Ready",
                2: (
                    "MongoDB 8.0.34 (33,441 docs), ESR Compound Index IXSCAN p95=0.636 ms / p50=0.530 ms vs "
                    "COLLSCAN=5.950 ms (outputs/mongo_verify.json)"
                ),
            },
            10: {
                1: "[x] Ready",
                2: (
                    "Kafka 4.1.0 KRaft -> Dual-Sink 30m Watermark -> 33 KiB Sketches -> WAL Crash Recovery "
                    "(0 dups) -> Tier-2 Recon (100%)"
                ),
            },
            11: {
                1: "[x] Ready",
                2: (
                    "evidence/session9/G_deployment.md: 4-container Docker Compose (10.5/16 GB RAM) + Cloud "
                    "K8s/YARN scale-out design"
                ),
            },
            12: {
                1: "[x] Ready",
                2: (
                    "evidence/session9/D_data_validation.csv, E_model_validation.csv, H_project_card.md (B1, "
                    "B2, B3) & 27/27 pytest"
                ),
            },
            13: {
                1: "[x] Ready",
                2: (
                    "Dockerfile, docker-compose.yml, requirements.txt, seed=42, SHA-256 manifest, README.md "
                    "& DATA_SETUP.md execution"
                ),
            },
            14: {
                1: "[x] Ready",
                2: (
                    "HCMC_Smart_Traffic_Final_Report.pdf, HCMC_Smart_Traffic_Final_Presentation.pptx (17 "
                    "slides), PRESENTATION_SCRIPT.md, README.md"
                ),
            },
            15: {
                1: "[x] Ready",
                2: (
                    "Minh Quyền (Data/Spark ETL), Thành Đạt (Graph/K-Means/ML), Công Minh "
                    "(Kafka/Sketches/MongoDB/UI) documented in Report & Script"
                ),
            },
            16: {
                1: "[x] Ready",
                2: (
                    "Live Kafka->Consumer->MongoDB->Streamlit (:8501) monitor + ui-demo.html + segment 21260 "
                    "/ 47407 traces."
                ),
            },
        },
        31: {
            1: {
                3: "24 / 25",
                4: (
                    "All 27/27 tests pass (22 platform + 5 Spark); exceptAll=0; Future-Shuffle=0.0; Card B "
                    "(B1, B2, B3) cross-checked raw_data<->data."
                ),
            },
            2: {
                3: "14 / 15",
                4: (
                    "2-Stage 8-Bucket Salted Monoid (687x->1.023x skew), Train-Only 3-Tier Vmax, 33 KiB "
                    "Sketches, Two-Tier Recon."
                ),
            },
            3: {
                3: "24 / 25",
                4: (
                    "Full Zones A-F integrated + complete evidence/session9/ package (A_architecture through "
                    "I_readiness)."
                ),
            },
            4: {
                3: "9 / 10",
                4: (
                    "Added [I] vs [P] architecture badges; refined 'Predictive ML (Past-Only Priors)' & '7D "
                    "Behavioral Clusters'."
                ),
            },
            5: {
                3: "19 / 20",
                4: (
                    "Explicitly links monoids, Catalyst Exchange/BroadcastJoin, Parquet pruning, ESR IXSCAN, "
                    "and 30m Watermark."
                ),
            },
            6: {
                3: "5 / 5",
                4: "All Session 9 & Session 10 deliverables, scripts, Word manual, and slides completed on time.",
            },
        },
        32: {
            1: {
                1: (
                    "Architecture diagram needed explicit visual markers separating Implemented [I] "
                    "components from Proposed [P] city-wide extensions."
                ),
                2: (
                    "Created evidence/session9/A_architecture.png with [I] Implemented (solid arrows) vs [P] "
                    "Proposed (dashed arrows) badges on every node."
                ),
                3: "Minh Quyền (25MS23280)",
                4: "[x] Done",
            },
            2: {
                1: (
                    "Card B audit of 'Causal ML' label, Line-Graph PageRank congestion meaning, and 7D "
                    "K-Means spatial contiguity."
                ),
                2: (
                    "Renamed to 'Predictive ML (Past-Only Priors tau<t)' and '7D Behavioral Clusters'; "
                    "proved via B2 (#21969 Nguyễn Kiệm PR=2.73 runs at 45.4 km/h LOS A)."
                ),
                3: "Thành Đạt (25MS23306)",
                4: "[x] Done",
            },
            3: {
                1: (
                    "Quantifying how 1.0 KiB HLL++ (3.98% err) & 32.0 KiB Bloom (0.616% FPR) affect "
                    "operational road alerts (Card B3)."
                ),
                2: (
                    "Executed B3 across N=500..10,027 segments; proved HLL++ only feeds global KPI banner (0 "
                    "false road alerts) while Tier-2 Recon restores 100%."
                ),
                3: "Công Minh (25MS23310)",
                4: "[x] Done",
            },
        },
        33: {
            1: {
                0: (
                    "Card B Defense: What does the system know at the exact forecast time on the 74,606 "
                    "(88.15%) road segments with zero GPS observations?"
                ),
                1: (
                    "It has 0 live GPS pings and never fabricates sensor readings. It uses 3 pre-computed, "
                    "leak-free signals from segment_static_features.parquet: (1) road geometry & "
                    "Train-fitted Tier-2/3 Vmax, (2) topological Line-Graph PageRank & in/out degree, and "
                    "(3) 7D K-Means behavioral cluster & street-level past-only priors (tau < t) combined "
                    "with ICT clock harmonics, explicitly badged [COLD-START STRUCTURAL PRIOR] on Tab 4."
                ),
            },
            2: {
                0: "If Naive Persistence achieves 94.84% overall 6-class accuracy, why is Random Forest (23D) needed?",
                1: (
                    "Persistence achieves 94.84% overall because most LOS labels stay unchanged; RF reaches "
                    "94.50%. On 61 true changes, persistence is 0/61 and RF is 2/61 by default or 9/61 at "
                    "the VAL threshold. Conditional new-state Top-1 is 24/61 when change is assumed known. "
                    "Severe-event AP is 0.9471 vs 0.9367."
                ),
            },
            3: {
                0: (
                    "Does a high Line-Graph PageRank score prove that a road segment is congested, and are "
                    "your 7D K-Means clusters contiguous districts?"
                ),
                1: (
                    "No to both (verified in Mini-Test B2): segment_id=21969 (Nguyễn Kiệm) has "
                    "PageRank=2.7313 (2.73x network mean) but averages 45.41 km/h (LOS A) because its wide "
                    "multi-lane carriageway absorbs merging inflows—so PageRank measures topological "
                    "funneling vulnerability, not realized congestion. Likewise, 7D K-Means uses 7 "
                    "behavioral/morphological features without lat/lon, producing non-contiguous Road "
                    "Archetypes across districts."
                ),
            },
        },
        34: {
            1: {
                1: (
                    "Added [I] Implemented vs [P] Proposed badges (A_architecture.png); clarified "
                    "'Predictive ML (Past-Only Priors tau<t)' & '7D Behavioral Clusters' (Card B)."
                )
            },
            2: {
                1: (
                    "Spark 3.5.7 controlled test: BroadcastHashJoin cuts Exchange stages 10 -> 2 (-25.7% "
                    "runtime: 1,418.6 -> 1,054.2 ms); 8-Bucket Salted Monoid cuts skew 687.43x -> 1.023x "
                    "(+19.2 ms, 0.0 diff)."
                )
            },
            3: {
                1: (
                    "Data counts reconcile. RF test accuracy 94.50%, severe AP 0.9471. On 61 true changes: "
                    "default 2/61, VAL-calibrated 9/61, conditional new-state ranking 24/61. Transition "
                    "detection ROC-AUC 0.7664 and AP 0.3007."
                )
            },
            4: {
                1: (
                    "Replayed GPS stream via Kafka 4.1 KRaft (30m Watermark + Dual-Sink); WAL crash recovery "
                    "(0 dups); Tier-2 Recon (100%); MongoDB 8.0 ESR IXSCAN (p95=0.636 ms, p50=0.530 ms vs "
                    "5.950 ms COLLSCAN)."
                )
            },
            5: {
                1: (
                    "88.15% of road segments have no GPS observation in the sample. TEST contains eight "
                    "non-severe-to-E/F onsets; the VAL-selected onset threshold detected zero."
                )
            },
        },
        36: {
            1: {1: "NTMQ · NTD · PCM [x] Verified (python pipeline.py & pytest 27/27 passed)"},
            2: {1: "NTMQ · NTD · PCM [x] Verified (evidence/session9/A_architecture.png)"},
            3: {1: "NTMQ · NTD · PCM [x] Verified (evidence/session9/B_benchmark.csv & C_query_plan.txt)"},
            4: {
                1: (
                    "NTMQ · NTD · PCM [x] Verified (evidence/session9/D_data_validation.csv, "
                    "E_model_validation.csv, H_project_card.md)"
                )
            },
            5: {1: "NTMQ (Spark/Data) · NTD (Graph/ML) · PCM (Stream/Mongo/UI) [x] Verified"},
        },
    }


def get_docx_qa_append_map() -> dict[int, str]:
    """Return paragraph index -> detailed in-document answer appended below prompts in the Manual."""
    return {
        # --- Section 2.2 Architecture Challenge Questions (P025..P029) ---
        25: (
            "Team answer. The main flow is GPS CSV → 30-minute window → Random Forest score → serving "
            "record → Streamlit. Raw ping 77975 on Nguyễn Bỉnh Khiêm (segment 21260) was recorded at "
            "07:59:35 ICT on 29 Dec 2020, with speed 4 km/h. Spark groups it into the 07:30–08:00 ICT "
            "window. The exported window has one observation, Vmax 40 km/h, ratio 0.10, and LOS F. The "
            "next slot is labeled 08:00 ICT and its observed LOS is F. The saved serving record predicts "
            "F with P(E/F) = 0.9971 and status version 2. Trace event_id 1e3aa19fb29455452105f2b2 "
            "through raw_data/segment_status.csv, data/dataset.csv, and "
            "data/lakehouse/serving_traffic_scores.parquet. Minh Quyền can show ETL, Thành Đạt the "
            "forecast, and Công Minh the serving lookup."
        ),
        26: (
            "Team answer. Kafka is optional for forecasting over the historical sample: CSV or Parquet "
            "can feed the same batch ETL and offline model evaluation. Kafka has a concrete role in the "
            "replay demo. It buffers events independently of the producer and exposes partition offsets "
            "to consumers. Removing it preserves historical reports and batch scores but removes "
            "broker-backed replay and weakens the evidence for the streaming design. See producer.py, "
            "spark_batch.py and outputs/replay_stream_summary.json."
        ),
        27: (
            "Team answer. Batch processing cleans historical GPS, aggregates windows, fits the model and "
            "recomputes corrected windows. Streaming ingestion publishes historical events to Kafka and "
            "the replay path exercises watermarking and checkpoint state. Online scoring applies the "
            "frozen model to available features. Online serving reads scores from MongoDB or the Parquet "
            "snapshot via Streamlit. Online learning is [N/A]; model fitting takes place offline. See "
            "pipeline.py, producer.py, starter_stream.py and app.py."
        ),
        28: (
            "Team answer. Deliverable A labels local Docker, Spark batch, Kafka broker delivery and "
            "MongoDB/Streamlit as [I]. The Kafka broker check published 3,000 messages across three "
            "partitions. The four-batch WAL recovery exercise uses a local replay DataFrame. City-wide "
            "live feeds, multi-node Kubernetes/YARN and automated signal control are [P]. See "
            "A_architecture.png, outputs/replay_stream_summary.json and outputs/query_progress.json."
        ),
        29: (
            "Team answer. If the replay scorer stops, provisional scores stop updating. The simulated "
            "restart restored Bloom, HLL and EWMA state, skipped the committed batch and recorded zero "
            "duplicate side effects in that run. If MongoDB is unreachable, Streamlit can show the "
            "latest saved serving Parquet snapshot, so live reads and freshness are affected. If Kafka "
            "is unavailable, broker replay pauses; historical batch ETL can still read its CSV inputs. "
            "See starter_stream.py, app.py and outputs/query_progress.json."
        ),
        # --- Section 2.3 One-Technology Deletion Test (P032) ---
        32: (
            "Team answer. Design-only deletion test: remove MongoDB and serve a refreshed Parquet "
            "snapshot. Functionality: historical views and batch forecasts still work; indexed per-event "
            "lookup and per-key versioned upserts would need a replacement. Scalability: frequent "
            "snapshot refreshes increase file I/O, and append-only micro-batches would require "
            "compaction. Latency: the measured MongoDB warm p95 is 0.636 ms; the Parquet alternative has "
            "no comparable benchmark. Complexity: one fewer service and simpler writes, with more work "
            "to publish a consistent snapshot. Assessment: the indexed NoSQL write/read-back "
            "demonstration would be lost. See app.py and outputs/mongo_verify.json."
        ),
        # --- Section 6.4 Evidence-Log Convention (P066) ---
        66: (
            "↳ [Completed Evidence Manifest in evidence/session9/ (Sample: 2020-07..2021-04, 90,938 "
            "pings, 84,633 segs)]:\n"
            "  • A_architecture.{png,md} — [MEASURED + DESIGN LABELS] End-to-end [I] vs [P] diagram and "
            "Section 2.1 trace (segment 21260).\n"
            "  • B_benchmark.{csv,png,md} — [MEASURED] Spark 3.5.7 controlled test (python "
            "spark_batch.py): SMJ vs BHJ vs 8-Salt Monoid.\n"
            "  • C_query_plan.{txt,md} — [MEASURED] Spark 3.5.7 EXPLAIN FORMATTED physical plans & "
            "Parquet Partition/Schema pruning.\n"
            "  • D_data_validation.{csv,md} — [MEASURED] 7-point Quality Funnel, conservation (0 Silent "
            "Drops), Vmax & Monoid parity.\n"
            "  • E_model_validation.{csv,md} — [MEASURED] 7-Model Ablation, 61-case Transition Regime "
            "eval & Future-Shuffle diff=0.0.\n"
            "  • F_serving_stream.{json,md} — [MEASURED] MongoDB 8.0 IXSCAN (0.636 ms p95) & Kafka 30m "
            "Watermark + WAL recovery.\n"
            "  • G_deployment.md — [MEASURED PoC + DESIGN SCALE-OUT] 4-container Docker (10.5/16 GB RAM) "
            "+ Cloud K8s design.\n"
            "  • H_project_card.{csv,png,md} (+ H_B1, H_B2, H_B3 CSVs) — [MEASURED] Card B 7 prompts, "
            "B1/B2/B3 tests & 60s defense.\n"
            "  • I_readiness.md & I_session9_summary_slide.png — [MEASURED] 16/16 check, 95/100 rubric & "
            "Session 10 summary slide."
        ),
        # --- Section 7 CARD B Context Verification (P086) ---
        86: (
            "↳ [Verified Project Context — Measured Real Data vs. Controlled Replay Simulation]:\n"
            "  • 100% Real Empirical Data (MEASURED): HCMC OpenStreetMap road topology (577,967 raw "
            "nodes -> 52,425 active intersections, 5,553 streets, 84,633 directed segments, 151,929 "
            "line-graph transitions) and 90,938 empirical GPS probe pings (segment_status.csv, "
            "2020-07-03 to 2021-04-22) aggregating into 33,441 30m windows across 10,027 observed "
            "segments. All Spark SQL monoids, Line-Graph PageRank, 7D K-Means, FPGrowth (335 rules), and "
            "23D Random Forest / GBT models are trained and evaluated on this real empirical data.\n"
            "  • Controlled Event-Time Stream Replay (SIMULATED TRANSPORT ONLY, seed=42): Because "
            "historical CSVs are archived, producer.py replays the real 33,441 empirical windows into "
            "Kafka 4.1.0 KRaft while injecting controlled transport anomalies (334 exact duplicates "
            "[1.0%], 1,170 in-watermark late events <=30m [3.5%], and 501 out-of-watermark late events "
            ">30m [1.5%]) solely to test 30m Watermark eviction, 32 KiB Bloom dedup, WAL crash recovery, "
            "and Tier-2 Batch Reconciliation. Zero synthetic speed or synthetic road records enter ML."
        ),
        # --- Section 7 CARD B: All 7 Technical Challenge Prompts (P088..P094) ---
        88: (
            "Team answer. The code labels each 30-minute window by its start. A prediction issued at "
            "10:00 ICT can use the completed [09:30, 10:00) window (slot label 09:30) to predict [10:00, "
            "10:30), whose LOS becomes observable at 10:30. A row labeled 10:00 already summarizes that "
            "latter window, so using it at a 10:00 wall-clock issue time would leak information. The "
            "evaluation pairs slot labels exactly 30 minutes apart. A wall-clock audit must also exclude "
            "arrivals after issuance and subsequent revisions.\n"
            "14 local inputs: current_los_ordinal, lag1_los_ordinal, lag2_los_ordinal, "
            "trailing_severe_rate, mean_velocity, velocity_ratio, obs_count, length_m, max_velocity, "
            "street_level, weekday, period_sin, period_cos, gap_minutes.\n"
            "9 graph and history inputs: pagerank_score, in_degree, out_degree, neighbor_los_ordinal, "
            "cluster_id, segment_train_prior_los, segment_train_severe_prior, street_causal_prior_los, "
            "street_causal_prior_sev. Lag features use previous observed windows, which can be more than "
            "30 minutes apart. Segment and street histories can include the current completed window. "
            "Train-fitted artifacts must already be available. The B1 CSV illustrates these rules; it "
            "has no per-feature arrival trace. See pipeline.py (FULL_ML_FEATURES, "
            "add_temporal_and_neighbor_lags and _attach_causal_priors_and_neighbors) and "
            "traffic_data/spark_etl.py."
        ),
        89: (
            "Team answer. Use “predictive ML with time-ordered features.” The code uses “causal” in the "
            "names of expanding history features. Random Forest and GBT estimate future LOS or "
            "severe-congestion probability from observations. A claim about the effect of changing a "
            "traffic signal would require an intervention or an identification design and assumptions "
            "about confounding. See pipeline.py and outputs/ablation_results.csv."
        ),
        90: (
            "Team answer. Across 33,441 observed segment-windows from July 2020 to April 2021, 10,027 of "
            "84,633 segments (11.85%) appear in the dataset. The other 74,606 (88.15%) have no observed "
            "GPS in this sample. Coverage varies by time: August 2020 has 3,640 distinct segments over "
            "7,294 windows, December has 4,042 over 15,105, and February 2021 has one segment and one "
            "window (grouped by UTC slot month). A zero-observation segment has unknown measured speed "
            "and LOS. The UI provides a labeled structural prior for these roads. See data/dataset.csv "
            "and H_B3_missingness_sketches.csv."
        ),
        91: (
            "Team answer. Line-graph PageRank measures structural importance of a road segment. In B2, "
            "Nguyễn Kiệm segment 21969 had PageRank 2.7313 and mean speed 45.41 km/h over 24 observed "
            "windows, with a mean speed ratio of 0.9082 and LOS A classification. This is a "
            "counterexample to using high PageRank alone as evidence of observed congestion. See "
            "evidence/session9/H_B2_graph_zone_falsification.csv."
        ),
        92: (
            "Team answer. The seven-feature K-Means model uses road geometry, speed and LOS profiles, "
            "severe rate and PageRank. There is no coordinate input or graph contiguity constraint. "
            "Cluster 1 includes Hà Huy Giáp, Lý Thường Kiệt and Quốc lộ 13. Report these as behavioral "
            "and morphological road clusters. See pipeline.py (CLUSTER_FEATURES) and "
            "H_B2_graph_zone_falsification.csv."
        ),
        93: (
            "Team answer. B3 reports 334/334 injected duplicates detected and a Bloom false-positive "
            "rate of 0.616%. A false positive can suppress a valid event in the provisional path and "
            "change a window score or alert; retaining Bronze events allows a later correction. The 1 "
            "KiB HLL sketch estimated 9,627.89 distinct segments vs 10,027 exact (3.98% error). This "
            "estimate feeds a coverage KPI and is outside the per-segment alert scoring path. See "
            "outputs/metrics.json and H_B3_missingness_sketches.csv."
        ),
        94: (
            "Team answer. Phase B issues a provisional score using the data available at issuance and "
            "status_version 1. Phase C recomputes from the retained events, backfilled 501 late windows "
            "in the replay and publishes corrected scores with version 2. The saved guard check rejected "
            "25/25 stale writes. Current storage keeps the latest score and lifecycle metadata per "
            "event_id. Preserving the original probability and LOS for audit still requires an "
            "append-only snapshot per issuance and version, with issued_at, model version, input cutoff "
            "and score. See pipeline.py (score_and_verify_serving_lifecycle), starter_stream.py and "
            "outputs/query_progress.json."
        ),
        # --- Section 7 CARD B: One-Minute Defense (P097) ---
        97: (
            "Team answer. At forecast time on a road with no GPS observation, the system knows its map "
            "geometry and road class, its connectivity and PageRank, and its behavioral cluster and "
            "historical street-level priors where available. These signals support a labeled cold-start "
            "estimate. Measured current speed and LOS remain unknown. In this dataset 74,606 of 84,633 "
            "segments have no observations, so model scores on observed TEST windows cannot validate "
            "cold-start accuracy. The dashboard should show that distinction to operators."
        ),
        # --- Section 10 Demo Evidence Sequence (P206..P213) ---
        206: (
            "↳ [Live Demo Asset]: Show Slide 05 & evidence/session9/A_architecture.png pointing out [I] "
            "solid vs [P] dashed flows."
        ),
        207: "Demo: trace raw GPS ping 77975 to the stored score for Nguyễn Bỉnh Khiêm, segment 21260 (Section 2.1).",
        208: (
            "↳ [Live Demo Asset]: Show 5-Tuple Monoid (cnt, sum_v, min_v, max_v, severe_cnt) + 8-Bucket "
            "Salting in traffic_data/spark_etl.py (diff=0.0)."
        ),
        209: (
            "↳ [Live Demo Asset]: Open evidence/session9/C_query_plan.txt showing BroadcastHashJoin (10->2 "
            "Exchanges) & Parquet PartitionFilters."
        ),
        210: (
            "↳ [Live Demo Asset]: Show outputs/ablation_results.csv, outputs/transition_evaluation.csv (61 "
            "transitions) & Card B Mini-Tests B1/B2."
        ),
        211: (
            "↳ [Live Demo Asset]: Show outputs/mongo_verify.json (33,441 docs, ESR IXSCAN p95=0.636 ms, "
            "nReturned=25, keysExamined=26) & live Tab 2."
        ),
        212: (
            "↳ [Live Demo Asset]: Show live Kafka->Consumer->MongoDB->Streamlit (:8501) monitor (200 msgs -> "
            "192 v1 upserts, 33 KiB Sketches, WAL 0-dup)."
        ),
        213: (
            "↳ [Live Demo Asset]: Close with evidence/session9/B_benchmark.png, Table 19 Honest Limitations "
            "(88.15% Cold-Start), and pytest 27/27."
        ),
    }


def get_docx_placeholder_blocks() -> dict[int, str]:
    """Return paragraph index -> multi-line block inserted into blank placeholder paragraphs."""
    return {
        18: (
            "[Team Output Standard Compliance]: Every section in this completed manual cites exact "
            "executable commands (python pipeline.py, python spark_batch.py, python starter_stream.py, "
            "pytest -v), measured numeric outputs from outputs/*.json and evidence/session9/*, artifact "
            "file paths, and explicit [I] Implemented vs [P] Proposed labels."
        ),
        22: (
            "[Deliverable A — Component-by-Component Truth Classification "
            "(evidence/session9/A_architecture.png)]:\n"
            "• [I] IMPLEMENTED & MEASURED (Solid Arrows): Zone A1 Raw CSVs (38.50 MB: 577,967 nodes, "
            "84,633 segments, 90,938 pings) | Zone A2 Kafka 4.1.0 KRaft Replay (producer.py, 3 "
            "partitions) | Zone B1 Ordered Quality Funnel (ANSI try_cast, NFC 695->649 streets, UTC+7 "
            "rollover, 0 Silent Drops) | Zone B2 Dead-Letter Quarantine (580 segments) & Spark WAL "
            "Checkpoints | Zone C Speed Layer (30m Watermark, Dual-Sink, 32 KiB Bloom k=4, 1 KiB HLL++ "
            "p=10, EWMA alpha=0.15, Read-Only Scorer v1) | Zone D Spark 3.5.7 Batch (8-Bucket Salted "
            "5-Tuple Monoid, BroadcastHashJoin, Line-Graph PageRank, 7D K-Means, 335 FPGrowth rules) | "
            "Zone E Frozen SHA-256 Registry (23D Random Forest, Train-Only 3-Tier Vmax, Static Feature "
            "Store 84,633 rows) | Zone F1 MongoDB 8.0.34 (33,441 docs, ESR IXSCAN 0.636 ms p95) | Zone "
            "F2 Parquet Lakehouse (partitionBy year_month, 10 dirs) | Zone F3 5-Tab Streamlit TOC UI "
            "(:8501).\n"
            "• [P] PROPOSED / DESIGN-ONLY (Dashed Arrows): Zone A3 Live City-Wide Telco/Bus/Taxi Stream "
            "(8–17B pings/day, 1.5–3.0 TB/day) | Multi-Node Cloud Kubernetes (EKS/GKE SparkOperator) "
            "Auto-Scaling & S3 Delta Lake | Closed-Loop Automated SCATS Traffic-Light Hardware "
            "Actuation.\n"
            "• [N/A] NOT USED: Cassandra, Redis, Legacy Java MapReduce Daemons, Online Streaming Weight "
            "Updates."
        ),
        30: (
            "Architecture checks completed above. Broker delivery, simulated restart and stored serving "
            "output have separate evidence."
        ),
        40: (
            "Answer 1: Switching from SortMergeJoin to BroadcastHashJoin pinned segments (84,633 rows, "
            "~6.8 MB) & streets (5,553 rows, ~0.4 MB) in executor RAM, cutting Catalyst Exchange shuffle "
            "stages from 10 -> 2 (-80%) and runtime by 25.7% (1,418.60 -> 1,054.17 ms). Adding 2-Stage "
            "8-Bucket Salted 5-Tuple Monoid collapsed peak partition skew from 687.43x -> 1.023x "
            "(CV=0.0057) with +19.20 ms overhead (1,073.37 ms) and 0.0 error."
        ),
        42: (
            "Answer 2: BroadcastHashJoin stops scaling if dimension tables grow beyond executor "
            "broadcast memory (>100–256 MB, risking OOM). Fixed 8-bucket salting stops scaling when a "
            "single segment exceeds ~10^7 pings/30m on a >64-core cluster, requiring adaptive skew "
            "salting (32–64 buckets) or Spark AQE skew join."
        ),
        44: (
            "Answer 3: This experiment ran on a single-host 4-container Docker setup (10.5/16 GB RAM) "
            "over a 38.50 MB (90,938 pings) Proof-of-Scale dataset where shuffles traverse local "
            "disk/loopback rather than a multi-rack network fabric under 1.5–3.0 TB/day city-wide load. "
            "(Measured chart embedded below: evidence/session9/B_benchmark.png)"
        ),
        46: (
            "[Verified Spark 3.5.7 Physical Plan Excerpts — Command: python spark_batch.py | File: "
            "evidence/session9/C_query_plan.txt]:\n"
            "  1) Baseline (autoBroadcastJoinThreshold=-1): SortMergeJoin [street_id] -> Sort -> "
            "Exchange hashpartitioning(street_id, 8) -> SortMergeJoin [segment_id] -> Sort -> Exchange "
            "hashpartitioning(segment_id, 8) [10 Exchange stages total].\n"
            "  2) Optimized: BroadcastHashJoin [street_id], BuildRight -> BroadcastHashJoin "
            "[segment_id], BuildRight -> HashAggregate(keys=[segment_id, date, period], "
            "functions=[count, sum, min, max, sum(is_low_speed)]) -> Exchange hashpartitioning(..., 8) "
            "[ONLY 2 Exchange stages total; two_way_exceptAll_diff = 0].\n"
            "  3) Parquet Pruning: FileScan parquet [segment_id, date, period, LOS, year_month] "
            "PartitionFilters: [isnotnull(year_month), (year_month = 2020-08)], ReadSchema: "
            "struct<segment_id:bigint,date:string,period:string,LOS:string>."
        ),
        51: (
            "Model verification. Reloaded the frozen Random Forest and imputer in outputs/model.joblib "
            "and scored data/dataset.csv without refitting. The 1,182 labeled TEST rows reproduce "
            "six-class accuracy 94.50%, macro-F1 0.5543, severe ROC-AUC 0.9882 and average precision "
            "0.9471. Among the 61 true changes, the default prediction is correct 2 times; the threshold "
            "of 0.30 selected on VAL raises this to 9. Ranking only alternative states with the true "
            "change assumed known gives 24 correct. For detecting whether LOS changes across all TEST "
            "rows, ROC-AUC is 0.7664 and AP is 0.3007. The separate severe-onset threshold of 0.15 "
            "detects 0 of 8 new E/F cases. Sources: ablation_results.csv, transition_evaluation.csv and "
            "metrics.json."
        ),
        56: (
            "[Verified MongoDB 8.0 Execution Evidence — File: outputs/mongo_verify.json & "
            "evidence/session9/F_serving_stream.json]:\n"
            "  • Write Proof: 33,441 docs written across 4 Spark partitions (8,131 / 8,485 / 8,222 / "
            "8,603 rows in 69 bulk UpdateOne batches of 500); count_after_idempotent_replay = 33,441 (0 "
            "duplicates).\n"
            "  • Read & Explain Proof: Winning plan LIMIT(25) -> PROJECTION_SIMPLE -> FETCH -> "
            "SORT_MERGE -> IXSCAN on level_prob_segment_idx {street_level:1, severe_prob:-1, "
            "segment_id:1} with in_memory_sort=False, nReturned=25, totalKeysExamined=26, "
            "totalDocsExamined=25, warm_median_ms=0.530 ms, warm_p95_ms=0.636 ms (<10 ms SLA verified)."
        ),
        58: (
            "[Verified Streaming & WAL Recovery Accounting — Command: python starter_stream.py | File: "
            "outputs/replay_stream_summary.json]:\n"
            "  • Event Accounting: 33,775 stream window events processed = 33,441 unique valid windows + "
            "334 injected duplicates (334/334 caught by 32.0 KiB bit-packed Bloom Filter, empirical "
            "FPR=0.616%).\n"
            "  • Watermark & Two-Tier Reconciliation: 1,170 in-watermark late events (<=30m) processed "
            "online (98.5% provisional completeness, tier1_exceptAll_diff=0); 501 out-of-watermark late "
            "segment-windows (>30m) + 3,953 downstream lag windows healed to 100.0% completeness by "
            "nightly Tier-2 Batch Reconciliation from Sink 1."
        ),
        64: (
            "[Deployment Envelope Summary]: All [I] measurements above were executed inside our "
            "reproducible 10.5 / 16 GB RAM 4-container Docker Compose cluster over the 38.50 MB HCMC "
            "dataset, while multi-node Kubernetes auto-scaling and automated SCATS signal actuation are "
            "transparently separated as [P] Proposed production extensions."
        ),
        70: (
            "[Team Card Selection]: Our project is assigned to CARD B — HCMC Traffic, 30-Min Congestion "
            "Forecast & Graph Analytics (completed in full below, including context verification, all 7 "
            "technical prompts, executable Mini-Tests B1/B2/B3, and the 60-second defense). Cards A and "
            "C–H are marked [N/A — Assigned to Card B]."
        ),
        96: (
            "Card B evidence. B1: the 46-row matrix illustrates feature timing, and code tests check "
            "exact slot alignment and selected future perturbations. Production arrival times remain "
            "unverified. B2: Nguyễn Kiệm segment 21969 gives a high-PageRank/free-flow counterexample; "
            "K-Means groups structurally similar roads across areas. B3: the controlled HLL estimate "
            "differs from the exact count by 3.98%; the Bloom false-positive rate is 0.616%. See the "
            "three Card B CSVs and the answers above."
        ),
        214: (
            "Readiness slide snapshot. Use the verified metrics and definitions in Sections 4.2 and 10. ("
            "& evidence/session9/I_session9_summary_slide.png)]:"
        ),
        216: (
            "Audit Package Verified: All tables, section questions (Section 2.2, 2.3, Section 3.1, "
            "Section 7 Card B 7 prompts + B1/B2/B3 raw_data<->data cross-check + 60s defense), embedded "
            "visual proofs, 22/22 platform tests, and 5/5 Spark tests pass (27/27)."
        ),
    }
