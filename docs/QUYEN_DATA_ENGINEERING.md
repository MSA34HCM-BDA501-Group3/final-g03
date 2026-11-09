# Quyền: Spark data engineering handoff

Hướng dẫn đầy đủ bằng tiếng Việt và checklist nghiệm thu: [HUONG_DAN_BRANCH_QUYEN_VI.md](HUONG_DAN_BRANCH_QUYEN_VI.md).

Branch: `feat/quyen-spark-data-engineering`. This is a separate raw-data batch path;
the legacy `pipeline.py` and its UI output contracts are not switched automatically.

## Run

```powershell
$env:TRAFFIC_FIT_CUTOFF = '2020-08-01T00:00:00Z'
docker compose -f compose.quyen.yml run --build --rm batch
docker compose -f compose.quyen.yml run --rm --entrypoint python batch -m unittest discover -s tests -p test_quyen_spark.py -v
```

Every run requires a **new output directory**. Existing snapshots are rejected,
including incomplete runs, to protect immutable Bronze and preprocessing artifacts.
Choose a new `--output-root` using `docker compose ... run batch --output-root ...
--train-end ...`. No code requires `train.csv`. Runtime is PySpark 4.2.0; the provided
Dockerfile uses Java21, an explicit deviation from proposal Java17. The new image
was built successfully and both the final focused tests and raw-data snapshot run
were executed from its packaged source, without mounting repository code.

TRAIN/warmup fit cutoff is mandatory and **exclusive**, UTC. Do not reuse the example
date without agreeing with Đạt on temporal boundaries. A fit-on-TRAIN lookup may
only be applied causally after that fit cutoff. For supervised rows within TRAIN,
Đạt must use warmup or temporal out-of-fold lookup snapshots. Batch generation of
historical windows before cutoff is allowed for audit, not proof of leak-free ML.

## Shared contract for Minh / Đạt (Q11)

`traffic_data.spark_etl` functions accept/return Spark DataFrames and are usable in
`foreachBatch`; they do not open Kafka/Mongo or train ML models. A streaming consumer
must keep delivery dedup state across batches; calling `deduplicate_events` per
microbatch alone does not dedup across microbatches.

| Field | Meaning |
|---|---|
| raw_event_id | Stable **ping identity**, mapped from source `_id`; retry preserves it |
| segment_id | bigint FK to accepted/repairable GIS topology |
| velocity | Finite double, inclusive [0,200] km/h; zero stays zero |
| updated_at | String event time, must include UTC/Z or offset at producer boundary |
| event_ts | Parsed UTC Spark timestamp; session timezone is explicitly UTC |
| raw_payload/source/audited_at | Original input fields, source identity and audit timestamp |
| first_failing_rule | One ordered rule per rejected record, never multiple rule rows |
| window_start | UTC 30-minute tumbling start; canonical aggregation key with segment_id |
| window_id | SHA256(segment_id + UTC window_start); different from raw_event_id |
| obs_count/sum_velocity/min_velocity/max_velocity_obs/severe_ping_count | Five-part monoid, severe ping means velocity<15km/h (not LOS label) |
| mean_velocity | sum_velocity / obs_count; no clipping, no mean of partial means |
| max_velocity/vmax_impute_tier | Frozen TRAIN GPS P85 → TRAIN median(level,type) → 80/60/50/40 level prior |
| observed_max_velocity | Original GIS metadata, retained separately; not silently substituted into GPS lookup |
| date/date_utc/period_idx/weekday | ICT local date (`date`), UTC calendar date (`date_utc`), slot 0–47 and Monday 0–Sunday 6, including midnight rollover |
| period_sin/period_cos | ICT cyclic slot, no learned category vocabulary |
| LOS | A≥.80, B≥.65, C≥.50, D≥.35, E≥.20, F otherwise, from mean_velocity/max_velocity |
| length_imputed | Invalid length was repaired using positive GIS median, fallback27m if unavailable |

Rules: E1 identity/segment parse → E2 timestamp parse → E3 velocity → E4 FK.
After valid parsing, different canonical payloads with one raw_event_id are all
quarantined as E5. Same-ID/same-payload retries collapse once. Distinct pings within
the same window remain distinct. Source IDs are assumed unique across this dataset;
Minh must namespace them if multiple fleets/sources overlap.

GIS rules: S1 keys → S2 length → S3 road type → S4 level. Invalid length is logged in
quarantine **and** explicitly repaired for otherwise valid road entities; therefore
GIS quarantine is an audit ledger and need not be disjoint from accepted geometry.
Invalid topology keys/road types/levels are excluded. Unknown street/node references
fail the batch to prevent silent broken joins. `normalize_name` applies NFC, Ð→Đ
and whitespace normalization; administrative entity filtering for mining remains
Đạt's responsibility.

Event accounting: input deliveries = accepted unique + duplicate deliveries removed
+ rejected deliveries (including all conflicting-ID deliveries). No malformed event
is repaired using LOS or target labels. Strict malformed CSV structure fails ingestion
rather than silently truncating columns; semantically invalid cells go to quarantine.

## Outputs and evidence

- `bronze_raw_events/`: original string GPS deliveries, including rejected/retries.
- `quarantine/events/`, `quarantine/segments/`: original payload + first failure.
- `active_nodes/`, `segments/`: pruned GIS and normalized accepted/repaired geometry.
- `preprocessing/velocity_lookup/`: frozen lookup with explicit cutoff provenance.
- `silver_cleaned_windows/`: exact dedup + salted five-part aggregation.
- `curated_gold/year_month=*/`: derived LOS, features and ICT monthly partitions.
- `window_metrics/batch_id=0/`: current finite batch aggregate snapshot.
- `manifest.json`: input SHA256, counts, all-field independent baseline comparison,
  salted-vs-unsalted and join two-way exceptAll, measured query timings/Exchanges.
- `sort_merge_plan.txt`, `broadcast_plan.txt`, `pruning_plan.txt`: actual executed plans.
- `spark_events/`, `task_metrics.json`: actual task durations, shuffle records/bytes,
  success and partition IDs for matched 1-stage unsalted vs 2-stage salted workloads.

Do not infer speedup from one ordered local benchmark: JIT/cache effects and input
partitioning matter. Inspect per-stage/task distributions; both algorithms may be
balanced on this small dataset. No fixed expected Exchange count is asserted.

## Checklist status

Q01–Q11 have source, focused Spark tests, and integrated team confirmations (`D-C01..D-C08`, `M-C01..M-C11` in `HUONG_DAN_BRANCH_QUYEN_VI.md`).
Tests deliberately mutate every aggregate, remove/add/duplicate keys, inject bad
GPS, change future velocities, exercise ID collisions, LOS boundaries/zero velocity,
ICT date rollover (`date` + `date_utc`), and actual Spark `BroadcastHashJoin`/Parquet pruning.

## Validation on 08–09/11/2026

- Verified on both **Apache Spark 3.5.7 (Java 17)** (`song-recommender-spark:3.5.7`) and **PySpark 4.2.0 (Java 21)** (`Dockerfile.spark`).
- Unit & integration tests: **5/5 Spark tests pass (`test_quyen_spark.py`)** and **14/14 Platform V3 tests pass (`test_platform_v3.py`)** (`19/19` total in `pytest -v`).
- Full raw snapshot using exclusive warmup cutoff `2020-08-01T00:00:00Z`, `local[2]`, 4 shuffle partitions:
- 90,938 input deliveries = 90,787 accepted unique + 151 quarantined + 0 retries.
  All 151 are `E4_UNKNOWN_SEGMENT` after 427 non-road GIS entities (`S3_NON_ROAD_TYPE`) were excluded (accounting for exactly `33,441 - 33,394 = 47` windows).
- 33,394 accepted windows, 52,181 active nodes (`52,425` over all `84,633` segments).
- 580 GIS quarantine records use first-failure accounting (`154` `S2_NONPOSITIVE_LENGTH` + `426` `S3_NON_ROAD_TYPE`, with 1 overlapping segment).
- Independent raw Python fold: **0 mismatches in all five monoid fields and mean**;
  **0 missing/extra/duplicate window keys**. Salted (2-stage) vs unsalted (1-stage) and baseline vs broadcast two-way `exceptAll` both `0`.
- Final executed optimized plan contains **three `BroadcastHashJoin`** nodes (`0` `SortMergeJoin`s) for `segments ⋈ lookup`, `windows ⋈ dim`, and `result ⋈ streets`; observed Exchange nodes drop from `5` shuffle `Exchange`s to `3` `BroadcastExchange`s (`0` shuffle `Exchange`s).
- Timing sample on Spark 3.5.7: sort-merge `1.040s` vs broadcast `0.721s`; 1-stage unsalted `0.400s` vs 2-stage salted `0.496s` (with final reducer stage max/median task ratio `1.15` vs `1.25`).
- Compact evidence is preserved in [`outputs/quyen_validation/`](../outputs/quyen_validation/).

API reference checked while implementing: [Spark try_cast](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.try_cast.html),
[Spark percentile_approx](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html).

