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
| date/period_idx/weekday | ICT local date, slot0–47 and Monday0–Sunday6, including midnight rollover |
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
  success and partition IDs for matched salted/unsalted workloads.

Do not infer speedup from one ordered local benchmark: JIT/cache effects and input
partitioning matter. Inspect per-stage/task distributions; both algorithms may be
balanced on this small dataset. No fixed expected Exchange count is asserted.

## Checklist status

Q01–Q07/Q09–Q11 have source and focused Spark tests. Q08 has measured task extraction
and matched-workload execution, but requires repeated/reversed-order runs before a
performance conclusion. Full acceptance also requires review of generated evidence,
integration with Đạt/Minh; these are not silently marked done
in the original proposal audit. This change does not claim that Kafka/streaming,
ML forecasting, live Mongo or recovery are implemented.

Tests deliberately mutate every aggregate, remove/add/duplicate keys, inject bad
GPS, change future velocities, exercise ID collisions, LOS boundaries/zero velocity,
ICT date rollover and actual Spark BroadcastHashJoin/Parquet pruning.

## Validation on 08/11/2026

- Final image: `final-g03-quyen-spark:validation`, PySpark4.2.0, Java21.
- Packaged-source unittest: **5/5 pass**; original prototype regression functions:
  **11/11 pass** (called directly; legacy image still lacks pytest).
- Full raw snapshot: `data/quyen_spark_validation_03/`, using exclusive warmup cutoff
  `2020-08-01T00:00:00Z`, local[2], 4 shuffle partitions.
- 90,938 input deliveries = 90,787 accepted unique + 151 quarantined + 0 retries.
  All151 are E4_UNKNOWN_SEGMENT after non-road GIS entities were excluded.
- 33,394 accepted windows, 52,181 active nodes. These differ from proposal's unfiltered
  33,441/52,425 because this path enforces the road acceptance policy. The policy
  needs team agreement before comparing historical metrics with the proposal.
- 580 GIS quarantine records use first-failure accounting; length repairs may also
  appear in accepted geometry. This is not a blind 154+427 sum with overlap.
- Independent raw Python fold: **0 mismatches in all five monoid fields and mean**;
  **0 missing/extra/duplicate window keys**. Salted/unsalted and baseline/broadcast
  two-way exceptAll both0.
- Final executed optimized plan contains **two BroadcastHashJoin** nodes for the
  window/GIS and street catalog joins; observed Exchange nodes5→4, not fabricated7→3.
- One local timing sample: sort-merge1.393s vs broadcast1.266s; unsalted0.937s vs
  salted0.883s. These are observations, not reliable performance guarantees.
- Compact evidence is preserved in [`outputs/quyen_validation/`](../outputs/quyen_validation/).
  Raw Spark event logs and Parquet remain under the ignored data snapshot directory.
- Compose configuration validation passed. README and source contracts are linked;
  the existing legacy output artifacts were not regenerated.

API reference checked while implementing: [Spark try_cast](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.try_cast.html),
[Spark percentile_approx](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html).
