# Deliverable F — NoSQL Serving & Streaming Audit (`F_serving_stream`)

## 5.1 MongoDB 8.0 Serving Store (`bda501.traffic_scores`)
- **Exact Document Written:** `33,441` JSON documents (`event_id`, `segment_id`, `street_name`, `street_level`, `slot_ts`, `mean_velocity`, `max_velocity`, `LOS`, `predicted_los_30m`, `severe_prob`, `ewma_hotspot_score`, `lifecycle_status` [`STREAM_PROVISIONAL` v1 vs `RECONCILED_COMPLETE` v2], `status_version`, `version_ts`, `model_sha256`).
- **Primary Read-Back Query:** Top-K arterial early-warning lookup for `TOC` dashboard:
  `db.traffic_scores.find({"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}}).sort([("severe_prob", -1), ("segment_id", 1)]).limit(50)`
- **Key & Index Design:** Unique key `event_id` (`{segment_id}_{date}_{period}`) + ESR Compound Index `{"street_level": 1, "severe_prob": -1, "segment_id": 1}`.
- **Measured Read/Write Proof (`outputs/mongo_verify.json`):** `33,441` docs upserted across `4` Spark partitions (`69` bulk batches of `500` docs); idempotent replay count = `33,441` (`0` duplicates). `explain("executionStats")` confirms **`IXSCAN` = `0.636 ms` (`p50 = 0.530 ms`) vs `COLLSCAN` = `5.950 ms` (`9.35x` speedup, `0` in-memory `SORT` stage)**.

## 5.2 Streaming & Near-Real-Time Design (`producer.py` + `starter_stream.py`)
- **Nature of Source:** Historical HCMC GPS telemetry (`90,938` raw pings / `33,441` 30m windows) replayed deterministically (`seed=42`) through **`Apache Kafka 4.1.0 KRaft`** (`topic = hcmc-traffic-events`, `3` partitions keyed by `segment_id`) preserving original event timestamps (`event_ts`).
- **Event-Time Watermark & Dual-Sink:** `Sink 1` appends `100%` of valid events directly to `bronze_raw_events` before watermark filtering; `Sink 2` enforces a **`30-minute Event-Time Watermark` (`1,800s`)**, routing on-time/in-watermark events (`98.5%`) to `33.0 KiB` bounded sketches (`32.0 KiB` Bloom Filter, `1.0 KiB` HLL++, `EWMA` $\alpha=0.15$) and read-only `.predict_proba()` scoring (`v1`).
- **Late Data & Crash Recovery:** `501` out-of-watermark late windows (`1.5%`, `>30m` delay) are safely excluded from `Sink 2` RAM state (`98.51%` provisional completeness) and recovered by nightly Batch **Tier-2 Reconciliation** from `Sink 1` to reach **`100.0%` (`33,441 / 33,441`)** completeness (`v2`) with `0` stale overwrites (`400` stale `v1` replays rejected by `status_version` guard). Simulated container kill & WAL restart (`outputs/checkpoints/`) recovers with **`0` duplicate or lost records**.
