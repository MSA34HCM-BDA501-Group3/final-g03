# Deliverable F — NoSQL Serving & Streaming Audit (`F_serving_stream`)

## 5.1 MongoDB 8.0 Serving Store (`bda501.traffic_scores`)
- **Exact Document Written:** `33,394` JSON documents (`1,234` arterial severe alerts; schema: `event_id`, `segment_id`, `street_name`, `street_level`, `date`, `period`, `current_los`, `predicted_next_los`, `severe_prob`, `pagerank_score`, `cluster_id`, `lifecycle_status`, `status_version`, `model_version`).
- **Primary Read-Back Query:** Top-K arterial early-warning lookup for `TOC` operators:
  `db.traffic_scores.find({"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}}).sort([("severe_prob", -1), ("segment_id", 1)]).limit(25)`
- **Key & Index Design:** Unique key `event_id` + ESR Compound Index `level_prob_segment_idx` `{"street_level": 1, "severe_prob": -1, "segment_id": 1}`.
- **Measured Read/Write Proof (`groupexe-knowledgetest-mongo-1` & `outputs/mongo_verify.json`):** `33,394` docs upserted across `4` partitions (`69` bulk batches), idempotent replay = `33,394`, atomic `$status_version` pipeline guard rejected `25/25` stale v1 retries. `explain('executionStats')` confirms `IXSCAN` (`in_memory_sort=False`, `nReturned=25`, `totalKeysExamined=26`, `totalDocsExamined=25`): live interleaved (5 warmup + 25 runs) `IXSCAN median = 0.940 ms` (`p95 = 1.220 ms`) vs `COLLSCAN = 8.200 ms` (`8.72x`); saved `mongo_verify.json` `warm_median = 0.360 ms`, `warm_p95 = 0.603 ms` vs `COLLSCAN = 11.289 ms` (**`31.36x` speedup**).

## 5.2 Streaming, Spark Structured Streaming & Live Kafka Monitor
- **Historical Replay Nature:** Historical HCMC GPS telemetry (`90,938` pings / `33,394` 30m windows, `seed=42`) replayed through **`Apache Kafka 4.1.0 KRaft`** (`hcmc-traffic-events`, 3 partitions).
- **Spark 3.5.7 Structured Streaming (`outputs/spark_runtime_evidence.json`):** Sink 2 watermark `2020-12-01T12:30:00.000Z`, `3` batches committed, `watermark_advanced = True`.
- **Live Kafka Monitor (`outputs/live_kafka_telemetry.json`):** `6,000` messages consumed (`242` batches), `3,060` duplicates filtered by `32.0 KiB` RotatingBloomFilter, `61` late >30m held for Tier-2, `2,879` `STREAM_PROVISIONAL (v1)` scored (`HLL++ = 2,994.6` est. active segments, `1,322` active EWMA decay states).
- **Two-Tier Reconciliation & WAL Recovery (`outputs/query_progress.json`):** `tier1_exceptAll_diff_count = 0` (`100.0%` in-WM match); `500` out-of-WM late windows + `609` downstream lag windows healed (`98.5% -> 100.0%`), `384` rescored (`213` prob changed); `RotatingBloomFilter 32.0 KiB` (`k=10`, `13` FPs, `0.0389%` empirical vs `0.0792%` theoretical), `HLL++ 1.0 KiB` (`9,620.2` vs `9,995`, `3.75%` err), WAL `73,714 B`.
