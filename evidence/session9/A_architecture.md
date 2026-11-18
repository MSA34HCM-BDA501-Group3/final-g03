# Deliverable A — End-to-End Integration Truth Audit (`A_architecture`)

- **Data Period / Sample:** HCMC Road Network (`84,633` directed segments, `577,967` raw OSM nodes, `5,553` streets) + Empirical GPS Probe Dataset (`90,938` raw GPS pings -> `33,441` 30-minute windows across `2020-07` to `2021-04`).
- **Diagram File:** `evidence/session9/A_architecture.png`
- **Status Classification:**
  - **`[I] Implemented` (Measured & Verified in Repository / Docker Compose):**
    1. **Zone A1 & A2 (`producer.py` + `Kafka 4.1.0 KRaft`):** Reads raw CSV/Parquet telemetry and replays event-time-stamped JSON messages into 3 Kafka partitions (`topic = hcmc-traffic-events`, partitioned by `segment_id`).
    2. **Zone B1 & B2 (`pipeline.py`, `traffic_data/spark_etl.py`, `starter_stream.py`):** Ordered 5-step Quality Funnel (`ANSI try_cast`, NFC normalization `695 -> 649` streets, `UTC+7` rollover for `4,320` windows, `0 Silent Drops`), Dead-Letter Quarantine (`quarantine_events.parquet`), and WAL Checkpoints (`offset_log.json` & `state_wal.json`).
    3. **Zone C (`starter_stream.py` & `spark_engine.py`):** Dual-Sink Stream Router (`Sink 1` appends 100% raw events to `bronze_raw_events`; `Sink 2` enforces `30m Event-Time Watermark`), `33.0 KiB` bounded-memory sketches (`32.0 KiB` bit-packed Bloom Filter, `1.0 KiB` HyperLogLog++, bounded `EWMA` $\alpha=0.15$), and read-only online `.predict_proba()` scoring.
    4. **Zone D (`spark_batch.py`, `traffic_data/spark_etl.py`, `pipeline.py`):** Spark 3.5.7 Batch ETL with `2-Stage 8-Bucket Key Salting + 5-Tuple Monoid`, `BroadcastHashJoin`, orphan node pruning (`577,967 -> 52,425` active nodes), `Line-Graph PageRank` ($\beta=0.85$), `SON/FPGrowth` (`335` rules), and `7D K-Means` (`k=6`).
    5. **Zone E (`outputs/model.joblib`, `outputs/vmax_lookup.json`, `data/lakehouse/segment_static_features.parquet`):** Phase A Frozen Registry locked with `SHA-256` manifest (`outputs/model_registry_manifest.json`).
    6. **Zone F1, F2, F3 (`data/lakehouse/curated_gold`, `MongoDB 8.0.34`, `app.py`):** Columnar Parquet partitioned by `year_month`, MongoDB `bda501.traffic_scores` (`33,441` upserted docs, `ESR Compound Index IXSCAN = 0.636 ms`), and 5-tab Streamlit Dashboard (`localhost:8501`).
  - **`[P] Proposed / Design-Only` (Explicitly labeled as architectural scale-out design):**
    1. Live telco/bus/ride-hailing city-wide streaming feed (`8–17` billion pings/day, `1.5–3.0 TB/day`).
    2. Multi-node cloud Kubernetes / YARN auto-scaling cluster (current runtime is a 4-container Docker Compose cluster capped at `10.5 / 16 GB RAM` on a single host).
    3. Direct hardware actuation into HCMC `SCATS` traffic signal controllers (current system is human-in-the-loop Decision Support for `TOC` operators).
