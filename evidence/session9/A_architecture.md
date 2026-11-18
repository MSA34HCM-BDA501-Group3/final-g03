# Deliverable A — End-to-End Integration Truth Audit (`A_architecture`)

- **Data Period / Sample:** HCMC Road Network (`84,633` raw -> `84,206` clean motorized segments, `577,967` raw -> `52,181` active intersections, `5,553` streets) + Empirical GPS Probes (`90,938` raw -> `90,787` accepted pings -> `33,394` 30m windows across `2020-07-03` to `2021-04-22`).
- **Diagram File:** `evidence/session9/A_architecture.png`
- **Status Classification:**
  - **`[I] Implemented` (Measured & Verified in Repository / Docker Compose):**
    1. **Zone A1 & A2 (`producer.py` + `Kafka 4.1.0 KRaft`):** Reads raw CSV/Parquet telemetry and replays event-time-stamped JSON messages into 3 Kafka partitions (`topic = hcmc-traffic-events`, partitioned by `segment_id`).
    2. **Zone B1 & B2 (`pipeline.py`, `traffic_data/spark_etl.py`, `starter_stream.py`):** Ordered Quality Funnel (`ANSI try_cast`, NFC `695 -> 641` streets, `0 Silent Drops`), Dead-Letter Quarantine (`580` bad segment records, `151` rejected pings), and WAL Checkpoints (`73,713 B`).
    3. **Zone C (`starter_stream.py` & `spark_engine.py`):** Dual-Sink Stream Router (`Sink 1` appends 100% raw events; `Sink 2` enforces `30m Event-Time Watermark`), `33.0 KiB` bounded sketches (`32.0 KiB` Bloom, `1.0 KiB` HLL++, bounded `EWMA` $\alpha=0.15$), and read-only `.predict_proba()`.
    4. **Zone D (`spark_batch.py`, `traffic_data/spark_etl.py`, `pipeline.py`):** Spark 3.5.7 Batch ETL (`groupexe-knowledgetest-spark-1`) with `2-Stage 8-Bucket Key Salting + 5-Tuple Monoid`, `BroadcastHashJoin` (`5 -> 1` Exchanges), orphan node pruning (`577,967 -> 52,181` active nodes), `Line-Graph PageRank` (`92,453` edges, `314` dangling), `FPGrowth` (`335` rules), and `7D K-Means` (`k=6`).
    5. **Zone E (`outputs/model.joblib`, `outputs/vmax_lookup.json`, `data/lakehouse/segment_static_features.parquet`):** Phase A Frozen Registry locked with `SHA-256` manifest (`outputs/model_registry_manifest.json`).
    6. **Zone F1, F2, F3 (`data/lakehouse/curated_gold`, `MongoDB 8.0.34`, `app.py`, `toc_backend`):** Columnar Parquet partitioned by `year_month`, MongoDB `bda501.traffic_scores` (`33,394` docs, `ESR IXSCAN` live median `0.870 ms`, saved p95 `0.692 ms`), Streamlit Dashboard (`:8501`), and React+FastAPI TOC UI (`:8000`).
  - **`[P] Proposed / Design-Only`:**
    1. Live telco/bus/ride-hailing city-wide streaming feed (`8–17` billion pings/day, `1.5–3.0 TB/day`).
    2. Multi-node cloud Kubernetes (`EKS/GKE SparkOperator`) / `YARN` auto-scaling & `S3 Delta Lake`.
    3. Closed-loop hardware actuation into HCMC `SCATS` traffic signal controllers.
