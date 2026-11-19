# Deliverable G — Cloud/Cluster Deployment, Reproducibility & Limitations (`G_deployment`)

## 6.1 Deployable Spark Architecture (`docker-compose.yml` -> Cloud Kubernetes / YARN)
| Deployment Element | Implemented Local Cluster (`[I]`) & Proposed Cloud Scale-Out (`[P]`) |
|---|---|
| **Cluster Manager** | `[I]` Docker Compose `5` default services (`kafka 1536M`, `mongo 2048M`, `spark 6144M`, `toc-web 1024M :8000`, `live-consumer 512M` = `11.0 GB RAM` max limit sum) + `batch`/`stream` profiles (`docker compose up -d --build`). `[P]` Managed Cloud Spark on Kubernetes (`EKS/GKE SparkOperator`) or `EMR YARN`. |
| **Driver Placement** | `[I]` Co-located in `groupexe-knowledgetest-spark-1` (`4 GB` heap, `4` cores). `[P]` Dedicated cluster-mode driver pod (`4 vCPU`, `8 GB RAM`) isolated from UI traffic. |
| **Executor Placement & Scaling** | `[I]` `4` executor threads (`spark.sql.shuffle.partitions = 8`). `[P]` Dynamic Allocation (`2..16` pods) scaling on Kafka lag & `8-64` salt buckets. |
| **Storage Location & Access** | `[I]` Bind-mounted Columnar Parquet (`data/lakehouse/curated_gold`, `partitionBy('year_month')`). `[P]` Object storage (`S3 / GCS / MinIO`) with `Delta Lake / Iceberg`. |
| **Fault Tolerance & Checkpoints** | `[I]` Dual-Sink WAL (`73,714 B` + Spark WAL) + MongoDB `33,394` docs with atomic `$status_version` guard + SHA-256 Lock (`model=a2f411846508`, `static_store=75328ac2d5e3`, `vmax_lookup=ee7f692daad8`). |
| **Observability** | `[I]` JSON audit logs (`metrics.json`, `spark_runtime_evidence.json`, `mongo_verify.json`, `query_progress.json`, `live_kafka_telemetry.json`) surfaced in the unified **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)**. |
| **Security & Least Privilege** | `[I]` `.gitignore` blocks `.env`/`*.pem`/`*.key`; read-only SHA-256 verification blocks online model tampering. |
| **Cost / Performance Trade-Off** | Trading `+87.92 ms` 2-stage salting overhead and `33.0 KiB` sketches (`3.75%` HLL++ error, `0.0389%` Bloom FPR) to eliminate skew (`684.95x` -> `1.0197x`). |

## 6.2 Reproducibility Smoke Test (7/7 Verified `[x]`)
- `[x]` **Software versions recorded:** Python `3.13`, PySpark `3.5.7`, Kafka `4.1.0`, MongoDB `8.0.34`, scikit-learn (`Dockerfile`, `Dockerfile.spark35`, `requirements.txt`).
- `[x]` **Dependencies/configuration recorded:** `requirements.txt`, `docker-compose.yml`.
- `[x]` **Exact run commands available:** `docker compose up -d --build`, `python pipeline.py`, `python spark_batch.py`, `python starter_stream.py`, `pytest -v`.
- `[x]` **Deterministic seed set:** `RANDOM_SEED = 42` across `pipeline.py`, `producer.py`, `starter_stream.py`, `spark_engine.py`.
- `[x]` **README starts from raw/input data:** `raw_data/` CSVs (`90,938` pings, `84,633` segments) + committed `data/demo_sample/`.
- `[x]` **Key output reproduced without hidden steps:** `39/39` platform tests + `5/5` Spark container tests (`44/44` total).
- `[x]` **No real credentials/private data embedded:** Verified via `.gitignore` and `git status`.
