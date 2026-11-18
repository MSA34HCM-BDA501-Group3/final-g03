# Deliverable G — Cloud/Cluster Deployment, Reproducibility & Limitations (`G_deployment`)

## 6.1 Deployable Spark Architecture (`docker-compose.yml` -> Cloud Kubernetes / YARN Scale-Out)
| Deployment Element | Implemented Local Cluster (`[I]`) & Proposed Cloud Scale-Out (`[P]`) |
|---|---|
| **Cluster Manager** | `[I]` Docker Compose 4-service topology (`kafka`, `spark`, `mongodb`, `streamlit`) capped at `10.5 / 16 GB RAM`. `[P]` Managed Cloud Spark on Kubernetes (`EKS/GKE SparkOperator`) or `EMR/Dataproc YARN`. |
| **Driver Placement** | `[I]` Co-located inside `spark` container (`2.5 GB` heap, `2` cores). `[P]` Dedicated driver pod in cluster mode (`--deploy-mode cluster`, `4 vCPU`, `8 GB RAM`) isolated from user UI traffic. |
| **Executor Placement & Scaling** | `[I]` `4` executor threads (`spark.sql.shuffle.partitions = 8`). `[P]` Dynamic Allocation (`2..16` executor pods, `4 vCPU / 16 GB RAM` each) scaling on Kafka consumer lag & shuffle backlog (`8-64` salt buckets). |
| **Storage Location & Access** | `[I]` Bind-mounted Columnar Parquet Lakehouse (`data/lakehouse/curated_gold`, `partitionBy('year_month')`). `[P]` Object storage (`S3 / GCS / MinIO`) with `Delta Lake / Apache Iceberg` ACID transaction logs. |
| **Fault Tolerance & Checkpoints** | `[I]` Dual-Sink WAL checkpoints (`outputs/checkpoints/raw/offset_log.json` & `agg/state_wal.json`) + `MongoDB` idempotent `UpdateOne(upsert=True)` with `status_version` guard. |
| **Observability** | `[I]` Structured JSON telemetry (`outputs/spark_runtime_evidence.json`, `outputs/query_progress.json`, `outputs/mongo_verify.json`) + Streamlit Tab 5 Infrastructure Audit. |
| **Security & Least Privilege** | `[I]` `.gitignore` blocks `.env`, `*.pem`, `*.key`, `*secret*`; read-only SHA-256 verification (`verify_frozen_registry_integrity`) blocks online model tampering. |
| **Cost / Performance Trade-Off** | Trading `+19.2 ms` Stage-1 salting overhead and `+33.0 KiB` sketch approximation (`3.98%` HLL++ error) to prevent executor OOM and keep total cluster RAM at `10.5 / 16 GB` without expensive GPU/RAM hardware. |

## 6.2 Reproducibility Smoke Test (7/7 Verified `[x]`)
- `[x]` **Software versions recorded:** Python `3.11+ / 3.13`, PySpark `3.5.7`, Kafka `4.1.0`, MongoDB `8.0.34`, scikit-learn `1.5+` (`Dockerfile`, `requirements.txt`, `outputs/spark_runtime_evidence.json`).
- `[x]` **Dependencies/configuration recorded:** `requirements.txt`, `Dockerfile`, `docker-compose.yml` (`mem_limit` & JVM flags).
- `[x]` **Exact run commands available:** `python pipeline.py`, `python spark_batch.py`, `python starter_stream.py`, `pytest -v`.
- `[x]` **Deterministic seed set:** `RANDOM_SEED = 42` enforced across `pipeline.py`, `producer.py`, `starter_stream.py`, `spark_engine.py`.
- `[x]` **README starts from raw/input data:** `README.md` documents full execution from `raw_data/` CSVs to `outputs/` and `:8501`.
- `[x]` **Key output reproduced without hidden steps:** Verified by `19/19` automated tests in `tests/test_platform_v3.py` and `4/4` Spark tests in `tests/test_quyen_spark.py`.
- `[x]` **No real credentials/private data embedded:** Verified via `.gitignore` and git staging audit (`0` secrets).
