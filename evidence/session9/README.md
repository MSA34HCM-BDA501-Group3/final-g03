# Session 9 current handoff

This bundle records the durable historical replay implementation. `session9_original/`
and `session9_rerun/` remain unchanged. They contain earlier measurements; this folder
does not promote those measurements to fresh passes. Dates in the manifest include UTC
and ICT. Source identity includes HEAD, the tracked diff, and hashes of untracked source.

The online path is `producer.py → Kafka → traffic_online.py → journaled MongoDB →
app.py`. Spark remains batch analytics. `starter_stream.py` and Spark's file-stream
demonstration are separate historical checks. Delivery is at-least-once with exact,
idempotent durable processing. The deployment uses one host, one worker per run, a
shared OS-lock/checkpoint volume and single-node MongoDB.

| Package | Evidence |
|---|---|
| A architecture | [A_architecture.md](A_architecture.md) |
| B controlled benchmarks | [benchmark/results.json](benchmark/results.json) and four full strategy plans |
| C analytical queries | [C_queries.md](C_queries.md), query text/sample outputs in benchmark/results.json |
| D data validation | [D_data_validation.md](D_data_validation.md) |
| E model and availability | [E_model_validation.md](E_model_validation.md), [46-row B1 audit](H_B1_availability.csv) |
| F serving and recovery | [F_trace.json](F_trace.json), [raw smoke](raw_smoke.json), [dashboard screenshot](F_ui.png), [crashes](tests/), [outages](outages/) |
| G deployment and limits | [G_deployment.md](G_deployment.md) |
| H Card B | [H_card_B.md](H_card_B.md), [B2](H_B2_static_features.json), [B3](H_B3_sketch.json) |
| I readiness | [I_readiness.md](I_readiness.md), [validation](validation.json), [source/artifact manifest](manifest.json) |

Build and validate from the repository root:

```powershell
docker compose build spark app
docker compose up -d --wait kafka mongo
python -m scripts.session9_validate
docker compose run --rm spark python -m scripts.session9_benchmark
docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 spark python -m scripts.session9_replay_smoke
python -m scripts.session9_outages
python -m scripts.session9_handoff --git-only
# Read the new run ID from raw_smoke.json; substitute it below.
docker compose run --rm spark python -m scripts.session9_handoff --run-id RAW_RUN_ID
```

Production-shaped replay demo using the default topic (historical PoC only):

```powershell
$env:TRAFFIC_RUN_ID = 'demo-20261118'
docker compose --profile stream up -d stream app
docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e TRAFFIC_RUN_ID=$env:TRAFFIC_RUN_ID spark python producer.py --mode raw --max-messages 100 --output-summary outputs/online_publish_summary.json
```

Raw events before the frozen artifact cutoff remain in Bronze and warm-start observed
lags but do not receive retrospective forecasts. For a small scored demo use the smoke
script, which selects actual post-cutoff pings. It creates a unique topic/run and keeps
the original dataset and batch outputs intact. To display that run, set `TRAFFIC_RUN_ID`
to the run in `raw_smoke.json` and recreate `app` with Compose. No startup command
deletes a checkpoint. Use a new run ID if artifacts or worker source changes.

The test log is authoritative for passed/failed/skipped counts. **G7 handoff is PASS**:
the 12-page manual, 16-page report and 16-slide PPTX/PDF were regenerated and all 44
pages/slides were visually reviewed. See [artifact_review.json](artifact_review.json),
[structural/content checks](g7_artifact_checks.json), and [G7 validation](g7_validation.json).
The slide PDF comes from the actual PPTX export. The manual has 25 working linked cells
for its 16-item checklist and 9 red flags. Prior binaries are in g7_archive; preserved
original/rerun bundles remain unchanged. This does not close the explicit G1/G3/G5/G6
limitations or establish all gates operationally Ready.
