# Session 9 evidence index

This folder maps the completed lab manual's required A–I evidence to files in this checkout. Status is based on repository contents inspected on 2026-11-18; it is not a fresh execution of the full pipeline. `raw_data/` and `data/` are absent because they are excluded by `.gitignore`.

| Item | Status in this checkout | Evidence / limitation |
|---|---|---|
| A · Architecture | Available, linked | [Architecture classification](A_architecture.md); diagram at `outputs/s78_production_readiness_architecture.png`. |
| B · Benchmark | Partial | [Benchmark summary](B_benchmark.md) and CSV. The SMJ timing is transcribed from the attached manual; its original benchmark capture is not in this checkout. |
| C · Query plan | Available, linked | [Query-plan summary](C_query_plan.md); Spark plan excerpts and pruning evidence are under `outputs/`. |
| D · Data validation | Partial | [Validation matrix](D_data_validation.csv). Stored output evidence exists, but source data is missing, so results cannot be regenerated here. |
| E · Model validation | Available as stored outputs; not freshly reproduced | [Model summary](E_model_validation.md), `outputs/metrics.json`, `outputs/ablation_results.csv`, and `outputs/transition_evaluation.csv`. |
| F · Serving and stream | Available as stored outputs; not freshly reproduced | [Serving/stream summary](F_serving_stream.md), `outputs/mongo_verify.json`, `outputs/replay_stream_summary.json`, and `outputs/query_progress.json`. |
| G · Deployment | Partial | [Deployment review](G_deployment.md), Compose files, and Dockerfiles. Cloud scale-out is design-only; local services still have no authentication. |
| H · Card B | Partial | [Card B summary](H_project_card.md). The attached manual has the responses; B1/B2/B3 CSV artifacts are not in this checkout. |
| I · Readiness | Current audit available | [Readiness audit](I_readiness.md). The manual's historical 95/100 self-score is not independently verified against this checkout. |

Use [the data setup guide](../../docs/DATA_SETUP.md) to restore the public source files. Re-run the listed commands before treating any stored metric as a current release result.
