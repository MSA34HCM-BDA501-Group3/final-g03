# H · Card B — HCMC Traffic

The attached Session 9 manual identifies this project as Card B and records the following evidence-based boundaries:

1. **Forecast timing:** the model evaluation joins observed slots exactly 30 minutes apart. A row labeled `10:00` summarizes a window that completes at `10:30`; it cannot be used for a prediction issued at `10:00`. Per-feature arrival time, late revisions, and durable issuance-time history remain unverified.
2. **Graph / cold-start falsification:** PageRank ranks structural connectivity, not congestion. A high-PageRank road can be free-flowing; segment `21969` (Nguyễn Kiệm) is cited in the manual as such a counterexample. K-Means groups 7D behavior and does not establish spatial contiguity.
3. **Sketch trade-offs:** the recorded Bloom filter removes the 334 injected duplicates but has a nonzero false-positive probability, so valid events can be suppressed in the provisional path. HLL++ is an approximate global segment-count KPI, not an input to individual road alerts.

The manual references `H_B1_point_in_time_matrix.csv`, `H_B2_graph_zone_falsification.csv`, and `H_B3_missingness_sketches.csv`; none of those files is present in this repository. The text summary above does not replace those test artifacts. See `outputs/metrics.json`, `outputs/replay_stream_summary.json`, and the attached manual for the original claims.
