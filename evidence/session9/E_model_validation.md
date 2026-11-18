# E · Model and analytical validation

Stored evaluation in [`metrics.json`](../../outputs/metrics.json) reports a strict exact-`t+30m` chronological test of 1,182 rows (185 severe E/F cases). The frozen 23-feature Random Forest reports:

- 6-class accuracy: **0.9450**; macro-F1: **0.5543**.
- Severe E/F ROC-AUC: **0.9882**; PR-AUC: **0.9471**; F1: **0.9409**.
- Seven model configurations are recorded in [`ablation_results.csv`](../../outputs/ablation_results.csv).
- Transition analysis is in [`transition_evaluation.csv`](../../outputs/transition_evaluation.csv): 61 state changes and 8 new severe-onset cases. The attached manual reports the selected onset threshold detects **0/8** new severe cases; therefore the high aggregate severe-class metrics must not be presented as reliable early-onset detection.

The test dataset and raw inputs are absent, so these saved metrics were not reproduced in this audit. The current platform also does not verify per-feature production arrival timestamps or a durable wall-clock issuance history; an offline exact-slot evaluation is not proof of real-time point-in-time availability.
