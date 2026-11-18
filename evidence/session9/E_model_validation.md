# Model and point in time availability

Online loads the existing frozen RF/GBT, imputer, Vmax, fitted cluster and priors from
the hashed registry. It never fits them. The run pins all three artifact hashes, fitting
cutoffs, feature policy and worker source. The usable cutoff is the later TRAIN/VAL
cutoff because model selection uses validation. Earlier source windows retain Bronze
observations and warm-start lags without retrospective forecast issuance.

The common `pipeline.score_feature_snapshot` API scores all 23 named features. The
online builder uses observed cutoff-bounded aggregates/lags and frozen topology,
Vmax, cluster and priors. Its neighbor policy uses the frozen street prior; batch's
historical same-slot neighbor policy differs. Online accuracy has not been revalidated
for this policy. Whole-data coverage fields (`obs_windows`, `is_cold_start`) never enter
online features. Missing source segments are retained in Bronze without inventing a
feature vector. Static topology collection time remains historically unverified.

`H_B1_availability.csv` contains two actual replay issuance origins times 23 features.
The `available_at` for observed features is a conservative upper bound from all raw
events at the sequence cutoff; source_time is the latest observed event before source
window end. Frozen feature rows use the registry cutoff as simulated availability.
These are replay assertions, not recovered historical arrival logs. All historical
operation verdicts are UNVERIFIED. Snapshots preserve actual receive cutoff, simulated
arrival cutoff, source interval, target interval, issuance timestamp and artifact version.

Targets are `[source_end, source_end + 30 minutes)`. Closing a source slot at its end
does not provide 30 minutes of warning before target start. Late arrival can issue a
revision after the target has begun or ended. Revision IDs use durable sequence numbers
and can have gaps. History remains append-only; latest compares revisions atomically.

RF accuracy 94.50%, persistence 94.84%, severe PR-AUC 0.9471 and transition statistics
remain preserved rerun measurements, not new online results. Network coverage of
cold-start predictions does not establish their accuracy.
