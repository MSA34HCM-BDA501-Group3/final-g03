# Card B evidence and defense

1. B1 has 46 rows covering two replay origins and the real 23-feature schema. Replay
   cutoffs pass where recorded; historical operational availability stays UNVERIFIED.
2. B2 exports the real static row for segment 21969 and feature names. PageRank is
   structural importance. A similar cluster does not establish adjacent geography or
   a causal explanation about lanes absorbing traffic.
3. B3 compares exact duplicate/distinct identities with Bloom and HLL on exactly the
   raw-smoke deliveries. Its sample is small; errors are not a full-dataset estimate.
   Online decisions always consult exact durable identity, so sketches cannot drop
   aggregate input or change an alert.
4. `[start,end)` UTC windows separate event time, simulated arrival time and actual
   receive time. ICT is only the calendar feature/display transformation.
5. The watermark is observed maximum event time minus 30 minutes. Simulation labels
   are evaluator-only. Beyond-watermark events enter Bronze and trigger reconciliation,
   including affected downstream observed lags. Historical revisions remain readable.
6. Recovery is at-least-once plus idempotency. True process kills around ingress,
   forecasts and broker commit converge without extra fixture forecasts. Atomic latest
   protects against concurrent stale writes. This is a single-host coordination contract.
7. Forecast coverage, historical batch accuracy, query latency, ingress-to-forecast
   latency and UI freshness have different denominators and clocks. None establishes
   live-city operation or guaranteed 30-minute advance warning.

One-minute defense: We now execute raw historical GPS through Kafka into journaled
Mongo ingress, an exact deduplicated Bronze collection, cutoff-bounded feature snapshots,
frozen model scores and immutable forecast history. The UI selects a particular run and
shows revision, issuance, target interval and freshness. Five real crash positions and
real service stops exercise recovery on small controlled runs. The controlled Spark
experiments use common inputs, parity, two warmups and five alternating measurements.
Historical point-in-time availability, online policy accuracy, distributed scaling and
rendered binary handoff readiness remain explicitly unverified.
