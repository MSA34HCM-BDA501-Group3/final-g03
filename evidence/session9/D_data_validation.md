# Data validation provenance

The five input SHA-256 values in `manifest.json` are asserted equal to the preserved
rerun input manifest. The controlled benchmark reads all 90,938 valid source pings and
produces 33,441 windows. Salted and unsalted monoids have zero two-way exceptAll
differences; the joined populations also have zero differences. Timings follow parity.

Independent full-source baseline results remain in
`../session9_rerun/monoid_baseline_verification.json` with their original provenance.
That baseline is not a new pipeline rerun. The full test suite includes the existing
baseline, split/purge and Spark tests plus new durable-service checks.

The motor-road engineering path has a different population: 90,787 pings and 33,394
windows. Its exclusions account for 151 pings and 47 windows. Do not mix these
denominators with the all-source benchmark. No runtime stage-count or task-skew
reduction claim follows from the historical Exchange node counts or different-key
population statistics.

New service tests reconcile six fixture deliveries with four unique raw events, one
duplicate delivery and one replay-clock control. Malformed/conflicting payloads are
quarantined. The watermark boundary test accepts equality and identifies one microsecond
before the watermark as beyond it. Simulation labels do not enter processing decisions.
