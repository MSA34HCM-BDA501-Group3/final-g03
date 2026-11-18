# Controlled single host measurements

All variants use the same source inputs, eight shuffle partitions, local two-thread
Spark, disabled AQE, cached inputs and a collect action. Each pair receives two warmups
followed by five measured rounds with alternating order. Raw timings, settings, result
counts and zero two-way parity differences are in `benchmark/results.json`.

| Comparison | Variant | Median seconds |
|---|---|---:|
| Join strategy | Sort merge | 0.310514 |
| Join strategy | Broadcast hash | 0.261904 |
| Aggregation | Unsalted monoid | 0.124255 |
| Aggregation | Salted monoid | 0.116502 |

The two join variants each return 90,938 rows; both aggregation variants return 33,441
windows. These are warm local measurements of different physical strategies. They do
not establish cold-start cost, distributed scaling, city-stream throughput or a task-skew
reduction. The historical 687.43x and 1.023x statistics describe different populations
and are withdrawn as a before/after skew claim.

All four full physical plans, two analytical query plans and 335 raw task records are
retained under `benchmark/`. Raw Spark event logs are retained and checksummed too.
