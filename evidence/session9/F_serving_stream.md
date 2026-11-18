# F · NoSQL serving and stream evidence

Stored records in `outputs/` document:

- MongoDB 8.0.34 write/read-back for 33,441 documents, 69 bulk batches, and idempotent replay count remaining 33,441.
- Compound index `level_prob_segment_idx` used for the high-risk arterial query; recorded warm p95 is 0.636 ms with no in-memory sort.
- Kafka replay summary: 3,000 broker-delivery messages across three partitions; the controlled replay also records 334 duplicates, 1,170 in-watermark late events, and 501 out-of-watermark events.
- WAL recovery: the replayed committed batch is skipped after restart, with zero recorded duplicate side effects.
- Bloom storage: 32,768 bytes; HyperLogLog++ storage: 1,024 bytes. These are replay/benchmark results, not a production city-wide streaming SLO.

Evidence: [`mongo_verify.json`](../../outputs/mongo_verify.json), [`replay_stream_summary.json`](../../outputs/replay_stream_summary.json), [`query_progress.json`](../../outputs/query_progress.json), and [`metrics.json`](../../outputs/metrics.json). They are historical snapshots; this audit did not connect to a running MongoDB or Kafka service.
