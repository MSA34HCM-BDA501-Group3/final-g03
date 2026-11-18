# Deployment contract and limitations

Build Python 3.13.9, Java 17 and PySpark 3.5.7 from `Dockerfile.session9`; the dependency
lock is the preserved rerun environment. Compose Kafka uses `kafka:29092` internally,
Kafka 4.1.0 and MongoDB 8.0.38. No external project image, network or container is
required. `Dockerfile.spark` uses the same Python/Java/Spark version family.

`traffic_online.py --help` exposes brokers, Mongo URI, isolated database, run ID, topic,
registry, checkpoint directory, idle-exit control and explicit crash failpoints.
Defaults also support KAFKA_BOOTSTRAP_SERVERS, MONGO_URI, TRAFFIC_DATABASE,
TRAFFIC_RUN_ID, TRAFFIC_TOPIC, TRAFFIC_REGISTRY and TRAFFIC_CHECKPOINT_DIR.
The online database must begin with `session9_`; tests, outages and actual replay use
separate databases/runs. All workers for the same run must share the checkpoint volume.
OS locks release after a real process kill. Never delete Mongo ingress/raw/history or
checkpoint files to restart a run. Compose restarts an exiting online worker on failure.

Mongo uses acknowledged journaled writes (`w=1,j=true`). Its single node does not prove
HA, network-partition tolerance, AP classification or exactly-once Kafka/Mongo atomicity.
Kafka container restart is tested; broker recreation/storage migration is not tested.
The ledger is the recoverable checkpoint after a durable write but before broker commit.

This worker intentionally rebuilds closed windows and downstream lags on each delivery.
It is a small replay implementation with quadratic work and growing exact state, not
a billion-event streaming capacity result. Sketch memory is evaluator-only and does
not describe the memory of this consumer. No full-dataset online throughput or SLO is
claimed. Broker-to-ingress latency and browser render freshness are separate from the
recorded ingress-to-forecast timings. Warm query p95 is a fourth, independent metric.

Outage tests stop real services, prove startup failure while unavailable, then restore
health and restart the process to convergence. They do not prove uninterrupted service
or mid-request outage recovery. Crash fixtures use one partition; the actual raw smoke
uses a segment partition plus replay-clock control. Cross-partition crash parity remains
unverified. Retention/compaction and distributed worker coordination remain future work.
