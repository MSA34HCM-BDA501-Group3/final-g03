# G · Deployment and operations review

## Implemented proof-of-concept

The repository includes Docker Compose definitions for Kafka, MongoDB, Spark, Streamlit, and optional batch/stream profiles. `Dockerfile` serves the UI; `Dockerfile.spark35` builds the Spark 3.5.7 runtime used by the verifier. `compose.quyen.yml` intentionally uses the separate PySpark 4.2.0 / Java 21 environment for its Spark ETL job.

The root Compose configuration was corrected during this audit to build the missing Spark 3.5.7 image, use the same `bda501-net` / `mongo` DNS names expected by `pipeline.py`, and bind exposed development ports to loopback. Run only on a trusted local machine: MongoDB and Kafka still use unauthenticated PLAINTEXT inside the bridge network. Do not expose this Compose stack to an untrusted network.

## Design-only production extensions

Multi-node Kubernetes/Spark Operator, cloud object storage, high-availability MongoDB, TLS/SASL/ACLs, managed secrets, observability/on-call, and automated traffic-signal control are proposals, not implemented services. The architecture diagram labels this boundary. The Docker and service configuration changes have not been built or started in this environment because Docker is unavailable.
