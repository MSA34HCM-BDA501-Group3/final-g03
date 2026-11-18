# A · Architecture truth classification

The diagram distinguishes the implemented local proof-of-concept from proposed production expansion:

- **[I] Implemented / measured:** CSV-based HCMC inputs; Kafka replay; quality funnel; Spark batch/stream processing; graph mining and model training; frozen model artifacts; Parquet outputs; MongoDB serving; Streamlit UI. Supporting code and runtime records are linked from the root `README.md` and `outputs/`.
- **[P] Proposed / design-only:** city-wide multi-provider live feeds, multi-node Kubernetes/Spark Operator, durable cloud object storage, high-availability database, production observability/security plane, and automated traffic-signal actuation.
- **[N/A] Not used:** online model-weight updates; the documented platform does not train from the live stream.

The rendered diagram already labels Zones A–F as implemented and Zone G as proposed: [production-readiness architecture diagram](../../outputs/s78_production_readiness_architecture.png). The detailed component/data-flow model is in [system-design.md](../../product/05-architecture/system-design.md).

**Evidence limitation:** the diagram and saved output metrics are present, but the input data and `evidence/session9/` package cited by the attached manual were absent from this checkout. The source-to-serving example for segment `21260` is therefore a documented historical trace, not one re-executed during this audit.

For interpretation, Kafka carries a controlled replay of archived observations; it is not evidence of a live city-wide traffic feed.
