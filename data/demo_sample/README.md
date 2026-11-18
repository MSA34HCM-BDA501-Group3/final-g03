# Lightweight Committed Demo & Audit Sample (`data/demo_sample/`)

Deterministic real subset extracted from `raw_data/` and `data/` (`seed=42`) so clean Git checkouts
(without the full 38.50 MB `raw_data/` directory) can immediately run:
- Live End-to-End Kafka Replay Demo (`producer.py` -> `Kafka` -> `starter_stream.py --from-kafka` -> `MongoDB 8.0` -> `Streamlit :8501`)
- Session 9 Card B (`B1/B2/B3`) smoke tests when `raw_data/` is not yet downloaded.

Full 90,938-ping / 84,633-segment reproduction instructions are in `docs/DATA_SETUP.md`.
