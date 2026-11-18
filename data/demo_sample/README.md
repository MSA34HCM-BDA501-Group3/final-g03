# Lightweight Committed Demo & Audit Sample (`data/demo_sample/`)

Deterministic real subset extracted from `raw_data/` and `data/` (`seed=42`, `sample_static_features.parquet` SHA-256 prefix `f26ebdf7d396`) so clean Git checkouts
(without the full 38.50 MB `raw_data/` directory) can immediately run:
- Live End-to-End Kafka Replay Demo (`producer.py` -> `Kafka` -> `starter_stream.py --from-kafka` -> `MongoDB 8.0` -> unified **ReactJS + FastAPI TOC Command Center at `http://localhost:8000`** via `toc-web`)
- Session 9 Card B (`B1/B2/B3`) smoke tests when `raw_data/` is not yet downloaded.

Full 90,787 accepted motorized pings / 84,206 road segments reproduction instructions are in `docs/DATA_SETUP.md`.
