"""Small real-input Kafka-to-Mongo smoke; separate from synthetic crash fixtures."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

import pandas as pd

from producer import generate_raw_ping_replay, publish_to_kafka_broker
from traffic_online import FrozenScorer, MongoStore


def main():
    scorer = FrozenScorer("outputs/model_registry_manifest.json")
    raw = pd.read_csv("raw_data/segment_status.csv")
    raw = raw[pd.to_datetime(raw.updated_at, utc=True) >= scorer.cutoff]
    counts = raw.groupby("segment_id").size().sort_values(ascending=False)
    segment = int(counts.index[0])
    selected = raw[raw.segment_id == segment].sort_values("updated_at").head(20)
    replay, summary = generate_raw_ping_replay(selected)
    run = "raw-" + uuid.uuid4().hex
    topic = "session9-" + run
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    started = time.perf_counter()
    delivery = publish_to_kafka_broker(replay, brokers, topic=topic, max_messages=0, run_id=run)
    assert delivery["status"] == "verified", delivery
    command = [
        sys.executable,
        "traffic_online.py",
        "--run-id",
        run,
        "--topic",
        topic,
        "--database",
        "session9_online",
        "--brokers",
        brokers,
        "--idle-exit-seconds",
        "5",
    ]
    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    store = MongoStore(os.getenv("MONGO_URI", "mongodb://mongo:27017"), "session9_online", run)
    assert store.db.raw_events.count_documents({"run_id": run}) == len(selected)
    assert store.db.traffic_scores.count_documents({"run_id": run}) > 0
    trace = store.db.traffic_scores.find_one({"run_id": run})
    root = Path("evidence/session9")
    root.mkdir(parents=True, exist_ok=True)
    (root / "raw_smoke.json").write_text(
        json.dumps(
            dict(
                run_id=run,
                topic=topic,
                database="session9_online",
                source="raw_data/segment_status.csv",
                selected_source_ids=selected._id.tolist(),
                selected_segment=segment,
                elapsed_including_startup_and_idle_seconds=time.perf_counter() - started,
                replay_summary=summary,
                broker_delivery=delivery,
                consumer_log=result.stdout + result.stderr,
                latest_score=trace,
            ),
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(json.dumps({"run_id": run, "topic": topic, "raw_count": len(selected)}))


if __name__ == "__main__":
    main()
