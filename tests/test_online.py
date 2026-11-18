"""Required real-service tests: no mock Mongo/Kafka and no integration skips."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pytest

from producer import compute_event_id
from traffic_online import FrozenScorer, MongoStore, Worker, canonical_event, iso, timestamp, window_start

URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVIDENCE = Path(os.getenv("TRAFFIC_TEST_EVIDENCE", "evidence/session9/tests"))


@pytest.fixture(scope="module")
def scorer():
    return FrozenScorer("outputs/model_registry_manifest.json")


def events(scorer, run):
    segment = int(scorer.static[scorer.static.street_level.isin([1, 2])].index[0])
    start = window_start(scorer.cutoff) + timedelta(days=1)

    def ping(name, minute, arrival, velocity):
        ts = start + timedelta(minutes=minute)
        slot = window_start(ts)
        return dict(
            schema_version=1,
            run_id=run,
            raw_event_id=name,
            segment_id=segment,
            event_id=compute_event_id(segment, slot.strftime("%Y-%m-%d"), f"period_{slot.hour}_{slot.minute:02d}"),
            event_ts=iso(ts),
            simulated_arrival_ts=iso(start + timedelta(minutes=arrival)),
            velocity=velocity,
        )

    a = ping("a", 1, 1, 10)
    return [
        a,
        dict(a, is_duplicate=False, lateness_bucket="invented"),
        ping("b", 31, 31, 15),
        ping("c", 61, 61, 20),
        ping("late", 2, 63, 80),
        dict(
            schema_version=1,
            run_id=run,
            event_type="replay_clock",
            simulated_arrival_ts=iso(start + timedelta(minutes=100)),
        ),
    ]


def store(run):
    return MongoStore(URI, "session9_tests", run)


def test_canonical_contract_and_boundaries(scorer):
    payload = events(scorer, "contract")[0]
    normalized = canonical_event(payload, "contract")
    assert normalized == canonical_event(
        dict(payload, is_duplicate=True, lateness_bucket="LATE_OUT_OF_WATERMARK"), "contract"
    )
    for changed in (
        {"raw_event_id": None},
        {"velocity": float("nan")},
        {"segment_id": 1.5},
        {"schema_version": 2},
        {"event_ts": "2020-01-01T00:00:00"},
    ):
        with pytest.raises((ValueError, TypeError)):
            canonical_event(dict(payload, **changed), "contract")
    assert window_start(timestamp("2020-01-01T00:30:00Z")).minute == 30
    # A model artifact from the future cannot score this source interval.
    early = dict(
        normalized,
        event_ts="2019-01-01T00:01:00+00:00",
        simulated_arrival_ts="2019-01-01T00:01:00+00:00",
        source_window_start="2019-01-01T00:00:00+00:00",
        source_window_end="2019-01-01T00:30:00+00:00",
    )
    assert scorer.build([early], timestamp("2019-01-01T01:00:00Z")) == []


def test_exact_identity_labels_history_and_atomic_latest(scorer):
    runs = ["labels-" + uuid.uuid4().hex for _ in range(2)]
    results = []
    for flip, run in enumerate(runs):
        s = store(run)
        for offset, payload in enumerate(events(scorer, run)):
            payload.update(is_duplicate=bool(flip), lateness_bucket="ON_TIME" if flip else "LATE_OUT_OF_WATERMARK")
            s.ingress("oracle", 0, offset, payload)
            Worker(s, scorer).drain()
        assert s.db.raw_events.count_documents({"run_id": run}) == 4
        assert s.db.ingress.count_documents({"run_id": run, "duplicate": True}) == 1
        assert s.db.ingress.count_documents({"run_id": run, "beyond_watermark": True}) == 1
        history = list(s.db.forecast_history.find({"run_id": run}).sort("revision", 1))
        assert history
        event = history[0]["event_id"]
        revisions = list(s.db.forecast_history.find({"run_id": run, "event_id": event}).sort("revision", 1))
        assert len(revisions) == 2
        assert revisions[0]["features"]["obs_count"] == 1
        assert revisions[-1]["features"]["obs_count"] == 2
        assert revisions[-1]["features"]["mean_velocity"] == 45
        count = len(history)
        Worker(s, scorer).drain()
        assert s.db.forecast_history.count_documents({"run_id": run}) == count
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(s.latest, revisions * 8))
        latest = s.db.traffic_scores.find_one({"run_id": run, "event_id": event})
        assert latest["revision"] == revisions[-1]["revision"]
        results.append([(h["event_id"], h["revision"], h["features"], h["prediction"]) for h in history])
        s.client.close()
    assert results[0] == results[1]


def test_watermark_boundary_and_quarantine(scorer):
    run = "boundary-" + uuid.uuid4().hex
    s = store(run)
    original = events(scorer, run)
    # Observe a 61-minute event, establishing watermark at minute 31 exactly.
    s.ingress("boundary", 0, 0, original[3])
    Worker(s, scorer).drain()
    exact = dict(original[2], raw_event_id="exact", simulated_arrival_ts=original[4]["simulated_arrival_ts"])
    before_ts = timestamp(exact["event_ts"]) - timedelta(microseconds=1)
    before = dict(exact, raw_event_id="before", event_ts=iso(before_ts))
    s.ingress("boundary", 0, 1, exact)
    s.ingress("boundary", 0, 2, before)
    s.ingress("boundary", 0, 3, dict(exact, raw_event_id=None))
    s.ingress("boundary", 0, 4, dict(exact, velocity=101))  # same ID, conflicting payload
    Worker(s, scorer).drain()
    assert s.db.ingress.find_one({"run_id": run, "offset": 1})["beyond_watermark"] is False
    assert s.db.ingress.find_one({"run_id": run, "offset": 2})["beyond_watermark"] is True
    assert s.db.ingress.count_documents({"run_id": run, "status": "quarantined"}) == 2
    assert s.db.raw_events.count_documents({"run_id": run}) == 3
    s.client.close()


@pytest.mark.parametrize("stage", ["before_ingress", "after_ingress", "after_raw", "after_forecast", "before_commit"])
def test_real_process_crash_and_broker_offsets(stage, scorer, tmp_path):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from producer import _ensure_kafka_topic

    run = "crash-" + uuid.uuid4().hex
    topic = "session9-" + uuid.uuid4().hex
    _ensure_kafka_topic(BROKERS.split(","), topic)
    producer = KafkaProducer(bootstrap_servers=BROKERS.split(","), value_serializer=lambda v: json.dumps(v).encode())
    records = events(scorer, run)
    for payload in records:
        producer.send(topic, key=str(payload.get("segment_id", 0)).encode(), partition=0, value=payload).get(timeout=15)
    producer.close()
    command = [
        sys.executable,
        "traffic_online.py",
        "--brokers",
        BROKERS,
        "--mongo-uri",
        URI,
        "--database",
        "session9_tests",
        "--run-id",
        run,
        "--topic",
        topic,
        "--checkpoint-dir",
        str(tmp_path),
        "--idle-exit-seconds",
        "4",
    ]
    killed = subprocess.run([*command, "--crash-at", stage], capture_output=True, text=True, timeout=60)
    assert killed.returncode != 0 and f'"crash_at": "{stage}"' in killed.stdout, killed.stdout + killed.stderr
    s = store(run)
    before = list(s.db.forecast_history.find({"run_id": run}))
    if stage == "after_forecast":
        changed_registry = dict(scorer.manifest, model_version="changed-model-version")
        for key, path in scorer.paths.items():
            changed_registry[key] = str(path.resolve())
        registry_path = tmp_path / "changed_registry.json"
        registry_path.write_text(json.dumps(changed_registry), encoding="utf-8")
        rejected = subprocess.run(
            [*command, "--registry", str(registry_path)], capture_output=True, text=True, timeout=30
        )
        assert rejected.returncode != 0 and "run registry or worker changed" in rejected.stderr
        pin = s.db.runs.find_one({"_id": run})
        s.db.runs.update_one({"_id": run}, {"$set": {"shared_source_sha256": {"pipeline.py": "changed"}}})
        rejected = subprocess.run(command, capture_output=True, text=True, timeout=30)
        assert rejected.returncode != 0 and "run registry or worker changed" in rejected.stderr
        s.db.runs.replace_one({"_id": run}, pin)
        assert list(s.db.forecast_history.find({"run_id": run})) == before
    restarted = subprocess.run(command, capture_output=True, text=True, timeout=90)
    assert restarted.returncode == 0, restarted.stdout + restarted.stderr
    assert s.db.ingress.count_documents({"run_id": run}) == len(records)
    assert s.db.ingress.count_documents({"run_id": run, "status": "complete"}) == len(records)
    assert s.db.raw_events.count_documents({"run_id": run}) == 4
    for old in before:
        assert s.db.forecast_history.find_one({"_id": old["_id"]}) == old
    history = list(s.db.forecast_history.find({"run_id": run}))
    assert len(history) == 5  # first, revised first, revised second lag, second, final third
    consumer = KafkaConsumer(bootstrap_servers=BROKERS.split(","), group_id=f"session9-session9_tests-{run}")
    committed = consumer.committed(TopicPartition(topic, 0))
    consumer.close()
    assert committed == len(records)
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / f"crash_{stage}.json").write_text(
        json.dumps(
            dict(
                stage=stage,
                run_id=run,
                topic=topic,
                killed_returncode=killed.returncode,
                crash_log=killed.stdout,
                restart_log=restarted.stdout + restarted.stderr,
                broker_committed=committed,
                ingress_count=len(records),
                raw_count=4,
                history=history,
                history_before_restart=before,
                checked_at=iso(datetime.now(timezone.utc)),
            ),
            default=str,
            indent=2,
        ),
        encoding="utf-8",
    )
    s.client.close()
