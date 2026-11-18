"""Durable, single-worker historical Kafka replay. Mongo is the checkpoint.

At-least-once delivery with idempotent processing; no cross-system transaction.
All writes use acknowledged journaled storage. Evaluator labels are never read.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.metadata
import json
import math
import os
import platform
from pathlib import Path
import signal
import time

UTC = timezone.utc
SLOT = timedelta(minutes=30)


def timestamp(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return result.astimezone(UTC)


def iso(value):
    return value.astimezone(UTC).isoformat()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def window_start(value):
    return value.replace(minute=(value.minute // 30) * 30, second=0, microsecond=0)


def canonical_event(payload, run_id):
    from producer import compute_event_id

    if payload.get("schema_version") != 1 or payload.get("run_id") != run_id:
        raise ValueError("unsupported schema_version or run_id")
    if payload.get("event_type") == "replay_clock":
        return {"event_type": "replay_clock", "simulated_arrival_ts": iso(timestamp(payload["simulated_arrival_ts"]))}
    event_ts = timestamp(payload["event_ts"])
    arrival = timestamp(payload["simulated_arrival_ts"])
    if arrival < event_ts:
        raise ValueError("arrival precedes event")
    if isinstance(payload["segment_id"], bool) or float(payload["segment_id"]) != int(payload["segment_id"]):
        raise ValueError("segment_id must be an integer")
    segment = int(payload["segment_id"])
    speed = float(payload["velocity"])
    if segment < 0 or not math.isfinite(speed) or not 0 <= speed <= 200:
        raise ValueError("invalid segment or velocity")
    raw_id = payload["raw_event_id"]
    if not isinstance(raw_id, str) or not raw_id.strip() or len(raw_id) > 128:
        raise ValueError("invalid raw_event_id")
    start = window_start(event_ts)
    period = f"period_{start.hour}_{start.minute:02d}"
    event_id = compute_event_id(segment, start.strftime("%Y-%m-%d"), period)
    if payload.get("event_id") != event_id:
        raise ValueError("event_id does not match UTC window")
    return dict(
        raw_event_id=raw_id,
        event_id=event_id,
        segment_id=segment,
        velocity=speed,
        event_ts=iso(event_ts),
        simulated_arrival_ts=iso(arrival),
        source_window_start=iso(start),
        source_window_end=iso(start + SLOT),
    )


class FrozenScorer:
    """Load hashed artifacts once. Never fit or consume whole-data coverage fields."""

    def __init__(self, registry):
        import joblib
        import pandas as pd
        from pipeline import FULL_ML_FEATURES

        registry = Path(registry)
        self.manifest = json.loads(registry.read_text(encoding="utf-8"))
        m = self.manifest
        self.paths = {}
        for path_key, hash_key in (
            ("artifact_path", "model_sha256"),
            ("static_feature_store_path", "static_feature_store_sha256"),
            ("vmax_lookup_path", "vmax_lookup_sha256"),
        ):
            path = registry.parent / m[path_key]
            if path_key == "static_feature_store_path" and not path.is_file():
                # Legacy registry used a basename for the separately stored lakehouse.
                path = registry.parent.parent / "data" / "lakehouse" / m[path_key]
            if hashlib.sha256(path.read_bytes()).hexdigest() != m[hash_key]:
                raise ValueError(f"registry hash mismatch: {path}")
            self.paths[path_key] = path
        self.artifacts = joblib.load(registry.parent / m["artifact_path"])
        if self.artifacts["full_features"] != FULL_ML_FEATURES:
            raise ValueError("feature schema mismatch")
        self.features = FULL_ML_FEATURES
        self.static = pd.read_parquet(self.paths["static_feature_store_path"]).set_index("segment_id")
        # Model selection uses VAL, so VAL cutoff is also an artifact availability bound.
        self.cutoff = max(timestamp(m["train_cutoff_utc"]), timestamp(m["val_cutoff_utc"]))
        self.version = (
            m["model_version"]
            + ":"
            + digest(
                {
                    k: m[k]
                    for k in (
                        "model_sha256",
                        "static_feature_store_sha256",
                        "vmax_lookup_sha256",
                        "train_cutoff_utc",
                        "val_cutoff_utc",
                    )
                }
            )
        )

    def build(self, raw, clock):
        import pandas as pd
        from pipeline import LOS_TO_ORDINAL, derive_los, score_feature_snapshot

        groups = {}
        for event in raw:
            if timestamp(event["simulated_arrival_ts"]) > clock:
                continue
            key = (event["segment_id"], event["source_window_start"])
            groups.setdefault(key, []).append(event)
        rows, histories = [], {}
        for (segment, start), events in sorted(groups.items(), key=lambda item: (item[0][1], item[0][0])):
            start = timestamp(start)
            if start + SLOT > clock:
                continue
            if segment not in self.static.index:
                continue
            static = self.static.loc[segment]
            # Only frozen topology, cluster, Vmax and fitted priors are allowed.
            static_cols = [
                "length_m",
                "max_velocity",
                "street_level",
                "pagerank_score",
                "in_degree",
                "out_degree",
                "cluster_id",
                "segment_train_prior_los",
                "segment_train_severe_prior",
                "street_causal_prior_los",
                "street_causal_prior_sev",
            ]
            row = {c: float(static[c]) for c in static_cols}
            speed = sum(e["velocity"] for e in events) / len(events)
            ratio = min(speed / row["max_velocity"], 1.5)
            los = LOS_TO_ORDINAL[derive_los(ratio)]
            previous = histories.setdefault(segment, [])
            local = start + timedelta(hours=7)
            angle = 2 * math.pi * (local.hour * 2 + local.minute // 30) / 48
            # Observed lags, with first-observation fallback matching batch lag semantics.
            row.update(
                current_los_ordinal=los,
                lag1_los_ordinal=previous[-1][1] if previous else los,
                lag2_los_ordinal=previous[-2][1] if len(previous) > 1 else (previous[-1][1] if previous else los),
                trailing_severe_rate=sum(x >= 4 for x in ([p[1] for p in previous[-3:]] + [los]))
                / min(4, len(previous) + 1),
                mean_velocity=speed,
                velocity_ratio=ratio,
                obs_count=len(events),
                weekday=local.weekday(),
                period_sin=math.sin(angle),
                period_cos=math.cos(angle),
                gap_minutes=min(1440, max(30, (start - previous[-1][0]).total_seconds() / 60)) if previous else 180,
                neighbor_los_ordinal=row["street_causal_prior_los"],
            )
            previous.append((start, los))
            if start < self.cutoff:
                continue  # retain observed warm-start lags but never issue before artifact availability
            row.update(event_id=events[0]["event_id"], segment_id=segment)
            # Explicit frozen-prior neighbor policy; historical batch neighbor policy differs.
            scored = score_feature_snapshot(pd.DataFrame([row]), self.artifacts).iloc[0].to_dict()
            rows.append(
                dict(
                    features={c: row[c] for c in self.features},
                    prediction=scored,
                    event_id=row["event_id"],
                    segment_id=segment,
                    street_name=str(static["street_name"]),
                    street_level=int(static["street_level"]),
                    source_window_start=iso(start),
                    source_window_end=iso(start + SLOT),
                    target_start=iso(start + SLOT),
                    target_end=iso(start + 2 * SLOT),
                    raw_event_ids=sorted(e["raw_event_id"] for e in events),
                )
            )
        return rows


class MongoStore:
    def __init__(self, uri, database, run_id):
        from pymongo import MongoClient
        from pymongo.write_concern import WriteConcern

        if not database.startswith("session9_"):
            raise ValueError("online database must use isolated session9_ namespace")
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
        self.db = self.client.get_database(database, write_concern=WriteConcern(w=1, j=True))
        self.run_id = run_id
        self.db.ingress.create_index([("run_id", 1), ("sequence", 1)], unique=True)
        self.db.raw_events.create_index([("run_id", 1), ("raw_event_id", 1)], unique=True)
        self.db.forecast_history.create_index([("run_id", 1), ("event_id", 1), ("revision", 1)], unique=True)
        self.db.traffic_scores.create_index([("run_id", 1), ("event_id", 1)], unique=True)

    def ingress(self, topic, partition, offset, payload, received_at=None):
        from pymongo import ReturnDocument

        key = digest([self.run_id, topic, partition, offset])
        existing = self.db.ingress.find_one({"_id": key})
        if existing:
            return existing
        counter = self.db.counters.find_one_and_update(
            {"_id": self.run_id}, {"$inc": {"sequence": 1}}, upsert=True, return_document=ReturnDocument.AFTER
        )
        doc = dict(
            _id=key,
            run_id=self.run_id,
            topic=topic,
            partition=partition,
            offset=offset,
            sequence=counter["sequence"],
            payload=payload,
            received_at=iso(received_at or datetime.now(UTC)),
            status="pending",
        )
        self.db.ingress.update_one({"_id": key}, {"$setOnInsert": doc}, upsert=True)
        return self.db.ingress.find_one({"_id": key})

    def latest(self, doc):
        # Pipeline update compares version atomically, including concurrent stale writers.
        self.db.traffic_scores.update_one(
            {"_id": digest([self.run_id, doc["event_id"]])},
            [
                {
                    "$replaceWith": {
                        "$cond": [
                            {"$lt": [{"$ifNull": ["$revision", -1]}, doc["revision"]]},
                            {"$literal": {**doc, "_id": digest([self.run_id, doc["event_id"]])}},
                            "$$ROOT",
                        ]
                    }
                }
            ],
            upsert=True,
        )


class Worker:
    def __init__(self, store, scorer, crash=lambda stage: None):
        self.store, self.scorer, self.crash = store, scorer, crash

    def drain(self):
        db, run_id = self.store.db, self.store.run_id
        for delivery in db.ingress.find({"run_id": run_id, "status": "pending"}).sort("sequence", 1):
            seq = delivery["sequence"]
            try:
                event = canonical_event(delivery["payload"], run_id)
            except (ValueError, TypeError, KeyError, OverflowError) as exc:
                db.ingress.update_one({"_id": delivery["_id"]}, {"$set": {"status": "quarantined", "reason": str(exc)}})
                continue
            first = None
            if event.get("event_type") != "replay_clock":
                raw_key = digest([run_id, event["raw_event_id"]])
                identity = {k: v for k, v in event.items() if k != "simulated_arrival_ts"}
                first = db.raw_events.find_one({"_id": raw_key})
                if first and first["identity_digest"] != digest(identity):
                    db.ingress.update_one(
                        {"_id": delivery["_id"]}, {"$set": {"status": "quarantined", "reason": "identity conflict"}}
                    )
                    continue
                db.raw_events.update_one(
                    {"_id": raw_key},
                    {
                        "$setOnInsert": {
                            **event,
                            "run_id": run_id,
                            "sequence": seq,
                            "identity_digest": digest(identity),
                            "received_at": delivery["received_at"],
                            "broker_delivery_id": delivery["_id"],
                        }
                    },
                    upsert=True,
                )
            db.ingress.update_one(
                {"_id": delivery["_id"]}, {"$set": {"validated_arrival": event["simulated_arrival_ts"]}}
            )
            self.crash("after_raw")
            raw = list(db.raw_events.find({"run_id": run_id, "sequence": {"$lte": seq}}))
            # Replay clock and watermark are monotonic and derived from observed timestamps.
            clock = max(
                timestamp(e["validated_arrival"])
                for e in db.ingress.find(
                    {"run_id": run_id, "sequence": {"$lte": seq}, "validated_arrival": {"$exists": True}}
                )
            )
            max_event = max((timestamp(e["event_ts"]) for e in raw), default=clock)
            watermark = max_event - SLOT
            prior = [e for e in raw if e["sequence"] < seq]
            prior_watermark = max((timestamp(e["event_ts"]) for e in prior), default=max_event) - SLOT
            too_late = "event_ts" in event and timestamp(event["event_ts"]) < prior_watermark
            # Reconciliation recomputes all closed windows and affected downstream lags.
            for built in self.scorer.build(raw, clock):
                content_hash = digest([built, self.scorer.version])
                previous = db.forecast_history.find_one(
                    {"run_id": run_id, "event_id": built["event_id"], "revision": {"$lt": seq}}, sort=[("revision", -1)]
                )
                if previous and previous["input_digest"] == content_hash:
                    continue
                revision_id = digest([run_id, built["event_id"], seq])
                doc = dict(
                    run_id=run_id,
                    revision=seq,
                    issued_at=iso(datetime.now(UTC)),
                    input_cutoff=delivery["received_at"],
                    input_sequence_cutoff=seq,
                    simulated_input_cutoff=iso(clock),
                    watermark=iso(watermark),
                    model_version=self.scorer.version,
                    feature_version="online-frozen-priors-v1",
                    input_digest=content_hash,
                    lifecycle_status="RECONCILED" if too_late else "PROVISIONAL",
                    completeness="observed_only",
                    broker_delivery_id=delivery["_id"],
                    feature_snapshot_id=revision_id,
                    **built,
                )
                db.feature_snapshots.update_one({"_id": revision_id}, {"$setOnInsert": doc}, upsert=True)
                doc = db.feature_snapshots.find_one({"_id": revision_id})
                db.forecast_history.update_one({"_id": revision_id}, {"$setOnInsert": doc}, upsert=True)
                self.crash("after_forecast")
                doc = db.forecast_history.find_one({"_id": revision_id})
                # Store score fields at top level for existing UI queries.
                self.store.latest({**doc, **doc["prediction"]})
            db.ingress.update_one(
                {"_id": delivery["_id"]},
                {
                    "$set": {
                        "status": "complete",
                        "raw_event_id": event.get("raw_event_id"),
                        "duplicate": bool(first and first["sequence"] < seq),
                        "beyond_watermark": too_late,
                        "watermark": iso(watermark),
                        "processed_at": iso(datetime.now(UTC)),
                    }
                },
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--database", default=os.getenv("TRAFFIC_DATABASE", "session9_online"))
    parser.add_argument("--run-id", default=os.getenv("TRAFFIC_RUN_ID", "replay-v1"))
    parser.add_argument("--topic", default=os.getenv("TRAFFIC_TOPIC", "hcmc-traffic-events"))
    parser.add_argument(
        "--registry", type=Path, default=Path(os.getenv("TRAFFIC_REGISTRY", "outputs/model_registry_manifest.json"))
    )
    parser.add_argument("--idle-exit-seconds", type=float, default=0)
    parser.add_argument(
        "--checkpoint-dir", type=Path, default=Path(os.getenv("TRAFFIC_CHECKPOINT_DIR", "outputs/online_checkpoints"))
    )
    parser.add_argument(
        "--crash-at", choices=["before_ingress", "after_ingress", "after_raw", "after_forecast", "before_commit"]
    )
    args = parser.parse_args()
    from kafka import KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndMetadata

    def crash(stage):
        if args.crash_at == stage:
            print(json.dumps({"crash_at": stage, "pid": os.getpid()}), flush=True)
            os.kill(os.getpid(), signal.SIGKILL if hasattr(signal, "SIGKILL") else signal.SIGTERM)

    # Single-host PoC: an OS lock on the shared checkpoint volume is released on kill.
    # Every worker for this database/run must mount the SAME checkpoint directory.
    args.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    lock = (args.checkpoint_dir / (digest([args.database, args.run_id]) + ".lock")).open("a+b")
    if os.name == "nt":
        import msvcrt

        lock.write(b"0")
        lock.flush()
        lock.seek(0)
        msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    store = MongoStore(args.mongo_uri, args.database, args.run_id)
    consumer = None
    try:
        worker = Worker(store, FrozenScorer(args.registry), crash)
        pin = {
            "model_version": worker.scorer.version,
            "feature_version": "online-frozen-priors-v1",
            "worker_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "shared_source_sha256": {
                name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                for name in ("pipeline.py", "producer.py", "baseline.py")
            },
            "runtime_versions": {
                "python": platform.python_version(),
                **{name: importlib.metadata.version(name) for name in ("numpy", "pandas", "scikit-learn", "joblib")},
            },
        }
        store.db.runs.update_one({"_id": args.run_id}, {"$setOnInsert": pin}, upsert=True)
        stored_pin = store.db.runs.find_one({"_id": args.run_id})
        if any(stored_pin.get(k) != v for k, v in pin.items()):
            raise ValueError("run registry or worker changed; use a new run_id")
        worker.drain()  # Recover unprocessed durable ingress before polling.
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.brokers.split(","),
            group_id=f"session9-{args.database}-{args.run_id}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=1,
            max_poll_interval_ms=900000,
        )
        last_record = time.monotonic()
        assigned = False
        while True:
            batches = consumer.poll(timeout_ms=1000, max_records=1)
            if consumer.assignment() and not assigned:
                assigned = True
                last_record = time.monotonic()
            for records in batches.values():
                for record in records:
                    last_record = time.monotonic()
                    crash("before_ingress")
                    try:
                        payload = json.loads(record.value)
                        if not isinstance(payload, dict):
                            payload = {"invalid_payload": repr(payload)}
                    except (ValueError, UnicodeDecodeError):
                        payload = {"invalid_payload_hex": record.value.hex()}
                    store.ingress(record.topic, record.partition, record.offset, payload)
                    crash("after_ingress")
                    worker.drain()
                    crash("before_commit")
                    consumer.commit(
                        {TopicPartition(record.topic, record.partition): OffsetAndMetadata(record.offset + 1, "")}
                    )
            if assigned and args.idle_exit_seconds and time.monotonic() - last_record >= args.idle_exit_seconds:
                break
    finally:
        if consumer:
            consumer.close(autocommit=False)
        store.client.close()
        lock.close()


if __name__ == "__main__":
    main()
