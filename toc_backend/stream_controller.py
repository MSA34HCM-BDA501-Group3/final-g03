"""Live Kafka Replay Controller (`story` & `stress` modes) for the FastAPI TOC Server."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from live_kafka_stream import (
    DEFAULT_SCORES_JSON_PATH,
    DEFAULT_TELEMETRY_PATH,
    reset_live_stream_state,
)

ROOT = Path(__file__).resolve().parent.parent
_MONGO_LOCK = threading.Lock()
_MONGO_CLIENTS: dict[str, Any] = {}
_PRODUCER_LOCK = threading.Lock()
_ACTIVE_PRODUCER: subprocess.Popen[bytes] | None = None


def _resolve_mongo_uri() -> str:
    env_uri = os.environ.get("MONGO_URI", "").strip()
    return env_uri if env_uri else "mongodb://127.0.0.1:27017"


def _resolve_kafka_brokers() -> str:
    env_brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    return env_brokers if env_brokers else "127.0.0.1:9092"


def get_shared_mongo_client(uri: str) -> Any | None:
    """Return a pooled, thread-safe `pymongo.MongoClient` instance for `uri`."""
    with _MONGO_LOCK:
        client = _MONGO_CLIENTS.get(uri)
        if client is None:
            try:
                import pymongo  # type: ignore[import-not-found]

                client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=700)
                _MONGO_CLIENTS[uri] = client
            except Exception:
                return None
        return client


def _query_mongo_live_status(uri: str, limit: int) -> dict[str, Any] | None:
    client = get_shared_mongo_client(uri)
    if client is None:
        return None
    try:
        db = client["bda501"]
        tel = db["live_stream_telemetry"].find_one({"_id": "current"}, {"_id": 0})
        if tel is None:
            return None
        docs = list(
            db["live_stream_scores"]
            .find({}, {"_id": 0})
            .sort([("seq_id", -1), ("severe_prob", -1)])
            .limit(limit)
        )
        return {"telemetry": dict(tel), "recent_scores": docs}
    except Exception:
        return None


def get_live_stream_status(limit: int = 20) -> dict[str, Any]:
    """Fetch live Kafka -> Sketches -> RF -> MongoDB 8.0 telemetry and latest `v1` predictions."""
    uris = [_resolve_mongo_uri(), "mongodb://127.0.0.1:27017", "mongodb://mongo:27017"]
    for uri in dict.fromkeys(uris):
        live_res = _query_mongo_live_status(uri, limit)
        if live_res is not None:
            return live_res

    tel_fallback = (
        json.loads((ROOT / DEFAULT_TELEMETRY_PATH).read_text(encoding="utf-8"))
        if (ROOT / DEFAULT_TELEMETRY_PATH).is_file()
        else {"status": "IDLE", "messages_consumed": 0}
    )
    scores_fallback = (
        json.loads((ROOT / DEFAULT_SCORES_JSON_PATH).read_text(encoding="utf-8"))[:limit]
        if (ROOT / DEFAULT_SCORES_JSON_PATH).is_file()
        else []
    )
    return {"telemetry": tel_fallback, "recent_scores": scores_fallback}


def _stop_active_producer_locked() -> None:
    global _ACTIVE_PRODUCER
    proc = _ACTIVE_PRODUCER
    if proc is not None and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
    _ACTIVE_PRODUCER = None


def _reset_live_state_only() -> dict[str, Any]:
    return reset_live_stream_state(
        mongo_uri=_resolve_mongo_uri(),
        telemetry_path=ROOT / DEFAULT_TELEMETRY_PATH,
        scores_json_path=ROOT / DEFAULT_SCORES_JSON_PATH,
        reset_signal_path=ROOT / "outputs" / "live_reset_signal.json",
    )


def reset_live_stream() -> dict[str, Any]:
    """Stop active producer, reset MongoDB live collections, and signal `live-consumer` via `reset_epoch`."""
    with _PRODUCER_LOCK:
        _stop_active_producer_locked()
        return _reset_live_state_only()


def _build_producer_cmd(mode: str, scenario: str, max_messages: int, brokers: str) -> list[str]:
    if mode == "story":
        return [
            sys.executable,
            "producer.py",
            "--mode",
            "story",
            "--scenario",
            scenario,
            "--bootstrap-servers",
            brokers,
            "--topic",
            "hcmc-traffic-events",
            "--frame-pause",
            "1.7",
        ]
    return [
        sys.executable,
        "producer.py",
        "--mode",
        "window",
        "--bootstrap-servers",
        brokers,
        "--topic",
        "hcmc-traffic-events",
        "--max-messages",
        str(max(10, min(int(max_messages), 3000))),
        "--replay-speed",
        "0.25",
    ]


def start_live_stream_replay(
    mode: str = "story",
    scenario: str = "noon_transition",
    max_messages: int = 200,
    reset_first: bool = True,
) -> dict[str, Any]:
    """Launch asynchronous `producer.py` (`story` or `stress`) into Apache Kafka 4.1 KRaft."""
    global _ACTIVE_PRODUCER
    brokers = _resolve_kafka_brokers()
    cmd = _build_producer_cmd(mode, scenario, max_messages, brokers)
    with _PRODUCER_LOCK:
        _stop_active_producer_locked()
        if reset_first:
            _reset_live_state_only()
        _ACTIVE_PRODUCER = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    return {
        "started": True,
        "mode": mode,
        "scenario": scenario if mode == "story" else None,
        "max_messages": max_messages if mode != "story" else None,
        "bootstrap_servers": brokers,
    }
