"""Stop/restart only this Compose project's services and verify replay recovery."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import uuid


ROOT = Path("evidence/session9/outages")


def prepare(service):
    import pandas as pd
    from producer import generate_raw_ping_replay, publish_to_kafka_broker

    raw_info = json.loads(Path("evidence/session9/raw_smoke.json").read_text())
    raw = pd.read_csv("raw_data/segment_status.csv")
    selected = raw[raw._id.isin(raw_info["selected_source_ids"])].head(4)
    frame, _ = generate_raw_ping_replay(selected)
    run = "outage-" + service + "-" + uuid.uuid4().hex
    topic = "session9-" + run
    delivery = publish_to_kafka_broker(frame, "kafka:29092", topic=topic, max_messages=0, run_id=run)
    assert delivery["status"] == "verified", delivery
    ROOT.mkdir(parents=True, exist_ok=True)
    (ROOT / f"{service}_fixture.json").write_text(
        json.dumps(dict(run_id=run, topic=topic, expected_raw=len(selected), delivery=delivery), indent=2)
    )


def verify(service):
    from traffic_online import MongoStore

    fixture = json.loads((ROOT / f"{service}_fixture.json").read_text())
    s = MongoStore(os.getenv("MONGO_URI", "mongodb://mongo:27017"), "session9_outages", fixture["run_id"])
    scope = {"run_id": fixture["run_id"]}
    assert s.db.raw_events.count_documents(scope) == fixture["expected_raw"]
    assert s.db.ingress.count_documents({**scope, "status": "pending"}) == 0
    assert s.db.traffic_scores.count_documents(scope) > 0
    return dict(
        fixture=fixture,
        ingress=list(s.db.ingress.find(scope)),
        history=list(s.db.forecast_history.find(scope)),
        latest=list(s.db.traffic_scores.find(scope)),
    )


def host():
    ROOT.mkdir(parents=True, exist_ok=True)

    def docker(*args, timeout=120):
        result = subprocess.run(["docker", "compose", *args], capture_output=True, text=True, timeout=timeout)
        return dict(
            command=["docker", "compose", *args],
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    for service in ("mongo", "kafka"):
        prep = docker(
            "run", "--rm", "--no-deps", "spark", "python", "-m", "scripts.session9_outages", "--prepare", service
        )
        assert prep["returncode"] == 0, prep
        fixture = json.loads((ROOT / f"{service}_fixture.json").read_text())
        command = (
            "run",
            "--rm",
            "--no-deps",
            "-e",
            "KAFKA_BOOTSTRAP_SERVERS=kafka:29092",
            "spark",
            "python",
            "traffic_online.py",
            "--database",
            "session9_outages",
            "--run-id",
            fixture["run_id"],
            "--topic",
            fixture["topic"],
            "--idle-exit-seconds",
            "5",
        )
        stopped = docker("stop", service)
        try:
            failed = docker(*command)
            assert failed["returncode"] != 0, failed
        finally:
            restored = docker("up", "-d", "--wait", service)
        recovered = docker(*command)
        assert recovered["returncode"] == 0, recovered
        checked = docker(
            "run", "--rm", "--no-deps", "spark", "python", "-m", "scripts.session9_outages", "--verify", service
        )
        assert checked["returncode"] == 0, checked
        (ROOT / f"{service}_recovery.json").write_text(
            json.dumps(
                dict(
                    scope="service unavailable at consumer startup; process exits then restarts after restoration",
                    preparation=prep,
                    stopped=stopped,
                    failure=failed,
                    restored=restored,
                    recovered=recovered,
                    durable_readback=json.loads(checked["stdout"]),
                ),
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"{service}: outage failure and restored recovery verified", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare")
    parser.add_argument("--verify")
    args = parser.parse_args()
    if args.prepare:
        prepare(args.prepare)
    elif args.verify:
        print(json.dumps(verify(args.verify), default=str))
    else:
        host()
