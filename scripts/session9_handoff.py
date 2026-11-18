"""Build traceable A-I evidence; never promotes preserved measurements to new passes."""

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess

from traffic_online import FrozenScorer, MongoStore, iso, timestamp

ROOT = Path("evidence/session9")


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")


def cards(store, scorer, run):
    from pipeline import _simulate_stream_sketches
    import pandas as pd

    db = store.db
    snapshots = list(db.feature_snapshots.find({"run_id": run}).sort("revision", 1).limit(2))
    if len(snapshots) != 2:
        raise ValueError("B1 requires two real forecast origins")
    rows = []
    static_features = {
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
        "neighbor_los_ordinal",
    }
    for origin, snapshot in enumerate(snapshots, 1):
        raw = list(db.raw_events.find({"run_id": run, "sequence": {"$lte": snapshot["input_sequence_cutoff"]}}))
        latest_source = max(
            timestamp(e["event_ts"]) for e in raw if timestamp(e["event_ts"]) < timestamp(snapshot["source_window_end"])
        )
        available = max(timestamp(e["received_at"]) for e in raw)
        for feature in scorer.features:
            frozen = feature in static_features
            rows.append(
                dict(
                    origin=origin,
                    event_id=snapshot["event_id"],
                    revision=snapshot["revision"],
                    feature=feature,
                    value=snapshot["features"][feature],
                    source_time=iso(scorer.cutoff) if frozen else iso(latest_source),
                    available_at=iso(scorer.cutoff) if frozen else iso(available),
                    artifact_cutoff=iso(scorer.cutoff),
                    input_cutoff=snapshot["input_cutoff"],
                    simulated_input_cutoff=snapshot["simulated_input_cutoff"],
                    replay_verdict="PASS"
                    if available <= timestamp(snapshot["input_cutoff"])
                    and scorer.cutoff <= timestamp(snapshot["simulated_input_cutoff"])
                    else "FAIL",
                    historical_operation_verdict="UNVERIFIED",
                    policy="frozen street prior"
                    if feature == "neighbor_los_ordinal"
                    else ("frozen artifact" if frozen else "observed at sequence cutoff"),
                )
            )
    with (ROOT / "H_B1_availability.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    assert len(rows) == 46
    sample_segment = 21969 if 21969 in scorer.static.index else int(scorer.static.index[0])
    sample = scorer.static.loc[sample_segment].to_dict()
    write_json(
        ROOT / "H_B2_static_features.json",
        dict(
            feature_list=scorer.features,
            sample_segment_id=sample_segment,
            sample=sample,
            interpretation=(
                "PageRank measures topology. Cluster similarity does not imply "
                "geographical continuity or establish causation."
            ),
        ),
    )
    deliveries = list(db.ingress.find({"run_id": run}).sort("sequence", 1))
    ping_deliveries = [d["payload"] for d in deliveries if "raw_event_id" in d["payload"]]
    dup, fp, exact, hll, memory = _simulate_stream_sketches(pd.DataFrame(ping_deliveries))
    write_json(
        ROOT / "H_B3_sketch.json",
        dict(
            run_id=run,
            deliveries=len(ping_deliveries),
            duplicates=dup,
            bloom_false_positives=fp,
            exact_segments=exact,
            hll_segments=hll,
            memory=memory,
            aggregate_impact=0,
            alert_impact=0,
            reason=(
                "Online decisions always use exact Mongo identities; sketches are evaluator-only on this same stream."
            ),
        ),
    )
    score = db.traffic_scores.find_one({"run_id": run})
    feature = db.feature_snapshots.find_one({"_id": score["feature_snapshot_id"]})
    source = db.raw_events.find_one({"run_id": run, "raw_event_id": feature["raw_event_ids"][0]})
    ingress = db.ingress.find_one({"_id": source["broker_delivery_id"]})
    history = db.forecast_history.find_one(
        {"run_id": run, "event_id": score["event_id"], "revision": score["revision"]}
    )
    from app import _try_query_live_mongo

    os.environ["TRAFFIC_DATABASE"] = store.db.name
    os.environ["TRAFFIC_RUN_ID"] = run
    ui = _try_query_live_mongo(os.getenv("MONGO_URI", "mongodb://mongo:27017"), 25)
    assert ui is not None and ui[1]["run_id"] == run
    latency = [
        (
            timestamp(h["issued_at"]) - timestamp(db.ingress.find_one({"_id": h["broker_delivery_id"]})["received_at"])
        ).total_seconds()
        * 1000
        for h in db.forecast_history.find({"run_id": run})
    ]
    write_json(
        ROOT / "F_trace.json",
        dict(
            run_id=run,
            raw_event=source,
            broker_ingress=ingress,
            snapshot=feature,
            history=history,
            mongo_latest=score,
            ui_metadata=ui[1],
            latency_scope="actual ingress receive to first forecast issuance; replay fixture includes process restart",
            ingress_to_forecast_ms=latency,
            broker_to_ingress_latency="UNMEASURED",
            ui_render_freshness="UNMEASURED",
        ),
    )


def provenance(command):
    def git(*args):
        return subprocess.check_output(["git", *args])

    # Include untracked source content: git diff alone does not identify this working tree.
    sources = [
        p
        for p in Path(".").rglob("*")
        if p.is_file()
        and p.suffix in {".py", ".yml", ".txt", ".md"}
        and not any(
            part
            in {".git", "tmp", "outputs", "evidence", "raw_data", "data", "__pycache__", ".pytest_cache", ".ruff_cache"}
            for part in p.parts
        )
    ]
    sources += [p for p in Path(".").glob("Dockerfile*") if p.is_file()]
    source_hashes = {p.as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(sources)}
    inputs = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in Path("raw_data").glob("*.csv")}
    expected = json.loads(Path("evidence/session9_rerun/input_hashes.json").read_text(encoding="utf-8-sig"))
    expected_hashes = {item["Path"].replace("\\", "/").split("/")[-1]: item["Hash"].lower() for item in expected}
    assert len(inputs) == 5 and inputs == expected_hashes, "input hashes changed"
    utc = datetime.now(timezone.utc)
    git_info = (
        dict(
            commit=git("rev-parse", "HEAD").decode().strip(),
            tracked_diff_sha256=hashlib.sha256(git("diff", "--binary", "HEAD")).hexdigest(),
        )
        if shutil.which("git")
        else json.loads((ROOT / "host_git.json").read_text(encoding="utf-8"))
    )
    review_path = ROOT / "artifact_review.json"
    review = json.loads(review_path.read_text(encoding="utf-8")) if review_path.exists() else {}

    def binary_status(path):
        reviewed = review.get("files", {}).get(path.name, {})
        if review.get("status") == "pass" and reviewed.get("sha256") == hashlib.sha256(path.read_bytes()).hexdigest():
            return "G7 regenerated and render-reviewed; see artifact_review.json"
        return "historical or not matched to current G7 visual-review hash"

    manifest = dict(
        **git_info,
        source_hashes=source_hashes,
        source_snapshot_sha256=hashlib.sha256(json.dumps(source_hashes, sort_keys=True).encode()).hexdigest(),
        inputs_sha256=inputs,
        preserved_expected_inputs=expected,
        utc=iso(utc),
        ict=utc.astimezone(timezone(__import__("datetime").timedelta(hours=7))).isoformat(),
        command=command,
        python=platform.python_version(),
        dependencies={
            name: importlib.metadata.version(name)
            for name in ("pandas", "pyspark", "scikit-learn", "pymongo", "kafka-python-ng")
        },
        binary_deliverables={
            p.as_posix(): {
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
                "status": binary_status(p),
            }
            for p in Path(".").iterdir()
            if p.is_file() and p.suffix.lower() in {".pdf", ".docx", ".pptx"}
        },
        artifacts_sha256={
            p.as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in ROOT.rglob("*")
            if p.is_file() and p.name != "manifest.json"
        },
        provenance=(
            "session9_original and session9_rerun preserved. "
            "New evidence is scoped to the commands and fixture stated in each file."
        ),
    )
    write_json(ROOT / "manifest.json", manifest)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id")
    parser.add_argument("--database", default="session9_online")
    parser.add_argument("--git-only", action="store_true")
    parser.add_argument("--command", default="python scripts/session9_handoff.py")
    args = parser.parse_args()
    ROOT.mkdir(parents=True, exist_ok=True)
    if args.git_only:
        write_json(
            ROOT / "host_git.json",
            dict(
                commit=subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip(),
                tracked_diff_sha256=hashlib.sha256(
                    subprocess.check_output(["git", "diff", "--binary", "HEAD"])
                ).hexdigest(),
            ),
        )
        return
    if args.run_id:
        cards(
            MongoStore(os.getenv("MONGO_URI", "mongodb://mongo:27017"), args.database, args.run_id),
            FrozenScorer("outputs/model_registry_manifest.json"),
            args.run_id,
        )
    provenance(args.command)


if __name__ == "__main__":
    main()
