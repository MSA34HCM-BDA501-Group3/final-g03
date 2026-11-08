"""Raw GPS -> verified Spark Medallion snapshot. See docs/QUYEN_DATA_ENGINEERING.md."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time

from pyspark.sql import SparkSession, functions as F

from traffic_data.spark_etl import (
    aggregate_monoid, clean_events, clean_segments, deduplicate_events,
    differences, enrich_windows, fit_velocity_lookup, physical_nodes, prune_nodes, read_strings,
)


def execute(args):
    root = Path(args.output_root)
    # Never overwrite a previous snapshot or the legacy data/outputs tree.
    root.mkdir(parents=True, exist_ok=False)
    log_dir = root / "spark_events"
    log_dir.mkdir()
    spark = SparkSession.builder.master(args.master).appName("hcmc-quyen-batch") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.compress", "false") \
        .config("spark.eventLog.dir", log_dir.resolve().as_uri()) \
        .config("spark.sql.shuffle.partitions", str(args.partitions)).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        raw = Path(args.raw_dir)
        segments, bad_segments = clean_segments(read_strings(spark, str(raw / "segments.csv")))
        if segments.groupBy("segment_id").count().filter("count > 1").limit(1).count():
            raise ValueError("Duplicate segment IDs in topology")
        segments = segments.cache()
        streets = read_strings(spark, str(raw / "streets.csv"))
        streets = streets.select(F.col("_id").try_cast("bigint").alias("street_id"),
                                 F.col("name").alias("master_street_name"),
                                 F.col("type").alias("master_street_type"),
                                 F.col("level").try_cast("int").alias("master_street_level"))
        if streets.filter("street_id IS NULL").limit(1).count() \
                or streets.groupBy("street_id").count().filter("count > 1").limit(1).count():
            raise ValueError("Street catalog must have unique valid IDs")
        street_ids = streets.select("street_id")
        orphan_segments = segments.join(street_ids, "street_id", "left_anti")
        if orphan_segments.limit(1).count():
            raise ValueError("Topology contains unknown street_id")
        nodes = prune_nodes(read_strings(spark, str(raw / "nodes.csv")), segments).cache()
        endpoints = segments.select(F.col("s_node_id").alias("node_id")).union(
            segments.select(F.col("e_node_id").alias("node_id"))).distinct()
        if endpoints.join(nodes.select("node_id"), "node_id", "left_anti").limit(1).count():
            raise ValueError("Topology contains unknown node endpoint")
        events_raw = read_strings(spark, str(raw / "segment_status.csv")) \
            .withColumnRenamed("_id", "raw_event_id")
        events_raw.write.mode("error").parquet(str(root / "bronze_raw_events"))
        clean, rejected = clean_events(events_raw, segments)
        events, conflicts = deduplicate_events(clean)
        events = events.cache()
        rejected = rejected.unionByName(conflicts, allowMissingColumns=True)
        rejected.write.mode("error").parquet(str(root / "quarantine" / "events"))
        bad_segments.write.mode("error").parquet(str(root / "quarantine" / "segments"))
        lookup = fit_velocity_lookup(events, segments, args.train_end).cache()
        lookup.write.mode("error").parquet(str(root / "preprocessing" / "velocity_lookup"))
        nodes.write.mode("error").parquet(str(root / "active_nodes"))
        segments.write.mode("error").parquet(str(root / "segments"))
        # Same keys, same accepted input, same partition count; actual task metrics
        # are collected from Spark event logs after shutdown, not fabricated.
        events.count()
        aggregate_times = {}
        for label, salted in [("monoid_unsalted", False), ("monoid_salted", True)]:
            spark.sparkContext.setJobGroup(label, label)
            started = time.perf_counter()
            aggregate_monoid(events, salted=salted).collect()
            aggregate_times[label] = time.perf_counter() - started
        spark.sparkContext.setJobGroup("pipeline", "pipeline")
        windows = aggregate_monoid(events).cache()
        unsalted = aggregate_monoid(events, salted=False)
        monoid_diff = differences(windows, unsalted)
        if any(monoid_diff.values()):
            raise AssertionError(f"Salted monoid differs: {monoid_diff}")
        windows.write.mode("error").parquet(str(root / "silver_cleaned_windows"))
        baseline = enrich_windows(windows, segments, lookup, broadcast=False, streets=streets)
        optimized = enrich_windows(windows, segments, lookup, streets=streets)
        timings = {}
        plans = {}
        node_names = {}
        for name, frame in [("sort_merge", baseline), ("broadcast", optimized)]:
            start = time.perf_counter()
            frame.collect()  # Full query materialization for comparable small-project runs.
            timings[name] = time.perf_counter() - start
            plans[name] = frame._jdf.queryExecution().executedPlan().toString()
            node_names[name] = physical_nodes(frame)
            (root / f"{name}_plan.txt").write_text(plans[name], encoding="utf-8")
        join_diff = differences(baseline, optimized)
        if any(join_diff.values()) or "BroadcastHashJoin" not in node_names["broadcast"]:
            raise AssertionError("Join equivalence/physical strategy verification failed")
        optimized.write.mode("error").partitionBy("year_month").parquet(str(root / "curated_gold"))
        windows.write.mode("error").parquet(str(root / "window_metrics" / "batch_id=0"))
        month = optimized.select("year_month").first()[0]
        scan = spark.read.parquet(str(root / "curated_gold")).filter(F.col("year_month") == month) \
            .select("segment_id", "mean_velocity")
        scan.collect()
        (root / "pruning_plan.txt").write_text(scan._jdf.queryExecution().executedPlan().toString(), encoding="utf-8")
        # Compare all monoid components with a genuinely independent Python fold.
        from baseline import compare_window_aggregates
        import csv
        with (raw / "segment_status.csv").open(encoding="utf-8", newline="") as handle:
            raw_rows = list(csv.DictReader(handle))
        accepted_ids = {row.raw_event_id for row in events.select("raw_event_id").collect()}
        baseline_report = compare_window_aggregates(
            [r for r in raw_rows if r["_id"] in accepted_ids],
            [r.asDict() for r in windows.collect()],
        )
        if not baseline_report["equivalent"]:
            raise AssertionError(f"Independent baseline mismatch: {baseline_report}")
        input_count, accepted_count = events_raw.count(), events.count()
        quality_rejected, conflict_count = rejected.count(), conflicts.count()
        metrics = {
            "spark_version": spark.version, "train_end_exclusive_utc": args.train_end,
            "input_events": input_count, "accepted_unique_events": accepted_count,
            "rejected_events": quality_rejected,
            "rejected_by_rule": {r.first_failing_rule: r["count"]
                                 for r in rejected.groupBy("first_failing_rule").count().collect()},
            "duplicate_deliveries_removed": clean.count() - conflict_count - accepted_count,
            "quarantined_segments": bad_segments.count(), "active_nodes": nodes.count(),
            "windows": windows.count(), "monoid_difference": monoid_diff,
            "independent_baseline": baseline_report, "join_difference": join_diff,
            "join_seconds": timings, "aggregate_seconds": aggregate_times,
            "physical_node_names": node_names,
            "exchange_counts": {name: sum("Exchange" in node for node in names)
                                for name, names in node_names.items()},
            "benchmark_limitations": "single ordered local run, not a claim of speedup; task metrics in task_metrics.json",
            "inputs_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in [raw / f for f in ["nodes.csv", "streets.csv", "segments.csv", "segment_status.csv"]]},
        }
        (root / "manifest.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        print(json.dumps(metrics, indent=2))
    finally:
        spark.stop()
    write_task_metrics(log_dir, root / "task_metrics.json")


def write_task_metrics(log_dir: Path, destination: Path):
    """Extract measured task durations/records/shuffle from uncompressed event logs."""
    groups, tasks = {}, []
    for path in sorted(log_dir.rglob("*")):
        if not path.is_file() or not path.name.startswith("events_"):
            continue
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                event = json.loads(line)
                if event.get("Event") == "SparkListenerJobStart":
                    group = (event.get("Properties") or {}).get("spark.jobGroup.id")
                    for stage in event["Stage IDs"]:
                        groups[stage] = group
                elif event.get("Event") == "SparkListenerTaskEnd":
                    info, metric = event["Task Info"], event.get("Task Metrics", {})
                    read = metric.get("Shuffle Read Metrics", {})
                    tasks.append({"stage": event["Stage ID"], "partition": info["Index"],
                                  "duration_ms": info["Finish Time"] - info["Launch Time"],
                                  "executor_run_ms": metric.get("Executor Run Time"),
                                  "shuffle_read_bytes": read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0),
                                  "shuffle_write_bytes": metric.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0),
                                  "shuffle_records_read": read.get("Total Records Read", 0),
                                  "successful": event.get("Task End Reason", {}).get("Reason") == "Success"})
    for task in tasks:
        task["job_group"] = groups.get(task["stage"])
    measured = [t for t in tasks if t["job_group"] in {"monoid_salted", "monoid_unsalted"}]
    import statistics
    summaries = []
    for stage in sorted({t["stage"] for t in measured}):
        stage_tasks = [t for t in measured if t["stage"] == stage and t["successful"]]
        if not stage_tasks:
            continue
        durations = [t["duration_ms"] for t in stage_tasks]
        median = statistics.median(durations)
        summaries.append({"stage": stage, "job_group": stage_tasks[0]["job_group"],
                          "successful_tasks": len(stage_tasks), "median_duration_ms": median,
                          "max_duration_ms": max(durations),
                          "max_to_median_duration": max(durations) / median if median else None,
                          "shuffle_read_bytes": sum(t["shuffle_read_bytes"] for t in stage_tasks),
                          "shuffle_write_bytes": sum(t["shuffle_write_bytes"] for t in stage_tasks)})
    destination.write_text(json.dumps({"source": "SparkListenerTaskEnd", "stages": summaries,
                                       "tasks": measured}, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", default="raw_data")
    parser.add_argument("--output-root", required=True, help="New snapshot path; existing directory is rejected")
    parser.add_argument("--train-end", required=True, help="Exclusive UTC warmup/fit cutoff, e.g. 2020-08-01T00:00:00Z")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--partitions", type=int, default=8)
    execute(parser.parse_args())


if __name__ == "__main__":
    main()
