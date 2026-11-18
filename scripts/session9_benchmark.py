"""Controlled local Spark benchmarks and two analytical query evidence bundles."""

import json
from pathlib import Path
import statistics
import time

from pyspark.sql import SparkSession, functions as F

from spark_batch import write_task_metrics
from traffic_data.spark_etl import aggregate_monoid, differences


def main():
    root = Path("evidence/session9/benchmark")
    root.mkdir(parents=True, exist_ok=True)
    logs = root / "spark_events"
    logs.mkdir(exist_ok=True)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("session9-controlled")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.dir", logs.resolve().as_uri())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        events = (
            spark.read.option("header", True)
            .csv("raw_data/segment_status.csv")
            .select(
                F.col("_id").alias("raw_event_id"),
                F.col("segment_id").cast("long").alias("segment_id"),
                F.col("velocity").cast("double").alias("velocity"),
                F.to_timestamp("updated_at").alias("event_ts"),
            )
            .filter("segment_id is not null and velocity between 0 and 200 and event_ts is not null")
            .cache()
        )
        dims = (
            spark.read.option("header", True)
            .csv("raw_data/segments.csv")
            .select(F.col("_id").cast("long").alias("segment_id"), "street_id")
            .cache()
        )
        events.count()
        dims.count()
        variants = {
            "smj": events.hint("merge").join(dims.hint("merge"), "segment_id"),
            "bhj": events.join(F.broadcast(dims), "segment_id"),
            "unsalted": aggregate_monoid(events, salted=False),
            "salted": aggregate_monoid(events, salted=True),
        }
        parity = {
            "join": differences(variants["smj"], variants["bhj"]),
            "aggregate": differences(variants["unsalted"], variants["salted"]),
        }
        assert all(not any(result.values()) for result in parity.values()), parity
        timings = []
        for pair in (("smj", "bhj"), ("unsalted", "salted")):
            for iteration in range(7):
                order = pair if iteration % 2 == 0 else pair[::-1]
                for name in order:
                    group = f"{name}_{iteration}"
                    spark.sparkContext.setJobGroup(group, group)
                    started = time.perf_counter()
                    count = len(variants[name].collect())
                    timings.append(
                        dict(
                            variant=name,
                            iteration=iteration,
                            warmup=iteration < 2,
                            seconds=time.perf_counter() - started,
                            rows=count,
                            job_group=group,
                        )
                    )
                    (root / f"{name}_plan.txt").write_text(
                        variants[name]._jdf.queryExecution().toString(), encoding="utf-8"
                    )
        events.createOrReplaceTempView("gps")
        dims.createOrReplaceTempView("segments")
        sql = (
            "SELECT street_id, count(*) AS pings, avg(velocity) AS mean_velocity "
            "FROM gps JOIN segments USING(segment_id) GROUP BY street_id ORDER BY pings DESC LIMIT 10"
        )
        query = spark.sql(sql)
        gold = spark.read.parquet("data/lakehouse/curated_gold")
        # Use actual partition columns when present; projection is explicit in either case.
        month_col = next((c for c in ("year_month", "month") if c in gold.columns), None)
        if month_col:
            selected = gold.select(month_col).first()[0]
            scan = gold.filter(F.col(month_col) == selected).select("segment_id", "mean_velocity")
            scan_text = f"curated_gold.filter({month_col} == {selected!r}).select(segment_id, mean_velocity)"
        else:
            scan = gold.select("segment_id", "mean_velocity")
            scan_text = "curated_gold.select(segment_id, mean_velocity); no partition pruning claim"
        queries = []
        for name, frame, text in (("street_summary", query, sql), ("projected_scan", scan, scan_text)):
            sample = [row.asDict() for row in frame.limit(10).collect()]
            (root / f"query_{name}_plan.txt").write_text(frame._jdf.queryExecution().toString(), encoding="utf-8")
            queries.append(dict(name=name, text=text, sample=sample))
        summary = dict(
            settings=dict(
                master="local[2]",
                partitions=8,
                aqe=False,
                cache="inputs only",
                action="collect",
                warmups=2,
                measurements=5,
            ),
            parity=parity,
            timings=timings,
            median_seconds={
                name: statistics.median(t["seconds"] for t in timings if t["variant"] == name and not t["warmup"])
                for name in variants
            },
            queries=queries,
            limitation=("Single host. Task stages differ for salted aggregation; "
                        "no partition-skew reduction or cluster-scaling claim."),
        )
        (root / "results.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        print(json.dumps(summary, default=str))
    finally:
        spark.stop()
    write_task_metrics(logs, root / "task_metrics.json", include_all_groups=True)


if __name__ == "__main__":
    main()
