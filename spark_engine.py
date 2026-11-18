"""Unified PySpark & PyMongo Distributed Execution Engine (spark_engine.py).

Executes real Apache Spark (Catalyst SQL, Structured Streaming, FPGrowth, KMeans, MLlib Pipeline)
and real PyMongo partitioned bulk upserts with atomic $status_version guards + MongoDB 8.0
explain("executionStats") when executed inside the Spark runtime container or local PySpark environment.
Never fabricates fallback plans or metrics: if a runtime is unreachable, returns status='not_verified'.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import shutil
import time
from pathlib import Path
from typing import Any

VALID_ROAD_TYPES = (
    "motorway",
    "motorway_link",
    "trunk",
    "trunk_link",
    "primary",
    "primary_link",
    "secondary",
    "secondary_link",
    "tertiary",
    "tertiary_link",
    "residential",
    "unclassified",
    "service",
    "living_street",
    "pedestrian",
    "bridge",
)

FULL_ML_FEATURES_23D = [
    "current_los_ordinal",
    "lag1_los_ordinal",
    "lag2_los_ordinal",
    "trailing_severe_rate",
    "mean_velocity",
    "velocity_ratio",
    "obs_count",
    "length_m",
    "max_velocity",
    "street_level",
    "weekday",
    "period_sin",
    "period_cos",
    "gap_minutes",
    "pagerank_score",
    "in_degree",
    "out_degree",
    "neighbor_los_ordinal",
    "cluster_id",
    "segment_train_prior_los",
    "segment_train_severe_prior",
    "street_causal_prior_los",
    "street_causal_prior_sev",
]


def _iter_child_plan_nodes(plan_node: dict[str, Any]) -> Any:
    for key in ("inputStage", "queryPlan", "outerStage", "innerStage"):
        if key in plan_node:
            yield plan_node[key]
    for key in ("inputStages", "shards"):
        children = plan_node.get(key)
        if isinstance(children, list):
            yield from children


def _extract_mongo_stages(plan_node: Any) -> list[str]:
    if not isinstance(plan_node, dict):
        return []
    stage = plan_node.get("stage")
    stages = [stage] if isinstance(stage, str) else []
    for child in _iter_child_plan_nodes(plan_node):
        stages.extend(_extract_mongo_stages(child))
    return stages


def _extract_mongo_index_name(plan_node: Any) -> str | None:
    if not isinstance(plan_node, dict):
        return None
    if isinstance(plan_node.get("indexName"), str):
        return str(plan_node["indexName"])
    for child in _iter_child_plan_nodes(plan_node):
        found = _extract_mongo_index_name(child)
        if found:
            return found
    return None


def _capture_plan_text(df: Any) -> str:
    """Capture the executed physical plan string without duplicating simpleString()."""
    return str(df._jdf.queryExecution().executedPlan().toString())


def _live_physical_nodes(df: Any) -> list[str]:
    """Walk executedPlan() nodes, excluding historical plans nested inside InMemoryRelation."""
    names: list[str] = []

    def visit(node: Any) -> None:
        names.append(str(node.nodeName()))
        children = node.children().iterator()
        while children.hasNext():
            visit(children.next())

    visit(df._jdf.queryExecution().executedPlan())
    return names


def _quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    pos = q * (len(sorted_vals) - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


def _summarize_ms(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    q25 = _quantile(ordered, 0.25)
    median = _quantile(ordered, 0.50)
    q75 = _quantile(ordered, 0.75)
    return {
        "min_ms": round(ordered[0], 2),
        "median_ms": round(median, 2),
        "p95_ms": round(_quantile(ordered, 0.95), 2),
        "iqr_ms": round(q75 - q25, 2),
        "max_ms": round(ordered[-1], 2),
    }


def _measure_alternating_pair(
    op_a: Any,
    op_b: Any,
    warmup: int = 2,
    repeats: int = 5,
) -> tuple[list[float], list[float]]:
    """Run interleaved warmup + measured passes to control JVM/JIT and cache warmness bias."""
    times_a: list[float] = []
    times_b: list[float] = []
    for idx in range(warmup + repeats):
        t0 = time.perf_counter()
        op_a()
        dur_a = (time.perf_counter() - t0) * 1000.0
        t1 = time.perf_counter()
        op_b()
        dur_b = (time.perf_counter() - t1) * 1000.0
        if idx >= warmup:
            times_a.append(dur_a)
            times_b.append(dur_b)
    return times_a, times_b


def _prepare_catalyst_inputs(spark: Any, raw_dir: Path) -> tuple[Any, Any, Any]:
    """Load raw tables, apply VALID_ROAD_TYPES filter (16 road types), and cache aligned inputs."""
    from pyspark.sql import functions as F

    status_raw = spark.read.option("header", "true").csv((raw_dir / "segment_status.csv").as_posix())
    segments_raw = spark.read.option("header", "true").csv((raw_dir / "segments.csv").as_posix())
    streets_raw = spark.read.option("header", "true").csv((raw_dir / "streets.csv").as_posix())

    segments_clean = (
        segments_raw.select(
            F.col("_id").cast("int").alias("segment_id"),
            F.col("street_id").cast("int").alias("street_id"),
            F.col("street_level").cast("int").alias("street_level"),
            F.col("street_type"),
            F.col("length").cast("double").alias("length"),
        )
        .filter(
            F.col("segment_id").isNotNull()
            & F.col("street_type").isin(list(VALID_ROAD_TYPES))
            & F.col("street_level").isin(1, 2, 3, 4)
        )
        .cache()
    )
    streets_clean = streets_raw.select(
        F.col("_id").cast("int").alias("street_id"),
        F.col("name").alias("raw_street_name"),
    ).cache()

    status_valid = (
        status_raw.withColumn("segment_id", F.col("segment_id").cast("int"))
        .withColumn("velocity", F.col("velocity").cast("double"))
        .withColumn("event_ts", F.to_timestamp(F.col("updated_at")))
        .filter(
            F.col("segment_id").isNotNull()
            & F.col("velocity").isNotNull()
            & F.col("event_ts").isNotNull()
            & (F.col("velocity") >= 0.0)
            & (F.col("velocity") <= 200.0)
        )
        .join(F.broadcast(segments_clean.select("segment_id")), on="segment_id", how="inner")
        .withColumn("date", F.date_format(F.col("event_ts"), "yyyy-MM-dd"))
        .withColumn(
            "period",
            F.concat(
                F.lit("period_"),
                F.hour(F.col("event_ts")).cast("string"),
                F.lit("_"),
                F.when(F.minute(F.col("event_ts")) < 30, F.lit("00")).otherwise(F.lit("30")),
            ),
        )
        .withColumn("is_low_speed", F.when(F.col("velocity") < 15.0, F.lit(1)).otherwise(F.lit(0)))
        .cache()
    )
    segments_clean.count()
    streets_clean.count()
    status_valid.count()
    return status_valid, segments_clean, streets_clean


def _build_unsalted_monoid(status_valid: Any) -> Any:
    from pyspark.sql import functions as F

    return (
        status_valid.groupBy("segment_id", "date", "period")
        .agg(
            F.count("velocity").alias("obs_count"),
            F.sum("velocity").alias("sum_velocity"),
            F.min("velocity").alias("min_velocity"),
            F.max("velocity").alias("max_velocity_obs"),
            F.sum("is_low_speed").alias("severe_ping_count"),
        )
        .withColumn("mean_velocity", F.col("sum_velocity") / F.col("obs_count"))
    )


def _build_salted_monoid(status_valid: Any, buckets: int = 8) -> tuple[Any, Any]:
    from pyspark.sql import functions as F

    salted_df = status_valid.withColumn(
        "salt", F.pmod(F.xxhash64("segment_id", "updated_at", "velocity"), F.lit(buckets))
    )
    stage1 = salted_df.groupBy("segment_id", "date", "period", "salt").agg(
        F.count("velocity").alias("p_cnt"),
        F.sum("velocity").alias("p_sum"),
        F.min("velocity").alias("p_min"),
        F.max("velocity").alias("p_max"),
        F.sum("is_low_speed").alias("p_sev"),
    )
    stage2 = (
        stage1.groupBy("segment_id", "date", "period")
        .agg(
            F.sum("p_cnt").alias("obs_count"),
            F.sum("p_sum").alias("sum_velocity"),
            F.min("p_min").alias("min_velocity"),
            F.max("p_max").alias("max_velocity_obs"),
            F.sum("p_sev").alias("severe_ping_count"),
        )
        .withColumn("mean_velocity", F.col("sum_velocity") / F.col("obs_count"))
    )
    return stage1, stage2


def _verify_monoid_equivalence(unsalted_df: Any, salted_df: Any) -> int:
    """Verify exact two-way exceptAll parity across all 5 monoid components plus rounded mean."""
    from pyspark.sql import functions as F

    def proj(frame: Any) -> Any:
        return frame.select(
            "segment_id",
            "date",
            "period",
            "obs_count",
            F.round(F.col("sum_velocity"), 5).alias("sum_velocity"),
            "min_velocity",
            "max_velocity_obs",
            "severe_ping_count",
            F.round(F.col("mean_velocity"), 5).alias("mean_velocity"),
        )

    left_p = proj(unsalted_df)
    right_p = proj(salted_df)
    return int(left_p.exceptAll(right_p).count() + right_p.exceptAll(left_p).count())


def _run_monoid_experiment(status_valid: Any) -> tuple[dict[str, Any], Any]:
    """Experiment 2: Controlled 2-warmup + 5-run alternating Unsalted vs 8-Bucket Salted Monoid."""
    u_times, s_times = _measure_alternating_pair(
        lambda: _build_unsalted_monoid(status_valid).write.format("noop").mode("overwrite").save(),
        lambda: _build_salted_monoid(status_valid, buckets=8)[1].write.format("noop").mode("overwrite").save(),
        warmup=2,
        repeats=5,
    )
    unsalted_agg = _build_unsalted_monoid(status_valid)
    stage1_df, stage2_df = _build_salted_monoid(status_valid, buckets=8)
    unsalted_cnt = int(unsalted_agg.count())
    stage1_cnt = int(stage1_df.count())
    stage2_cnt = int(stage2_df.count())
    diff_cnt = _verify_monoid_equivalence(unsalted_agg, stage2_df)

    u_stats = _summarize_ms(u_times)
    s_stats = _summarize_ms(s_times)
    result = {
        "unsalted_windows": unsalted_cnt,
        "stage1_salted_groups": stage1_cnt,
        "stage2_reduced_windows": stage2_cnt,
        "unsalted_median_ms": u_stats["median_ms"],
        "salted_median_ms": s_stats["median_ms"],
        "unsalted_iqr_ms": u_stats["iqr_ms"],
        "salted_iqr_ms": s_stats["iqr_ms"],
        "unsalted_runtime_ms": u_stats["median_ms"],
        "salted_runtime_ms": s_stats["median_ms"],
        "two_way_exceptAll_diff": diff_cnt,
    }
    return result, unsalted_agg


def _run_join_experiment(
    spark: Any,
    unsalted_agg: Any,
    segments_clean: Any,
    streets_clean: Any,
) -> dict[str, Any]:
    """Experiment 1: Controlled 2-warmup + 5-run alternating SortMergeJoin vs BroadcastHashJoin."""
    from pyspark.sql import functions as F

    def build_base() -> Any:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        return unsalted_agg.join(segments_clean, on="segment_id", how="inner").join(
            streets_clean, on="street_id", how="left"
        )

    def build_opt() -> Any:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
        return unsalted_agg.join(F.broadcast(segments_clean), on="segment_id", how="inner").join(
            F.broadcast(streets_clean), on="street_id", how="left"
        )

    base_times, opt_times = _measure_alternating_pair(
        lambda: build_base().write.format("noop").mode("overwrite").save(),
        lambda: build_opt().write.format("noop").mode("overwrite").save(),
        warmup=2,
        repeats=5,
    )
    base_stats = _summarize_ms(base_times)
    opt_stats = _summarize_ms(opt_times)
    base_med = base_stats["median_ms"]
    opt_med = opt_stats["median_ms"]
    speedup_pct = round((base_med - opt_med) / base_med * 100.0, 2) if base_med > 0 else 0.0

    joined_base = build_base()
    base_plan = _capture_plan_text(joined_base)
    base_nodes = _live_physical_nodes(joined_base)
    base_exchanges = len(re.findall(r"\bExchange\b", base_plan))
    base_smj = sum(1 for n in base_nodes if n in {"SortMergeJoin", "ShuffledHashJoin"})

    joined_opt = build_opt()
    opt_plan = _capture_plan_text(joined_opt)
    opt_nodes = _live_physical_nodes(joined_opt)
    opt_exchanges = len(re.findall(r"\bExchange\b", opt_plan))
    opt_bhj = sum(1 for n in opt_nodes if n == "BroadcastHashJoin")

    join_cols = ["segment_id", "date", "period", "obs_count", "street_id", "street_level"]
    diff_ab = joined_base.select(join_cols).exceptAll(joined_opt.select(join_cols)).count()
    diff_ba = joined_opt.select(join_cols).exceptAll(joined_base.select(join_cols)).count()

    return {
        "baseline_median_ms": base_med,
        "baseline_iqr_ms": base_stats["iqr_ms"],
        "optimized_median_ms": opt_med,
        "optimized_iqr_ms": opt_stats["iqr_ms"],
        "join_speedup_pct": speedup_pct,
        "baseline_exchange_stages_measured": int(base_exchanges),
        "baseline_shuffle_joins_measured": int(base_smj),
        "optimized_exchange_stages_measured": int(opt_exchanges),
        "optimized_broadcast_joins_measured": int(opt_bhj),
        "broadcast_hash_join_present": opt_bhj > 0,
        "two_way_exceptAll_diff": int(diff_ab + diff_ba),
        "baseline_plan_excerpt": base_plan[:1200],
        "optimized_plan_excerpt": opt_plan[:1200],
    }


def _run_spark_catalyst_and_monoid(spark: Any, raw_dir: Path, lakehouse_dir: Path) -> dict[str, Any]:
    from pyspark.sql import functions as F

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    status_valid, segments_clean, streets_clean = _prepare_catalyst_inputs(spark, raw_dir)
    monoid_info, unsalted_agg = _run_monoid_experiment(status_valid)
    catalyst_info = _run_join_experiment(spark, unsalted_agg, segments_clean, streets_clean)

    gold_root = lakehouse_dir / "curated_gold"
    pruning_plan = ""
    has_partition_filter = False
    if gold_root.is_dir():
        gold_df = (
            spark.read.parquet(gold_root.as_posix())
            .filter(F.col("year_month") == "2020-08")
            .select("segment_id", "date", "period", "LOS")
        )
        pruning_plan = _capture_plan_text(gold_df)
        has_partition_filter = "PartitionFilters" in pruning_plan and "ReadSchema" in pruning_plan

    return {
        "status": "verified",
        "verified_by_spark_runtime": True,
        "spark_version": spark.version,
        "monoid_salting_spark": monoid_info,
        "catalyst_join_spark": catalyst_info,
        "parquet_pruning_spark": {
            "partition_filters_and_read_schema_verified": has_partition_filter,
            "pruning_plan_excerpt": pruning_plan[:900],
        },
    }


def _filter_spark_train_split(df: Any) -> Any:
    """Filter Spark DataFrame strictly to the timestamp-purged TRAIN split (D02/D07/D08)."""
    from pyspark.sql import functions as F

    cols = set(df.columns)
    if "split_set" in cols:
        return df.filter(F.col("split_set") == "TRAIN")
    if "slot_ts" in cols and "target_ts" in cols:
        cutoff = F.to_timestamp(F.lit("2020-12-12T08:00:00Z"))
        return df.filter((F.to_timestamp(F.col("slot_ts")) <= cutoff) & (F.to_timestamp(F.col("target_ts")) <= cutoff))
    return df.filter(F.col("date") <= "2020-12-12")


def run_spark_structured_streaming_dual_sink(
    spark: Any,
    df: Any,
    output_dir: Path,
) -> dict[str, Any]:
    """[M03/M04/M12] Execute 2-batch Dual-Sink Spark Structured Streaming using real slot_ts."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    stream_in_dir = output_dir / "stream_landing_json"
    bronze_sink_dir = output_dir / "spark_bronze_stream_sink"
    ck_spark_raw = output_dir / "checkpoints" / "spark_raw_wal"
    ck_spark_agg = output_dir / "checkpoints" / "spark_agg_wal"
    for target in (stream_in_dir, bronze_sink_dir, ck_spark_raw, ck_spark_agg):
        shutil.rmtree(target, ignore_errors=True)
    stream_in_dir.mkdir(parents=True, exist_ok=True)

    ordered = (
        df.filter(F.col("slot_ts").isNotNull())
        .select(
            F.col("event_id"),
            F.col("segment_id").cast("int"),
            F.col("mean_velocity").cast("double").alias("velocity"),
            F.col("slot_ts").cast("string").alias("event_ts_str"),
        )
        .orderBy("event_ts_str")
    )
    batch0 = ordered.limit(200)
    batch1 = ordered.filter(F.col("event_ts_str") >= "2020-12-01").limit(200)
    if batch1.limit(1).count() == 0:
        batch1 = ordered.orderBy(F.col("event_ts_str").desc()).limit(200)

    batch0.coalesce(1).write.mode("overwrite").json((stream_in_dir / "batch0").as_posix())
    batch1.coalesce(1).write.mode("overwrite").json((stream_in_dir / "batch1").as_posix())

    stream_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("segment_id", IntegerType(), True),
            StructField("velocity", DoubleType(), True),
            StructField("event_ts_str", StringType(), True),
        ]
    )
    raw_stream = (
        spark.readStream.schema(stream_schema)
        .option("maxFilesPerTrigger", 1)
        .json((stream_in_dir / "batch*").as_posix())
        .withColumn("event_ts", F.to_timestamp("event_ts_str"))
    )
    q1 = (
        raw_stream.writeStream.format("parquet")
        .outputMode("append")
        .option("path", bronze_sink_dir.as_posix())
        .option("checkpointLocation", ck_spark_raw.as_posix())
        .trigger(availableNow=True)
        .start()
    )
    q1.awaitTermination(timeout=45)
    last_progress = q1.lastProgress

    agg_stream = (
        raw_stream.withWatermark("event_ts", "30 minutes")
        .groupBy(F.window("event_ts", "30 minutes"))
        .agg(
            F.count("event_id").alias("events_in_window"),
            F.approx_count_distinct("segment_id", rsd=0.05).alias("hll_active_segments"),
        )
    )
    q2 = (
        agg_stream.writeStream.format("memory")
        .queryName("hcmc_agg_sink2")
        .outputMode("update")
        .option("checkpointLocation", ck_spark_agg.as_posix())
        .trigger(availableNow=True)
        .start()
    )
    q2.awaitTermination(timeout=45)
    agg_progress = q2.lastProgress
    agg_recent = list(q2.recentProgress)
    q2.stop()

    wm_str = (agg_progress or {}).get("eventTime", {}).get("watermark")
    return {
        "spark_structured_streaming_wal_created": (ck_spark_raw / "offsets").is_dir(),
        "spark_sink1_output_path": bronze_sink_dir.as_posix(),
        "spark_sink1_last_progress": last_progress,
        "spark_sink2_last_progress": agg_progress,
        "spark_sink2_watermark": wm_str,
        "spark_sink2_watermark_advanced": bool(wm_str and not str(wm_str).startswith("1970-01-01")),
        "spark_sink2_batches_committed": len(agg_recent),
    }


def _verify_kmeans_fit(
    df: Any,
    train_cluster_df: Any,
    km_model: Any,
    km_preds: Any,
    num_features: int,
) -> dict[str, Any]:
    """Programmatically verify KMeans convergence (k=6), StandardScaler, and TRAIN-only split purity."""
    from pyspark.sql import functions as F

    scaler_stage = km_model.stages[1]
    km_stage = km_model.stages[2]
    clusters_found = int(km_preds.select("prediction").distinct().count())
    cost = float(km_stage.summary.trainingCost)
    converged = len(km_stage.clusterCenters()) == 6 and clusters_found == 6 and math.isfinite(cost) and cost > 0.0

    if "split_set" in df.columns:
        has_non_train = df.filter(F.col("split_set") != "TRAIN").limit(1).count() > 0
        train_pure = train_cluster_df.filter(F.col("split_set") != "TRAIN").limit(1).count() == 0
        train_only_verified = bool(has_non_train and train_pure and converged)
    else:
        train_only_verified = bool(train_cluster_df.count() < df.count() and converged)

    stds = list(scaler_stage.std)
    scaler_verified = (
        len(scaler_stage.mean) == num_features
        and len(stds) == num_features
        and all(math.isfinite(float(s)) and float(s) > 0.0 for s in stds)
    )
    return {
        "spark_kmeans_clusters_populated": clusters_found,
        "spark_kmeans_train_only_verified": train_only_verified,
        "spark_kmeans_standard_scaler_verified": scaler_verified,
        "spark_kmeans_training_cost": round(cost, 4),
        "spark_kmeans_iterations": int(km_stage.summary.numIter),
    }


def _evaluate_spark_rf_test(fitted_pipe: Any, test_spark: Any) -> dict[str, Any]:
    """Evaluate fitted SparkRF PipelineModel on held-out TEST rows (6-class accuracy + severe ROC-AUC)."""
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.functions import array_to_vector, vector_to_array
    from pyspark.sql import functions as F

    test_preds = fitted_pipe.transform(test_spark).cache()
    test_rows = int(test_preds.count())
    if test_rows == 0:
        return {
            "spark_rf_test_rows": 0,
            "spark_rf_test_accuracy_6class": None,
            "spark_rf_test_severe_roc_auc": None,
        }
    acc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    acc6 = round(float(acc_eval.evaluate(test_preds)), 4)

    n_classes = len(test_preds.select("probability").first()[0])
    prob_arr = vector_to_array(F.col("probability"))
    sev_prob = sum((prob_arr[i] for i in range(4, n_classes)), F.lit(0.0))
    bin_df = test_preds.withColumn("sev_label", (F.col("label") >= 4.0).cast("double")).withColumn(
        "sev_raw", array_to_vector(F.array(F.lit(1.0) - sev_prob, sev_prob))
    )
    auc_eval = BinaryClassificationEvaluator(
        labelCol="sev_label", rawPredictionCol="sev_raw", metricName="areaUnderROC"
    )
    sev_auc = round(float(auc_eval.evaluate(bin_df)), 4)
    return {
        "spark_rf_test_rows": test_rows,
        "spark_rf_test_accuracy_6class": acc6,
        "spark_rf_test_severe_roc_auc": sev_auc,
    }


def _run_spark_mllib_and_streaming(
    spark: Any,
    dataset_path: Path,
    lakehouse_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    from pyspark.ml import Pipeline, PipelineModel
    from pyspark.ml.classification import RandomForestClassifier as SparkRF
    from pyspark.ml.clustering import KMeans as SparkKMeans
    from pyspark.ml.feature import (
        Imputer,
        OneHotEncoder,
        StandardScaler as SparkStandardScaler,
        StringIndexer,
        VectorAssembler,
    )
    from pyspark.ml.fpm import FPGrowth
    from pyspark.sql import functions as F

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(dataset_path.as_posix())
    if "current_los_ordinal" not in df.columns:
        df = df.withColumn("current_los_ordinal", F.col("los_ordinal").cast("double"))

    sev_baskets = (
        df.filter((F.col("is_severe") == 1) & F.col("street_name").isNotNull())
        .groupBy("date", "period")
        .agg(F.collect_set("street_name").alias("items"))
        .filter(F.size("items") >= 2)
    )
    fp_model = FPGrowth(itemsCol="items", minSupport=0.02, minConfidence=0.5).fit(sev_baskets)
    fp_rules_count = int(fp_model.associationRules.count())

    cluster_cols = [
        "segment_train_prior_los",
        "segment_train_severe_prior",
        "mean_velocity",
        "velocity_ratio",
        "length_m",
        "max_velocity",
        "pagerank_score",
    ]
    train_cluster_df = _filter_spark_train_split(df)
    seg_df = (
        train_cluster_df.groupBy("segment_id")
        .agg(*[F.avg(F.col(c).cast("double")).alias(c) for c in cluster_cols])
        .na.fill(0.0)
    )
    km_model = Pipeline(
        stages=[
            VectorAssembler(inputCols=cluster_cols, outputCol="raw_features", handleInvalid="keep"),
            SparkStandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
            SparkKMeans(k=6, seed=42, featuresCol="features"),
        ]
    ).fit(seg_df)
    km_preds = km_model.transform(seg_df)
    km_train_segments = int(seg_df.count())
    km_info = _verify_kmeans_fit(df, train_cluster_df, km_model, km_preds, len(cluster_cols))

    sup_df = df.filter(F.col("target_next_los_ordinal").isNotNull()).withColumn(
        "label", F.col("target_next_los_ordinal").cast("double")
    )
    num_cols = list(FULL_ML_FEATURES_23D)
    for col_name in num_cols:
        sup_df = sup_df.withColumn(col_name, F.col(col_name).cast("double"))

    imp_out = [f"{c}_imp" for c in num_cols]
    pipeline = Pipeline(
        stages=[
            Imputer(inputCols=num_cols, outputCols=imp_out, strategy="median"),
            StringIndexer(
                inputCols=["street_type", "period"],
                outputCols=["street_type_idx", "period_idx_cat"],
                handleInvalid="keep",
            ),
            OneHotEncoder(
                inputCols=["street_type_idx", "period_idx_cat"],
                outputCols=["street_type_ohe", "period_ohe"],
                handleInvalid="keep",
            ),
            VectorAssembler(
                inputCols=imp_out + ["street_type_ohe", "period_ohe"],
                outputCol="features",
                handleInvalid="keep",
            ),
            SparkRF(featuresCol="features", labelCol="label", numTrees=25, maxDepth=6, seed=42),
        ]
    )

    train_spark = _filter_spark_train_split(sup_df)
    train_spark_rows = int(train_spark.count())
    fitted_pipe = pipeline.fit(train_spark)
    model_save_dir = output_dir / "spark_pipeline_model"
    fitted_pipe.write().overwrite().save(model_save_dir.as_posix())

    reloaded_pipe = PipelineModel.load(model_save_dir.as_posix())
    sample_test = sup_df.limit(50)
    pred_a = [int(r.prediction) for r in fitted_pipe.transform(sample_test).select("prediction").collect()]
    pred_b = [int(r.prediction) for r in reloaded_pipe.transform(sample_test).select("prediction").collect()]

    unseen_df = (
        sample_test.limit(2)
        .withColumn("period", F.lit("period_99_00"))
        .withColumn("street_type", F.lit("unseen_road_type"))
    )
    unseen_preds = reloaded_pipe.transform(unseen_df).select("prediction").count()

    if "split_set" in sup_df.columns:
        test_spark = sup_df.filter(F.col("split_set") == "TEST")
    else:
        test_spark = sup_df.filter(F.to_timestamp(F.col("slot_ts")) > F.to_timestamp(F.lit("2020-12-25T03:30:00Z")))
    test_eval = _evaluate_spark_rf_test(fitted_pipe, test_spark)
    stream_info = run_spark_structured_streaming_dual_sink(spark, df, output_dir)

    return {
        "spark_fpgrowth_rules_count": fp_rules_count,
        **km_info,
        "spark_kmeans_train_segments": km_train_segments,
        "spark_rf_train_rows": train_spark_rows,
        "spark_rf_numeric_features_count": len(num_cols),
        **test_eval,
        "spark_pipeline_model_saved": (model_save_dir / "metadata").is_dir(),
        "spark_pipeline_reload_parity": pred_a == pred_b,
        "spark_unseen_category_handled": int(unseen_preds) == 2,
        **stream_info,
    }


def _sanitize_scalar(val: Any) -> Any:
    if hasattr(val, "item") and not isinstance(val, (bytes, str)):
        val = val.item()
    if hasattr(val, "isoformat"):
        return str(val.isoformat())
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _sanitize_mongo_doc(raw_doc: dict[str, Any]) -> dict[str, Any]:
    """Convert serving record into a BSON-safe dict preserving real status_version semantics."""
    clean: dict[str, Any] = {}
    for key, val in raw_doc.items():
        if key == "_id":
            continue
        sanitized = _sanitize_scalar(val)
        if sanitized is not None:
            clean[str(key)] = sanitized
    clean["event_id"] = str(raw_doc["event_id"])
    raw_ver = clean.get("status_version")
    if raw_ver is not None:
        clean["status_version"] = int(raw_ver)
    else:
        status_str = str(clean.get("lifecycle_status", ""))
        clean["status_version"] = 2 if status_str in {"BATCH_VERIFIED", "BATCH_RECONCILED"} else 1
    return clean


def _upsert_partition_to_mongo(
    docs: list[dict[str, Any]],
    mongo_uri: str,
    db_name: str = "bda501",
    coll_name: str = "traffic_scores",
    batch_size: int = 500,
) -> dict[str, int]:
    from pymongo import MongoClient, UpdateOne

    client: MongoClient = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    coll = client[db_name][coll_name]
    upserted = 0
    matched = 0
    modified = 0
    batches = 0

    for offset in range(0, len(docs), batch_size):
        chunk = docs[offset : offset + batch_size]
        ops = []
        for raw_doc in chunk:
            doc = _sanitize_mongo_doc(raw_doc)
            incoming_ver = int(doc["status_version"])
            ops.append(
                UpdateOne(
                    {"event_id": doc["event_id"]},
                    [
                        {
                            "$replaceRoot": {
                                "newRoot": {
                                    "$cond": [
                                        {"$gt": [{"$ifNull": ["$status_version", 0]}, incoming_ver]},
                                        "$$ROOT",
                                        {"$mergeObjects": ["$$ROOT", {"$literal": doc}]},
                                    ]
                                }
                            }
                        }
                    ],
                    upsert=True,
                )
            )
        if ops:
            res = coll.bulk_write(ops, ordered=False)
            upserted += int(res.upserted_count)
            matched += int(res.matched_count)
            modified += int(res.modified_count)
            batches += 1
    client.close()
    return {
        "batches": batches,
        "upserted": upserted,
        "matched": matched,
        "modified": modified,
        "rows": len(docs),
    }


def _load_serving_frame(serving_parquet_path: Path, spark: Any = None) -> Any:
    import pandas as pd

    try:
        return pd.read_parquet(serving_parquet_path)
    except ImportError:
        owns_spark = False
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.master("local[2]").appName("MongoLoader").getOrCreate()
            owns_spark = True
        sdf = spark.read.parquet(serving_parquet_path.as_posix())
        rows = [r.asDict() for r in sdf.collect()]
        if owns_spark:
            spark.stop()
        return pd.DataFrame(rows)


def _benchmark_mongo_queries(
    coll: Any,
    query_filter: dict[str, Any],
    query_sort: list[tuple[str, int]],
) -> dict[str, Any]:
    """Measure real IXSCAN (100 warm runs) and COLLSCAN (30 warm runs) latencies."""
    t_cold = time.perf_counter()
    _ = list(coll.find(query_filter, {"_id": 0}).sort(query_sort).limit(25))
    cold_ms = round((time.perf_counter() - t_cold) * 1000.0, 3)

    warm_latencies: list[float] = []
    for _ in range(100):
        t_w = time.perf_counter()
        _ = list(coll.find(query_filter, {"_id": 0}).sort(query_sort).limit(25))
        warm_latencies.append((time.perf_counter() - t_w) * 1000.0)
    warm_sorted = sorted(warm_latencies)
    ix_median = round(_quantile(warm_sorted, 0.50), 3)
    ix_p95 = round(_quantile(warm_sorted, 0.95), 3)

    # Forced natural scan (COLLSCAN) benchmark: 1 warmup + 30 measured runs
    _ = list(coll.find(query_filter, {"_id": 0}).sort(query_sort).limit(25).hint([("$natural", 1)]))
    collscan_latencies: list[float] = []
    for _ in range(30):
        t_c = time.perf_counter()
        _ = list(coll.find(query_filter, {"_id": 0}).sort(query_sort).limit(25).hint([("$natural", 1)]))
        collscan_latencies.append((time.perf_counter() - t_c) * 1000.0)
    coll_sorted = sorted(collscan_latencies)
    coll_median = round(_quantile(coll_sorted, 0.50), 3)
    coll_p95 = round(_quantile(coll_sorted, 0.95), 3)

    return {
        "cold_query_ms": cold_ms,
        "warm_min_ms": round(warm_sorted[0], 3),
        "warm_median_ms": ix_median,
        "warm_p95_ms": ix_p95,
        "warm_max_ms": round(warm_sorted[-1], 3),
        "sla_under_10ms_verified": ix_p95 < 10.0,
        "collscan_warm_median_ms": coll_median,
        "collscan_warm_p95_ms": coll_p95,
        "ixscan_vs_collscan_speedup_x": round(coll_median / max(ix_median, 1e-6), 2),
    }


def _verify_live_mongo_version_guard(
    serving_df: Any,
    coll: Any,
    mongo_uri: str,
    expected_total: int,
) -> tuple[int, bool]:
    """Send 25 stale v1 retry documents against live v2 Mongo docs and confirm atomic rejection."""
    v2_rows = serving_df[serving_df["status_version"] == 2].head(25)
    stale_docs = [
        {**row, "lifecycle_status": "STREAM_PROVISIONAL", "status_version": 1}
        for row in v2_rows.to_dict(orient="records")
    ]
    if not stale_docs:
        return 0, False
    guard_res = _upsert_partition_to_mongo(stale_docs, mongo_uri=mongo_uri, batch_size=500)
    target_eids = [str(doc["event_id"]) for doc in stale_docs]
    count_after = int(coll.count_documents({}))
    still_v2 = int(coll.count_documents({"event_id": {"$in": target_eids}, "status_version": {"$gte": 2}}))
    rejected = len(stale_docs) - int(guard_res["modified"])
    verified = (
        len(stale_docs) == 25
        and int(guard_res["modified"]) == 0
        and still_v2 == len(stale_docs)
        and count_after == expected_total
    )
    return rejected, verified


def run_mongo_bulk_upsert_and_explain(
    serving_parquet_path: Path,
    mongo_uri: str,
    output_path: Path,
    model_sha256: str = "",
    spark: Any = None,
) -> dict[str, Any]:
    """[M07/M08/M09] Bulk upsert serving docs across 4 partitions into MongoDB 8.0 and run explain."""
    serving_df = _load_serving_frame(serving_parquet_path, spark=spark)
    part_counts = [int((serving_df["write_partition"] == pid).sum()) for pid in range(4)]
    bulk_batches = sum(math.ceil(cnt / 500) for cnt in part_counts)
    arterial_df = serving_df[
        (serving_df["street_level"].isin([1, 2])) & (serving_df["severe_prob"] >= 0.70)
    ].sort_values(["severe_prob", "segment_id"], ascending=[False, True])
    top_5_alerts = arterial_df.head(5).to_dict(orient="records")

    try:
        from pymongo import ASCENDING, DESCENDING, MongoClient

        client: MongoClient = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        mongo_version = str(client.server_info().get("version", "unknown"))
        coll = client["bda501"]["traffic_scores"]
        coll.delete_many({})
        coll.create_index([("event_id", ASCENDING)], unique=True, name="event_id_unique")
        coll.create_index(
            [("street_level", ASCENDING), ("severe_prob", DESCENDING), ("segment_id", ASCENDING)],
            name="level_prob_segment_idx",
        )

        partition_summaries = []
        for pid in range(4):
            p_docs = serving_df[serving_df["write_partition"] == pid].to_dict(orient="records")
            p_res = _upsert_partition_to_mongo(p_docs, mongo_uri=mongo_uri, batch_size=500)
            partition_summaries.append({"partition_id": pid, **p_res})

        count_after_first_write = int(coll.count_documents({}))

        p0_docs = serving_df[serving_df["write_partition"] == 0].head(500).to_dict(orient="records")
        _upsert_partition_to_mongo(p0_docs, mongo_uri=mongo_uri, batch_size=500)
        count_after_replay = int(coll.count_documents({}))

        stale_rejected, guard_verified = _verify_live_mongo_version_guard(
            serving_df, coll, mongo_uri, count_after_first_write
        )

        query_filter = {"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}}
        query_sort = [("severe_prob", DESCENDING), ("segment_id", ASCENDING)]
        latency_stats = _benchmark_mongo_queries(coll, query_filter, query_sort)

        explain_doc = coll.find(query_filter, {"_id": 0}).sort(query_sort).limit(25).explain()
        client.close()

        winning_plan = explain_doc.get("queryPlanner", {}).get("winningPlan", {})
        exec_stats_raw = explain_doc.get("executionStats", {})
        stages = _extract_mongo_stages(winning_plan)
        idx_name = _extract_mongo_index_name(winning_plan)

        report = {
            "component": "[C16] MongoDB 8.0 Operational Serving Store (bda501.traffic_scores)",
            "status": "verified",
            "database": "bda501",
            "collection": "traffic_scores",
            "mongo_server_version": mongo_version,
            "write_protocol": "PyMongo 4-partitioned 500-doc Bulk UpdateOne with atomic $status_version pipeline guard",
            "write_partitions": 4,
            "partition_row_counts": part_counts,
            "partition_write_summaries": partition_summaries,
            "total_rows_upserted": count_after_first_write,
            "count_after_idempotent_replay": count_after_replay,
            "idempotent_replay_verified": count_after_first_write == count_after_replay == len(serving_df),
            "stale_v1_rejected_by_mongo": stale_rejected,
            "mongo_version_guard_verified": guard_verified,
            "bulk_write_batches": int(bulk_batches),
            "batch_size": 500,
            "frozen_model_sha256": model_sha256,
            "indexes_created": {
                "event_id_unique": {"key": [("event_id", 1)], "unique": True},
                "level_prob_segment_idx": {
                    "key": [("street_level", 1), ("severe_prob", -1), ("segment_id", 1)],
                    "unique": False,
                },
            },
            "uc2_arterial_query": {
                "filter": {"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}},
                "sort": [("severe_prob", -1), ("segment_id", 1)],
                "limit": 25,
                "total_matching_windows": int(len(arterial_df)),
                "top_5_arterial_alerts": top_5_alerts,
                "latency_benchmark_ms": latency_stats,
            },
            "ixscan_explain_verification": {
                "status": "verified",
                "verified_against_mongo_8_0": mongo_version.startswith("8."),
                "index_used": idx_name,
                "winning_stages": stages,
                "in_memory_sort": "SORT" in stages,
                "raw_winning_plan": winning_plan,
                "execution_stats": {
                    "nReturned": int(exec_stats_raw.get("nReturned", 0)),
                    "totalKeysExamined": int(exec_stats_raw.get("totalKeysExamined", 0)),
                    "totalDocsExamined": int(exec_stats_raw.get("totalDocsExamined", 0)),
                    "executionTimeMillis": int(exec_stats_raw.get("executionTimeMillis", 0)),
                },
            },
        }
    except Exception as exc:
        report = {
            "component": "[C16] MongoDB 8.0 Operational Serving Store (bda501.traffic_scores)",
            "status": "not_verified",
            "database": "bda501",
            "collection": "traffic_scores",
            "write_partitions": 4,
            "partition_row_counts": part_counts,
            "total_rows_upserted": 0,
            "stale_v1_rejected_by_mongo": 0,
            "mongo_version_guard_verified": False,
            "bulk_write_batches": int(bulk_batches),
            "batch_size": 500,
            "frozen_model_sha256": model_sha256,
            "error_reason": f"{type(exc).__name__}: {exc}",
            "uc2_arterial_query": {
                "filter": {"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}},
                "sort": [("severe_prob", -1), ("segment_id", 1)],
                "limit": 25,
                "total_matching_windows": int(len(arterial_df)),
                "top_5_arterial_alerts": top_5_alerts,
            },
            "ixscan_explain_verification": {
                "status": "not_verified",
                "verified_against_mongo_8_0": False,
                "error_reason": f"{type(exc).__name__}: {exc}",
                "index_used": None,
                "winning_stages": [],
                "in_memory_sort": None,
                "execution_stats": None,
            },
        }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Unified PySpark & PyMongo Verification Runner.")
    parser.add_argument("--raw-dir", type=Path, default=Path("raw_data"))
    parser.add_argument("--dataset-path", type=Path, default=Path("data/dataset.csv"))
    parser.add_argument("--lakehouse-dir", type=Path, default=Path("data/lakehouse"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    parser.add_argument("--mongo-uri", type=str, default="mongodb://mongo:27017")
    parser.add_argument("--model-sha256", type=str, default="")
    parser.add_argument("--mode", choices=["all", "spark", "mongo"], default="all")
    args = parser.parse_args()

    result: dict[str, Any] = {}
    spark = None
    try:
        if args.mode in ("all", "spark"):
            from pyspark.sql import SparkSession

            spark = (
                SparkSession.builder.master("local[4]")
                .appName("HCMC_Smart_Traffic_V3_Spark_Verifier")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("ERROR")
            catalyst_info = _run_spark_catalyst_and_monoid(spark, args.raw_dir, args.lakehouse_dir)
            mllib_info = _run_spark_mllib_and_streaming(spark, args.dataset_path, args.lakehouse_dir, args.output_dir)
            result["spark_verification"] = {**catalyst_info, **mllib_info}
            (args.output_dir / "spark_runtime_evidence.json").write_text(
                json.dumps(result["spark_verification"], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        if args.mode in ("all", "mongo"):
            serving_parquet = args.lakehouse_dir / "serving_traffic_scores.parquet"
            if serving_parquet.is_file():
                result["mongo_verification"] = run_mongo_bulk_upsert_and_explain(
                    serving_parquet,
                    mongo_uri=args.mongo_uri,
                    output_path=args.output_dir / "mongo_verify.json",
                    model_sha256=args.model_sha256,
                    spark=spark,
                )
    finally:
        if spark is not None:
            spark.stop()

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
