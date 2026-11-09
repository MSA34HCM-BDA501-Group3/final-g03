"""Quyen's batch/shared transformations. Compatible with Spark 3.5.7 (Java 17) & Spark 4.2.0 (Java 21).

UTC internal timestamps; no train.csv dependency; no model training or serving side effects.
"""
from __future__ import annotations

import math
import unicodedata

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

if not hasattr(Column, "try_cast"):
    def _try_cast(self: Column, dtype: str) -> Column:
        return F.expr(f"try_cast({self._jc.expr().sql()} as {dtype})")

    Column.try_cast = _try_cast  # type: ignore[attr-defined]

KEYS = ["segment_id", "window_start"]
MONOID = ["obs_count", "sum_velocity", "min_velocity", "max_velocity_obs", "severe_ping_count"]
ROAD_TYPES = [
    "motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link",
    "secondary", "secondary_link", "tertiary", "tertiary_link", "residential",
    "unclassified", "service", "living_street", "pedestrian", "bridge",
]


def read_strings(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", False).option("mode", "FAILFAST").csv(path)


def _payload(frame: DataFrame, source: str) -> DataFrame:
    return frame.withColumn("raw_payload", F.to_json(F.struct(*[F.col(c) for c in frame.columns]))) \
        .withColumn("source", F.lit(source)).withColumn("audited_at", F.current_timestamp())


def _first_rule(frame: DataFrame, rules: list[tuple]) -> DataFrame:
    rule = F.lit(None).cast("string")
    for name, failed in reversed(rules):
        rule = F.when(failed, F.lit(name)).otherwise(rule)
    return frame.withColumn("first_failing_rule", rule)


@F.udf(returnType=StringType())
def normalize_name(value: str | None) -> str:
    return " ".join(unicodedata.normalize("NFC", value or "").replace("Ð", "Đ").replace("ð", "đ").split())


def clean_segments(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Validate GIS segments with ordered rules S1 -> S2 -> S3 -> S4.

    Quarantine is an audit ledger recording the first failing rule (including S2
    non-positive length), whereas S1/S3/S4 are terminal exclusions and S2-only
    valid road segments receive median length imputation with length_imputed=True.
    """
    frame = _payload(raw, "segments.csv")
    for source, target, dtype in [
        ("_id", "segment_id", "bigint"), ("s_node_id", "s_node_id", "bigint"),
        ("e_node_id", "e_node_id", "bigint"), ("street_id", "street_id", "bigint"),
        ("length", "length_m", "double"), ("street_level", "street_level", "int"),
        ("max_velocity", "observed_max_velocity", "double"),
    ]:
        frame = frame.withColumn(target, F.col(source).try_cast(dtype))
    frame = frame.withColumn("street_name", normalize_name("street_name"))
    frame = _first_rule(frame, [
        ("S1_INVALID_KEY", F.col("segment_id").isNull() | F.col("s_node_id").isNull()
         | F.col("e_node_id").isNull() | F.col("street_id").isNull()),
        ("S2_NONPOSITIVE_LENGTH", F.col("length_m").isNull() | F.isnan("length_m") | (F.col("length_m") <= 0)
         | (F.abs(F.col("length_m")) == float("inf"))),
        ("S3_NON_ROAD_TYPE", F.col("street_type").isNull() | ~F.col("street_type").isin(ROAD_TYPES)),
        ("S4_INVALID_LEVEL", F.col("street_level").isNull() | ~F.col("street_level").isin(1, 2, 3, 4)),
    ])
    # Preserve the original failure in quarantine even when a length is repairable.
    # Invalid IDs/types/levels remain excluded. Length repair is explicit in output.
    invalid_length = F.col("length_m").isNull() | F.isnan("length_m") | (F.col("length_m") <= 0) \
        | (F.abs(F.col("length_m")) == float("inf"))
    median = frame.filter(~invalid_length).agg(F.percentile_approx("length_m", .5, 10000).alias("length_median"))
    good = frame.filter(
        F.col("segment_id").isNotNull() & F.col("s_node_id").isNotNull()
        & F.col("e_node_id").isNotNull() & F.col("street_id").isNotNull()
        & F.col("street_type").isin(ROAD_TYPES) & F.col("street_level").isin(1, 2, 3, 4)
    ).crossJoin(F.broadcast(median)).withColumn("length_imputed", invalid_length) \
        .withColumn("length_m", F.when(invalid_length, F.coalesce("length_median", F.lit(27.)))
                    .otherwise(F.col("length_m"))).select(
        "segment_id", "s_node_id", "e_node_id", "street_id", "length_m", "street_level",
        "street_name", "street_type", "observed_max_velocity", "length_imputed",
    )
    return good, frame.filter("first_failing_rule IS NOT NULL")


def clean_events(raw: DataFrame, segments: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Shared with foreachBatch; raw IDs must be stable across delivery retries.

    Required string columns: raw_event_id, segment_id, velocity, updated_at.
    Identity conflicts are rejected, not arbitrarily resolved by dropDuplicates.
    """
    frame = _payload(raw, "segment_status")
    frame = frame.withColumn("segment_id", F.col("segment_id").try_cast("bigint")) \
        .withColumn("velocity", F.col("velocity").try_cast("double")) \
        .withColumn("event_ts", F.try_to_timestamp("updated_at"))
    known = segments.select("segment_id").distinct().withColumn("known_segment", F.lit(True))
    frame = frame.join(F.broadcast(known), "segment_id", "left")
    frame = _first_rule(frame, [
        ("E1_INVALID_ID", F.col("raw_event_id").isNull() | (F.trim("raw_event_id") == "")
         | F.col("segment_id").isNull()),
        ("E2_INVALID_TIMESTAMP", F.col("event_ts").isNull()
         | ~F.col("updated_at").rlike(r"(?i)(Z|[+-]\d{2}:\d{2})$")),
        ("E3_INVALID_VELOCITY", F.col("velocity").isNull() | F.isnan("velocity")
         | ~F.col("velocity").between(0, 200)),
        ("E4_UNKNOWN_SEGMENT", F.col("known_segment").isNull()),
    ])
    return frame.filter("first_failing_rule IS NULL"), frame.filter("first_failing_rule IS NOT NULL")


def deduplicate_events(clean: DataFrame) -> tuple[DataFrame, DataFrame]:
    fingerprint = F.sha2(F.to_json(F.struct("segment_id", "event_ts", "velocity")), 256)
    marked = clean.withColumn("identity_payload", fingerprint)
    conflicts = marked.groupBy("raw_event_id").agg(F.countDistinct("identity_payload").alias("variants")) \
        .filter("variants > 1").select("raw_event_id")
    bad = marked.join(conflicts, "raw_event_id", "inner") \
        .withColumn("first_failing_rule", F.lit("E5_CONFLICTING_EVENT_ID"))
    good = marked.join(conflicts, "raw_event_id", "left_anti").dropDuplicates(["raw_event_id"])
    return good, bad


def aggregate_monoid(events: DataFrame, salted: bool = True, buckets: int = 8) -> DataFrame:
    if buckets < 1:
        raise ValueError("buckets must be positive")
    rows = events.withColumn("window_start", F.window("event_ts", "30 minutes").getField("start")) \
        .withColumn("severe_ping", (F.col("velocity") < 15).cast("long"))
    if not salted:
        direct = rows.groupBy(*KEYS).agg(
            F.count("velocity").alias("obs_count"), F.sum("velocity").alias("sum_velocity"),
            F.min("velocity").alias("min_velocity"), F.max("velocity").alias("max_velocity_obs"),
            F.sum("severe_ping").alias("severe_ping_count"),
        )
        return direct.withColumn("mean_velocity", F.col("sum_velocity") / F.col("obs_count"))
    rows = rows.withColumn("salt", F.pmod(F.xxhash64("raw_event_id"), F.lit(buckets)))
    partial = rows.groupBy(*KEYS, "salt").agg(
        F.count("velocity").alias("obs_count"), F.sum("velocity").alias("sum_velocity"),
        F.min("velocity").alias("min_velocity"), F.max("velocity").alias("max_velocity_obs"),
        F.sum("severe_ping").alias("severe_ping_count"),
    )
    merged = partial.groupBy(*KEYS).agg(
        F.sum("obs_count").alias("obs_count"), F.sum("sum_velocity").alias("sum_velocity"),
        F.min("min_velocity").alias("min_velocity"), F.max("max_velocity_obs").alias("max_velocity_obs"),
        F.sum("severe_ping_count").alias("severe_ping_count"),
    )
    return merged.withColumn("mean_velocity", F.col("sum_velocity") / F.col("obs_count"))


def fit_velocity_lookup(
    events: DataFrame, segments: DataFrame, train_end: str, use_observed_gis_vmax: bool = False,
) -> DataFrame:
    """Exclusive cutoff. Segment P85 and road-level medians use TRAIN GPS only.

    Lookup must be fitted on a warmup prefix before supervised rows to avoid
    intra-TRAIN leakage. By default (use_observed_gis_vmax=False), observed GIS
    vmax is retained as segment metadata and imputation uses SEGMENT_P85 ->
    LEVEL_TYPE_MEDIAN -> LEVEL_PRIOR {1: 80, 2: 60, 3: 50, 4: 40}.
    """
    train = events.filter(F.col("event_ts") < F.to_timestamp(F.lit(train_end)))
    p85 = train.groupBy("segment_id").agg(F.percentile_approx("velocity", 0.85, 10000).alias("segment_p85"))
    p85 = p85.filter("segment_p85 > 0")
    profiles = segments.join(p85, "segment_id", "left")
    medians = profiles.groupBy("street_level", "street_type").agg(
        F.percentile_approx("segment_p85", 0.5, 10000).alias("road_median"))
    lookup = profiles.join(medians, ["street_level", "street_type"], "left")
    prior = F.create_map(*[F.lit(x) for pair in [(1, 80.), (2, 60.), (3, 50.), (4, 40.)] for x in pair])
    if use_observed_gis_vmax:
        obs_col = F.when(
            F.col("observed_max_velocity").isNotNull()
            & ~F.isnan("observed_max_velocity")
            & (F.col("observed_max_velocity") > 0)
            & (F.col("observed_max_velocity") <= 200),
            F.col("observed_max_velocity"),
        )
        lookup = lookup.withColumn(
            "max_velocity",
            F.coalesce(obs_col, "segment_p85", "road_median", prior[F.col("street_level")]),
        ).withColumn(
            "vmax_impute_tier",
            F.when(obs_col.isNotNull(), "SEGMENT_OBSERVED_VMAX")
            .when(F.col("segment_p85").isNotNull(), "SEGMENT_P85")
            .when(F.col("road_median").isNotNull(), "LEVEL_TYPE_MEDIAN")
            .otherwise("LEVEL_PRIOR"),
        )
    else:
        lookup = lookup.withColumn(
            "max_velocity",
            F.coalesce("segment_p85", "road_median", prior[F.col("street_level")]),
        ).withColumn(
            "vmax_impute_tier",
            F.when(F.col("segment_p85").isNotNull(), "SEGMENT_P85")
            .when(F.col("road_median").isNotNull(), "LEVEL_TYPE_MEDIAN")
            .otherwise("LEVEL_PRIOR"),
        )
    return lookup.withColumn("fit_before_utc", F.lit(train_end)) \
        .select("segment_id", "max_velocity", "vmax_impute_tier", "fit_before_utc")


def derive_los(ratio):
    result = F.lit("F")
    for threshold, label in reversed([(0.80, "A"), (0.65, "B"), (0.50, "C"), (0.35, "D"), (0.20, "E")]):
        result = F.when(ratio >= threshold, label).otherwise(result)
    return F.when(ratio.isNull(), F.lit(None).cast("string")).otherwise(result)


def enrich_windows(
    windows: DataFrame, segments: DataFrame, lookup: DataFrame,
    broadcast: bool = True, streets: DataFrame | None = None,
) -> DataFrame:
    dim = segments.join(F.broadcast(lookup) if broadcast else lookup, "segment_id")
    result = windows.join(F.broadcast(dim) if broadcast else dim, "segment_id")
    if streets is not None:
        result = result.join(F.broadcast(streets) if broadcast else streets, "street_id")
    result = result.withColumn("velocity_ratio", F.col("mean_velocity") / F.col("max_velocity")) \
        .withColumn("LOS", derive_los(F.col("velocity_ratio"))) \
        .withColumn("date_utc", F.date_format("window_start", "yyyy-MM-dd")) \
        .withColumn("local_ts", F.from_utc_timestamp("window_start", "Asia/Ho_Chi_Minh"))
    slot = F.hour("local_ts") * 2 + (F.minute("local_ts") / 30).cast("int")
    return result.withColumn("date", F.date_format("local_ts", "yyyy-MM-dd")) \
        .withColumn("period_idx", slot).withColumn("weekday", F.weekday("local_ts")) \
        .withColumn("period_sin", F.sin(slot * (2 * math.pi / 48))) \
        .withColumn("period_cos", F.cos(slot * (2 * math.pi / 48))) \
        .withColumn("year_month", F.date_format("local_ts", "yyyy-MM")) \
        .withColumn("window_id", F.sha2(F.concat_ws("|", "segment_id", F.col("window_start").cast("string")), 256))


def prune_nodes(nodes: DataFrame, segments: DataFrame) -> DataFrame:
    ids = segments.select(F.col("s_node_id").alias("node_id")).union(
        segments.select(F.col("e_node_id").alias("node_id"))).distinct()
    return nodes.withColumn("node_id", F.col("_id").try_cast("bigint")).join(ids, "node_id", "left_semi")


def differences(left: DataFrame, right: DataFrame) -> dict[str, int]:
    columns = sorted(left.columns)
    if set(columns) != set(right.columns):
        raise ValueError("Comparison schemas have different columns")
    left_proj = left.select([F.col(c).alias(c) for c in columns])
    right_proj = right.select([F.col(c).alias(c) for c in columns])
    return {
        "left_only": left_proj.exceptAll(right_proj).count(),
        "right_only": right_proj.exceptAll(left_proj).count(),
    }


def physical_nodes(frame: DataFrame) -> list[str]:
    """Walk executed nodes, excluding historical plans printed beneath cache scans."""
    names = []

    def visit(node):
        names.append(node.nodeName())
        children = node.children().iterator()
        while children.hasNext():
            visit(children.next())

    visit(frame._jdf.queryExecution().executedPlan())
    return names

