"""Quyen's batch/shared transformations. Compatible with Spark 3.5.7 (Java 17) & Spark 4.2.0 (Java 21).

UTC internal timestamps; no train.csv dependency; no model training or serving side effects.
"""

from __future__ import annotations

import math
import unicodedata

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

if not hasattr(Column, "try_cast"):

    def _try_cast(self: Column, dtype: str) -> Column:
        return F.expr(f"try_cast({self._jc.expr().sql()} as {dtype})")

    Column.try_cast = _try_cast  # type: ignore[attr-defined]

KEYS = ["segment_id", "window_start"]
MONOID = ["obs_count", "sum_velocity", "min_velocity", "max_velocity_obs", "severe_ping_count"]
ROAD_TYPES = [
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
]


def read_strings(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", False).option("mode", "FAILFAST").csv(path)


def _payload(frame: DataFrame, source: str) -> DataFrame:
    return (
        frame.withColumn("raw_payload", F.to_json(F.struct(*[F.col(c) for c in frame.columns])))
        .withColumn("source", F.lit(source))
        .withColumn("audited_at", F.current_timestamp())
    )


def _first_rule(frame: DataFrame, rules: list[tuple]) -> DataFrame:
    rule = F.lit(None).cast("string")
    for name, failed in reversed(rules):
        rule = F.when(failed, F.lit(name)).otherwise(rule)
    return frame.withColumn("first_failing_rule", rule)


def _build_nfc_pairs() -> tuple[tuple[str, str], ...]:
    """Precompute static Vietnamese decomposed-to-NFC substitution table for pure Spark SQL."""
    vowel_mod = [
        ("a\u0302", "â"),
        ("A\u0302", "Â"),
        ("a\u0306", "ă"),
        ("A\u0306", "Ă"),
        ("e\u0302", "ê"),
        ("E\u0302", "Ê"),
        ("o\u0302", "ô"),
        ("O\u0302", "Ô"),
        ("o\u031b", "ơ"),
        ("O\u031b", "Ơ"),
        ("u\u031b", "ư"),
        ("U\u031b", "Ư"),
    ]
    tones = ("\u0300", "\u0301", "\u0303", "\u0309", "\u0323")
    bases = "aâăeêioôơuưyAÂĂEÊIOÔƠUƯY"
    tone_pairs = []
    for base in bases:
        for tone in tones:
            composed = unicodedata.normalize("NFC", base + tone)
            if len(composed) == 1:
                tone_pairs.append((base + tone, composed))
    return tuple(vowel_mod + tone_pairs)


_NFC_PAIRS = _build_nfc_pairs()

_KEEP_DUONG_PREFIX_REGEX = (
    r"^[Đđ]ường\s+(?:"
    r"[Ss][ốô](?:$|[^\p{L}\p{N}_])"
    r"|(?:[Cc]ao\s+[Tt]ốc|[Dd]ẫn|[Hh]ầm|[Tt]ỉnh|[Vv]ành\s+[Đđ]ai|[Vv]en|[Đđ]ất|"
    r"[Ll]iên\s+(?:[Kk]hu|[Ấấ]p|[Xx]ã))(?:$|[^\p{L}\p{N}_])"
    r"|(?:[A-Z]{1,4}\d*[A-Za-zÀ-ỹ]*|\d+[A-Za-zÀ-ỹ]*)(?:\s*[-–]|$)"
    r")"
)


def normalize_name(value: Column | str) -> Column:
    """Normalize Vietnamese street names in pure Spark SQL (NFC, Eth, whitespace, prefix, admin suffix)."""
    col = F.col(value) if isinstance(value, str) else value
    text = F.translate(F.coalesce(col.cast("string"), F.lit("")), "Ðð", "Đđ")
    composed = F.regexp_replace(text, "\u0323([\u0302\u0306])", "$1\u0323")
    for pat, rep in _NFC_PAIRS:
        composed = F.regexp_replace(composed, pat, rep)
    text = F.when(text.rlike("[\u0300-\u036f]"), composed).otherwise(text)
    text = F.trim(F.regexp_replace(text, r"\s+", " "))
    text = F.when(
        text.rlike(r"^[Đđ]ường\s+") & ~text.rlike(_KEEP_DUONG_PREFIX_REGEX),
        F.regexp_replace(text, r"^[Đđ]ường\s+", ""),
    ).otherwise(text)
    text = F.regexp_replace(text, r"^Xa\s+[lL]ộ\s+", "Xa Lộ ")
    text = F.regexp_replace(
        text,
        r"(?iu)(?:\s*[-–,]+\s*)(?:phường|quận|xã|huyện|thị trấn|khu phố|ấp|tp\.?|thành phố)(?:$|[^\p{L}\p{N}_]).*$",
        "",
    )
    return F.trim(text)


def clean_segments(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Validate GIS segments with ordered rules S1 -> S2 -> S3 -> S4.

    Quarantine is an audit ledger recording the first failing rule (including S2
    non-positive length), whereas S1/S3/S4 are terminal exclusions and S2-only
    valid road segments receive median length imputation with length_imputed=True.
    """
    frame = _payload(raw, "segments.csv")
    for source, target, dtype in [
        ("_id", "segment_id", "bigint"),
        ("s_node_id", "s_node_id", "bigint"),
        ("e_node_id", "e_node_id", "bigint"),
        ("street_id", "street_id", "bigint"),
        ("length", "length_m", "double"),
        ("street_level", "street_level", "int"),
        ("max_velocity", "observed_max_velocity", "double"),
    ]:
        frame = frame.withColumn(target, F.col(source).try_cast(dtype))
    frame = frame.withColumn("street_name", normalize_name("street_name"))
    frame = _first_rule(
        frame,
        [
            (
                "S1_INVALID_KEY",
                F.col("segment_id").isNull()
                | F.col("s_node_id").isNull()
                | F.col("e_node_id").isNull()
                | F.col("street_id").isNull()
                | (F.col("s_node_id") == F.col("e_node_id")),
            ),
            (
                "S2_NONPOSITIVE_LENGTH",
                F.col("length_m").isNull()
                | F.isnan("length_m")
                | (F.col("length_m") <= 0)
                | (F.abs(F.col("length_m")) == float("inf")),
            ),
            ("S3_NON_ROAD_TYPE", F.col("street_type").isNull() | ~F.col("street_type").isin(ROAD_TYPES)),
            ("S4_INVALID_LEVEL", F.col("street_level").isNull() | ~F.col("street_level").isin(1, 2, 3, 4)),
        ],
    )
    # Preserve the original failure in quarantine even when a length is repairable.
    # Invalid IDs/types/levels remain excluded. Length repair is explicit in output.
    invalid_length = (
        F.col("length_m").isNull()
        | F.isnan("length_m")
        | (F.col("length_m") <= 0)
        | (F.abs(F.col("length_m")) == float("inf"))
    )
    median = frame.filter(~invalid_length).agg(F.percentile_approx("length_m", 0.5, 10000).alias("length_median"))
    good = (
        frame.filter(
            F.col("segment_id").isNotNull()
            & F.col("s_node_id").isNotNull()
            & F.col("e_node_id").isNotNull()
            & F.col("street_id").isNotNull()
            & (F.col("s_node_id") != F.col("e_node_id"))
            & F.col("street_type").isin(ROAD_TYPES)
            & F.col("street_level").isin(1, 2, 3, 4)
        )
        .crossJoin(F.broadcast(median))
        .withColumn("length_imputed", invalid_length)
        .withColumn(
            "length_m", F.when(invalid_length, F.coalesce("length_median", F.lit(27.0))).otherwise(F.col("length_m"))
        )
        .select(
            "segment_id",
            "s_node_id",
            "e_node_id",
            "street_id",
            "length_m",
            "street_level",
            "street_name",
            "street_type",
            "observed_max_velocity",
            "length_imputed",
        )
    )
    return good, frame.filter("first_failing_rule IS NOT NULL")


def clean_events(raw: DataFrame, segments: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Shared with foreachBatch; raw IDs must be stable across delivery retries.

    Required string columns: raw_event_id, segment_id, velocity, updated_at.
    Identity conflicts are rejected, not arbitrarily resolved by dropDuplicates.
    """
    frame = _payload(raw, "segment_status")
    frame = (
        frame.withColumn("segment_id", F.col("segment_id").try_cast("bigint"))
        .withColumn("velocity", F.col("velocity").try_cast("double"))
        .withColumn("event_ts", F.try_to_timestamp("updated_at"))
    )
    known = segments.select("segment_id").distinct().withColumn("known_segment", F.lit(True))
    frame = frame.join(F.broadcast(known), "segment_id", "left")
    frame = _first_rule(
        frame,
        [
            (
                "E1_INVALID_ID",
                F.col("raw_event_id").isNull() | (F.trim("raw_event_id") == "") | F.col("segment_id").isNull(),
            ),
            (
                "E2_INVALID_TIMESTAMP",
                F.col("event_ts").isNull() | ~F.col("updated_at").rlike(r"(?i)(Z|[+-]\d{2}:\d{2})$"),
            ),
            (
                "E3_INVALID_VELOCITY",
                F.col("velocity").isNull() | F.isnan("velocity") | ~F.col("velocity").between(0, 200),
            ),
            ("E4_UNKNOWN_SEGMENT", F.col("known_segment").isNull()),
        ],
    )
    return frame.filter("first_failing_rule IS NULL"), frame.filter("first_failing_rule IS NOT NULL")


def deduplicate_events(clean: DataFrame) -> tuple[DataFrame, DataFrame]:
    fingerprint = F.sha2(F.to_json(F.struct("segment_id", "event_ts", "velocity")), 256)
    marked = clean.withColumn("identity_payload", fingerprint)
    conflicts = (
        marked.groupBy("raw_event_id")
        .agg(F.countDistinct("identity_payload").alias("variants"))
        .filter("variants > 1")
        .select("raw_event_id")
    )
    bad = marked.join(conflicts, "raw_event_id", "inner").withColumn(
        "first_failing_rule", F.lit("E5_CONFLICTING_EVENT_ID")
    )
    good = marked.join(conflicts, "raw_event_id", "left_anti").dropDuplicates(["raw_event_id"])
    return good, bad


def aggregate_monoid(events: DataFrame, salted: bool = True, buckets: int = 8) -> DataFrame:
    if buckets < 1:
        raise ValueError("buckets must be positive")
    rows = events.withColumn("window_start", F.window("event_ts", "30 minutes").getField("start")).withColumn(
        "severe_ping", (F.col("velocity") < 15).cast("long")
    )
    if not salted:
        direct = rows.groupBy(*KEYS).agg(
            F.count("velocity").alias("obs_count"),
            F.sum("velocity").alias("sum_velocity"),
            F.min("velocity").alias("min_velocity"),
            F.max("velocity").alias("max_velocity_obs"),
            F.sum("severe_ping").alias("severe_ping_count"),
        )
        return direct.withColumn("mean_velocity", F.col("sum_velocity") / F.col("obs_count"))
    rows = rows.withColumn("salt", F.pmod(F.xxhash64("raw_event_id"), F.lit(buckets)))
    partial = rows.groupBy(*KEYS, "salt").agg(
        F.count("velocity").alias("obs_count"),
        F.sum("velocity").alias("sum_velocity"),
        F.min("velocity").alias("min_velocity"),
        F.max("velocity").alias("max_velocity_obs"),
        F.sum("severe_ping").alias("severe_ping_count"),
    )
    merged = partial.groupBy(*KEYS).agg(
        F.sum("obs_count").alias("obs_count"),
        F.sum("sum_velocity").alias("sum_velocity"),
        F.min("min_velocity").alias("min_velocity"),
        F.max("max_velocity_obs").alias("max_velocity_obs"),
        F.sum("severe_ping_count").alias("severe_ping_count"),
    )
    return merged.withColumn("mean_velocity", F.col("sum_velocity") / F.col("obs_count"))


def flag_synthetic_burst_events(
    events: DataFrame,
    min_simultaneous_segments: int = 1000,
) -> DataFrame:
    """Flag pings falling in 30m UTC slots with >= min_simultaneous_segments distinct segments."""
    if "is_synthetic_burst" in events.columns:
        return events
    with_win = events.withColumn("_burst_window_start", F.window("event_ts", "30 minutes").getField("start"))
    burst_slots = (
        with_win.groupBy("_burst_window_start")
        .agg(F.countDistinct("segment_id").alias("_slot_distinct_segments"))
        .withColumn("is_synthetic_burst", F.col("_slot_distinct_segments") >= F.lit(min_simultaneous_segments))
        .select("_burst_window_start", "is_synthetic_burst")
    )
    return with_win.join(burst_slots, "_burst_window_start", "left").drop("_burst_window_start")


def fit_velocity_lookup(
    events: DataFrame,
    segments: DataFrame,
    train_end: str,
    use_observed_gis_vmax: bool = True,
    min_p85_obs: int = 5,
    min_burst_segments: int = 1000,
) -> DataFrame:
    """Exclusive cutoff. Segment P85 (>= min_p85_obs pings) and road medians use organic TRAIN data only.

    Lookup fits strictly before train_end UTC (excluding synthetic burst slots) to avoid leakage
    and resolves 3 tiers:
    SEGMENT_OBSERVED_VMAX (when use_observed_gis_vmax=True and in (0, 200]) ->
    SEGMENT_P85 (count >= min_p85_obs and > 0) -> LEVEL_TYPE_MEDIAN (distinct segments) ->
    LEVEL_PRIOR {1: 80, 2: 60, 3: 50, 4: 40}.
    """
    train = events.filter(F.col("event_ts") < F.to_timestamp(F.lit(train_end)))
    if "is_synthetic_burst" not in train.columns:
        train = flag_synthetic_burst_events(train, min_simultaneous_segments=min_burst_segments)
    train = train.filter(~F.coalesce(F.col("is_synthetic_burst").cast("boolean"), F.lit(False)))
    p85 = (
        train.groupBy("segment_id")
        .agg(
            F.count("velocity").alias("ping_count"),
            F.percentile_approx("velocity", 0.85, 10000).alias("segment_p85"),
        )
        .filter((F.col("ping_count") >= min_p85_obs) & (F.col("segment_p85") > 0))
        .drop("ping_count")
    )
    distinct_segments = segments.dropDuplicates(["segment_id"])
    profiles = distinct_segments.join(p85, "segment_id", "left")
    if use_observed_gis_vmax:
        obs_col = F.when(
            F.col("observed_max_velocity").isNotNull()
            & ~F.isnan("observed_max_velocity")
            & (F.col("observed_max_velocity") > 0)
            & (F.col("observed_max_velocity") <= 200),
            F.col("observed_max_velocity"),
        )
    else:
        obs_col = F.lit(None).cast("double")
    profiles = profiles.withColumn("_tier1_vmax", F.coalesce(obs_col, F.col("segment_p85")))
    medians = profiles.groupBy("street_level", "street_type").agg(
        F.percentile_approx("_tier1_vmax", 0.5, 10000).alias("road_median")
    )
    lookup = profiles.join(medians, ["street_level", "street_type"], "left")
    prior = F.create_map(*[F.lit(x) for pair in [(1, 80.0), (2, 60.0), (3, 50.0), (4, 40.0)] for x in pair])
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
    return lookup.withColumn("fit_before_utc", F.lit(train_end)).select(
        "segment_id", "max_velocity", "vmax_impute_tier", "fit_before_utc"
    )


def derive_los(ratio):
    result = F.lit("F")
    for threshold, label in reversed([(0.80, "A"), (0.65, "B"), (0.50, "C"), (0.35, "D"), (0.20, "E")]):
        result = F.when(ratio >= threshold, label).otherwise(result)
    return F.when(ratio.isNull(), F.lit(None).cast("string")).otherwise(result)


def derive_urban_los(
    mean_velocity_col: Column | str,
    raw_max_velocity_col: Column | str | None = None,
) -> Column:
    """Derive HCMC urban 5 km/h speed-band LOS (A..F) matching BkTraffic & pipeline.py thresholds."""
    vel = (F.col(mean_velocity_col) if isinstance(mean_velocity_col, str) else mean_velocity_col).cast("double")
    if raw_max_velocity_col is None:
        shift = F.lit(0.0)
    else:
        raw_v = (
            F.col(raw_max_velocity_col) if isinstance(raw_max_velocity_col, str) else raw_max_velocity_col
        ).cast("double")
        safe_raw = F.when(
            raw_v.isNull() | F.isnan(raw_v) | (F.abs(raw_v) == float("inf")), F.lit(0.0)
        ).otherwise(raw_v)
        shift = (
            F.when(safe_raw >= F.lit(70.0), F.lit(10.0))
            .when(safe_raw >= F.lit(60.0), F.lit(5.0))
            .otherwise(F.lit(0.0))
        )
    safe_vel = F.when(vel.isNull() | F.isnan(vel), F.lit(-1.0)).otherwise(vel)
    result = F.lit("F")
    for threshold, label in reversed([(35.0, "A"), (30.0, "B"), (25.0, "C"), (20.0, "D"), (15.0, "E")]):
        result = F.when(safe_vel >= (F.lit(threshold) + shift), F.lit(label)).otherwise(result)
    return result


def _resolve_raw_vmax_col(frame: DataFrame) -> Column | None:
    if "raw_max_velocity" in frame.columns:
        return F.col("raw_max_velocity")
    if "observed_max_velocity" in frame.columns:
        return F.col("observed_max_velocity")
    return None


def enrich_windows(
    windows: DataFrame,
    segments: DataFrame,
    lookup: DataFrame,
    broadcast: bool = True,
    streets: DataFrame | None = None,
) -> DataFrame:
    """Enrich 30m windows with topology, velocity lookup, urban LOS, and UTC + ICT temporal contract."""
    dim = segments.join(F.broadcast(lookup) if broadcast else lookup, "segment_id")
    result = windows.join(F.broadcast(dim) if broadcast else dim, "segment_id")
    if streets is not None:
        result = result.join(F.broadcast(streets) if broadcast else streets, "street_id")
    raw_vmax_col = _resolve_raw_vmax_col(result)
    result = (
        result.withColumn("velocity_ratio", F.col("mean_velocity") / F.col("max_velocity"))
        .withColumn("ratio_los", derive_los(F.col("velocity_ratio")))
        .withColumn("LOS", derive_urban_los(F.col("mean_velocity"), raw_vmax_col))
        .withColumn("date_utc", F.date_format("window_start", "yyyy-MM-dd"))
        .withColumn("local_ts", F.from_utc_timestamp("window_start", "Asia/Ho_Chi_Minh"))
    )
    utc_slot = F.hour("window_start") * 2 + (F.minute("window_start") / 30).cast("int")
    ict_slot = F.hour("local_ts") * 2 + (F.minute("local_ts") / 30).cast("int")
    utc_period = F.concat(
        F.lit("period_"),
        F.hour("window_start").cast("string"),
        F.lit("_"),
        F.when(F.minute("window_start") < 30, F.lit("00")).otherwise(F.lit("30")),
    )
    ict_period = F.concat(
        F.lit("period_"),
        F.hour("local_ts").cast("string"),
        F.lit("_"),
        F.when(F.minute("local_ts") < 30, F.lit("00")).otherwise(F.lit("30")),
    )
    return (
        result.withColumn("date", F.col("date_utc"))
        .withColumn("year_month", F.date_format("window_start", "yyyy-MM"))
        .withColumn("period", utc_period)
        .withColumn("period_idx", utc_slot)
        .withColumn("ict_date", F.date_format("local_ts", "yyyy-MM-dd"))
        .withColumn("ict_year_month", F.date_format("local_ts", "yyyy-MM"))
        .withColumn("ict_period", ict_period)
        .withColumn("ict_period_idx", ict_slot)
        .withColumn("ict_weekday", F.weekday("local_ts"))
        .withColumn("weekday", F.weekday("local_ts"))
        .withColumn("period_sin", F.sin(ict_slot * (2 * math.pi / 48)))
        .withColumn("period_cos", F.cos(ict_slot * (2 * math.pi / 48)))
        .withColumn("window_id", F.sha2(F.concat_ws("|", "segment_id", F.col("window_start").cast("string")), 256))
    )


def prune_nodes(nodes: DataFrame, segments: DataFrame) -> DataFrame:
    ids = (
        segments.select(F.col("s_node_id").alias("node_id"))
        .union(segments.select(F.col("e_node_id").alias("node_id")))
        .distinct()
    )
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
