"""Real local-Spark tests; run with stdlib unittest or pytest."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

from baseline import compare_window_aggregates
from spark_batch import write_task_metrics

try:
    from pyspark.sql import SparkSession, functions as F
    from traffic_data.spark_etl import (
        aggregate_monoid,
        clean_events,
        clean_segments,
        deduplicate_events,
        derive_los,
        derive_urban_los,
        differences,
        enrich_windows,
        fit_velocity_lookup,
        physical_nodes,
        prune_nodes,
    )

    HAS_PYSPARK = True
except ModuleNotFoundError:
    HAS_PYSPARK = False

SPARK_CONTAINER_CANDIDATES = ("groupexe-knowledgetest-spark-1",)
CONTAINER_WORKDIR = "/tmp/quyen_test"
CONTAINER_PYTHONPATH = "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/tmp/quyen_test"


def _running_docker_containers() -> set[str]:
    try:
        proc = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return set()
    return set(proc.stdout.splitlines()) if proc.returncode == 0 else set()


def _has_spark_runtime() -> bool:
    if HAS_PYSPARK:
        return True
    if os.environ.get("BDA501_SPARK_CONTAINER", "").strip():
        return True
    running = _running_docker_containers()
    return any(candidate in running for candidate in SPARK_CONTAINER_CANDIDATES)


def _resolve_spark_container() -> str:
    """Locate a running Spark 3.5.7 container for host-delegated Spark tests."""
    env_override = os.environ.get("BDA501_SPARK_CONTAINER", "").strip()
    if env_override:
        return env_override
    running = _running_docker_containers()
    for candidate in SPARK_CONTAINER_CANDIDATES:
        if candidate in running:
            return candidate
    raise unittest.SkipTest(f"No running Spark container found among {SPARK_CONTAINER_CANDIDATES}")


def _run_tests_in_spark_container() -> tuple[set[str], str]:
    """Sync source files and execute test_quyen_spark.py inside the live Spark 3.5.7 container."""
    spark_container = _resolve_spark_container()
    repo_root = Path(__file__).resolve().parent.parent
    subprocess.run(
        [
            "docker",
            "exec",
            spark_container,
            "mkdir",
            "-p",
            f"{CONTAINER_WORKDIR}/traffic_data",
            f"{CONTAINER_WORKDIR}/tests",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    files_to_copy = [
        ("traffic_data/__init__.py", f"{CONTAINER_WORKDIR}/traffic_data/__init__.py"),
        ("traffic_data/spark_etl.py", f"{CONTAINER_WORKDIR}/traffic_data/spark_etl.py"),
        ("baseline.py", f"{CONTAINER_WORKDIR}/baseline.py"),
        ("spark_batch.py", f"{CONTAINER_WORKDIR}/spark_batch.py"),
        ("tests/test_quyen_spark.py", f"{CONTAINER_WORKDIR}/tests/test_quyen_spark.py"),
    ]
    for rel_src, dst in files_to_copy:
        subprocess.run(
            ["docker", "cp", str(repo_root / rel_src), f"{spark_container}:{dst}"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    proc = subprocess.run(
        [
            "docker",
            "exec",
            "-w",
            CONTAINER_WORKDIR,
            "-e",
            f"PYTHONPATH={CONTAINER_PYTHONPATH}",
            spark_container,
            "python3",
            "-W",
            "ignore",
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-p",
            "test_quyen_spark.py",
            "-v",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    combined = f"{proc.stdout}\n{proc.stderr}"
    if proc.returncode != 0:
        raise RuntimeError(f"Spark container unittest failed (rc={proc.returncode}):\n{combined}")
    pattern = r"(test_[a-z0-9_]+)\s+\(test_quyen_spark\.SparkDataEngineeringTests(?:\.\1)?\)\s+\.\.\.\s+ok"
    passed = set(re.findall(pattern, combined))
    return passed, combined


@unittest.skipUnless(
    _has_spark_runtime(),
    "PySpark is not installed locally and no running Spark container found",
)
class SparkDataEngineeringTests(unittest.TestCase):
    spark = None
    _container_passed: set[str] = set()
    _container_log: str = ""

    @classmethod
    def setUpClass(cls):
        if HAS_PYSPARK:
            cls.spark = (
                SparkSession.builder.master("local[2]")
                .appName("quyen-tests")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.ansi.enabled", "true")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate()
            )
            cls.spark.sparkContext.setLogLevel("ERROR")
        else:
            cls._container_passed, cls._container_log = _run_tests_in_spark_container()

    @classmethod
    def tearDownClass(cls):
        if cls.spark is not None:
            cls.spark.stop()

    def segments(self):
        return self.spark.createDataFrame(
            [
                ("1", "10", "20", "101", "100", "2", "NaN", "Đường A", "primary"),
                ("2", "20", "30", "101", "50", "2", "60", "Đường A", "primary"),
                ("3", "30", "40", "102", "50", "4", None, "Đường B", "service"),
            ],
            "_id string,s_node_id string,e_node_id string,street_id string,length string,street_level string,"
            "max_velocity string,street_name string,street_type string",
        )

    def events(self, rows):
        schema = "raw_event_id string,segment_id string,velocity string,updated_at string"
        return self.spark.createDataFrame(rows, schema)

    def test_first_failure_and_identity(self):
        if not HAS_PYSPARK:
            self.assertIn("test_first_failure_and_identity", self._container_passed, self._container_log)
            return
        segments, _ = clean_segments(self.segments())
        raw = self.events(
            [
                ("a", "1", "10", "2020-07-03T17:01:00Z"),
                ("a", "1", "10", "2020-07-03T17:01:00Z"),
                ("b", "1", "20", "2020-07-03T17:02:00Z"),
                (None, "bad", "N/A", "bad"),
                ("c", "1", "N/A", "bad"),
                ("d", "1", "NaN", "2020-07-03T17:01:00Z"),
                ("e", "999", "20", "2020-07-03T17:01:00Z"),
                ("f", "1", "201", "2020-07-03T17:01:00Z"),
            ]
        )
        clean, rejected = clean_events(raw, segments)
        unique, conflicts = deduplicate_events(clean)
        self.assertEqual(unique.count(), 2)  # Same window, two distinct valid pings.
        self.assertEqual(conflicts.count(), 0)
        self.assertEqual(
            sorted(r.first_failing_rule for r in rejected.collect()),
            [
                "E1_INVALID_ID",
                "E2_INVALID_TIMESTAMP",
                "E3_INVALID_VELOCITY",
                "E3_INVALID_VELOCITY",
                "E4_UNKNOWN_SEGMENT",
            ],
        )
        self.assertEqual(clean.count() + rejected.count(), raw.count())
        collision, bad = deduplicate_events(
            clean.unionByName(clean.filter("raw_event_id='a'").withColumn("velocity", F.lit(99.0)))
        )
        self.assertEqual(collision.count(), 1)
        self.assertEqual(bad.select("raw_event_id").distinct().first()[0], "a")

    def test_topology_quarantine_and_node_pruning(self):
        if not HAS_PYSPARK:
            self.assertIn("test_topology_quarantine_and_node_pruning", self._container_passed, self._container_log)
            return
        bad = self.segments().withColumn("length", F.lit("0")).withColumn("street_type", F.lit("bad"))
        good, rejected = clean_segments(bad)
        self.assertEqual(good.count(), 0)
        self.assertEqual({r.first_failing_rule for r in rejected.collect()}, {"S2_NONPOSITIVE_LENGTH"})
        segments, _ = clean_segments(self.segments())
        nodes = self.spark.createDataFrame([("10",), ("20",), ("30",), ("40",), ("999",)], "_id string")
        self.assertEqual(prune_nodes(nodes, segments).count(), 4)
        repaired, ledger = clean_segments(self.segments().withColumn("length", F.lit("0")))
        self.assertEqual(repaired.count(), 3)
        self.assertEqual(ledger.count(), 3)
        self.assertTrue(all(r.length_imputed and r.length_m == 27.0 for r in repaired.collect()))

    def _assert_corruption_checks(self, raw: list[dict], actual: list[dict]) -> None:
        self.assertTrue(compare_window_aggregates(raw, actual)["equivalent"])
        fields = ("obs_count", "sum_velocity", "min_velocity", "max_velocity_obs", "severe_ping_count", "mean_velocity")
        for field in fields:
            broken = [dict(actual[0], **{field: actual[0][field] + 1})]
            self.assertFalse(compare_window_aggregates(raw, broken)["equivalent"], field)
        self.assertEqual(compare_window_aggregates(raw, [])["missing_keys"], 1)
        self.assertEqual(compare_window_aggregates([], actual)["extra_keys"], 1)
        self.assertEqual(compare_window_aggregates(raw, actual * 2)["duplicate_keys"], 1)
        for bad in (
            raw + [dict(raw[0], velocity="99")],
            [dict(raw[0], velocity="250")],
            [dict(raw[0], updated_at="2020-07-03T17:01:00")],
        ):
            with self.assertRaises(ValueError):
                compare_window_aggregates(bad, actual)

    def _assert_sample_task_metrics_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir) / "spark_events"
            log_dir.mkdir()
            (log_dir / "events_1_app").write_text(
                json.dumps(
                    {
                        "Event": "SparkListenerJobStart",
                        "Stage IDs": [1, 2],
                        "Properties": {"spark.jobGroup.id": "monoid_salted"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (log_dir / "events_2_app").write_text(
                json.dumps(
                    {
                        "Event": "SparkListenerTaskEnd",
                        "Stage ID": 1,
                        "Task Info": {"Index": 0, "Launch Time": 100, "Finish Time": 140},
                        "Task Metrics": {
                            "Executor Run Time": 35,
                            "Shuffle Read Metrics": {
                                "Remote Bytes Read": 0,
                                "Local Bytes Read": 0,
                                "Total Records Read": 0,
                            },
                            "Shuffle Write Metrics": {"Shuffle Bytes Written": 512},
                        },
                        "Task End Reason": {"Reason": "Success"},
                    }
                )
                + "\n\n",
                encoding="utf-8",
            )
            (log_dir / "events_10_app").write_text(
                json.dumps(
                    {
                        "Event": "SparkListenerTaskEnd",
                        "Stage ID": 2,
                        "Task Info": {"Index": 1, "Launch Time": 200, "Finish Time": 260},
                        "Task Metrics": {
                            "Executor Run Time": 50,
                            "Shuffle Read Metrics": {
                                "Remote Bytes Read": 256,
                                "Local Bytes Read": 256,
                                "Total Records Read": 8,
                            },
                            "Shuffle Write Metrics": {"Shuffle Bytes Written": 0},
                        },
                        "Task End Reason": {"Reason": "Success"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            metrics_out = Path(temp_dir) / "task_metrics.json"
            write_task_metrics(log_dir, metrics_out)
            parsed_metrics = json.loads(metrics_out.read_text(encoding="utf-8"))
            self.assertEqual([t["stage"] for t in parsed_metrics["tasks"]], [1, 2])
            self.assertEqual([s["stage"] for s in parsed_metrics["stages"]], [1, 2])

    def test_monoid_reference_and_corruption(self):
        rows = [(str(i), "1", str(v), "2020-07-03T17:01:00Z") for i, v in enumerate([0, 10, 20, 100, 200])]
        raw = [dict(zip(["_id", "segment_id", "velocity", "updated_at"], r)) for r in rows]
        if not HAS_PYSPARK:
            self.assertIn("test_monoid_reference_and_corruption", self._container_passed, self._container_log)
            actual = [
                {
                    "segment_id": 1,
                    "window_start": "2020-07-03T17:00:00Z",
                    "obs_count": 5,
                    "sum_velocity": 330.0,
                    "min_velocity": 0.0,
                    "max_velocity_obs": 200.0,
                    "severe_ping_count": 2,
                    "mean_velocity": 66.0,
                }
            ]
        else:
            segments, _ = clean_segments(self.segments())
            events, _ = clean_events(self.events(rows + [rows[0]]), segments)
            events, _ = deduplicate_events(events)
            salted, plain = aggregate_monoid(events), aggregate_monoid(events, False)
            self.assertEqual(differences(salted, plain), {"left_only": 0, "right_only": 0})
            with self.assertRaises(ValueError):
                aggregate_monoid(events, buckets=0)
            actual = [r.asDict() for r in salted.collect()]
        self._assert_corruption_checks(raw, actual)
        self._assert_sample_task_metrics_roundtrip()

    def test_lookup_train_only_and_tiers(self):
        if not HAS_PYSPARK:
            self.assertIn("test_lookup_train_only_and_tiers", self._container_passed, self._container_log)
            return
        segments, _ = clean_segments(self.segments())
        train_cutoff = "2020-08-01T00:00:00Z"
        few_rows = [
            ("a1", "1", "20", "2020-07-01T00:00:00Z"),
            ("a2", "1", "30", "2020-07-01T00:01:00Z"),
            ("a3", "1", "40", "2020-07-01T00:02:00Z"),
            ("a4", "1", "50", "2020-07-01T00:03:00Z"),
            ("c", "2", "199", "2020-08-01T00:00:00Z"),
        ]
        events_few, _ = clean_events(self.events(few_rows), segments)
        lookup_few = fit_velocity_lookup(events_few, segments, train_cutoff)
        changed = events_few.withColumn(
            "velocity", F.when(F.col("raw_event_id") == "c", 1.0).otherwise(F.col("velocity"))
        )
        self.assertEqual(
            differences(lookup_few, fit_velocity_lookup(changed, segments, train_cutoff)),
            {"left_only": 0, "right_only": 0},
        )
        values_few = {r.segment_id: (r.max_velocity, r.vmax_impute_tier) for r in lookup_few.collect()}
        self.assertEqual(
            values_few,
            {1: (60.0, "LEVEL_TYPE_MEDIAN"), 2: (60.0, "SEGMENT_OBSERVED_VMAX"), 3: (40.0, "LEVEL_PRIOR")},
        )

        events_five, _ = clean_events(
            self.events(few_rows + [("a5", "1", "50", "2020-07-01T00:04:00Z")]), segments
        )
        lookup_five = fit_velocity_lookup(events_five, segments, train_cutoff)
        values_five = {r.segment_id: (r.max_velocity, r.vmax_impute_tier) for r in lookup_five.collect()}
        self.assertEqual(
            values_five,
            {1: (50.0, "SEGMENT_P85"), 2: (60.0, "SEGMENT_OBSERVED_VMAX"), 3: (40.0, "LEVEL_PRIOR")},
        )

        no_obs_lookup = fit_velocity_lookup(events_five, segments, train_cutoff, use_observed_gis_vmax=False)
        no_obs_values = {r.segment_id: (r.max_velocity, r.vmax_impute_tier) for r in no_obs_lookup.collect()}
        self.assertEqual(
            no_obs_values,
            {1: (50.0, "SEGMENT_P85"), 2: (50.0, "LEVEL_TYPE_MEDIAN"), 3: (40.0, "LEVEL_PRIOR")},
        )

        bad_obs_segs = segments.withColumn(
            "observed_max_velocity",
            F.when(F.col("segment_id") == 2, F.lit(float("inf"))).otherwise(F.col("observed_max_velocity")),
        )
        bad_obs_lookup = fit_velocity_lookup(events_five, bad_obs_segs, train_cutoff)
        bad_obs_values = {r.segment_id: (r.max_velocity, r.vmax_impute_tier) for r in bad_obs_lookup.collect()}
        self.assertEqual(bad_obs_values[2], (50.0, "LEVEL_TYPE_MEDIAN"))

        dup_raw_segs = self.spark.createDataFrame(
            [
                ("10", "10", "20", "101", "100", "2", "40", "Đường C", "primary"),
                ("10", "10", "20", "101", "100", "2", "40", "Đường C", "primary"),
                ("10", "10", "20", "101", "100", "2", "40", "Đường C", "primary"),
                ("11", "20", "30", "101", "100", "2", "60", "Đường C", "primary"),
                ("12", "30", "40", "101", "100", "2", "80", "Đường C", "primary"),
                ("13", "40", "50", "101", "100", "2", "NaN", "Đường C", "primary"),
            ],
            "_id string,s_node_id string,e_node_id string,street_id string,length string,street_level string,"
            "max_velocity string,street_name string,street_type string",
        )
        dup_segs, _ = clean_segments(dup_raw_segs)
        empty_events, _ = clean_events(self.events([]), dup_segs)
        distinct_lookup = fit_velocity_lookup(empty_events, dup_segs, train_cutoff)
        distinct_values = {r.segment_id: (r.max_velocity, r.vmax_impute_tier) for r in distinct_lookup.collect()}
        self.assertEqual(distinct_values[13], (60.0, "LEVEL_TYPE_MEDIAN"))

    def _assert_urban_los_bands(self) -> None:
        self.assertIsNot(derive_urban_los, derive_los)
        standard_rows = [
            (0, 35.0),
            (1, 34.99),
            (2, 30.0),
            (3, 25.0),
            (4, 20.0),
            (5, 15.0),
            (6, 14.99),
            (7, 0.0),
            (8, float("nan")),
            (9, None),
        ]
        std_labels = (
            self.spark.createDataFrame(standard_rows, "idx int, vel double")
            .select("idx", derive_urban_los(F.col("vel")).alias("LOS"))
            .orderBy("idx")
        )
        self.assertEqual(
            [r.LOS for r in std_labels.collect()],
            ["A", "B", "B", "C", "D", "E", "F", "F", "F", "F"],
        )

        shifted_rows = [
            (0, 18.0, 40.0),
            (1, 39.0, 60.0),
            (2, 40.0, 60.0),
            (3, 19.9, 60.0),
            (4, 44.0, 70.0),
            (5, 45.0, 70.0),
            (6, 24.9, 80.0),
            (7, 25.0, 80.0),
            (8, 35.0, None),
            (9, 35.0, float("nan")),
            (10, 35.0, float("inf")),
            (11, float("nan"), 60.0),
        ]
        shifted_labels = (
            self.spark.createDataFrame(shifted_rows, "idx int, vel double, raw_vmax double")
            .select("idx", derive_urban_los("vel", "raw_vmax").alias("LOS"))
            .orderBy("idx")
        )
        self.assertEqual(
            [r.LOS for r in shifted_labels.collect()],
            ["E", "B", "A", "F", "B", "A", "F", "E", "A", "A", "A", "F"],
        )

    def test_los_time_and_real_plans(self):
        if not HAS_PYSPARK:
            self.assertIn("test_los_time_and_real_plans", self._container_passed, self._container_log)
            return
        self._assert_urban_los_bands()
        ratios = [0.0, 0.199999, 0.20, 0.35, 0.50, 0.65, 0.80, 1.2]
        labels = (
            self.spark.createDataFrame([(r,) for r in ratios], "ratio double")
            .select("ratio", derive_los(F.col("ratio")).alias("LOS"))
            .orderBy("ratio")
        )
        self.assertEqual([r.LOS for r in labels.collect()], ["F", "F", "E", "D", "C", "B", "A", "A"])
        segments, _ = clean_segments(self.segments())
        segments = segments.cache()
        events, _ = clean_events(self.events([("a", "1", "0", "2020-07-03T17:01:00Z")]), segments)
        lookup = fit_velocity_lookup(events, segments, "2020-08-01T00:00:00Z").cache()
        windows = aggregate_monoid(events).cache()
        streets = self.spark.createDataFrame(
            [(101, "Road A"), (102, "Road B")], "street_id long,master_street_name string"
        )
        optimized = enrich_windows(windows, segments, lookup, streets=streets)
        baseline = enrich_windows(windows, segments, lookup, False, streets=streets)
        self.assertEqual(differences(optimized, baseline), {"left_only": 0, "right_only": 0})
        row = optimized.first()
        self.assertEqual(row.date, row.date_utc)
        self.assertEqual(
            (row.date, row.date_utc, row.year_month, row.ict_date, row.ict_period_idx, row.ict_weekday, row.LOS),
            ("2020-07-03", "2020-07-03", "2020-07", "2020-07-04", 0, 5, "F"),
        )
        self.assertEqual((row.period_idx, row.weekday), (34, 5))
        self.assertEqual(row.mean_velocity, 0.0)
        self.assertAlmostEqual(row.period_sin**2 + row.period_cos**2, 1.0)
        self.assertIn("BroadcastHashJoin", physical_nodes(optimized))
        self.assertGreaterEqual(physical_nodes(optimized).count("BroadcastHashJoin"), 3)
        self.assertNotIn("SortMergeJoin", physical_nodes(optimized))
        self.assertIn("SortMergeJoin", physical_nodes(baseline))
        with tempfile.TemporaryDirectory() as temp:
            optimized.write.partitionBy("year_month").parquet(temp + "/gold")
            pruned = self.spark.read.parquet(temp + "/gold").filter("year_month='2020-07'").select("segment_id")
            self.assertEqual(pruned.count(), 1)
            plan = pruned._jdf.queryExecution().executedPlan().toString()
            self.assertIn("PartitionFilters", plan)
            self.assertIn("ReadSchema: struct<segment_id:bigint>", plan)


if __name__ == "__main__":
    unittest.main()
