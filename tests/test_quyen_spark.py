"""Real local-Spark tests; run with stdlib unittest, no legacy artifacts needed."""
import tempfile
import unittest

from pyspark.sql import SparkSession, functions as F

from baseline import compare_window_aggregates
from traffic_data.spark_etl import (
    aggregate_monoid, clean_events, clean_segments, deduplicate_events,
    derive_los, differences, enrich_windows, fit_velocity_lookup, physical_nodes, prune_nodes,
)


class SparkDataEngineeringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("quyen-tests") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def segments(self):
        return self.spark.createDataFrame([
            ("1", "10", "20", "101", "100", "2", "NaN", "Đường A", "primary"),
            ("2", "20", "30", "101", "50", "2", "60", "Đường A", "primary"),
            ("3", "30", "40", "102", "50", "4", None, "Đường B", "service"),
        ], "_id string,s_node_id string,e_node_id string,street_id string,length string,street_level string,"
           "max_velocity string,street_name string,street_type string")

    def events(self, rows):
        return self.spark.createDataFrame(rows, "raw_event_id string,segment_id string,velocity string,updated_at string")

    def test_first_failure_and_identity(self):
        segments, _ = clean_segments(self.segments())
        raw = self.events([
            ("a", "1", "10", "2020-07-03T17:01:00Z"),
            ("a", "1", "10", "2020-07-03T17:01:00Z"),
            ("b", "1", "20", "2020-07-03T17:02:00Z"),
            (None, "bad", "N/A", "bad"),
            ("c", "1", "N/A", "bad"),
            ("d", "1", "NaN", "2020-07-03T17:01:00Z"),
            ("e", "999", "20", "2020-07-03T17:01:00Z"),
            ("f", "1", "201", "2020-07-03T17:01:00Z"),
        ])
        clean, rejected = clean_events(raw, segments)
        unique, conflicts = deduplicate_events(clean)
        self.assertEqual(unique.count(), 2)  # Same window, two distinct valid pings.
        self.assertEqual(conflicts.count(), 0)
        self.assertEqual(sorted(r.first_failing_rule for r in rejected.collect()),
                         ["E1_INVALID_ID", "E2_INVALID_TIMESTAMP", "E3_INVALID_VELOCITY",
                          "E3_INVALID_VELOCITY", "E4_UNKNOWN_SEGMENT"])
        self.assertEqual(clean.count() + rejected.count(), raw.count())
        collision, bad = deduplicate_events(clean.unionByName(
            clean.filter("raw_event_id='a'").withColumn("velocity", F.lit(99.))))
        self.assertEqual(collision.count(), 1)
        self.assertEqual(bad.select("raw_event_id").distinct().first()[0], "a")

    def test_topology_quarantine_and_node_pruning(self):
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
        self.assertTrue(all(r.length_imputed and r.length_m == 27. for r in repaired.collect()))

    def test_monoid_reference_and_corruption(self):
        segments, _ = clean_segments(self.segments())
        rows = [(str(i), "1", str(v), "2020-07-03T17:01:00Z") for i, v in enumerate([0, 10, 20, 100, 200])]
        events, _ = clean_events(self.events(rows + [rows[0]]), segments)
        events, _ = deduplicate_events(events)
        salted, plain = aggregate_monoid(events), aggregate_monoid(events, False)
        self.assertEqual(differences(salted, plain), {"left_only": 0, "right_only": 0})
        actual = [r.asDict() for r in salted.collect()]
        raw = [dict(zip(["_id", "segment_id", "velocity", "updated_at"], r)) for r in rows]
        self.assertTrue(compare_window_aggregates(raw, actual)["equivalent"])
        for field in ["obs_count", "sum_velocity", "min_velocity", "max_velocity_obs", "severe_ping_count", "mean_velocity"]:
            broken = [dict(actual[0], **{field: actual[0][field] + 1})]
            self.assertFalse(compare_window_aggregates(raw, broken)["equivalent"], field)
        self.assertEqual(compare_window_aggregates(raw, [])["missing_keys"], 1)
        self.assertEqual(compare_window_aggregates([], actual)["extra_keys"], 1)
        self.assertEqual(compare_window_aggregates(raw, actual * 2)["duplicate_keys"], 1)

    def test_lookup_train_only_and_tiers(self):
        segments, _ = clean_segments(self.segments())
        raw = self.events([("a", "1", "20", "2020-07-01T00:00:00Z"),
                           ("b", "1", "40", "2020-07-01T00:01:00Z"),
                           ("c", "2", "199", "2020-08-01T00:00:00Z")])
        events, _ = clean_events(raw, segments)
        lookup = fit_velocity_lookup(events, segments, "2020-08-01T00:00:00Z")
        changed = events.withColumn("velocity", F.when(F.col("raw_event_id") == "c", 1.).otherwise(F.col("velocity")))
        self.assertEqual(differences(lookup, fit_velocity_lookup(changed, segments, "2020-08-01T00:00:00Z")),
                         {"left_only": 0, "right_only": 0})
        values = {r.segment_id: (r.max_velocity, r.vmax_impute_tier) for r in lookup.collect()}
        self.assertEqual(values, {1: (40., "SEGMENT_P85"), 2: (40., "LEVEL_TYPE_MEDIAN"), 3: (40., "LEVEL_PRIOR")})

    def test_los_time_and_real_plans(self):
        ratios = [0., .199999, .20, .35, .50, .65, .80, 1.2]
        labels = self.spark.createDataFrame([(r,) for r in ratios], "ratio double") \
            .select("ratio", derive_los(F.col("ratio")).alias("LOS")).orderBy("ratio")
        self.assertEqual([r.LOS for r in labels.collect()], ["F", "F", "E", "D", "C", "B", "A", "A"])
        segments, _ = clean_segments(self.segments())
        events, _ = clean_events(self.events([("a", "1", "0", "2020-07-03T17:01:00Z")]), segments)
        lookup = fit_velocity_lookup(events, segments, "2020-08-01T00:00:00Z")
        windows = aggregate_monoid(events)
        streets = self.spark.createDataFrame([(101, "Road A"), (102, "Road B")],
                                            "street_id long,master_street_name string")
        optimized = enrich_windows(windows, segments, lookup, streets=streets)
        baseline = enrich_windows(windows, segments, lookup, False, streets=streets)
        self.assertEqual(differences(optimized, baseline), {"left_only": 0, "right_only": 0})
        row = optimized.first()
        self.assertEqual((row.date, row.period_idx, row.weekday, row.LOS), ("2020-07-04", 0, 5, "F"))
        self.assertEqual(row.mean_velocity, 0.)
        self.assertAlmostEqual(row.period_sin ** 2 + row.period_cos ** 2, 1.)
        self.assertIn("BroadcastHashJoin", physical_nodes(optimized))
        self.assertGreaterEqual(physical_nodes(optimized).count("BroadcastHashJoin"), 2)
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
