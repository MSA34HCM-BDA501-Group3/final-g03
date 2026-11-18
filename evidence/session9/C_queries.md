# Two analytical queries

The exact query text and sample rows are in `benchmark/results.json`. Full logical and
physical plans are in `benchmark/query_street_summary_plan.txt` and
`benchmark/query_projected_scan_plan.txt`.

The street summary joins actual raw GPS pings to segment metadata, groups by street,
and orders the top ten ping counts. Its plan exposes join exchanges and aggregation
shuffle. This is exploratory traffic sampling; ping counts do not establish total road
traffic volume.

The second query reads the preserved curated Gold Parquet, selects a real `year_month`
partition and projects `segment_id, mean_velocity`. Its file scan reports PartitionFilters
and ReadSchema. The partition selected is recorded in the result, rather than assuming
the historical August example. This plan proves the chosen filter/projection, not a
fresh rebuild of the preserved Gold data or runtime stage-count improvement.
