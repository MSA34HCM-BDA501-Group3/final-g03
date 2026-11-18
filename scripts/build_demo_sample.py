"""Build deterministic lightweight `data/demo_sample/` from `raw_data/` & `data/`.

Allows clean Git checkouts (where `raw_data/` & full `data/` are gitignored) to run
the end-to-end Live Kafka Demo and Card B (`B1/B2/B3`) smoke checks out of the box.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
MUST_SEGMENT_IDS = [
    47407,
    47487,
    21064,
    61781,
    21969,
    69738,
    51314,
    34019,
    83729,
    36770,
    3073,
    3078,
    50017,
    62682,
    34723,
]


def build_demo_sample(root: Path = ROOT) -> dict[str, int]:
    """Extract deterministic real subsets into `data/demo_sample/`."""
    out = root / "data" / "demo_sample"
    out.mkdir(parents=True, exist_ok=True)

    segs = pd.read_csv(root / "raw_data" / "segments.csv")
    stat = pd.read_csv(root / "raw_data" / "segment_status.csv")
    ds = pd.read_csv(root / "data" / "dataset.csv")
    static_df = pd.read_parquet(root / "data" / "lakehouse" / "segment_static_features.parquet")

    must_win = ds[ds["segment_id"].isin(MUST_SEGMENT_IDS)]
    trans_win = ds[ds["target_t30m_los_ordinal"].notna() & (ds["los_ordinal"] != ds["target_t30m_los_ordinal"])].head(
        80
    )
    art_win = ds[ds["street_level"].isin([1, 2])].head(150)
    sample_win = pd.concat([must_win, trans_win, art_win, ds.head(150)]).drop_duplicates(subset=["event_id"]).head(300)
    sample_win.to_csv(out / "sample_windows_23d.csv", index=False)

    win_sids = set(sample_win["segment_id"].astype(int).tolist()) | set(MUST_SEGMENT_IDS)
    cold_parts = [grp.head(40) for _, grp in static_df[static_df["is_cold_start"]].groupby("cluster_id")]
    cold_sample = pd.concat(cold_parts, ignore_index=True)
    keep_sids = sorted(win_sids | set(cold_sample["segment_id"].astype(int).tolist()))[:500]

    sample_segs = segs[segs["_id"].isin(keep_sids)].head(500)
    sample_segs.to_csv(out / "sample_segments.csv", index=False)

    sample_static = static_df[static_df["segment_id"].isin(sample_segs["_id"])].copy()
    sample_static.to_parquet(out / "sample_static_features.parquet", index=False)

    must_pings = stat[stat["segment_id"].isin([47407, 21969, 21064, 34019, 83729, 69738, 51314])].head(250)
    other_pings = stat[stat["segment_id"].isin(sample_segs["_id"])].head(400)
    sample_stat = (
        pd.concat([stat[stat["_id"] == 15281], must_pings, other_pings]).drop_duplicates(subset=["_id"]).head(500)
    )
    sample_stat.to_csv(out / "sample_raw_status.csv", index=False)

    readme = (
        "# Lightweight Committed Demo & Audit Sample (`data/demo_sample/`)\n\n"
        "Deterministic real subset extracted from `raw_data/` and `data/` (`seed=42`) so clean Git checkouts\n"
        "(without the full 38.50 MB `raw_data/` directory) can immediately run:\n"
        "- Live End-to-End Kafka Replay Demo (`producer.py` -> `Kafka` -> `starter_stream.py --from-kafka`"
        " -> `MongoDB 8.0` -> `Streamlit :8501`)\n"
        "- Session 9 Card B (`B1/B2/B3`) smoke tests when `raw_data/` is not yet downloaded.\n\n"
        "Full 90,938-ping / 84,633-segment reproduction instructions are in `docs/DATA_SETUP.md`.\n"
    )
    (out / "README.md").write_text(readme, encoding="utf-8")
    return {p.name: p.stat().st_size for p in sorted(out.iterdir())}


if __name__ == "__main__":
    sizes = build_demo_sample()
    for name, sz in sizes.items():
        print(f"{name}: {sz:,} bytes")
