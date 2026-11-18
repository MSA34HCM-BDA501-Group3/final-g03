"""Build deterministic lightweight `data/demo_sample/` from `raw_data/` & `data/`.

Allows clean Git checkouts (where `raw_data/` & full `data/` are gitignored) to run
the end-to-end Live Kafka Demo and Card B (`B1/B2/B3`) smoke checks out of the box.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from baseline import VALID_ROAD_TYPES  # noqa: E402
MUST_SEGMENT_IDS = [
    47407,
    21260,
    21262,
    21263,
    21064,
    22684,
    37420,
    36232,
    63481,
    56816,
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


def _sync_manifest_sample_sha(root: Path, sample_static_path: Path) -> None:
    manifest_path = root / "outputs" / "model_registry_manifest.json"
    if not manifest_path.is_file() or not sample_static_path.is_file():
        return
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["sample_static_feature_store_path"] = sample_static_path.name
    manifest["sample_static_feature_store_sha256"] = hashlib.sha256(sample_static_path.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def build_demo_sample(root: Path = ROOT) -> dict[str, int]:
    """Extract deterministic real subsets into `data/demo_sample/`."""
    out = root / "data" / "demo_sample"
    out.mkdir(parents=True, exist_ok=True)

    segs_all = pd.read_csv(root / "raw_data" / "segments.csv")
    segs = segs_all[segs_all["street_type"].isin(VALID_ROAD_TYPES)].copy()
    valid_seg_ids = set(segs["_id"].dropna().astype(int).tolist())
    stat = pd.read_csv(root / "raw_data" / "segment_status.csv")
    stat = stat[stat["segment_id"].isin(valid_seg_ids)].copy()
    ds = pd.read_csv(root / "data" / "dataset.csv")
    static_df = pd.read_parquet(root / "data" / "lakehouse" / "segment_static_features.parquet")

    noon_story = (
        ds[(ds["ict_date"] == "2021-01-05") & (ds["vn_hour"].between(10.5, 15.0))]
        .groupby("vn_hour", as_index=False)
        .head(14)
    )
    ctqt_story = (
        ds[(ds["ict_date"] == "2021-04-22") & (ds["vn_hour"].between(8.5, 12.0))]
        .groupby("vn_hour", as_index=False)
        .head(14)
    )
    morn_story = ds[(ds["ict_date"] == "2020-12-24") & (ds["vn_hour"] == 7.0)].head(30)
    must_win = ds[ds["segment_id"].isin(MUST_SEGMENT_IDS)]
    trans_win = ds[ds["target_t30m_los_ordinal"].notna() & (ds["los_ordinal"] != ds["target_t30m_los_ordinal"])].head(
        80
    )
    art_win = ds[ds["street_level"].isin([1, 2])].head(80)
    sample_win = (
        pd.concat([noon_story, ctqt_story, morn_story, must_win, trans_win, art_win, ds.head(60)])
        .drop_duplicates(subset=["event_id"])
        .head(480)
    )
    sample_win.to_csv(out / "sample_windows_23d.csv", index=False)

    win_sids = set(sample_win["segment_id"].astype(int).tolist()) | (set(MUST_SEGMENT_IDS) & valid_seg_ids)
    cold_parts = [grp.head(35) for _, grp in static_df[static_df["is_cold_start"]].groupby("cluster_id")]
    cold_sample = pd.concat(cold_parts, ignore_index=True)
    keep_sids = sorted(win_sids | set(cold_sample["segment_id"].astype(int).tolist()))[:550]

    sample_segs = segs[segs["_id"].isin(keep_sids)].head(550)
    sample_segs.to_csv(out / "sample_segments.csv", index=False)

    sample_static = static_df[static_df["segment_id"].isin(sample_segs["_id"])].copy()
    sample_static_path = out / "sample_static_features.parquet"
    sample_static.to_parquet(sample_static_path, index=False)
    _sync_manifest_sample_sha(root, sample_static_path)

    must_pings = stat[
        stat["segment_id"].isin([47407, 21260, 21262, 21263, 21064, 22684, 21969, 34019, 83729, 37420, 36232])
    ].head(380)
    other_pings = stat[stat["segment_id"].isin(sample_segs["_id"])].head(300)
    sample_stat = (
        pd.concat([stat[stat["_id"].isin([15281, 77975])], must_pings, other_pings])
        .drop_duplicates(subset=["_id"])
        .head(600)
    )
    sample_stat.to_csv(out / "sample_raw_status.csv", index=False)

    readme = (
        "# Lightweight Committed Demo & Audit Sample (`data/demo_sample/`)\n\n"
        "Deterministic real subset extracted from `raw_data/` and `data/` (`seed=42`) so clean Git checkouts\n"
        "(without the full 38.50 MB `raw_data/` directory) can immediately run:\n"
        "- Live End-to-End Kafka Replay Demo (`producer.py` -> `Kafka` -> `starter_stream.py --from-kafka`"
        " -> `MongoDB 8.0` -> `FastAPI :8000` & `Streamlit :8501`)\n"
        "- Session 9 Card B (`B1/B2/B3`) smoke tests when `raw_data/` is not yet downloaded.\n\n"
        "Full 90,787 accepted motorized pings / 84,206 road segments reproduction instructions "
        "are in `docs/DATA_SETUP.md`.\n"
    )
    (out / "README.md").write_text(readme, encoding="utf-8")
    return {p.name: p.stat().st_size for p in sorted(out.iterdir())}


if __name__ == "__main__":
    sizes = build_demo_sample()
    for name, sz in sizes.items():
        print(f"{name}: {sz:,} bytes")
