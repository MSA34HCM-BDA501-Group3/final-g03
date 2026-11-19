"""Indexed In-Memory + Live MongoDB 8.0 Data Store for the HCMC TOC Command Center."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from pipeline import find_peer_segments, score_feature_snapshot
from toc_backend.story_scenarios import CURATED_SCENARIOS, build_window_semantics, format_clock_hhmm
from toc_backend.stream_controller import get_shared_mongo_client

ROOT = Path(__file__).resolve().parent.parent
LOS_ORDER = ["A", "B", "C", "D", "E", "F"]
ORD_TO_LOS = {idx: lbl for idx, lbl in enumerate(LOS_ORDER)}
LOS_TO_ORD = {lbl: idx for idx, lbl in enumerate(LOS_ORDER)}
WORSENING_ALERT_TYPES = frozenset(
    {"ONSET_A_D_TO_SEVERE", "SEVERE_WORSENING_E_TO_F", "WORSENING_TRANSITION"}
)
EARLY_WARNING_ALERT_TYPES = WORSENING_ALERT_TYPES | {"RECOVERING_TRANSITION"}


def _resolve_effective_forecast_los(
    rf_pred_los: str,
    rf_cal_los: str,
    change_prob: float,
    severe_prob: float,
    change_threshold: float,
) -> str:
    """Resolve display forecast LOS without letting non-severe calibration mask >=50% E/F risk."""
    raw_o = LOS_TO_ORD.get(rf_pred_los, 0)
    cal_o = LOS_TO_ORD.get(rf_cal_los, 0)
    if raw_o >= 4 and severe_prob >= 0.50 and cal_o < 4:
        return rf_pred_los
    return rf_cal_los if change_prob >= change_threshold else rf_pred_los


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def _read_csv_records(path: Path, limit: int = 50) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    df = pd.read_csv(path).head(limit).replace({np.nan: None})
    return df.to_dict(orient="records")


def _clean_street_name(val: Any, segment_id: int) -> str:
    """Return a clean street display name, falling back to `Segment #<id>` on NaN or blank."""
    if pd.notna(val):
        text = str(val).strip()
        if text and text.lower() != "nan":
            return text
    return f"Segment #{int(segment_id)}"


def _read_card_b1_pit_matrix(path: Path) -> list[dict[str, Any]]:
    """Read strictly the unique 23D feature rows from the first forecast origin in Card B1."""
    if not path.is_file():
        return []
    df = pd.read_csv(path)
    if df.empty:
        return []
    if "forecast_origin_ict" in df.columns:
        origins = df["forecast_origin_ict"].dropna()
        if not origins.empty:
            df = df[df["forecast_origin_ict"] == origins.iloc[0]]
    if "feature_idx" in df.columns:
        df = df.drop_duplicates(subset=["feature_idx"]).sort_values("feature_idx")
    return df.replace({np.nan: None}).to_dict(orient="records")


def _opt_round(val: Any, digits: int = 3) -> float | None:
    return round(float(val), digits) if val is not None else None


def _extract_mongo_latency_summary(mv: dict[str, Any]) -> dict[str, Any]:
    """Extract IXSCAN vs COLLSCAN latency benchmark stats with consistent field names."""
    lat = mv.get("uc2_arterial_query", {}).get("latency_benchmark_ms", {})
    speedup = lat.get("ixscan_vs_collscan_speedup_x", lat.get("speedup_vs_collscan_x"))
    speedup_r = _opt_round(speedup, 2)
    return {
        "cold_query_ms": _opt_round(lat.get("cold_query_ms"), 3),
        "warm_min_ms": _opt_round(lat.get("warm_min_ms"), 3),
        "warm_median_ms": _opt_round(lat.get("warm_median_ms"), 3),
        "warm_p95_ms": _opt_round(lat.get("warm_p95_ms"), 3),
        "warm_max_ms": _opt_round(lat.get("warm_max_ms"), 3),
        "collscan_warm_median_ms": _opt_round(lat.get("collscan_warm_median_ms"), 3),
        "collscan_warm_p95_ms": _opt_round(lat.get("collscan_warm_p95_ms"), 3),
        "ixscan_vs_collscan_speedup_x": speedup_r,
        "speedup_vs_collscan_x": speedup_r,
        "sla_under_10ms_verified": bool(lat.get("sla_under_10ms_verified", False)),
    }


def _build_mongo_verify_payload(mv: dict[str, Any]) -> dict[str, Any]:
    """Merge IXSCAN explain verification with UC-2 IXSCAN vs COLLSCAN latency benchmark."""
    ix = dict(mv.get("ixscan_explain_verification", {}))
    lat_summary = _extract_mongo_latency_summary(mv)
    uc2 = mv.get("uc2_arterial_query", {})
    return {
        **ix,
        "mongo_server_version": mv.get("mongo_server_version"),
        "total_rows_upserted": mv.get("total_rows_upserted"),
        "stale_v1_rejected_by_mongo": mv.get("stale_v1_rejected_by_mongo"),
        "idempotent_replay_verified": mv.get("idempotent_replay_verified"),
        "latency_benchmark_ms": lat_summary,
        "uc2_arterial_query": {
            "total_matching_windows": uc2.get("total_matching_windows"),
            "latency_benchmark_ms": lat_summary,
        },
    }


def _static_lifecycle_status(split_set: Any) -> str:
    split_str = str(split_set or "").upper()
    if split_str == "TRAIN":
        return "TRAIN_IN_SAMPLE"
    if split_str == "VAL":
        return "VAL_CALIBRATION"
    if split_str == "TEST":
        return "TEST_HOLDOUT"
    if split_str == "PURGED":
        return "PURGED_BOUNDARY"
    return f"{split_str}_STATIC" if split_str else "STATIC_ARCHIVE"


def _derive_ict_date_from_utc(date_str: Any, period_str: Any) -> str:
    d_val = str(date_str or "")
    p_val = str(period_str or "")
    if not d_val or not p_val.startswith("period_"):
        return d_val
    try:
        parts = p_val.replace("period_", "", 1).split("_")
        utc_dt = datetime.strptime(d_val, "%Y-%m-%d") + timedelta(hours=int(parts[0]), minutes=int(parts[1]))
        return (utc_dt + timedelta(hours=7)).strftime("%Y-%m-%d")
    except (ValueError, IndexError):
        return d_val


def _enrich_mongo_doc_ict(doc: dict[str, Any]) -> dict[str, Any]:
    out = dict(doc)
    if not out.get("ict_date"):
        out["ict_date"] = _derive_ict_date_from_utc(out.get("date"), out.get("period"))
    if out.get("segment_id") is not None:
        out["street_name"] = _clean_street_name(out.get("street_name"), int(out["segment_id"]))
    return out


def _populate_window_coords(win_df: pd.DataFrame, coord_map: dict[int, list[float]]) -> None:
    """Populate segment coordinate lookup from pre-joined window start/end coordinates."""
    if not {"lat_snode", "long_snode", "lat_enode", "long_enode"}.issubset(win_df.columns):
        return
    for r in win_df.drop_duplicates(subset=["segment_id"]).itertuples():
        if pd.notna(r.lat_snode) and pd.notna(r.lat_enode):
            coord_map[int(r.segment_id)] = [
                round(float(r.lat_snode), 6),
                round(float(r.long_snode), 6),
                round(float(r.lat_enode), 6),
                round(float(r.long_enode), 6),
            ]


def _load_csv_node_coords(root: Path, segs_df: pd.DataFrame, coord_map: dict[int, list[float]]) -> None:
    """Fill missing segment coordinates from `raw_data/nodes.csv` when available."""
    nodes_path = root / "raw_data" / "nodes.csv"
    if not (nodes_path.is_file() and not segs_df.empty):
        return
    nodes_df = pd.read_csv(nodes_path, usecols=["_id", "lat", "long"])
    n_lat = dict(zip(nodes_df["_id"].astype(int), nodes_df["lat"].astype(float)))
    n_lon = dict(zip(nodes_df["_id"].astype(int), nodes_df["long"].astype(float)))
    seg_id_col = "_id" if "_id" in segs_df.columns else "segment_id"
    for r in segs_df[[seg_id_col, "s_node_id", "e_node_id"]].dropna().itertuples(index=False):
        sid, sn, en = int(r[0]), int(r[1]), int(r[2])
        if sid not in coord_map and sn in n_lat and en in n_lat:
            coord_map[sid] = [round(n_lat[sn], 6), round(n_lon[sn], 6), round(n_lat[en], 6), round(n_lon[en], 6)]


def _build_node_coord_lookup(root: Path, win_df: pd.DataFrame, segs_df: pd.DataFrame) -> dict[int, list[float]]:
    """Map `segment_id -> [s_lat, s_lon, e_lat, e_lon]` from window table + optional `nodes.csv`."""
    coord_map: dict[int, list[float]] = {}
    _populate_window_coords(win_df, coord_map)
    _load_csv_node_coords(root, segs_df, coord_map)
    return coord_map


def _prescore_windows(df: pd.DataFrame, artifact: dict[str, Any]) -> pd.DataFrame:
    """Attach frozen 23D Random Forest raw, VAL-calibrated, severe_prob, and change_prob columns."""
    out = df.copy()
    if "current_los_ordinal" not in out.columns:
        out["current_los_ordinal"] = out["los_ordinal"]
    feats = artifact["full_features"]
    for col in feats:
        if col not in out.columns:
            out[col] = np.nan
    x_mat = artifact["ml_imputer"].transform(out[feats])
    probs_6 = artifact["rf_multiclass"].predict_proba(x_mat)
    cur_ord = out["los_ordinal"].fillna(0).astype(int).clip(0, 5).to_numpy()
    chg_prob = np.array([1.0 - probs_6[i, c] for i, c in enumerate(cur_ord)])
    alt = probs_6.copy()
    for i, c in enumerate(cur_ord):
        alt[i, c] = -1.0

    change_threshold = float(artifact.get("val_selected_change_threshold", 0.30))
    cal_ord = np.where(chg_prob >= change_threshold, alt.argmax(axis=1), cur_ord)
    raw_ord = probs_6.argmax(axis=1)

    out["rf_pred_ord"] = raw_ord
    out["rf_cal_ord"] = cal_ord
    out["rf_pred_los"] = [ORD_TO_LOS[int(v)] for v in raw_ord]
    out["rf_cal_los"] = [ORD_TO_LOS[int(v)] for v in cal_ord]
    out["rf_severe_prob"] = np.round(probs_6[:, 4:].sum(axis=1), 4)
    out["rf_change_prob"] = np.round(chg_prob, 4)
    out["actual_next_los"] = [
        ORD_TO_LOS.get(int(v), "N/A") if pd.notna(v) else None for v in out.get("target_t30m_los_ordinal", [])
    ]
    out["slot_dt"] = pd.to_datetime(out["slot_ts"], utc=True, errors="coerce")
    return out


def _index_raw_pings(raw_stat_df: pd.DataFrame) -> dict[int, pd.DataFrame]:
    """Group raw GPS pings (`segment_status.csv`) by `segment_id` with ICT timestamps."""
    if raw_stat_df.empty:
        return {}
    work = raw_stat_df.dropna(subset=["segment_id", "velocity", "updated_at"]).copy()
    if "_id" in work.columns:
        work["ping_id"] = pd.to_numeric(work["_id"], errors="coerce").fillna(0).astype(int)
    elif "ping_id" not in work.columns:
        work["ping_id"] = 0
    work["segment_id"] = work["segment_id"].astype(int)
    work["velocity"] = work["velocity"].astype(float)
    work["dt_utc"] = pd.to_datetime(work["updated_at"], utc=True, errors="coerce")
    work = work.dropna(subset=["dt_utc"]).sort_values("dt_utc")
    work["dt_ict"] = work["dt_utc"] + timedelta(hours=7)
    return {int(sid): grp for sid, grp in work.groupby("segment_id")}


def _query_mongo_benchmark_live(limit: int = 25, root: Path = ROOT) -> dict[str, Any]:
    """Query live MongoDB 8.0 `bda501.traffic_scores` with pooled client and dynamic p95 fallback."""
    mv = _read_json(root / "outputs" / "mongo_verify.json")
    lat = _extract_mongo_latency_summary(mv)
    benchmark_fields = {
        "benchmark_median_ms": lat["warm_median_ms"],
        "benchmark_p95_ms": lat["warm_p95_ms"],
        "collscan_warm_median_ms": lat["collscan_warm_median_ms"],
        "speedup_vs_collscan_x": lat["speedup_vs_collscan_x"],
    }
    env_uri = os.environ.get("MONGO_URI", "").strip()
    uris = [env_uri] if env_uri else ["mongodb://127.0.0.1:27017", "mongodb://mongo:27017"]
    for uri in dict.fromkeys(uris):
        client = get_shared_mongo_client(uri)
        if client is None:
            continue
        try:
            coll = client["bda501"]["traffic_scores"]
            total = int(coll.count_documents({}))
            if total > 0:
                t0 = time.perf_counter()
                docs = list(
                    coll.find({"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}}, {"_id": 0})
                    .sort([("severe_prob", -1), ("segment_id", 1)])
                    .limit(limit)
                )
                elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
                return {
                    "mode": "LIVE_MONGODB_8.0",
                    "total_docs": total,
                    "live_query_ms": elapsed_ms,
                    **benchmark_fields,
                    "top_docs": [_enrich_mongo_doc_ict(d) for d in docs[:12]],
                }
        except Exception:
            pass
    top_fallback = mv.get("uc2_arterial_query", {}).get("top_5_arterial_alerts", [])
    return {
        "mode": "SAVED_BENCHMARK_FALLBACK",
        "total_docs": int(mv.get("total_rows_upserted", 0)),
        "live_query_ms": None,
        **benchmark_fields,
        "top_docs": [_enrich_mongo_doc_ict(d) for d in top_fallback],
    }


def _filter_live_docs_by_slot(docs: list[dict[str, Any]], ict_date: str, vn_hour: float) -> dict[int, dict[str, Any]]:
    matched: dict[int, dict[str, Any]] = {}
    ordered = sorted(docs, key=lambda d: int(d.get("seq_id", 0)))
    for d in ordered:
        doc_date = str(d.get("ict_date") or _derive_ict_date_from_utc(d.get("date"), d.get("period")))
        doc_hour = d.get("vn_hour")
        if doc_date == ict_date and doc_hour is not None and abs(float(doc_hour) - float(vn_hour)) <= 0.05:
            matched[int(d["segment_id"])] = d
    return matched


class TOCDataStore:
    """Centralized read-only data & scoring layer serving all 6 Use Cases (UC-1..UC-6)."""

    def __init__(self, root: Path = ROOT) -> None:
        self.root = root
        ds_full = root / "data" / "dataset.csv"
        ds_sample = root / "data" / "demo_sample" / "sample_windows_23d.csv"
        self.is_demo_sample = not ds_full.is_file()
        raw_win = pd.read_csv(ds_full if ds_full.is_file() else ds_sample)

        segs_full = root / "raw_data" / "segments.csv"
        segs_sample = root / "data" / "demo_sample" / "sample_segments.csv"
        self.segs_df = pd.read_csv(segs_full if segs_full.is_file() else segs_sample)

        stat_full = root / "raw_data" / "segment_status.csv"
        stat_sample = root / "data" / "demo_sample" / "sample_raw_status.csv"
        raw_stat_df = pd.read_csv(stat_full if stat_full.is_file() else stat_sample)

        static_full = root / "data" / "lakehouse" / "segment_static_features.parquet"
        static_sample = root / "data" / "demo_sample" / "sample_static_features.parquet"
        self.static_df = pd.read_parquet(static_full if static_full.is_file() else static_sample)

        self.metrics = _read_json(root / "outputs" / "metrics.json")
        self.artifact = joblib.load(root / "outputs" / "model.joblib")
        self.change_threshold = float(self.artifact.get("val_selected_change_threshold", 0.30))
        self.win_df = _prescore_windows(raw_win, self.artifact)
        self.coord_map = _build_node_coord_lookup(root, self.win_df, self.segs_df)
        self.pings_by_seg = _index_raw_pings(raw_stat_df)
        self._index_topology()

    def _index_topology(self) -> None:
        seg_col = "_id" if "_id" in self.segs_df.columns else "segment_id"
        self.seg_nodes: dict[int, tuple[int, int, str]] = {}
        self.node_to_segs: dict[int, list[int]] = {}
        self.in_edges_by_node: dict[int, list[int]] = {}
        self.out_edges_by_node: dict[int, list[int]] = {}
        for r in (
            self.segs_df[[seg_col, "s_node_id", "e_node_id", "street_name"]]
            .dropna(subset=[seg_col])
            .itertuples(index=False)
        ):
            sid = int(r[0])
            sn = int(r[1]) if pd.notna(r[1]) else -1
            en = int(r[2]) if pd.notna(r[2]) else -1
            sname = _clean_street_name(r[3], sid)
            self.seg_nodes[sid] = (sn, en, sname)
            self.node_to_segs.setdefault(sn, []).append(sid)
            self.node_to_segs.setdefault(en, []).append(sid)
            self.out_edges_by_node.setdefault(sn, []).append(sid)
            self.in_edges_by_node.setdefault(en, []).append(sid)

    def get_scenarios(self) -> list[dict[str, Any]]:
        """Return curated rush-hour scenarios with their available 30m frames."""
        results: list[dict[str, Any]] = []
        for sc in CURATED_SCENARIOS:
            sub = self.win_df[
                (self.win_df["ict_date"] == sc["ict_date"])
                & (self.win_df["vn_hour"].between(sc["hour_min"], sc["hour_max"]))
            ]
            frames_grp = (
                sub.groupby(["ict_date", "vn_hour", "date", "period"])
                .agg(
                    active_segments=("segment_id", "nunique"),
                    early_warnings=("rf_cal_ord", lambda s: int((s > self.win_df.loc[s.index, "los_ordinal"]).sum())),
                    transitions_any=("rf_cal_ord", lambda s: int((s != self.win_df.loc[s.index, "los_ordinal"]).sum())),
                    severe_now=("is_severe", "sum"),
                )
                .reset_index()
                .sort_values("vn_hour")
            )
            frames = [
                {
                    "ict_date": str(r.ict_date),
                    "vn_hour": float(r.vn_hour),
                    "date": str(r.date),
                    "period": str(r.period),
                    "active_segments": int(r.active_segments),
                    "early_warnings": int(r.early_warnings),
                    "transitions_any": int(r.transitions_any),
                    "severe_now": int(r.severe_now),
                    **build_window_semantics(str(r.ict_date), float(r.vn_hour)),
                }
                for r in frames_grp.itertuples(index=False)
            ]
            results.append({**sc, "frames": frames})
        return results

    def _classify_alert_category(self, row: Any) -> tuple[int, str]:
        cur_o, cal_o, raw_o = int(row.los_ordinal), int(row.rf_cal_ord), int(row.rf_pred_ord)
        severe_prob = float(getattr(row, "rf_severe_prob", 0.0))
        eff_pred_o = max(cal_o, raw_o)
        if cur_o < 4 and eff_pred_o >= 4:
            return 0, "ONSET_A_D_TO_SEVERE"
        if cur_o == 4 and eff_pred_o >= 5:
            return 1, "SEVERE_WORSENING_E_TO_F"
        if eff_pred_o > cur_o:
            return 2, "WORSENING_TRANSITION"
        if cur_o >= 4 and (raw_o >= 4 or severe_prob >= 0.50):
            return 4, "PERSISTENT_SEVERE"
        if cal_o < cur_o:
            return 3, "RECOVERING_TRANSITION"
        if cur_o >= 4:
            return 4, "PERSISTENT_SEVERE"
        return 5, "STABLE_FLOW"

    def _fetch_live_scores_for_slot(self, ict_date: str, vn_hour: float) -> dict[int, dict[str, Any]]:
        env_uri = os.environ.get("MONGO_URI", "").strip()
        uris = [env_uri] if env_uri else ["mongodb://127.0.0.1:27017", "mongodb://mongo:27017"]
        for uri in dict.fromkeys(uris):
            client = get_shared_mongo_client(uri)
            if client is None:
                continue
            try:
                db = client["bda501"]
                if db["live_stream_telemetry"].find_one({"_id": "current"}, {"_id": 1}) is not None:
                    query = {
                        "ict_date": ict_date,
                        "vn_hour": {"$gte": float(vn_hour) - 0.05, "$lte": float(vn_hour) + 0.05},
                    }
                    docs = list(db["live_stream_scores"].find(query, {"_id": 0}))
                    return _filter_live_docs_by_slot(docs, ict_date, vn_hour)
            except Exception:
                pass
        scores_path = self.root / "outputs" / "live_kafka_scores.json"
        if scores_path.is_file():
            try:
                docs = json.loads(scores_path.read_text(encoding="utf-8"))
                return _filter_live_docs_by_slot(docs, ict_date, vn_hour)
            except Exception:
                return {}
        return {}

    def _overlay_live_scores(self, sub: pd.DataFrame, live_by_seg: dict[int, dict[str, Any]]) -> pd.DataFrame:
        sub = sub.copy()
        sub["lifecycle_status"] = [_static_lifecycle_status(s) for s in sub["split_set"]]
        if not live_by_seg:
            return sub
        for idx in sub.index:
            sid = int(sub.at[idx, "segment_id"])
            live_doc = live_by_seg.get(sid)
            if live_doc is None:
                continue
            cur_los = str(live_doc.get("current_los", sub.at[idx, "LOS"]))
            pred_los = str(live_doc.get("predicted_next_los", sub.at[idx, "rf_pred_los"]))
            cal_los = str(live_doc.get("cal_predicted_los", pred_los))
            sub.at[idx, "LOS"] = cur_los
            sub.at[idx, "los_ordinal"] = LOS_TO_ORD.get(cur_los, int(sub.at[idx, "los_ordinal"]))
            sub.at[idx, "mean_velocity"] = float(live_doc.get("mean_velocity", sub.at[idx, "mean_velocity"]))
            sub.at[idx, "rf_pred_los"] = pred_los
            sub.at[idx, "rf_pred_ord"] = LOS_TO_ORD.get(pred_los, int(sub.at[idx, "rf_pred_ord"]))
            sub.at[idx, "rf_cal_los"] = cal_los
            sub.at[idx, "rf_cal_ord"] = LOS_TO_ORD.get(cal_los, int(sub.at[idx, "rf_cal_ord"]))
            sub.at[idx, "rf_severe_prob"] = float(live_doc.get("severe_prob", sub.at[idx, "rf_severe_prob"]))
            sub.at[idx, "rf_change_prob"] = float(live_doc.get("change_prob", sub.at[idx, "rf_change_prob"]))
            sub.at[idx, "lifecycle_status"] = "STREAM_PROVISIONAL_V1"
        return sub

    def _build_map_polylines(self, rows: pd.DataFrame) -> list[dict[str, Any]]:
        polylines: list[dict[str, Any]] = []
        for r in rows.itertuples(index=False):
            sid = int(r.segment_id)
            coords = self.coord_map.get(sid)
            if not coords:
                continue
            polylines.append(
                {
                    "segment_id": sid,
                    "street_name": _clean_street_name(r.street_name, sid),
                    "street_level": int(r.street_level),
                    "coords": coords,
                    "mean_velocity": round(float(r.mean_velocity), 1),
                    "max_velocity": round(float(r.max_velocity), 0),
                    "obs_count": int(r.obs_count),
                    "current_los": str(r.LOS),
                    "rf_pred_los": str(r.rf_pred_los),
                    "rf_cal_los": str(r.rf_cal_los),
                    "effective_forecast_los": _resolve_effective_forecast_los(
                        str(r.rf_pred_los),
                        str(r.rf_cal_los),
                        float(r.rf_change_prob),
                        float(r.rf_severe_prob),
                        self.change_threshold,
                    ),
                    "actual_next_los": r.actual_next_los,
                    "severe_prob": float(r.rf_severe_prob),
                    "change_prob": float(r.rf_change_prob),
                    "alert_type": str(r.alert_type),
                    "pagerank_score": round(float(r.pagerank_score), 3),
                    "lifecycle_status": str(r.lifecycle_status),
                }
            )
        return polylines

    def _filter_alert_rows(self, sub: pd.DataFrame, alert_filter: str) -> pd.DataFrame:
        if alert_filter == "early_warning":
            filtered = sub[sub["alert_type"].isin(EARLY_WARNING_ALERT_TYPES)]
            return filtered if not filtered.empty else sub
        if alert_filter == "persistent_severe":
            return sub[sub["alert_type"] == "PERSISTENT_SEVERE"]
        return sub

    def _build_alert_rows(self, alert_df: pd.DataFrame) -> list[dict[str, Any]]:
        return [
            {
                "segment_id": int(r.segment_id),
                "street_name": _clean_street_name(r.street_name, int(r.segment_id)),
                "street_level": int(r.street_level),
                "obs_count": int(r.obs_count),
                "mean_velocity": round(float(r.mean_velocity), 1),
                "max_velocity": round(float(r.max_velocity), 0),
                "current_los": str(r.LOS),
                "persistence_los": str(r.LOS),
                "rf_pred_los": str(r.rf_pred_los),
                "rf_cal_los": str(r.rf_cal_los),
                "effective_forecast_los": _resolve_effective_forecast_los(
                    str(r.rf_pred_los),
                    str(r.rf_cal_los),
                    float(r.rf_change_prob),
                    float(r.rf_severe_prob),
                    self.change_threshold,
                ),
                "change_threshold": self.change_threshold,
                "actual_next_los": r.actual_next_los,
                "severe_prob": float(r.rf_severe_prob),
                "change_prob": float(r.rf_change_prob),
                "alert_type": str(r.alert_type),
                "pagerank_score": round(float(r.pagerank_score), 3),
                "lifecycle_status": str(r.lifecycle_status),
            }
            for r in alert_df.head(30).itertuples(index=False)
        ]

    def get_city_frame(
        self,
        ict_date: str = "2021-01-05",
        vn_hour: float = 12.0,
        alert_filter: str = "early_warning",
        max_map_segments: int = 450,
        include_live_overlay: bool = False,
    ) -> dict[str, Any]:
        """Return exact point-in-time `t` map segments & prioritized Early-Warning Alert Board."""
        sub = self.win_df[
            (self.win_df["ict_date"] == ict_date) & (np.isclose(self.win_df["vn_hour"], float(vn_hour), atol=0.05))
        ].copy()
        if sub.empty:
            return {
                "is_empty_slot": True,
                "window_semantics": build_window_semantics(ict_date, vn_hour),
                "summary": {
                    "active_segments": 0,
                    "early_warning_worsening_count": 0,
                    "recovering_count": 0,
                    "persistent_severe_count": 0,
                    "stable_flow_count": 0,
                    "live_matched_segments": 0,
                },
                "map_segments": [],
                "alerts": [],
                "mongo_benchmark": _query_mongo_benchmark_live(limit=12, root=self.root),
            }

        if include_live_overlay:
            live_by_seg = self._fetch_live_scores_for_slot(ict_date, float(vn_hour))
            sub = self._overlay_live_scores(sub, live_by_seg)
        else:
            sub["lifecycle_status"] = [_static_lifecycle_status(s) for s in sub["split_set"]]

        priorities = [self._classify_alert_category(r) for r in sub.itertuples(index=False)]
        sub["alert_rank"] = [p[0] for p in priorities]
        sub["alert_type"] = [p[1] for p in priorities]
        sub = sub.sort_values(["alert_rank", "rf_change_prob", "rf_severe_prob"], ascending=[True, False, False])

        polylines = self._build_map_polylines(sub.head(max_map_segments))
        alert_df = self._filter_alert_rows(sub, alert_filter)
        alerts = self._build_alert_rows(alert_df)

        return {
            "is_empty_slot": False,
            "window_semantics": build_window_semantics(ict_date, vn_hour),
            "summary": {
                "active_segments": int(sub["segment_id"].nunique()),
                "early_warning_worsening_count": int(sub["alert_type"].isin(WORSENING_ALERT_TYPES).sum()),
                "recovering_count": int((sub["alert_type"] == "RECOVERING_TRANSITION").sum()),
                "persistent_severe_count": int((sub["alert_type"] == "PERSISTENT_SEVERE").sum()),
                "stable_flow_count": int((sub["alert_type"] == "STABLE_FLOW").sum()),
                "live_matched_segments": int((sub["lifecycle_status"] == "STREAM_PROVISIONAL_V1").sum()),
            },
            "map_segments": polylines,
            "alerts": alerts,
            "mongo_benchmark": _query_mongo_benchmark_live(limit=12, root=self.root),
        }

    def _extract_raw_pings_around(
        self, segment_id: int, focus_slot_dt: pd.Timestamp
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Extract raw GPS pings in `[t-120m, t)` (past observed) and `[t, t+30m)` (future verification)."""
        p_df = self.pings_by_seg.get(segment_id)
        if p_df is None or p_df.empty or pd.isna(focus_slot_dt):
            return [], []
        issue_utc = focus_slot_dt + timedelta(minutes=30)
        past_start = issue_utc - timedelta(minutes=120)
        fut_end = issue_utc + timedelta(minutes=30)

        past_sub = p_df[(p_df["dt_utc"] >= past_start) & (p_df["dt_utc"] < issue_utc)]
        fut_sub = p_df[(p_df["dt_utc"] >= issue_utc) & (p_df["dt_utc"] < fut_end)]

        def _fmt(rows: pd.DataFrame, bucket: str) -> list[dict[str, Any]]:
            return [
                {
                    "ping_id": int(r.ping_id) if pd.notna(r.ping_id) else 0,
                    "time_ict": r.dt_ict.strftime("%H:%M:%S"),
                    "minutes_from_issue": round(float((r.dt_utc - issue_utc).total_seconds() / 60.0), 2),
                    "velocity_kmh": round(float(r.velocity), 1),
                    "bucket": bucket,
                }
                for r in rows.itertuples(index=False)
            ]

        return _fmt(past_sub.tail(35), "PAST_OBSERVED"), _fmt(fut_sub.head(20), "FUTURE_ACTUAL")

    def _expand_corridor_node(
        self,
        nid: int,
        target_street: str,
        seen_sids: set[int],
        visited_nodes: set[int],
        corridor_sids: list[int],
    ) -> set[int]:
        """Collect unseen same-street segments adjacent to `nid`."""
        next_nodes: set[int] = set()
        for cand in self.node_to_segs.get(nid, []):
            if cand in seen_sids or self.seg_nodes[cand][2] != target_street:
                continue
            seen_sids.add(cand)
            corridor_sids.append(cand)
            csn, cen, _ = self.seg_nodes[cand]
            next_nodes.update({csn, cen} - visited_nodes)
        return next_nodes

    def _trace_same_corridor(
        self, segment_id: int, target_street: str, max_hops: int = 12
    ) -> tuple[list[int], set[int]]:
        """Walk connected micro-segments sharing `street_name` to rebuild the corridor/roundabout."""
        sn, en, _ = self.seg_nodes[segment_id]
        frontier = {sn, en}
        visited_nodes = {sn, en}
        corridor_sids: list[int] = []
        seen_sids = {segment_id}
        for _ in range(max_hops):
            next_nodes: set[int] = set()
            for nid in frontier:
                next_nodes.update(
                    self._expand_corridor_node(nid, target_street, seen_sids, visited_nodes, corridor_sids)
                )
            visited_nodes.update(next_nodes)
            frontier = next_nodes
            if not frontier or len(corridor_sids) >= 28:
                break
        return corridor_sids, visited_nodes

    def _expand_node_edges(
        self,
        nid: int,
        edge_lookup: dict[int, list[int]],
        seen_sids: set[int],
        seen_nodes: set[int],
        found: list[int],
        node_idx: int,
        limit: int,
    ) -> tuple[set[int], bool]:
        """Collect unseen adjacent segments from one node; returns `(new_nodes, limit_reached)`."""
        next_nodes: set[int] = set()
        for cand in edge_lookup.get(nid, []):
            if cand in seen_sids:
                continue
            seen_sids.add(cand)
            found.append(cand)
            nxt = self.seg_nodes[cand][node_idx]
            if nxt not in seen_nodes:
                seen_nodes.add(nxt)
                next_nodes.add(nxt)
            if len(found) >= limit:
                return set(), True
        return next_nodes, False

    def _trace_directed_flow(
        self, seed_nodes: set[int], exclude_sids: set[int], upstream: bool, max_hops: int = 5, limit: int = 24
    ) -> list[int]:
        """Walk multi-hop directed feeder (`upstream=True`) or exit (`upstream=False`) cross-streets."""
        edge_lookup = self.in_edges_by_node if upstream else self.out_edges_by_node
        node_idx = 0 if upstream else 1
        frontier = set(seed_nodes)
        seen_nodes = set(seed_nodes)
        found: list[int] = []
        seen_sids = set(exclude_sids)
        for _ in range(max_hops):
            next_nodes: set[int] = set()
            for nid in frontier:
                hop_nodes, done = self._expand_node_edges(
                    nid, edge_lookup, seen_sids, seen_nodes, found, node_idx, limit
                )
                if done:
                    return found
                next_nodes.update(hop_nodes)
            frontier = next_nodes
            if not frontier:
                break
        return found

    def _build_neighbor_polylines(self, segment_id: int, ict_date: str, vn_hour: float) -> list[dict[str, Any]]:
        """Return multi-hop connected street corridor + upstream inflow & downstream outflow arteries."""
        sn_en = self.seg_nodes.get(segment_id)
        if not sn_en:
            return []
        _, _, target_street = sn_en
        corridor_sids, corridor_nodes = self._trace_same_corridor(segment_id, target_street)
        exclude = {segment_id, *corridor_sids}
        inflow_sids = self._trace_directed_flow(corridor_nodes, exclude, upstream=True)
        outflow_sids = self._trace_directed_flow(corridor_nodes, exclude | set(inflow_sids), upstream=False)

        active_slot = self.win_df[
            (self.win_df["ict_date"] == ict_date) & (np.isclose(self.win_df["vn_hour"], vn_hour, atol=0.05))
        ]
        slot_los = dict(zip(active_slot["segment_id"].astype(int), active_slot["LOS"].astype(str)))

        tagged = (
            [(sid, "SAME_CORRIDOR") for sid in corridor_sids]
            + [(sid, "UPSTREAM_INFLOW") for sid in inflow_sids]
            + [(sid, "DOWNSTREAM_OUTFLOW") for sid in outflow_sids]
        )
        neighbors: list[dict[str, Any]] = []
        for nsid, relation in tagged:
            coords = self.coord_map.get(nsid)
            n_info = self.seg_nodes.get(nsid)
            if coords and n_info:
                neighbors.append(
                    {
                        "segment_id": nsid,
                        "street_name": n_info[2],
                        "relation": relation,
                        "coords": coords,
                        "current_los": slot_los.get(nsid, "COLD_START"),
                    }
                )
        return neighbors

    def _build_cold_start_timeline(
        self,
        segment_id: int,
        static_row: pd.DataFrame,
        ict_date: str | None,
        vn_hour: float | None,
    ) -> dict[str, Any]:
        s_info = static_row.iloc[0]
        scored = score_feature_snapshot(static_row, self.artifact).iloc[0]
        f_date = ict_date or "2021-01-05"
        f_hour = float(vn_hour) if vn_hour is not None else 8.0
        vmax = float(s_info["max_velocity"])
        prior_vel = round(float(s_info["mean_velocity"]), 2)
        prior_los_ord = int(round(float(s_info.get("segment_train_prior_los", 0))))
        prior_los = ORD_TO_LOS.get(min(max(prior_los_ord, 0), 5), "A")
        pred_los = str(scored["predicted_next_los"])
        return {
            "segment_id": int(segment_id),
            "is_cold_start": True,
            "street_name": _clean_street_name(s_info["street_name"], segment_id),
            "street_level": int(s_info["street_level"]),
            "split_set": "COLD_START_STRUCTURAL_PRIOR",
            "max_velocity": round(vmax, 1),
            "coords": self.coord_map.get(int(segment_id)),
            "neighbors": self._build_neighbor_polylines(int(segment_id), f_date, f_hour),
            "window_semantics": build_window_semantics(f_date, f_hour),
            "history_windows": [],
            "raw_pings_past": [],
            "raw_pings_future": [],
            "forecast_comparison": {
                "current_los": prior_los,
                "current_velocity": prior_vel,
                "persistence_los": prior_los,
                "persistence_velocity": prior_vel,
                "rf_raw_los": pred_los,
                "rf_calibrated_los": pred_los,
                "effective_forecast_los": pred_los,
                "actual_next_los": None,
                "actual_next_velocity": None,
                "severe_prob": float(scored["severe_prob"]),
                "change_prob": 0.0,
            },
            "feature_drivers": {
                "obs_count_in_window": 0,
                "lag1_los": "Cold-Start (0 GPS)",
                "neighbor_los": "Cluster Prior",
                "pagerank_score": round(float(s_info["pagerank_score"]), 4),
                "cluster_id": int(s_info["cluster_id"]),
                "street_severe_prior": round(float(s_info.get("street_causal_prior_sev", 0.0)), 3),
            },
        }

    @staticmethod
    def _select_timeline_focus_row(
        seg_rows: pd.DataFrame, ict_date: str | None, vn_hour: float | None
    ) -> pd.Series:
        if ict_date is not None and vn_hour is not None:
            exact = seg_rows[
                (seg_rows["ict_date"] == ict_date) & (np.isclose(seg_rows["vn_hour"], float(vn_hour), atol=0.05))
            ]
            if not exact.empty:
                return exact.iloc[0]
        return seg_rows.iloc[-1]

    @staticmethod
    def _build_timeline_feature_drivers(focus_row: pd.Series) -> dict[str, Any]:
        lag1_raw = focus_row.get("lag1_los_ordinal")
        nb_raw = focus_row.get("neighbor_los_ordinal")
        return {
            "obs_count_in_window": int(focus_row["obs_count"]),
            "lag1_los": ORD_TO_LOS.get(int(lag1_raw), "N/A") if pd.notna(lag1_raw) else "Gap >30m",
            "neighbor_los": ORD_TO_LOS.get(int(round(float(nb_raw))), "N/A") if pd.notna(nb_raw) else "N/A",
            "pagerank_score": round(float(focus_row["pagerank_score"]), 4),
            "cluster_id": int(focus_row["cluster_id"]),
            "street_severe_prior": round(float(focus_row.get("street_causal_prior_sev", 0.0)), 3),
        }

    def get_segment_timeline(
        self,
        segment_id: int,
        ict_date: str | None = None,
        vn_hour: float | None = None,
    ) -> dict[str, Any]:
        """Build complete Raw GPS Pings -> 30m Window Closure at t -> Dashed +30m Forecast story."""
        seg_id = int(segment_id)
        seg_rows = self.win_df[self.win_df["segment_id"] == seg_id].sort_values("slot_dt")
        if seg_rows.empty:
            static_rows = self.static_df[self.static_df["segment_id"] == seg_id]
            if not static_rows.empty:
                return self._build_cold_start_timeline(seg_id, static_rows.iloc[[0]], ict_date, vn_hour)
            raise KeyError(f"Segment #{seg_id} not found in windows or static store")

        focus_row = self._select_timeline_focus_row(seg_rows, ict_date, vn_hour)
        f_dt = focus_row["slot_dt"]
        f_date = str(focus_row["ict_date"])
        f_hour = float(focus_row["vn_hour"])
        vmax = float(focus_row["max_velocity"])

        hist_sub = seg_rows[
            (seg_rows["slot_dt"] >= f_dt - timedelta(minutes=120)) & (seg_rows["slot_dt"] <= f_dt)
        ].tail(5)
        history_points = [
            {
                "window_label": f"{format_clock_hhmm(float(r.vn_hour))}–{format_clock_hhmm(float(r.vn_hour) + 0.5)}",
                "close_time_ict": format_clock_hhmm(float(r.vn_hour) + 0.5),
                "minutes_from_issue": round(float((r.slot_dt - f_dt).total_seconds() / 60.0), 1),
                "mean_velocity": round(float(r.mean_velocity), 2),
                "obs_count": int(r.obs_count),
                "los": str(r.LOS),
            }
            for r in hist_sub.itertuples(index=False)
        ]

        next_exact = seg_rows[seg_rows["slot_dt"] == f_dt + timedelta(minutes=30)]
        actual_next_vel = round(float(next_exact.iloc[0]["mean_velocity"]), 2) if not next_exact.empty else None
        past_pings, future_pings = self._extract_raw_pings_around(seg_id, f_dt)

        change_threshold = float(self.artifact.get("val_selected_change_threshold", 0.30))
        eff_rf_los = _resolve_effective_forecast_los(
            str(focus_row["rf_pred_los"]),
            str(focus_row["rf_cal_los"]),
            float(focus_row["rf_change_prob"]),
            float(focus_row["rf_severe_prob"]),
            change_threshold,
        )

        return {
            "segment_id": seg_id,
            "is_cold_start": False,
            "street_name": _clean_street_name(focus_row["street_name"], seg_id),
            "street_level": int(focus_row["street_level"]),
            "split_set": str(focus_row.get("split_set", "TEST")),
            "max_velocity": round(vmax, 1),
            "coords": self.coord_map.get(seg_id),
            "neighbors": self._build_neighbor_polylines(seg_id, f_date, f_hour),
            "window_semantics": build_window_semantics(f_date, f_hour),
            "history_windows": history_points,
            "raw_pings_past": past_pings,
            "raw_pings_future": future_pings,
            "forecast_comparison": {
                "current_los": str(focus_row["LOS"]),
                "current_velocity": round(float(focus_row["mean_velocity"]), 2),
                "persistence_los": str(focus_row["LOS"]),
                "persistence_velocity": round(float(focus_row["mean_velocity"]), 2),
                "rf_raw_los": str(focus_row["rf_pred_los"]),
                "rf_calibrated_los": str(focus_row["rf_cal_los"]),
                "effective_forecast_los": eff_rf_los,
                "actual_next_los": focus_row["actual_next_los"],
                "actual_next_velocity": actual_next_vel,
                "severe_prob": float(focus_row["rf_severe_prob"]),
                "change_prob": float(focus_row["rf_change_prob"]),
            },
            "feature_drivers": self._build_timeline_feature_drivers(focus_row),
        }

    def get_overview(self) -> dict[str, Any]:
        """Return UC-1 Top-25 EWMA Hotspots, 5 Headline KPIs, Quality Funnel, and LOS Breakdown."""
        hotspots = _read_csv_records(self.root / "outputs" / "hotspot_alerts.csv", limit=25)
        for h in hotspots:
            sid = int(h["segment_id"])
            h["street_name"] = _clean_street_name(h.get("street_name"), sid)
            h["coords"] = self.coord_map.get(sid)
        los_counts = {lbl: int((self.win_df["LOS"] == lbl).sum()) for lbl in LOS_ORDER}
        return {
            "is_demo_sample": self.is_demo_sample,
            "dataset_kpis": self.metrics.get("dataset", {}),
            "evaluation_kpis": self.metrics.get("evaluation", {}),
            "quality_funnel": self.metrics.get("quality_funnel_c5", {}),
            "quarantine_rules": _read_csv_records(self.root / "outputs" / "quarantine_summary.csv", limit=10),
            "los_distribution": los_counts,
            "ewma_hotspots": hotspots,
        }

    def get_graph_and_rules(self) -> dict[str, Any]:
        """Return UC-3 PageRank Top-35 + Card B2 Falsification + UC-4 SON/FPGrowth & Lagged Rules."""
        pr_rows = _read_csv_records(self.root / "outputs" / "road_pagerank_top.csv", limit=35)
        st_map = self.static_df.set_index("segment_id")
        latest_slots = (
            self.win_df.sort_values("slot_dt").groupby("segment_id").last()[["ict_date", "vn_hour"]]
        )
        for r in pr_rows:
            sid = int(r["segment_id"])
            r["street_name"] = _clean_street_name(r.get("street_name"), sid)
            r["coords"] = self.coord_map.get(sid)
            if sid in st_map.index:
                s_row = st_map.loc[sid]
                r["is_cold_start"] = bool(s_row["is_cold_start"])
                r["obs_windows"] = int(s_row["obs_windows"])
                r["mean_velocity"] = round(float(s_row["mean_velocity"]), 1)
            if sid in latest_slots.index:
                slot_row = latest_slots.loc[sid]
                r["latest_ict_date"] = str(slot_row["ict_date"])
                r["latest_vn_hour"] = float(slot_row["vn_hour"])
        b2_rows = _read_csv_records(self.root / "evidence" / "session9" / "H_B2_graph_zone_falsification.csv", limit=20)
        return {
            "pagerank_top": pr_rows,
            "card_b2_falsification": b2_rows,
            "co_congestion_rules": _read_csv_records(self.root / "outputs" / "co_congestion_rules.csv", limit=30),
            "lagged_rules": _read_csv_records(self.root / "outputs" / "lagged_congestion_rules.csv", limit=20),
        }

    def get_clusters_and_coldstart(self, target_segment_id: int | None = None, limit: int = 8) -> dict[str, Any]:
        """Return UC-5 7D K-Means Archetypes, Cold-Start Map Sample, Peer Lookup & Structural Prior Score."""
        grp = (
            self.static_df.groupby("cluster_id")
            .agg(
                total_segments=("segment_id", "count"),
                observed_segments=("is_cold_start", lambda s: int((~s).sum())),
                cold_start_segments=("is_cold_start", "sum"),
                mean_velocity_kmh=("mean_velocity", "mean"),
                mean_pagerank=("pagerank_score", "mean"),
            )
            .reset_index()
        )
        clusters = [
            {
                "cluster_id": int(r.cluster_id),
                "total_segments": int(r.total_segments),
                "observed_segments": int(r.observed_segments),
                "cold_start_segments": int(r.cold_start_segments),
                "mean_velocity_kmh": round(float(r.mean_velocity_kmh), 1),
                "mean_pagerank": round(float(r.mean_pagerank), 3),
            }
            for r in grp.itertuples(index=False)
        ]

        cold_candidates = (
            self.static_df[self.static_df["is_cold_start"]].sort_values("pagerank_score", ascending=False).head(150)
        )
        chosen_sid = (
            int(target_segment_id)
            if target_segment_id and int(target_segment_id) in set(self.static_df["segment_id"].astype(int))
            else int(cold_candidates.iloc[0]["segment_id"])
        )
        target_row = self.static_df[self.static_df["segment_id"] == chosen_sid].copy()
        scored_prior = score_feature_snapshot(target_row, self.artifact).iloc[0]
        peers_df = find_peer_segments(self.static_df, chosen_sid, limit=limit)

        map_sample_df = pd.concat(
            [
                self.static_df[~self.static_df["is_cold_start"]].head(180),
                self.static_df[self.static_df["is_cold_start"]]
                .sort_values("pagerank_score", ascending=False)
                .head(220),
            ],
            ignore_index=True,
        )
        map_points = [
            {
                "segment_id": int(r.segment_id),
                "street_name": _clean_street_name(r.street_name, int(r.segment_id)),
                "cluster_id": int(r.cluster_id),
                "is_cold_start": bool(r.is_cold_start),
                "pagerank_score": round(float(r.pagerank_score), 3),
                "coords": self.coord_map.get(int(r.segment_id)),
            }
            for r in map_points_iter(map_sample_df, self.coord_map)
        ]

        model_info = self.metrics.get("model", {})
        peers_records = peers_df.replace({np.nan: None}).to_dict(orient="records")
        for peer in peers_records:
            if peer.get("segment_id") is not None:
                peer["street_name"] = _clean_street_name(peer.get("street_name"), int(peer["segment_id"]))
        return {
            "total_network_segments": int(len(self.static_df)),
            "observed_segments_count": int((~self.static_df["is_cold_start"]).sum()),
            "cold_start_segments_count": int(self.static_df["is_cold_start"].sum()),
            "silhouette_score": float(model_info.get("silhouette_score", 0.0)),
            "train_segments_clustered": int(model_info.get("segments_clustered", 0)),
            "cluster_features": list(model_info.get("feature_columns", [])),
            "clusters": clusters,
            "cold_start_candidates": [
                {
                    "segment_id": int(r.segment_id),
                    "street_name": _clean_street_name(r.street_name, int(r.segment_id)),
                    "street_level": int(r.street_level),
                    "cluster_id": int(r.cluster_id),
                    "pagerank_score": round(float(r.pagerank_score), 3),
                }
                for r in cold_candidates.head(80).itertuples(index=False)
            ],
            "selected_segment": {
                "segment_id": chosen_sid,
                "street_name": _clean_street_name(target_row.iloc[0]["street_name"], chosen_sid),
                "street_level": int(target_row.iloc[0]["street_level"]),
                "cluster_id": int(target_row.iloc[0]["cluster_id"]),
                "is_cold_start": bool(target_row.iloc[0]["is_cold_start"]),
                "pagerank_score": round(float(target_row.iloc[0]["pagerank_score"]), 4),
                "max_velocity": round(float(target_row.iloc[0]["max_velocity"]), 1),
                "structural_prior_predicted_los": str(scored_prior["predicted_next_los"]),
                "structural_prior_severe_prob": round(float(scored_prior["severe_prob"]), 4),
                "badge": "COLD_START_STRUCTURAL_PRIOR" if bool(target_row.iloc[0]["is_cold_start"]) else "GPS_OBSERVED",
                "coords": self.coord_map.get(chosen_sid),
            },
            "peers": peers_records,
            "map_sample": map_points,
        }

    def get_infrastructure_audit(self) -> dict[str, Any]:
        """Return P1 Quality Funnel, UC-6 Spark 3.5.7, ML Ablation, Reconciliation & Card B1/B3 Audits."""
        ev_dir = self.root / "evidence" / "session9"
        static_info = self.metrics.get("static_feature_store_c13b", {})
        monoid_cat = dict(self.metrics.get("monoid_and_catalyst_c9", {}))
        if "spark_runtime_verification" not in monoid_cat:
            monoid_cat["spark_runtime_verification"] = _read_json(
                self.root / "outputs" / "spark_runtime_evidence.json"
            )
        mv_full = _read_json(self.root / "outputs" / "mongo_verify.json")
        return {
            "dataset": self.metrics.get("dataset", {}),
            "evaluation": self.metrics.get("evaluation", {}),
            "monoid_and_catalyst": monoid_cat,
            "cold_start_ratio_pct": float(static_info.get("cold_start_ratio_pct", 0.0)),
            "streaming_mmds": self.metrics.get("streaming_mmds", {}),
            "quality_funnel": self.metrics.get("quality_funnel_c5", {}),
            "quarantine_rules": _read_csv_records(self.root / "outputs" / "quarantine_summary.csv", limit=10),
            "ablation_results": _read_csv_records(self.root / "outputs" / "ablation_results.csv", limit=15),
            "transition_evaluation": _read_csv_records(self.root / "outputs" / "transition_evaluation.csv", limit=15),
            "spark_benchmark": _read_csv_records(ev_dir / "B_benchmark.csv", limit=10),
            "reconciliation_audit": _read_csv_records(self.root / "outputs" / "reconciliation_audit.csv", limit=12),
            "card_b1_pit_matrix": _read_card_b1_pit_matrix(ev_dir / "H_B1_point_in_time_matrix.csv"),
            "card_b3_sketches": _read_csv_records(ev_dir / "H_B3_missingness_sketches.csv", limit=12),
            "wal_progress": _read_json(self.root / "outputs" / "query_progress.json"),
            "mongo_verify": _build_mongo_verify_payload(mv_full),
            "frozen_manifest": _read_json(self.root / "outputs" / "model_registry_manifest.json"),
        }


def map_points_iter(df: pd.DataFrame, coord_map: dict[int, list[float]]) -> list[Any]:
    return [r for r in df.itertuples(index=False) if int(r.segment_id) in coord_map][:320]
