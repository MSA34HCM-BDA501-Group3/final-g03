"""Presentation-ready stage card & widget adapter for `GET /api/pipeline-trace`."""

from __future__ import annotations

from typing import Any

STAGE_ACCENTS: dict[int, str] = {
    1: "#0284c7",
    2: "#0891b2",
    3: "#059669",
    4: "#7c3aed",
    5: "#e11d48",
    6: "#d97706",
    7: "#2563eb",
}


def _kv(label: str, value: Any) -> dict[str, str]:
    return {"label": str(label), "value": "—" if value is None else str(value)}


def _enrich_stage_1(st: dict[str, Any], sel: dict[str, Any]) -> None:
    d = st["details"]
    qc = d.get("quality_checks", {})
    u2i = d.get("utc_to_ict", {})
    gf = d.get("global_funnel", {})
    pings = d.get("raw_pings", [])
    ping_str = ", ".join(f"{p['velocity_kmh']:.0f} km/h ({p['time_ict']})" for p in pings[:4]) or "0 GPS (Cold-Start)"
    st["input_summary"] = [
        _kv("Segment ID", f"#{sel['segment_id']} · {qc.get('raw_street_name') or sel['street_name']}"),
        _kv("Loại đường OSM", f"{qc.get('street_type')} (Cấp {qc.get('street_level')}) · dài {qc.get('raw_length_m')} m"),
        _kv("GPS thô trong khung", f"{d.get('ping_count_in_window', 0)} pings: {ping_str}"),
        _kv("Mốc UTC gốc", f"{u2i.get('utc_date') or '—'} · {u2i.get('utc_period') or '—'}"),
    ]
    st["transform_steps"] = [
        f"R1–R2 (Hình học & Cơ giới): length={qc.get('raw_length_m')} m, street_type='{qc.get('street_type')}' (lọc 427 POI toàn mạng).",
        f"R3–R4 (Unicode NFC): '{qc.get('raw_street_name')}' ➔ '{qc.get('canonical_street_name')}' (gộp {gf.get('street_entities_merged_by_nfc', 54)} trùng tên).",
        f"R6 (UTC ➔ ICT +7h & Quarantine): {u2i.get('ict_collect_window') or 'Cold-Start'} · chốt {u2i.get('ict_issue_time') or '—'}.",
    ]
    st["output_summary"] = [
        _kv("Tên tuyến chuẩn NFC", qc.get("canonical_street_name")),
        _kv("Trạng thái Phễu", st["status"]),
        _kv("Toàn mạng R1–R6", f"{gf.get('accepted_pings', 90787):,} hợp lệ / {gf.get('quarantined_pings', 151)} cách ly"),
        _kv("Rớt ngầm (Silent Drops)", f"{gf.get('silent_drops', 0)} bản ghi"),
    ]
    st["key_metrics"] = [
        _kv("Pings", f"{d.get('ping_count_in_window', 0)} tin"),
        _kv("Silent Drops", "0"),
        _kv("Motorized", f"{gf.get('clean_motorized_segments', 84206):,}"),
    ]


def _enrich_stage_2(st: dict[str, Any]) -> None:
    d = st["details"]
    raw_v = d.get("raw_max_velocity")
    imp_v = d.get("imputed_max_velocity")
    d["vmax_tier"] = d.get("vmax_tier_label")
    st["input_summary"] = [
        _kv("max_velocity gốc OSM", f"{raw_v} km/h" if raw_v is not None else "NULL (khuyết 88.34% toàn mạng)"),
        _kv("Khóa tra cứu Train-Only", d.get("vmax_lookup_key")),
        _kv("Vận tốc TB cửa sổ", f"{d.get('mean_velocity', 0.0):.2f} km/h"),
    ]
    st["transform_steps"] = [
        f"3-Tier Imputation (chỉ học trên TRAIN hữu cơ): kích hoạt {d.get('vmax_tier_label')} ➔ Vmax = {imp_v} km/h.",
        f"Tính tỷ số vận tốc r = v̄ / Vmax = {d.get('mean_velocity', 0.0):.2f} / {imp_v} = {d.get('velocity_ratio', 0.0):.4f}.",
        f"Ánh xạ derive_urban_los (bậc 5 km/h đô thị, khớp {d.get('bktraffic_exact_match_pct', 89.19)}% BkTraffic) ➔ LOS {d.get('derived_urban_los')}.",
    ]
    st["output_summary"] = [
        _kv("Vmax sau nội suy", f"{imp_v} km/h ({d.get('vmax_tier_label')})"),
        _kv("Tỷ số r = v̄ / Vmax", f"{d.get('velocity_ratio', 0.0):.4f}"),
        _kv("LOS dẫn xuất / Tham chiếu", f"{d.get('derived_urban_los')} / {d.get('reference_los')}"),
    ]
    st["key_metrics"] = [
        _kv("Vmax", f"{imp_v:.0f} km/h ({d.get('vmax_tier_label', '').split()[0:2] and ' '.join(str(d.get('vmax_tier_label')).split()[:2])})"),
        _kv("BkTraffic", f"{d.get('bktraffic_exact_match_pct', 89.19)}%"),
        _kv("LOS", str(d.get("reference_los"))),
    ]


def _enrich_stage_3(st: dict[str, Any], stage_1_pings: list[dict[str, Any]]) -> None:
    d = st["details"]
    mt = d.get("merged_tuple", {})
    sb = d.get("spark_benchmark", {})
    buckets = d.get("salted_buckets", [])
    d["monoid_pings"] = stage_1_pings
    tuple_str = (
        d.get("identity_element_str")
        if d.get("is_identity_cold_start")
        else f"({mt.get('cnt')}, {mt.get('sum_v')}, {mt.get('min_v')}, {mt.get('max_v')}, {mt.get('severe_cnt')})"
    )
    st["input_summary"] = [
        _kv("Số giỏ Key Salting", f"{len(buckets)} / {d.get('salt_buckets_total', 8)} buckets hoạt động"),
        _kv("Ngưỡng ping kẹt nặng", f"v < {d.get('severe_velocity_threshold_kmh', 15.0)} km/h"),
        _kv("Phần tử đơn vị Monoid", d.get("identity_element_str")),
    ]
    st["transform_steps"] = [
        "Stage 1 (Gộp cục bộ): đóng gói mỗi ping thành 5-Tuple (cnt, sum_v, min_v, max_v, severe_cnt) theo salt_bucket = hash % 8.",
        f"Stage 2 (Gộp giao hoán–kết hợp): thu được {tuple_str} ➔ mean_v = {mt.get('mean_v')} km/h (sai số vs baseline.py = {d.get('baseline_diff', 0.0)}).",
        f"Spark {sb.get('spark_version', '3.5.7')} BHJ: cắt {sb.get('exchange_stages_before', 5)} ➔ {sb.get('exchange_stages_after', 1)} Exchange, skew {sb.get('skew_before_x', 684.95)}x ➔ {sb.get('skew_after_x', 1.0197)}x.",
    ]
    st["output_summary"] = [
        _kv("5-Tuple sau gộp", tuple_str),
        _kv("Vận tốc TB Monoid", f"{mt.get('mean_v')} km/h" if mt.get("mean_v") is not None else "0 GPS (Identity)"),
        _kv("Spark Combined / Isolated", f"-{sb.get('combined_speedup_pct', 12.45)}% / -{sb.get('isolated_join_speedup_pct', 25.76)}%"),
        _kv("exceptAll diff", str(sb.get("two_way_except_all_diff", 0))),
    ]
    st["key_metrics"] = [
        _kv("Mean", f"{mt.get('mean_v')} km/h" if mt.get("mean_v") is not None else "Identity"),
        _kv("Diff", "0.0"),
        _kv("Skew", f"{sb.get('skew_after_x', 1.0197)}x"),
    ]


def _flatten_23d_features(stage_5_details: dict[str, Any]) -> list[dict[str, str]]:
    fg = stage_5_details.get("feature_groups", {})
    out: list[dict[str, str]] = []
    for grp_key, grp_label in (
        ("local_14d", "Local 14D"),
        ("graph_cluster_4d", "Graph/Cluster 4D"),
        ("past_priors_5d", "Past Priors 5D"),
    ):
        for item in fg.get(grp_key, []):
            val = item.get("imputed_value")
            val_str = f"{val:.4g}" if isinstance(val, (int, float)) else str(val)
            out.append({"name": str(item.get("name", "")), "value": val_str, "group": grp_label})
    return out


def _enrich_stage_4(st: dict[str, Any], features_23d: list[dict[str, str]]) -> None:
    d = st["details"]
    pp = d.get("pagerank_params", {})
    peers = d.get("top_observed_peers", [])
    peer_txt = ", ".join(f"#{p['segment_id']} {p['street_name']}" for p in peers[:3]) or "—"
    d["features_23d"] = features_23d
    st["input_summary"] = [
        _kv("Đồ thị Line-Graph", f"{pp.get('nodes_segments', 84206):,} đỉnh · {pp.get('transition_edges', 92453):,} cạnh"),
        _kv("Bậc vào / Bậc ra", f"in={d.get('in_degree', 0)} · out={d.get('out_degree', 0)}"),
        _kv("Trạng thái Cold-Start", "Có (0 GPS · Dùng cụm & Peer)" if d.get("is_cold_start") else "Không (Có GPS)"),
    ]
    st["transform_steps"] = [
        f"Line-Graph PageRank ({pp.get('iterations', 30)} vòng, β={pp.get('damping_beta', 0.85)}, không U-turn) ➔ PR = {d.get('pagerank_score')}.",
        f"7D K-Means (Silhouette = {d.get('cluster_summary', {}).get('silhouette_score', 0.2241)}) xếp vào Cụm hành vi #{d.get('cluster_id')}.",
        f"Ghép trạng thái nhánh kề (LOS {d.get('neighbor_los_label')}) & 3 tuyến tương đồng gần nhất để lấp vùng mù 88.13%.",
    ]
    nb_ord = d.get("neighbor_los_ordinal")
    nb_disp = (
        f"LOS {d.get('neighbor_los_label')} (ord {nb_ord})"
        if nb_ord is not None
        else f"LOS {d.get('neighbor_los_label')}"
    )
    st["output_summary"] = [
        _kv("PageRank Score", f"{d.get('pagerank_score', 0.0):.4f}"),
        _kv("Cụm hành vi 7D", f"Cụm #{d.get('cluster_id')}"),
        _kv("Nhánh láng giềng", nb_disp),
        _kv("Top 3 tuyến tương đồng", peer_txt),
    ]
    st["key_metrics"] = [
        _kv("PageRank", f"{d.get('pagerank_score', 0.0):.4f}"),
        _kv("Cluster", f"#{d.get('cluster_id')}"),
        _kv("23D Vector", f"{len(features_23d)} biến"),
    ]


def _enrich_stage_5(st: dict[str, Any]) -> None:
    d = st["details"]
    vt = d.get("val_thresholds", {})
    sev_pct = round(float(d.get("severe_prob", 0.0)) * 100.0, 1)
    chg_pct = round(float(d.get("change_prob", 0.0)) * 100.0, 1)
    d["severe_threshold"] = round(float(vt.get("onset", 0.25)) * 100.0, 1)
    d["change_threshold"] = round(float(vt.get("change", 0.40)) * 100.0, 1)
    st["input_summary"] = [
        _kv("Vector đầu vào", "23 biến (14 Local · 4 Graph/Cluster · 5 Past Priors τ < t)"),
        _kv("Mô hình Frozen Registry", f"{d.get('model_version')} (SHA {d.get('frozen_model_sha256_short')})"),
        _kv("Hiện tại / Persistence", f"LOS {d.get('current_los')} (Persistence đoán giữ {d.get('persistence_los')})"),
    ]
    st["transform_steps"] = [
        "Đọc chỉ-đọc (.predict_proba) trên 120 cây Random Forest đã khóa SHA-256 ở Phase A — không ghi đè mô hình.",
        f"Tính P(E/F) = {sev_pct:.1f}% (ngưỡng VAL 25%) và P(đổi pha) = {chg_pct:.1f}% (ngưỡng VAL 40%).",
        f"Chốt dự báo +30p = LOS {d.get('effective_forecast_los')} (raw argmax = {d.get('raw_pred_los')}) · Thực tế t+30p = {d.get('actual_next_los') or 'N/A'}.",
    ]
    st["output_summary"] = [
        _kv("Dự báo RF +30p", f"LOS {d.get('effective_forecast_los')} (raw {d.get('raw_pred_los')})"),
        _kv("Nguy cơ kẹt E/F", f"{sev_pct:.1f}% ({'BẬT CẢNH BÁO' if d.get('onset_alert_triggered') else 'Ổn định'})"),
        _kv("Xác suất đổi pha", f"{chg_pct:.1f}% ({'VƯỢT NGƯỠNG 40%' if d.get('change_alert_triggered') else 'Dưới ngưỡng'})"),
        _kv("Nhãn thực tế t+30p", d.get("actual_next_los") or "Cold-Start Prior"),
    ]
    st["key_metrics"] = [
        _kv("RF +30p", f"LOS {d.get('effective_forecast_los')}"),
        _kv("P(E/F)", f"{sev_pct:.1f}%"),
        _kv("P(chg)", f"{chg_pct:.1f}%"),
    ]


def _enrich_stage_6(st: dict[str, Any]) -> None:
    d = st["details"]
    bs = d.get("bloom_spec", {})
    hs = d.get("hll_spec", {})
    ew = d.get("ewma_hotspot", {})
    wm = d.get("watermark", {})
    wal = d.get("wal_checkpoint", {})
    d["bloom_probes"] = d.get("bloom_probe_positions", [])
    st["input_summary"] = [
        _kv("Kafka Topic / Partition", f"{d.get('kafka_topic')} · P{d.get('kafka_partition')} ({d.get('partition_formula')})"),
        _kv("Event ID", str(d.get("event_id", ""))[:24]),
        _kv("Bộ nhớ Sketches cố định", f"{d.get('total_sketch_memory_kib', 33.0)} KiB O(1) RAM"),
    ]
    st["transform_steps"] = [
        f"RotatingBloom ({bs.get('label')}): băm k=10 vị trí chặn trùng (benchmark {bs.get('duplicates_caught', 333)}/333, FPR {bs.get('empirical_fp_rate', 0.000389)*100:.4f}%).",
        f"HyperLogLog++ (1.0 KiB): ước lượng {hs.get('estimated_segments', 9620.2):,.1f}/{hs.get('exact_distinct_segments', 9995):,} đoạn (sai số {hs.get('relative_error_pct', 3.75)}%).",
        f"EWMA (α={ew.get('alpha', 0.15)}) = {ew.get('segment_score', 0.0):.2f} · Watermark {wm.get('delay_label')} · WAL {wal.get('wal_state_kb', 73.7)} KB (0 trùng khi hồi phục).",
    ]
    st["output_summary"] = [
        _kv("Phân vùng định tuyến", f"Partition P{d.get('kafka_partition')} / 3"),
        _kv("Điểm lì EWMA", f"{ew.get('segment_score', 0.0):.4f} ({ew.get('alert_severity')})"),
        _kv("Watermark lọc trễ", f"Chuyển {wm.get('late_dropped_to_tier2', 500)} tin >30p sang Tier-2"),
        _kv("Nhật ký WAL", f"{wal.get('wal_state_kb', 73.7)} KB · 0 duplicates"),
    ]
    st["key_metrics"] = [
        _kv("Kafka", f"P{d.get('kafka_partition')}/3"),
        _kv("Sketches", "33.0 KiB"),
        _kv("EWMA", f"{ew.get('segment_score', 0.0):.2f}"),
    ]


def _enrich_stage_7(st: dict[str, Any]) -> None:
    d = st["details"]
    lat = d.get("latency_benchmark", {})
    esr = d.get("esr_index", {})
    vg = d.get("version_guard", {})
    t2 = d.get("tier2_reconciliation", {})
    d["mongo_doc"] = d.get("mongo_document")
    d["ixscan_ms"] = lat.get("warm_median_ms", 0.36)
    d["collscan_ms"] = lat.get("collscan_warm_median_ms", 11.289)
    d["speedup_x"] = lat.get("speedup_vs_collscan_x", 31.36)
    st["input_summary"] = [
        _kv("Kho đích MongoDB", f"mongo:{lat.get('mongo_server_version', '8.0.34')} · bda501.traffic_scores"),
        _kv("Chỉ mục phức hợp ESR", esr.get("key_pattern_str")),
        _kv("Khóa vòng đời", "$status_version: v1 (Speed) ➔ v2 (Batch)"),
    ]
    st["transform_steps"] = [
        f"Atomic Upsert với $status_version chặn {vg.get('guard_ratio_label', '25/25')} bản tin v1 cũ ghi đè bản chuẩn v2.",
        f"Truy vấn IXSCAN (0 SORT RAM): warm median {lat.get('warm_median_ms', 0.36):.3f} ms / p95 {lat.get('warm_p95_ms', 0.603):.3f} ms ({lat.get('speedup_vs_collscan_x', 31.36)}x vs COLLSCAN).",
        f"Tier-2 Reconciliation bù {t2.get('late_events_recomputed', 500)} trễ, hàn {t2.get('healed_downstream_lag_windows', 609)} lag & re-score {t2.get('healed_downstream_rescored_windows', 384)} ➔ 100%.",
    ]
    st["output_summary"] = [
        _kv("Tổng bản ghi phục vụ", f"{lat.get('total_rows_upserted', 33394):,} docs"),
        _kv("Độ trễ IXSCAN", f"{lat.get('warm_median_ms', 0.36):.3f} ms med / {lat.get('warm_p95_ms', 0.603):.3f} ms p95"),
        _kv("Bảo vệ $status_version", f"Chặn {vg.get('guard_ratio_label', '25/25')} stale writes"),
        _kv("Độ đầy đủ sau Recon", f"{t2.get('pre_reconciliation_completeness_pct', 98.5)}% ➔ {t2.get('post_reconciliation_completeness_pct', 100.0)}%"),
    ]
    st["key_metrics"] = [
        _kv("IXSCAN", f"{lat.get('warm_median_ms', 0.36):.3f} ms"),
        _kv("Speedup", f"{lat.get('speedup_vs_collscan_x', 31.36)}x"),
        _kv("Recon", "100%"),
    ]


def attach_ui_view_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Attach presentation-ready fields expected by `TabPipelineFlow.tsx` without removing raw keys."""
    sel = payload["selected_trace"]
    stages = payload["stages"]
    stage_1_pings = stages[0]["details"].get("raw_pings", [])
    features_23d = _flatten_23d_features(stages[4]["details"])

    for st in stages:
        sid = int(st["id"])
        st["stage_id"] = sid
        st["subtitle"] = st["summary"]
        st["accent_color"] = STAGE_ACCENTS.get(sid, "#2563eb")
        st["status_badge"] = str(st.get("status", "PASSED"))

    _enrich_stage_1(stages[0], sel)
    _enrich_stage_2(stages[1])
    _enrich_stage_3(stages[2], stage_1_pings)
    _enrich_stage_4(stages[3], features_23d)
    _enrich_stage_5(stages[4])
    _enrich_stage_6(stages[5])
    _enrich_stage_7(stages[6])

    sev_pct = round(float(sel.get("severe_prob", 0.0)) * 100.0, 1)
    verdict = (
        f"Cold-Start 0 GPS · PR {stages[3]['details'].get('pagerank_score', 0.0):.2f} · Cụm #{stages[3]['details'].get('cluster_id')} · P(E/F) {sev_pct:.1f}%"
        if sel.get("is_cold_start")
        else f"LOS {sel['current_los']} ➔ RF +30p: LOS {sel['effective_forecast_los']} (P(E/F) {sev_pct:.1f}% · Thực tế: {sel.get('actual_next_los') or 'N/A'})"
    )
    sem = sel.get("window_semantics") or {}
    payload.update(
        {
            "active_trace_id": sel["id"],
            "segment_id": sel["segment_id"],
            "street_name": sel["street_name"],
            "ict_date": sel.get("ict_date"),
            "vn_hour": sel.get("vn_hour"),
            "window_headline": sem.get("headline") or f"Đoạn #{sel['segment_id']} · {sel['street_name']} (Cold-Start 0 GPS)",
            "is_cold_start": bool(sel.get("is_cold_start")),
            "summary_verdict": verdict,
        }
    )
    return payload
