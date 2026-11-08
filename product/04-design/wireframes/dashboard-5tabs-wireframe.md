# Thiết Kế Khung Giao Diện 5 Tab Điều Hành (5-Tab Dashboard Wireframe Specification)

```text
+---------------------------------------------------------------------------------------------------+
| 🚦 HCMC Smart Traffic Big Data Platform (V3 — Zones A–F · [C1]–[C17] · Zero-Conflict MLOps)       |
+-------------------+-------------------+-------------------+-------------------+-------------------+
| Active / Raw Nodes| Directed Segments | GPS Velocity Pings| 30m Gold Windows  | Severe E/F PR-AUC |
| 52,425 / 577,967  | 84,633 (151,929 E)| 90,938 (100% UTC) | 33,441 (10,027 S) | 0.7568 (F1=0.6670)|
+-------------------+-------------------+-------------------+-------------------+-------------------+
| [Tab 1: Overview & Hotspots] [Tab 2: Forecast & IXSCAN] [Tab 3: PageRank & Rules]                 |
| [Tab 4: 7D K-Means & ML Ablation] [Tab 5: O(1) Sketches, Recon & Execution Evidence]              |
+---------------------------------------------------------------------------------------------------+
| TAB CONTENT AREA:                                                                                 |
| - Interactive Controls (Segment / Corridor Selector, Cold-Start Toggle, Similarity K-Slider)      |
| - KPI Metric Cards (Real-time vs Reconciled Status, Velocity Ratio, PageRank, Cluster Archetype)  |
| - Analytical Dataframes & Visual Charts (ROC Curve, Confusion Matrix, Quarantine & Recon Tables)  |
+---------------------------------------------------------------------------------------------------+
```

## Bố Cục Chi Tiết Từng Tab (`[C17] Streamlit Gateway :8501`)

1. **`Tab 1` (`Overview, Quality Funnel & Decaying Hotspots — UC-1, UC-6`):**
   - Hiển thị sơ đồ kiến trúc `Zone A–F` (`system_architecture_v4.png`), bảng kiểm kê 5 bảng gốc (`file_inventory.csv`), bảng phân loại quy tắc lỗi đầu tiên (`quarantine_summary.csv`), và bảng cảnh báo `Top-25 Exponentially Decaying Hotspots` (`hotspot_alerts.csv`).
2. **`Tab 2` (`Live Congestion Forecast & MongoDB IXSCAN Serving — UC-2, UC-3`):**
   - Cho phép chọn bất kỳ cửa sổ lịch sử nào trong `33,441` cửa sổ HOẶC tra cứu suy diễn `Cold-Start` trên toàn bộ `84,633` đoạn đường từ `[C13b]` `segment_static_features.parquet`.
   - Hiển thị bảng truy vấn trục đường chính (`street_level IN [1,2]`, $\hat{p}_{\text{severe}} \ge 0.70$) từ `[C16]` `MongoDB 8.0` kèm nhãn `STREAM_PROVISIONAL` / `BATCH_RECONCILED`.
3. **`Tab 3` (`Directed Line-Graph PageRank & SON/FP-Growth Rules — UC-3, UC-4`):**
   - Hiển thị `Top-50 Structural Bottlenecks` (`road_pagerank_top.csv`), thống kê vùng mù cảm biến (`88.15%` = `74,606` đoạn `Cold-Start`), và `Top-30 Co-Congestion Association Rules` (`co_congestion_rules.csv`).
4. **`Tab 4` (`Train-Only 7D K-Means Regimes & Leak-Free ML Ablation — UC-5, P3`):**
   - Hiển thị hồ sơ `6` cụm hành vi `7D K-Means`, công cụ tìm `Top-K` đoạn đường tương đồng, bảng `Ablation Study` (`ablation_results.csv`), đường cong `ROC` (`roc_curve.png`) và `6-Class Confusion Matrix`.
5. **`Tab 5` (`O(1) Stream Sketches, Two-Tier Reconciliation & Execution Evidence — UC-6, P4`):**
   - Hiển thị chỉ số `32KB Bloom Filter`, `1KB HyperLogLog++`, bảng kiểm toán `reconciliation_audit.csv`, bằng chứng `Monoid vs. Baseline` (`monoid_baseline_verification.json`), `Catalyst BroadcastHashJoin` (`join_plan_evidence.json`) và `MongoDB 8.0 IXSCAN Explain` (`mongo_verify.json`).
