# Thiết Kế Khung Giao Diện 5 Tab Điều Hành (5-Tab Dashboard Wireframe Specification)

```text
+---------------------------------------------------------------------------------------------------+
| 🚦 HCMC Smart Traffic Big Data Platform (V3 — Zones A–F · [C1]–[C17] · Zero-Conflict MLOps)       |
+-------------------+-------------------+-------------------+-------------------+-------------------+
| Active / Raw Nodes| Directed Segments | GPS Velocity Pings| 30m Gold Windows  | Severe E/F PR-AUC |
| 52,425 / 577,967  | 84,633 (151,929 E)| 90,938 GPS Pings  | 33,441 (7,898 +30)| 0.9471 (F1=0.9409)|
+-------------------+-------------------+-------------------+-------------------+-------------------+
| [Tab 1: Overview & Hotspots] [Tab 2: Forecast & IXSCAN] [Tab 3: PageRank & Rules]                 |
| [Tab 4: 7D K-Means & Peer Search] [Tab 5: O(1) Sketches, Recon & Execution Evidence]              |
+---------------------------------------------------------------------------------------------------+
| TAB CONTENT AREA:                                                                                 |
| - Interactive Controls (Segment / Corridor Selector, Cold-Start Toggle, Similarity K-Slider)      |
| - KPI Metric Cards (Real-time STREAM_PROVISIONAL vs Reconciled Status, Velocity Ratio, PageRank)  |
| - Analytical Dataframes & Visual Charts (ROC Curve, Confusion Matrix, Quarantine & Recon Tables)  |
+---------------------------------------------------------------------------------------------------+
```

## Bố Cục Chi Tiết Từng Tab (`[C17] Streamlit Gateway :8501`)

1. **`Tab 1` (`Overview, Quality Funnel & Decaying Hotspots — UC-1, UC-6`):**
   - Hiển thị sơ đồ kiến trúc `Zone A–F` (`system_architecture_v4.png`), bảng kiểm kê 5 bảng gốc (`file_inventory.csv`), bảng phân loại quy tắc lỗi đầu tiên (`quarantine_summary.csv` — bao gồm `R4_UNICODE_NFD_AND_PREFIX_DRIFT = 2,071` cửa sổ), và bảng cảnh báo `Top-25 Exponentially Decaying Hotspots` (`hotspot_alerts.csv`).
2. **`Tab 2` (`Live Congestion Forecast, Ablation & MongoDB IXSCAN Serving — UC-2, P3`):**
   - Cho phép lọc theo tuyến đường (bao gồm cả tuyến thưa 1 quan sát) và chọn cửa sổ dự báo đúng chân trời `t + 30m`.
   - Hiển thị bảng truy vấn trục đường chính (`street_level IN [1,2]`, $\hat{p}_{\text{severe}} \ge 0.70$) từ `[C16]` `MongoDB 8.0` kèm phân bố vòng đời đầy đủ (`STREAM_PROVISIONAL`, `BATCH_RECONCILED`, `BATCH_VERIFIED`), bảng `Ablation Study` (`ablation_results.csv`), đường cong `ROC` (`roc_curve.png`) và `model_registry_manifest.json`.
3. **`Tab 3` (`Directed Line-Graph PageRank & SON/FP-Growth Rules — UC-3, UC-4`):**
   - Hiển thị `Top-50 Structural Bottlenecks` (`road_pagerank_top.csv`), `Top-30 Co-Congestion Association Rules` hai chiều (`co_congestion_rules.csv`), và `Top-20 Lagged Street Propagation Rules` (`lagged_congestion_rules.csv`).
4. **`Tab 4` (`Train-Only 7D K-Means Regimes & Cold-Start Peer Search — UC-5`):**
   - Hiển thị hồ sơ `6` cụm hành vi `7D K-Means` (`Silhouette = 0.2853`, `Davies-Bouldin = 1.1181` trên `6,926` đoạn `TRAIN`), và công cụ tìm `Top-K` đoạn đường tương đồng trên toàn bộ `84,633` đoạn đường (hỗ trợ cả `74,606` đoạn vùng mù cảm biến `Cold-Start` từ `[C13b]` `segment_static_features.parquet`).
5. **`Tab 5` (`O(1) Stream Sketches, Two-Tier Reconciliation & Execution Evidence — UC-6, P4`):**
   - Hiển thị chỉ số `32.0 KiB Bit-Packed Bloom Filter` (`32,768 B`), `1.0 KiB HyperLogLog++` (`1,024 B`), bảng kiểm toán `reconciliation_audit.csv`, bằng chứng `Catalyst BroadcastHashJoin` (`join_plan_evidence.json`), bằng chứng thực thi trực tiếp trên `PySpark 3.5.7` (`spark_runtime_evidence.json`), `MongoDB 8.0 IXSCAN Explain` (`mongo_verify.json`) và `WAL Recovery` (`query_progress.json`).
