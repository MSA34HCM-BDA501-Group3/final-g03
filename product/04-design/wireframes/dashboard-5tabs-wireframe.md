# Thiết Kế Khung Giao Diện 5 Tab Điều Hành (`Web TOC :8000` & `Streamlit :8501`)

```text
+---------------------------------------------------------------------------------------------------+
| 🚦 HCMC Smart Traffic Platform (V3 Wave 2 — Zones A–F · [C1]–[C17] · Web TOC :8000 & Streamlit :8501) |
+-------------------+-------------------+-------------------+-------------------+-------------------+
| Active / Raw Nodes| Clean Motorized   | Accepted GPS Pings| 30m Gold Windows  | Severe E/F PR-AUC |
| 52,181 / 577,967  | 84,206 (92,453 E) | 90,787 / 90,938   | 33,394 (7,885 +30)| 0.9595 (F1=0.9301)|
+-------------------+-------------------+-------------------+-------------------+-------------------+
| Web TOC (:8000):  [01 Điều Hành +30p] [02 Live Kafka & 33KiB] [03 PageRank & Rules] [04 KMeans] [05 Spark/Audit]
| Streamlit (:8501):[Tab 1: Bản đồ & Hotspots] [Tab 2: Dự báo & MongoDB] [Tab 3: PageRank & Rules]  |
|                   [Tab 4: 6 Nhóm & Cold-Start] [Tab 5: Luồng 33 KiB, Đối soát & Spark/Mongo Audit] |
+---------------------------------------------------------------------------------------------------+
| CONTENT AREA:                                                                                     |
| - Web TOC Clock Bar (4 Rush-Hour Scenarios, 🎬 Story Replay, ⚡ Stress-Test 200, Leaflet Map)     |
| - KPI Cards (STREAM_PROVISIONAL v1 vs BATCH_RECONCILED v2 vs BATCH_VERIFIED v2, Velocity Ratio)   |
| - Analytical Tables & Charts (ROC Curve, 6x6 Confusion Matrix, Ablation, Recon & IXSCAN Explain)  |
+---------------------------------------------------------------------------------------------------+
```

## 1. Bố Cục 5 Tab Trên `Web TOC` (`ReactJS + TypeScript + FastAPI :8000`, dịch vụ `toc-web`)
1. **`01 · Điều Hành & Dự Báo +30p (UC-1, UC-2)`:** Bản đồ Leaflet động TP.HCM, thanh Đồng hồ TOC `[t-30m, t) ➔ t ➔ [t, t+30p)`, biểu đồ GPS thô $\to$ dự báo `t+30p`, danh sách cảnh báo chuyển pha và `Top-25 EWMA Hotspots` (`#1 47407 Lý Thường Kiệt = 6.6652`).
2. **`02 · Live Kafka & Sketches 33 KiB (P4)`:** Telemetry trực tiếp từ `Kafka 4.1 KRaft` (`3` partitions), bộ đếm **`32.0 KiB RotatingBloom` (`2 × 16 KiB`)** & **`1.0 KiB 64-bit HLL`**, bảng bản ghi `STREAM_PROVISIONAL (v1)` vừa ghi vào `MongoDB 8.0`.
3. **`03 · PageRank & Luật Đồng Kẹt (UC-3, UC-4)`:** Top nút cổ chai `PageRank` không U-turn (`#1 21969 Nguyễn Kiệm PR=2.7799`), `12` luật đồng kẹt (`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.23`) & `14` luật trễ (`Lê Quang Định → Trần Quang Khải`, `Lift = 24.0`, `persistence_controlled=True`).
4. **`04 · 6 Cụm K-Means & Cold-Start (UC-5)`:** Phân bổ `6` cụm trên `84,206` đoạn cơ giới (`9,995` có GPS + `74,211` Cold-Start `88.13%`) và công cụ tìm đoạn đường tương đồng.
5. **`05 · Spark 3.5.7, Ablation & Đối Soát (UC-6)`:** Bằng chứng `Spark 3.5.7 BroadcastHashJoin` (`5 → 1` Exchange, `145.24 → 120.45 ms`), bảng `Ablation` 7 cấu hình sạch, `Two-Tier Reconciliation` (`tier1 = 0`, bù `500` trễ, hàn gắn `609`, chấm lại `384`, đổi `185`), `WAL = 73.7 KB` & `MongoDB IXSCAN` (`0.389 / 0.692 ms`).

## 2. Bố Cục 5 Tab Trên `Streamlit Gateway` (`app.py :8501`, dịch vụ `app`)
1. **`Tab 1` (`Bản đồ Điểm nóng & Dữ liệu — UC-1, UC-6`):** Bản đồ TP.HCM (`map_hcmc_hotspots_coldstart.png`), `Top-25 EWMA Hotspots` (`hotspot_alerts.csv`), bảng kiểm kê 5 bảng gốc (`file_inventory.csv`), bảng quy tắc kiểm định (`quarantine_summary.csv` — `R4 = 2,247` cửa sổ), và sơ đồ kiến trúc (`outputs/system_architecture.png`).
2. **`Tab 2` (`Dự báo +30 Phút & MongoDB — UC-2, P3`):** Dự báo đơn điểm (`#21064 Công Trường Quốc Tế`), truy vấn trục chính `MongoDB 8.0 IXSCAN` (`1,202` cửa sổ, `warm_p95 = 0.692 ms`), phân bố vòng đời (`32,405` `BATCH_VERIFIED v2` / `500` `BATCH_RECONCILED v2` / `489` `STREAM_PROVISIONAL v1`), `Live Kafka Monitor`, `ablation_results.csv`, `transition_evaluation.csv` và `roc_curve.png`.
3. **`Tab 3` (`Nút cổ chai & Cặp đường hay kẹt — UC-3, UC-4`):** `Top-50 PageRank` (`road_pagerank_top.csv`), `12` co-congestion rules (`co_congestion_rules.csv`) và `14` lagged rules (`lagged_congestion_rules.csv`).
4. **`Tab 4` (`6 Nhóm Đường & Vùng thiếu GPS — UC-5`):** Bản đồ `6` cụm (`map_hcmc_clusters_7d.png`), hồ sơ `6` cụm trên `3,609` đoạn `TRAIN` hữu cơ (`Silhouette = 0.2197, DBI = 1.3592`) và công cụ tìm `Top-K` đoạn tương đồng phủ đủ `84,206` đoạn cơ giới (`74,211` `Cold-Start`).
5. **`Tab 5` (`Luồng 33 KiB & Đối soát tin trễ — UC-6, P4`):** Chỉ số `32.0 KiB Bloom` (`32,768 B`, `FPR = 0.611%`), `1.0 KiB HLL` (`9,620.2 / 9,995`, sai số `3.75%`), bảng `reconciliation_audit.csv` (`tier1_exceptAll_diff = 0`, `500` trễ, `609` hàn gắn), `join_plan_evidence.json`, `spark_runtime_evidence.json`, `mongo_verify.json` và `query_progress.json` (`WAL = 73.7 KB`).
