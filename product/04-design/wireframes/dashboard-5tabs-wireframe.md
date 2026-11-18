# Thiết Kế Khung Giao Diện 5 Tab Điều Hành (`ReactJS + FastAPI TOC Command Center — :8000`)

```text
+---------------------------------------------------------------------------------------------------+
| 🚦 HCMC Smart Traffic TOC Command Center (Zones A–F · [C1]–[C17] · ReactJS + FastAPI :8000)       |
+-------------------+-------------------+-------------------+-------------------+-------------------+
| Active / Raw Nodes| Clean Motorized   | Accepted GPS Pings| 30m Gold Windows  | Severe E/F PR-AUC |
| 52,181 / 577,967  | 84,206 (92,453 E) | 90,787 / 90,938   | 33,394 (7,885 +30)| 0.9543 (F1=0.9362)|
+-------------------+-------------------+-------------------+-------------------+-------------------+
| Unified Web TOC (:8000, toc-web = toc_backend/main.py + toc_frontend/dist/):                      |
| [01 Điều Hành +30p] [02 Live Kafka & 33KiB] [03 PageRank & Rules] [04 KMeans] [05 Spark & Audit]  |
+---------------------------------------------------------------------------------------------------+
| CONTENT AREA:                                                                                     |
| - TopClockBar (4 Rush-Hour Scenarios, 🎬 Story Replay, ⚡ Stress-Test 200, Reset, Live Telemetry) |
| - Interactive Leaflet Map + ForecastTimelineChart ([t-30m, t) ➔ Chốt tại t ➔ Dự báo [t, t+30p))   |
| - KPI Cards (STREAM_PROVISIONAL v1 vs BATCH_RECONCILED v2 vs BATCH_VERIFIED v2, Velocity Ratio)   |
| - Analytical Tables & Charts (Ablation 7 Models, Quarantine, Recon, Spark BHJ & IXSCAN Explain)   |
+---------------------------------------------------------------------------------------------------+
```

## 1. Bố Cục 5 Tab Trên Ứng Dụng Web Hợp Nhất (`ReactJS + TypeScript + FastAPI :8000`, dịch vụ `toc-web`)

1. **`01 · Điều Hành & Dự Báo +30p (UC-1, UC-2)` (`TabCommandCenter.tsx`):**
   - Bản đồ Leaflet động TP.HCM (`InteractiveTrafficMap.tsx`), thanh Đồng hồ TOC (`TopClockBar.tsx`) với 4 kịch bản thực tế (`noon_transition`, `cong_truong_quoc_te`, `morning_rush_val`, `mega_city_rush`), biểu đồ chuỗi GPS thô $\to$ dự báo `t+30p` (`ForecastTimelineChart.tsx`), danh sách cảnh báo 6 nhóm và `Top-25 EWMA Hotspots` (`#1 47407 Lý Thường Kiệt = 6.6652`).
2. **`02 · Live Kafka & Sketches 33 KiB (P4)` (`TabLiveKafka.tsx`):**
   - Telemetry trực tiếp từ `Kafka 4.1 KRaft` (`3` partitions), bộ đếm **`32.0 KiB RotatingBloomFilter` (`2 × 16 KiB`, `333/333` trùng, `13` FPs, `0.0389%` FPR thực nghiệm vs `0.0792%` lý thuyết)** & **`1.0 KiB 64-bit HLL++` (`9,620.2 / 9,995`, sai số `3.75%`)**, bảng bản ghi `STREAM_PROVISIONAL (v1)` vừa ghi vào `MongoDB 8.0`, cùng điều khiển `🎬 Phát Story qua Kafka` / `⚡ Stress-Test 200 tin`.
3. **`03 · PageRank & Luật Đồng Kẹt (UC-3, UC-4)` (`TabGraphRules.tsx`):**
   - `Top-50 PageRank` không U-turn (`#1 21969 Nguyễn Kiệm PR = 2.7799`, `#2 37345 Cầu vượt Ngã tư Gò Dưa PR = 2.7196`), `12` luật đồng kẹt (`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.23`) & `14` luật trễ (`Lê Quang Định → Trần Quang Khải`, `Lift = 24.0`, `persistence_controlled = True`), hỗ trợ click nhảy trực tiếp về bản đồ Tab 1.
4. **`04 · 6 Cụm K-Means & Cold-Start (UC-5)` (`TabClustersColdStart.tsx`):**
   - Hồ sơ `6` cụm Unified `7D K-Means` (`3,609` đoạn `TRAIN` hữu cơ: `Silhouette = 0.2241, DBI = 1.3284, CH = 806.49`, kích thước cụm `846 / 924 / 708 / 136 / 840 / 155`) phủ đủ `84,206` đoạn cơ giới (`9,995` có GPS + `74,211` Cold-Start `88.13%`) và công cụ tìm `Top-K` đoạn đường tương đồng (thử nghiệm mặc định đoạn Cold-Start `#61781`).
5. **`05 · Spark 3.5.7, Ablation & Đối Soát (UC-6)` (`TabInfraAudit.tsx`):**
   - Bằng chứng `Spark 3.5.7 BroadcastHashJoin` (`5 → 1` Exchange, `166.18 → 123.37 ms`, `25.76%` speedup, `exceptAll = 0`), Monoid `97.92 vs 185.78 ms`, Spark MLlib organic (`32 / 3,609 / 1,885`), bảng `Ablation` 7 cấu hình sạch (`RF 23D ROC-AUC = 0.9894`, `PR-AUC = 0.9543`, `Acc = 0.9255`, `Severe F1 = 0.9362`), `Two-Tier Reconciliation` (`tier1 = 0`, bù `500` trễ, hàn gắn `609`, chấm lại `384`, đổi `213`), `WAL = 73.7 KB`, `MongoDB IXSCAN` (`0.360 / 0.603 ms`, `31.36x`, `1,234` alerts), bảng `Quarantine R1–R6` và đối chiếu Session 9 `Card B1–B3`.
