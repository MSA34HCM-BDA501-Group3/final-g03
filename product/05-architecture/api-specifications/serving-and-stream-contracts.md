# Đặc Tả Hợp Đồng Dữ Liệu Luồng, Serving & REST API (`Stream, MongoDB & FastAPI Contracts`)

## 1. Schema Sự Kiện Luồng Kafka (`[C1] -> [C4] Topic: hcmc-traffic-events`)

```json
{
  "event_id": "a1b2c3d4e5f60123456789ab",
  "segment_id": 21969,
  "velocity": 14.5,
  "updated_at": "2020-12-24T03:30:12Z",
  "is_injected_duplicate": false,
  "lateness_bucket": "ON_TIME"
}
```
- **Khóa phân vùng (`Partition Key`):** `str(segment_id)` $\mapsto \text{murmur2}(\text{segment\_id}) \bmod 3$.
- **Miền giá trị `lateness_bucket` (Replay `33,727` cửa sổ / `33,394` gốc):** `ON_TIME`, `LATE_IN_WATERMARK` (`1,168` trong ngưỡng $\le 30\text{m}$), `LATE_OUT_OF_WATERMARK` (`500` quá hạn $> 30\text{m}$ tại `226` khung giờ), và `333` bản sao trùng lặp (`seed=42`).

---

## 2. Schema Tài Liệu Phục Vụ Trên `MongoDB 8.0` (`[C16] bda501.traffic_scores`)

```json
{
  "event_id": "0c3fd227d8342a0956762627",
  "segment_id": 7121,
  "street_name": "Lý Thái Tổ",
  "street_level": 2,
  "date": "2020-08-02",
  "period": "period_23_30",
  "vn_hour": 6.5,
  "current_los": "F",
  "pagerank_score": 0.7695183824592429,
  "cluster_id": 0,
  "predicted_next_los": "F",
  "severe_prob": 1.0,
  "write_partition": 1,
  "scored_in_online_phase_b": false,
  "evaluation_split": "TRAIN",
  "phase_b_status": "STREAM_PROVISIONAL",
  "lifecycle_status": "BATCH_VERIFIED",
  "status_version": 2,
  "model_version": "rf_full_23d_v3"
}
```

### Chỉ Mục & Atomic Version Guard (`Required Indexes & Write Contract`)
1. **Unique Primary Key Index (`event_id_unique`):** `{"event_id": 1}` (`unique=True`).
2. **Compound Operational Serving Index (`level_prob_segment_idx`):** `{"street_level": 1, "severe_prob": -1, "segment_id": 1}`.
3. **Atomic `$status_version` Pipeline Guard:** Lệnh `UpdateOne(..., upsert=True)` dùng update pipeline chỉ cho phép cập nhật khi `$gte: ["$incoming_version", {"$ifNull": ["$status_version", 0]}]`, bảo đảm `v1 STREAM_PROVISIONAL` đến muộn không bao giờ ghi đè `v2 BATCH_RECONCILED / BATCH_VERIFIED` (`25/25` stale `v1` bị từ chối trong `outputs/mongo_verify.json`).

### Mẫu Truy Vấn Chuẩn `UC-2` (`Arterial Severe Congestion Query`)
- **Filter:** `{"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}}` (`1,234` cửa sổ khớp).
- **Sort & Limit:** `[("severe_prob", -1), ("segment_id", 1)]`, `.limit(25)`.
- **Explain Plan Contract:** `winningPlan` sử dụng `IXSCAN` trên `level_prob_segment_idx` (`winning_stages = ["LIMIT", "PROJECTION_SIMPLE", "FETCH", "SORT_MERGE", "IXSCAN", "IXSCAN"]`), `in_memory_sort = false`, `warm_median = 0.360 ms`, `warm_p95 = 0.603 ms` (`31.36x` so với `COLLSCAN median = 11.289 ms`, `p95 = 12.217 ms`).

---

## 3. Hợp Đồng REST API Web TOC Hợp Nhất (`toc_backend/main.py` — `FastAPI :8000`)

| Method & Endpoint | Vai trò Nghiệp vụ | Tham số / Payload Chính |
|---|---|---|
| `GET /api/health` | Kiểm tra tình trạng `TOCDataStore` & số cửa sổ nạp | — |
| `GET /api/overview` | Trả `Top-25 EWMA Hotspots` & thống kê toàn mạng | — |
| `GET /api/scenarios` | Danh sách `4` kịch bản rush-hour thực tế (`noon_transition`, `cong_truong_quoc_te`, `morning_rush_val`, `mega_city_rush`) | — |
| `GET /api/frame` | Khung trạng thái thành phố theo thời điểm ICT & lọc cảnh báo | `ict_date`, `vn_hour`, `alert_filter` |
| `GET /api/segment/{segment_id}/timeline` | Chuỗi GPS thô `[t-30m, t)` và dự báo `[t, t+30p)` của 1 đoạn | `segment_id`, `ict_date`, `vn_hour` |
| `GET /api/graph-rules` | Top `PageRank` (`#1 21969 Nguyễn Kiệm`), `12` co-rules & `14` lagged rules | — |
| `GET /api/clusters-coldstart` | Hồ sơ `6` cụm Unified `K-Means` & tìm đoạn tương đồng cho `74,211` `Cold-Start` | `segment_id`, `limit` |
| `GET /api/infrastructure-audit` | Số liệu `metrics.json`, `Spark 3.5.7`, `MongoDB IXSCAN` & `Two-Tier Recon` | — |
| `GET /api/stream/status` | Telemetry trực tiếp `Kafka`, `RotatingBloomFilter`, `64-bit HLL++`, `MongoDB v1` | `limit` |
| `POST /api/stream/start` & `/api/stream/reset` | Kích hoạt phát Story/Stress-Test `200` tin qua Kafka hoặc reset sketch | `mode`, `scenario`, `max_messages`, `reset_first` |
