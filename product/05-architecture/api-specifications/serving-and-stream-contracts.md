# Đặc Tả Hợp Đồng Dữ Liệu Luồng & Truy Vấn Phục Vụ (Stream & Serving API Contracts)

## 1. Schema Sự Kiện Luồng Kafka (`[C1] -> [C4] Topic: hcmc-traffic-events`)

```json
{
  "event_id": "a1b2c3d4e5f60123456789ab",
  "segment_id": 61781,
  "velocity": 14.5,
  "updated_at": "2020-12-24T03:30:12Z",
  "is_injected_duplicate": false,
  "lateness_bucket": "ON_TIME"
}
```
- **Khóa phân vùng (`Partition Key`):** `str(segment_id)` $\mapsto \text{murmur2}(\text{segment\_id}) \bmod 3$.
- **Miền giá trị `lateness_bucket`:** `ON_TIME` (`95.0%`), `LATE_IN_WATERMARK` (`3.5%`, độ trễ $\le 30\text{m}$), `LATE_OUT_OF_WATERMARK` (`1.5%`, độ trễ $> 30\text{m}$).

---

## 2. Schema Tài Liệu Phục Vụ Trên `MongoDB 8.0` (`[C16] bda501.traffic_scores`)

```json
{
  "event_id": "8f9d2a1b4c5e6f708192a3b4",
  "segment_id": 34723,
  "street_name": "Quốc lộ 1",
  "street_level": 1,
  "date": "2021-01-04",
  "period": "period_2_00",
  "vn_hour": 9.0,
  "current_los": "E",
  "predicted_next_los": "F",
  "severe_prob": 0.8642,
  "pagerank_score": 2.6430,
  "cluster_id": 3,
  "lifecycle_status": "BATCH_RECONCILED",
  "model_version": "rf_full_23d_v3",
  "scored_at": "2026-11-08T11:15:00Z"
}
```

### Chỉ Mục Bắt Buộc (`Required Indexes`)
1. **Unique Primary Key Index (`event_id_unique`):** `{"event_id": 1}` (`unique=True`).
2. **Compound Operational Serving Index (`level_prob_segment_idx`):** `{"street_level": 1, "severe_prob": -1, "segment_id": 1}`.

### Mẫu Truy Vấn Chuẩn `UC-2` (`Arterial Severe Congestion Query`)
- **Filter:** `{"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}}`
- **Sort:** `[("severe_prob", -1), ("segment_id", 1)]`
- **Limit:** `25`
- **Hợp đồng Kế hoạch Thực thi (`Explain Plan Contract`):** `winningPlan` bắt buộc sử dụng `IXSCAN` trên `level_prob_segment_idx` với `in_memory_sort = False` (`0` bước `SORT` trong RAM).
