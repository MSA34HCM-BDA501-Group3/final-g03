# ADR-002: Cơ Chế Đối Soát Hai Tầng & Ghi Đè Lũy Đẳng (Two-Tier Reconciliation & Idempotent Upsert)

- **Trạng thái (`Status`):** `Accepted & Implemented`
- **Bối cảnh (`Context`):** Khi xử lý luồng `GPS Telemetry` qua mạng di động, có `3.5%` (`1,170`) sự kiện đến trễ trong ngưỡng `Watermark = 30m` và `1.5%` (`501`) sự kiện đến trễ quá ngưỡng `Watermark` trên `319` cửa sổ. Nếu gộp chung kiểm định đúng đắn thuật toán (`Algorithmic Correctness`) với kiểm định độ đầy đủ dữ liệu (`Data Completeness`), người vận hành sẽ không phân biệt được sai lệch do lỗi code hay do sự kiện đến trễ sau khi cửa sổ đã đóng.
- **Quyết định (`Decision`):**
  1. **Dual-Sink Stream Router (`[C6]`):** Tách luồng thành `Sink 1` (lưu `100%` sự kiện thô vào `bronze_raw_events/`, không bị cắt bởi `Watermark`) và `Sink 2` (đóng cửa sổ tại `Watermark = 30m` để bảo vệ bộ nhớ `RAM`).
  2. **Tier 1 Audit (`In-Watermark Equivalence`):** Đối chiếu kết quả `Stream` với kết quả `Batch` tính trên cùng tập sự kiện trong `Watermark` (`32,940` sự kiện), yêu cầu `in_watermark_exact_match_pct == 100.0%` (`exceptAll == 0`).
  3. **Tier 2 Backfill (`Post-Watermark Healing`):** Sau giờ chốt, `[C12]` đọc `100%` `bronze_raw_events/`, phát hiện `319` cửa sổ có `dropped_late_events > 0`, tính lại `Monoid`, chữa lành `Online Lag Store` và thực hiện `Bulk UpdateOne({'event_id': eid}, {'$set': doc}, upsert=True)` lên `[C16]` `MongoDB 8.0` để nâng độ đầy đủ từ `98.50%` lên `100.0%`.
