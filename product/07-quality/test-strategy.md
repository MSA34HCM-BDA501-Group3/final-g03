# Chiến Lược Kiểm Thử & Đảm Bảo Chất Lượng (Test Strategy — Phase 07 QA)

## 1. Bốn Tầng Kiểm Thử Tự Động (`4-Layer Verification Pyramid`)

1. **Unit & Algebraic Property Tests (`Kiểm thử Đại số & Đơn vị`):**
   - Kiểm chứng tính giao hoán và kết hợp của vị nhóm `Commutative Monoid` $\mathbf{m} = (\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt})$ và `2-Stage Key Salting` (`[C9]`) so với `baseline.py` (`0` sai lệch).
   - Kiểm chứng hàm chuẩn hóa `Unicode NFC` (`_normalize_street_name`) khử sạch `100%` lỗi mã hóa `NFD/NFC`, `Ð/Đ` và nhãn hành chính `Phường/Quận` (`EC-3`).
2. **Data Quality & Anti-Leakage Invariant Tests (`Kiểm thử Bất biến Dữ liệu & Chống Rò rỉ`):**
   - Kiểm chứng cắt tỉa `525,542` đỉnh OSM mồ côi, giữ đúng `52,425` nút giao hoạt động (`EC-7`).
   - Kiểm chứng `3-Tier Hierarchical Velocity Imputation` điền khuyết `100%` `max_velocity` (`0` `NaN` còn sót — `EC-1`).
   - Kiểm chứng phân chia thời gian nghiêm ngặt `70/15/15` và xác nhận `Causal Expanding Priors` tại thời điểm $t$ không thay đổi khi hoán vị hoặc xóa dữ liệu ở thời điểm $t' > t$ (`EC-5`).
3. **Stream Sketching & Reconciliation Integration Tests (`Kiểm thử Tích hợp Luồng & Đối soát`):**
   - Kiểm chứng `32 KB Bloom Filter` bắt `100%` (`334 / 334`) bản ghi trùng lặp với `FPR < 3%` (`EC-8`).
   - Kiểm chứng `1 KB HyperLogLog++` ước lượng số đoạn đường hoạt động với sai số tương đối `< 5%`.
   - Kiểm chứng `Two-Tier Reconciliation` đạt `100.0%` khớp trong `Watermark` (`Tier 1`) và nâng độ đầy đủ từ `98.50%` lên `100.0%` sau khi bù `501` sự kiện trễ trên `319` cửa sổ (`Tier 2 — EC-9`).
4. **End-to-End Serving & Database Execution Plan Tests (`Kiểm thử Phục vụ & MongoDB IXSCAN`):**
   - Kiểm chứng `[C16]` `MongoDB 8.0` thực thi `Partitioned Bulk UpdateOne(upsert=True)` lũy đẳng (`EC-10`) và kế hoạch truy vấn `UC-2` sử dụng `Compound Index IXSCAN` với `in_memory_sort == False`.
