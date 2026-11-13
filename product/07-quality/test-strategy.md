# Chiến Lược Kiểm Thử & Đảm Bảo Chất Lượng (Test Strategy — Phase 07 QA)

## 1. Bốn Tầng Kiểm Thử Tự Động (`4-Layer Verification Pyramid`)

1. **Unit & Algebraic Property Tests (`Kiểm thử Đại số & Đơn vị`):**
   - Kiểm chứng tính giao hoán và kết hợp của đại số `5-Tuple Commutative Monoid` $\mathbf{m} = (\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt})$ trên phân hoạch không đều `[1, 2, 3]` (chứng minh bẫy sai số `Average-of-Averages = 11.11 km/h`) và `2-Stage Key Salting` (`[C9]`) so với `baseline.py` (`0` sai lệch).
   - Kiểm chứng hàm chuẩn hóa `Unicode NFC` (`_normalize_street_name`) khử sạch `100%` lỗi mã hóa `NFD/NFC`, `Ð/Đ`, tiền tố `Đường ` và tiền tố/hậu tố hành chính `Phường/Quận` trên `2,071` cửa sổ (`EC-3`).
2. **Data Quality & Anti-Leakage Invariant Tests (`Kiểm thử Bất biến Dữ liệu & Chống Rò rỉ`):**
   - Kiểm chứng lọc bỏ `525,542` điểm uốn cong trung gian (`shape points`) từ OSM, giữ đúng `52,425` giao lộ hoạt động (`EC-7`).
   - Kiểm chứng `3-Tier Train-Only Velocity Imputation` (`outputs/vmax_lookup.json`) điền khuyết `100%` `max_velocity` (`0` `NaN` còn sót) và bất biến khi nhiễu tập `VAL/TEST` (`EC-1`, `Q04`).
   - Kiểm chứng kết nối đúng chân trời `t+30m` (loại bỏ khoảng trống `60m`/`9h`), phân chia theo mốc thời gian `70/15/15` kèm `Target Cutoff Purge` (`max(train.target_ts) <= train_cutoff`), và xác nhận `Causal Expanding Priors` + `7D K-Means` tại thời điểm $t$ không thay đổi khi nhiễu dữ liệu tương lai $t' > t$ (`EC-5`, `D01–D04`).
   - Kiểm chứng `Validation Gate` tự động chọn `Random Forest (+Graph PageRank, Causal Priors & K-Means, 23D)` và `6x6 Confusion Matrix` có đường chéo `> 0` trên toàn bộ 6 mức `LOS A..F` (`D09`).
3. **Stream Sketching, WAL Recovery & Reconciliation Tests (`Kiểm thử Tích hợp Luồng, Phục hồi WAL & Đối soát`):**
   - Kiểm chứng `32.0 KiB Bit-Packed Bloom Filter` (`uint8[32768]`) bắt `100%` (`334 / 334`) bản ghi trùng lặp với `FPR = 0.00616 < 3%` (`EC-8`, `M05`).
   - Kiểm chứng `1.0 KiB HyperLogLog++` (`uint8[1024]`) ước lượng số đoạn đường hoạt động với sai số tương đối `3.98% < 5%` và cắt tỉa trạng thái suy giảm mũ dưới $\epsilon = 10^{-4}$.
   - Kiểm chứng `Two-Tier Reconciliation` đạt `100.0%` khớp trong `Watermark` (`Tier 1 exceptAll = 0`, kèm bài kiểm thử tiêm lỗi `Failure Injection` khi đột biến 1 dòng) và nâng độ đầy đủ từ `98.50%` lên `100.0%` sau khi bù `501` sự kiện trễ trên `219` khung giờ và chấm điểm lại `377` cửa sổ hạ lưu (`Tier 2 — EC-9`, `M10`).
   - Kiểm chứng `WAL Checkpoint Kill/Restart Recovery` (`0` hiệu ứng phụ trùng lặp), `Version Guard` (`status_version`: v2 chặn mọi bản tin v1 đến muộn), và `verify_frozen_registry_integrity()` (`SHA-256` trước và sau khi chấm điểm trực tuyến).
4. **End-to-End Serving & Database Execution Plan Tests (`Kiểm thử Phục vụ & MongoDB IXSCAN`):**
   - Kiểm chứng `[C16]` `MongoDB 8.0` thực thi `Idempotent Partitioned Bulk UpdateOne(upsert=True)` (`33,441` bản ghi qua `69` lô) và kế hoạch truy vấn `UC-2` sử dụng `Compound Index IXSCAN` (`level_prob_segment_idx`) với `in_memory_sort == False`, đồng thời trả về trạng thái trung thực `status = "not_verified"` khi container MongoDB bị ngắt (`M07–M09`, `T04`).
