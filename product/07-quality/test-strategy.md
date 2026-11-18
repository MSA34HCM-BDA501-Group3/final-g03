# Chiến Lược Kiểm Thử (Test Strategy — Leak-Free Verification)

## 1. Phân tầng Kiểm thử & Bất biến Dữ liệu (Invariants)
1. **Unit & Structural Invariants:**
   - **Topology & Cleaning:** Kiểm tra `84,633 -> 84,206` đoạn đường cơ giới sạch (`427` non-road loại, `580` GIS quarantine: `154 R1 + 426 R2`); đồ thị cắt tỉa từ `577,967` xuống `52,181` nút hoạt động (loại `525,786 = 90.97%`), `92,453` cạnh đồ thị đường (không U-turn), `314` nút cụt.
   - **Unicode NFC & ICT Timezone:** `2,247` cửa sổ chuẩn hóa NFC (`695 -> 641`, gộp `54` biến thể); `4,305` cửa sổ lật ngày ICT (`12.89%` của `33,394`).
   - **Pure 5-Tuple Monoid:** Đảm bảo tính kết hợp `(cnt, sum_v, min_v, max_v, severe_cnt)` trên mảng phân mảnh khớp `100.0%` (`0` sai lệch, `0.0 km/h` diff) và lỗi trung bình của trung bình `11.111111` trên `[1,2,3]` bị triệt tiêu hoàn toàn.
2. **Data Integrity & Leakage Guards:**
   - **Leak-Free Vmax Imputation & Burst Exclusion:** Nội suy `max_velocity` học thuần trên `TRAIN` hữu cơ (`4,946 / 7,650 / 19,601 / 1,197` cửa sổ; `9,869 / 472 / 54,259 / 19,606` đoạn); phát hiện `7,192` cửa sổ synthetic burst (`2020-08-02 23:30` & `2020-08-03 00:00`) và loại `3,596` cặp burst khỏi tập huấn luyện có giám sát, giữ `1,885` cặp `TRAIN` hữu cơ (`50` cặp xuyên biên đã purge), `1,167` `VAL` (`5` purge), `1,182` `TEST` (`7,885` cặp `+30m` chính xác).
   - **Clean Training Policy:** Kiểm tra không có `sample_weight` nhân tạo hay fallback override; chỉ dùng `class_weight` exponent `0.65` và `RF 23D` thắng `VAL` gate (`(val_accuracy_6class + val_pr_auc + val_roc_auc)/3 = 0.8882` vs `DT 23D = 0.8812`).
3. **Streaming, Spark & Database Contracts:**
   - **Bounded Sketches:** `RotatingBloomFilter` `32.0 KiB` (`2 x 16 KiB`, bắt `333/333` bản ghi trùng, `13` FPs, `0.0389%` empirical vs `0.0792%` theoretical) + `64-bit HLL++` `1.0 KiB` (`9,620.2 / 9,995`, sai số `3.75%`) + `WAL = 73.7 KB`.
   - **Two-Tier Reconciliation:** `tier1_exceptAll_diff = 0` (`100.0%`), Tier-2 bù `500` cửa sổ trễ / `226` khung giờ, chữa lành `609` cửa sổ lag, chấm lại `384` (`213` đổi xác suất).
   - **Spark 3.5.7 & MongoDB 8.0:** Catalyst `SortMergeJoin (5 Exchange, 166.18 ms) -> BroadcastHashJoin (1 Exchange, 123.37 ms, 25.76% speedup, 0 exceptAll diff)`; Monoid `97.92 ms` vs salted `185.78 ms`; Spark MLlib organic (`FPGrowth 32`, `KMeans 3,609`, `RF 1,885` đạt `test_acc = 0.9416, roc_auc = 0.9903`); MongoDB `33,394` docs (`1,234` arterial alerts), atomic `$status_version` guard từ chối `25/25` bản tin `v1` cũ, `IXSCAN 0.360/0.603 ms` (`31.36x` vs `COLLSCAN 11.289 ms`).

## 2. Hiện trạng Bộ Test Tự động trong `tests/` (`Gate 3 PASS`)
- Toàn bộ bộ kiểm thử tự động `pytest` (`tests/test_platform_v3.py` và `tests/test_quyen_spark.py`) bao phủ đầy đủ các kiểm định đơn vị, tiêm lỗi (`failure injection`), kiểm chứng Lakehouse/Spark/MongoDB và kiểm thử REST API của ứng dụng web hợp nhất **ReactJS + FastAPI (`:8000`)**.
- **Trạng thái hiện tại (`Gate 3 PASS`):** Toàn bộ assertion trong `tests/`, bằng chứng trên đĩa (`outputs/`), tài liệu quản trị (`product/`) và hồ sơ kiểm định Session 9 (`evidence/session9/`) đã được đồng bộ hoàn toàn với chuẩn thực nghiệm sạch không rò rỉ (`33,394`, `84,206`, `92,453`, `609`, `213`, `0.9894`, `0.9543`, `0.360 / 0.603 ms`).
