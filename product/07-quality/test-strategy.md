# Chiến Lược Kiểm Thử (Test Strategy — Wave 2 Verification)

## 1. Phân tầng Kiểm thử & Bất biến Dữ liệu (Invariants)
1. **Unit & Structural Invariants:**
   - **Topology & Cleaning:** Kiểm tra `84,633 -> 84,206` đoạn đường cơ giới sạch (`427` non-road loại, `580` GIS quarantine: `154 R1 + 426 R2`); đồ thị cắt tỉa từ `577,967` xuống `52,181` nút hoạt động (loại `525,786 = 90.97%`), `92,453` cạnh đồ thị đường (không U-turn), `314` nút cụt.
   - **Unicode NFC & ICT Timezone:** `2,247` cửa sổ chuẩn hóa NFC (`695 -> 641`, gộp `54` biến thể); `4,305` cửa sổ lật ngày ICT (`12.89%` của `33,394`).
   - **Pure 5-Tuple Monoid:** Đảm bảo tính kết hợp `(n, s1, s2, min_v, max_v)` trên mảng phân mảnh khớp `100.0%` (`0` sai lệch, `0.0 km/h` diff) và lỗi trung bình của trung bình `11.111111` trên `[1,2,3]` bị triệt tiêu hoàn toàn.
2. **Data Integrity & Leakage Guards:**
   - **Burst Audit & Exclusion:** Phát hiện `7,192` cửa sổ synthetic burst (`2020-08-02 23:30` & `2020-08-03 00:00`) và loại `3,596` cặp burst khỏi tập huấn luyện có giám sát, giữ `1,885` cặp `TRAIN` hữu cơ (`50` cặp xuyên biên đã purge), `1,167` `VAL` (`5` purge), `1,182` `TEST` (`7,885` cặp `+30m` chính xác).
   - **Clean Training Policy:** Kiểm tra không có `sample_weight` nhân tạo hay fallback override; chỉ dùng `class_weight` exponent `0.65` và `RF 23D` thắng `VAL` gate (`0.8990` vs `0.5343`).
3. **Streaming, Spark & Database Contracts:**
   - **Bounded Sketches:** `RotatingBloom` `32 KiB` (`2 x 16 KiB`, bắt `333/333` bản ghi trùng với `0.6109%` FPR) + `64-bit HLL` `1 KiB` (`9,620.2 / 9,995`, sai số `3.75%`) + `WAL = 73.7 KB`.
   - **Two-Tier Reconciliation:** `tier1_exceptAll_diff = 0` (`100.0%`), Tier-2 bù `500` cửa sổ trễ / `226` khung giờ, chữa lành `609` cửa sổ lag, chấm lại `384` (`185` đổi nhãn/xác suất).
   - **Spark 3.5.7 & MongoDB 8.0:** Catalyst `SortMergeJoin (5 Exchange, 145.24 ms) -> BroadcastHashJoin (1 Exchange, 120.45 ms, 17.07% speedup, 0 exceptAll diff)`; Monoid `147.11 ms` vs salted `222.60 ms`; MongoDB `33,394` docs, atomic `$status_version` guard từ chối `25/25` bản tin `v1` cũ, `IXSCAN 0.389/0.692 ms` (`23.55x` vs `COLLSCAN`).

## 2. Hiện trạng Bộ Test Tự động trong `tests/`
- `tests/test_platform_v3.py` (`23` hàm) và `tests/test_quyen_spark.py` (`5` phương thức) bao phủ các kiểm định tích hợp.
- **Trạng thái hiện tại:** Bộ assertion trong `tests/test_platform_v3.py` được viết theo các ngưỡng Wave 1 cũ (như `33,441` dòng, `46` NFC entities) nên đang chờ track cập nhật chuyên biệt để khớp với baseline Wave 2 (`33,394`, `84,206`, `92,453`, `609`, v.v.).
