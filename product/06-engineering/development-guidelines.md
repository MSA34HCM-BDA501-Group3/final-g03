# Quy Chuẩn Phát Triển & Kỹ Thuật Mã Nguồn (Development Guidelines)

## 1. Tiêu Chuẩn Chất Lượng Mã Nguồn (`Coding Standards`)

1. **Giới hạn Độ phức tạp Nhận thức (`Cognitive Complexity < 15`):** Mọi hàm và phương thức trong `pipeline.py`, `app.py`, `producer.py`, `baseline.py` và `tests/` bắt buộc có `Cognitive Complexity < 15`. Chia nhỏ logic phức tạp thành các hàm thuần khiết (`pure helper functions`) dễ kiểm thử.
2. **Kiểm tra Tĩnh & Định dạng Tự động (`Automated Linting`):** Chạy `ruff check .` trước khi đóng bất kỳ hạng mục công việc nào; đảm bảo `0` lỗi cú pháp, kiểu dữ liệu hoặc biến dư thừa.
3. **Tính Tái lập Tất định (`Deterministic Reproducibility`):**
   - Khóa sự kiện `event_id` sinh từ hàm băm tất định `SHA256(segment_id|date|period)[:24]`.
   - Mọi bộ sinh số ngẫu nhiên (`np.random.default_rng(42)`, `KMeans(random_state=42)`, `RandomForestClassifier(random_state=42)`) đều cố định `seed = 42`.
4. **Quy tắc Ngôn ngữ Tài liệu & Giao diện:** Giữ nguyên các thuật ngữ chuyên ngành bằng **Tiếng Anh** (`Batch Layer`, `Speed Layer`, `Watermark`, `Commutative Monoid`, `Directed Line-Graph PageRank`, `SON / FPGrowth`, `7D Behavioral K-Means`, `Causal Expanding Empirical-Bayes Priors`, `Bloom Filter`, `HyperLogLog++`, `Two-Tier Reconciliation`, `Idempotent Upsert`, `Compound Index IXSCAN`) và trình bày phần giải thích bằng **Tiếng Việt** rõ ràng.
