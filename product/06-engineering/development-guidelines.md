# Quy Chuẩn Phát Triển & Kỹ Thuật Mã Nguồn (Development Guidelines)

## 1. Tiêu Chuẩn Chất Lượng Mã Nguồn (`Coding Standards`)

1. **Giới hạn Độ phức tạp Nhận thức (`Cognitive Complexity < 15`):** Mọi hàm và phương thức trong `pipeline.py`, `app.py`, `producer.py`, `starter_stream.py`, `spark_engine.py`, `spark_batch.py`, `baseline.py`, `traffic_data/` và `tests/` bắt buộc có `Cognitive Complexity < 15` (`max-complexity=14`). Chia nhỏ logic phức tạp thành các hàm thuần khiết (`pure helper functions`) dễ kiểm thử.
2. **Kiểm tra Tĩnh & Định dạng Tự động (`Automated Linting`):** Chạy `ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"` trước khi đóng bất kỳ hạng mục công việc nào; đảm bảo `0` lỗi cú pháp, kiểu dữ liệu, độ phức tạp hàm hoặc biến dư thừa.
3. **Tính Tái lập Tất định (`Deterministic Reproducibility`):**
   - Khóa sự kiện cửa sổ `event_id` sinh từ hàm băm tất định `SHA256(segment_id|date|period)[:24]`, và khóa bản tin thô `raw_event_id` sinh từ `SHA256(raw_id|segment_id|event_ts|velocity)[:24]`.
   - Mọi bộ sinh số ngẫu nhiên (`np.random.default_rng(42)`, `KMeans(random_state=42)`, `RandomForestClassifier(random_state=42, n_jobs=1)`) đều cố định `seed = 42`.
4. **Quy tắc Ngôn ngữ Tài liệu & Giao diện:** Giữ nguyên các thuật ngữ chuyên ngành bằng **Tiếng Anh** (`Batch Layer`, `Speed Layer`, `Watermark`, `Commutative Monoid`, `Directed Line-Graph PageRank`, `SON / FPGrowth`, `7D Behavioral K-Means`, `Causal Expanding Empirical-Bayes Priors`, `Bit-Packed Bloom Filter`, `HyperLogLog++`, `Two-Tier Reconciliation`, `Idempotent Upsert`, `Compound Index IXSCAN`) và trình bày phần giải thích bằng **Tiếng Việt** rõ ràng.
