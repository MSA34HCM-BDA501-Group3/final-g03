# ADR-001: Vòng Đời MLOps 3 Pha Không Xung Đột (Zero-Conflict 3-Phase MLOps Lifecycle)

- **Trạng thái (`Status`):** `Accepted & Implemented`
- **Bối cảnh (`Context`):** Trong các hệ thống kết hợp `Batch Layer` và `Speed Layer`, nếu luồng trực tuyến tự gọi `.fit()` (cho `Imputer`, `StandardScaler`, `KMeans` hoặc `RandomForest`) trên từng lô nhỏ (`micro-batch`), tham số chuẩn hóa và ranh giới phân lớp sẽ thay đổi liên tục theo từng cửa sổ 5 giây và xung đột với mô hình được huấn luyện trên toàn bộ lịch sử ở `Batch Layer`.
- **Quyết định (`Decision`):** Áp dụng hợp đồng **3 Pha Tách Biệt Tuyệt Đối (`Zero-Conflict Contract`)**:
  1. **Phase A (`Offline Batch Fit`, $t \le T_{\text{train}}$):** Chỉ duy nhất `[C11]` trong `Zone D` được phép gọi `.fit()` trên tập `70% TRAIN` và xuất các hiện vật bất biến sang `Zone E` (`[C13a]` `outputs/model.joblib` & `outputs/model_registry_manifest.json`, `[C13b]` `data/lakehouse/segment_static_features.parquet`).
  2. **Phase B (`Online Speed Scoring`, $t > T_{\text{train}}$):** `[C8]` trong `Zone C` nạp `[C13a]` và `[C13b]` ở chế độ **chỉ đọc (`read-only`)**, chỉ gọi `.transform()` / `.predict_proba()` và gắn nhãn `lifecycle_status = "STREAM_PROVISIONAL"`.
  3. **Phase C (`Post-Cutoff Reconciliation`, $t > T_{\text{cutoff}}$):** `[C12]` nạp lại đúng cùng phiên bản `[C13a]` và `[C13b]` ở chế độ chỉ đọc để chấm điểm lại `319` cửa sổ có sự kiện đến trễ quá `Watermark`, ghi đè trạng thái `BATCH_RECONCILED` vào `[C16]` `MongoDB 8.0`.
- **Hệ quả (`Consequences`):** Đảm bảo đồng nhất thuật toán `100.0%` giữa `Stream` và `Batch` đối với mọi cửa sổ không có bản tin trễ ngoài `Watermark`, đồng thời cho phép kiểm toán minh bạch bằng `SHA-256 Model Digest`.
