# Danh Mục Công Việc Ưu Tiên (Prioritized Product Backlog — Leak-Free Ground Truth)

| Thứ tự Ưu tiên | Mã Hạng mục | Tên Hạng mục (`Backlog Item`) | Thành phần Kiến trúc | `DRI` Phụ trách | Trạng thái |
| :---: | :--- | :--- | :--- | :--- | :--- |
| **`P0`** | `BL-01` | Kiểm kê 5 bảng gốc, loại `427` phi đường bộ (`84,206` cơ giới), tỉa `525,786` (`90.97%`) nút phụ & phễu `R1–R6` | `[C2]`, `[C5]`, `[C15]` | Nguyễn Tạ Minh Quyền (`25ms23280`) | **Done** |
| **`P0`** | `BL-02` | Nội suy `3-Tier` không rò rỉ học từ `TRAIN` hữu cơ (`4,946/7,650/19,601/1,197`), đối chiếu `BkTraffic urban 5 km/h LOS` (`89.19%`) & `Unicode NFC` (`695 → 641`) | `[C5]` | Nguyễn Tạ Minh Quyền (`25ms23280`) | **Done** |
| **`P0`** | `BL-03` | Tổng hợp bằng **Pure 5-Tuple Monoid** (`33,394` cửa sổ, `0` lệch `baseline.py`), `8` salt buckets & `Spark 3.5.7 BHJ` (`5 → 1` Exchange, `25.76%` speedup) | `[C9]`, `[C14]` | Nguyễn Tạ Minh Quyền (`25ms23280`) | **Done** |
| **`P0`** | `BL-04` | Dựng `Directed Line Graph` **bỏ U-turn** (`84,206` đỉnh, `92,453` cạnh, `314` dangling) & `PageRank` (`#1 21969 Nguyễn Kiệm 2.7799`) | `[C10]` | Nguyễn Thành Đạt (`25ms23306`) | **Done** |
| **`P0`** | `BL-05` | Khai phá trên `TRAIN` hữu cơ (`12` co-rules `Lift 15.23`, `14` lagged `Lift 24.0` `persistence_controlled=True`) & Unified `7D K-Means` (`3,609` hữu cơ, `Silhouette = 0.2241`) | `[C10]`, `[C13b]` | Nguyễn Thành Đạt (`25ms23306`) | **Done** |
| **`P0`** | `BL-06` | Phát hiện/loại `7,192` burst (`3,596` cặp `TRAIN` $\to$ `1,885` hữu cơ), `RF 23D` sạch (`ROC-AUC = 0.9894`, `PR-AUC = 0.9543`, `McNemar p = 0.001953`) & khóa `[C13a/b]` (`a2f411846508`) | `[C11]`, `[C13a]`, `[C13b]` | Nguyễn Thành Đạt (`25ms23306`) | **Done** |
| **`P0`** | `BL-07` | Giả lập `Event-Time Replay` (`producer.py`), `32 KiB RotatingBloomFilter` (`0.0389%` FPR), `1 KiB 64-bit HLL++`, `EWMA` & `WAL 73.7 KB` | `[C1]`, `[C4]`, `[C6]`, `[C7]` | Phạm Công Minh (`25ms23310`) | **Done** |
| **`P0`** | `BL-08` | Chấm điểm `Phase B` (`489` `v1`) & `Phase C Recon` (`tier1 = 0`, bù `500` trễ, hàn gắn `609` hạ lưu, chấm lại `384`, đổi `213`) | `[C8]`, `[C12]` | Phạm Công Minh (`25ms23310`) | **Done** |
| **`P1`** | `BL-09` | Lakehouse (`bronze_raw_events`, `stream_replay_windows`, `curated_gold/year_month=*`) đọc sạch cả PyArrow/Spark & `MongoDB IXSCAN` (`0.360/0.603 ms`, `31.36x`, `1,234` alerts) | `[C14]`, `[C16]` | Nguyễn Tạ Minh Quyền & Phạm Công Minh | **Done** |
| **`P1`** | `BL-10` | Hợp nhất toàn bộ giao diện vận hành vào **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`, dịch vụ `toc-web`)** + `live-consumer` (`5` dịch vụ Docker mặc định = `11.0 GB RAM`) | `[C17]`, `toc_backend/`, `toc_frontend/` | Phạm Công Minh & Nguyễn Thành Đạt | **Done** |
