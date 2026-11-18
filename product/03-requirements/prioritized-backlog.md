# Danh Mục Công Việc Ưu Tiên (Prioritized Product Backlog — V3, Wave 2)

| Thứ tự Ưu tiên | Mã Hạng mục | Tên Hạng mục (`Backlog Item`) | Thành phần Kiến trúc | `DRI` Phụ trách | Trạng thái |
| :---: | :--- | :--- | :--- | :--- | :--- |
| **`P0`** | `BL-01` | Kiểm kê 5 bảng gốc, loại `427` phi đường bộ (`84,206` cơ giới), tỉa `525,786` (`90.97%`) nút phụ & phễu `R1–R6` | `[C2]`, `[C5]`, `[C15]` | Nguyễn Tạ Minh Quyền (`25ms23280`) | **Done** |
| **`P0`** | `BL-02` | Nội suy `3-Tier` học từ `TRAIN` hữu cơ, đối chiếu `BkTraffic urban 5 km/h LOS` (`89.19%`) & `Unicode NFC` (`695 → 641`) | `[C5]` | Nguyễn Tạ Minh Quyền (`25ms23280`) | **Done** |
| **`P0`** | `BL-03` | Tổng hợp bằng **Pure 5-Tuple Monoid** (`33,394` cửa sổ, `0` lệch `baseline.py`), `8` salt buckets & `Spark 3.5.7 BHJ` (`5 → 1` Exchange) | `[C9]`, `[C14]` | Nguyễn Tạ Minh Quyền (`25ms23280`) | **Done** |
| **`P0`** | `BL-04` | Dựng `Directed Line Graph` **bỏ U-turn** (`84,206` đỉnh, `92,453` cạnh, `314` dangling) & `PageRank` (`#1 21969 Nguyễn Kiệm 2.7799`) | `[C10]` | Nguyễn Thành Đạt (`25ms23306`) | **Done** |
| **`P0`** | `BL-05` | Khai phá trên `TRAIN` hữu cơ (`12` co-rules `Lift 15.23`, `14` lagged `Lift 24.0` `persistence_controlled=True`) & `7D K-Means` (`3,609` hữu cơ) | `[C10]`, `[C13b]` | Nguyễn Thành Đạt (`25ms23306`) | **Done** |
| **`P0`** | `BL-06` | Phát hiện/loại `7,192` burst (`3,596` cặp `TRAIN` $\to$ `1,885` hữu cơ), `RF 23D` sạch (`ROC-AUC=0.9900`, `PR-AUC=0.9595`, `McNemar p=0.015625`) & khóa `[C13a/b]` | `[C11]`, `[C13a]`, `[C13b]` | Nguyễn Thành Đạt (`25ms23306`) | **Done** |
| **`P0`** | `BL-07` | Giả lập `Event-Time Replay` (`producer.py`), `32 KiB RotatingBloom`, `1 KiB 64-bit HLL`, `EWMA` & `WAL 73.7 KB` | `[C1]`, `[C4]`, `[C6]`, `[C7]` | Phạm Công Minh (`25ms23310`) | **Done** |
| **`P0`** | `BL-08` | Chấm điểm `Phase B` (`489` `v1`) & `Phase C Recon` (`tier1=0`, bù `500` trễ, hàn gắn `609` hạ lưu, chấm lại `384`, đổi `185`) | `[C8]`, `[C12]` | Phạm Công Minh (`25ms23310`) | **Done** |
| **`P1`** | `BL-09` | Lakehouse (`bronze_raw_events`, `stream_replay_windows`, `curated_gold/year_month=*`) đọc sạch cả PyArrow/Spark & `MongoDB IXSCAN` (`0.389/0.692 ms`, `23.55x`) | `[C14]`, `[C16]` | Nguyễn Tạ Minh Quyền & Phạm Công Minh | **Done** |
| **`P1`** | `BL-10` | Triển khai bộ đôi **Web TOC `ReactJS + FastAPI (:8000)`** (`toc-web`) + **Dashboard `Streamlit (:8501)`** (`app`) & `live-consumer` | `[C17]`, `toc_backend/`, `toc_frontend/` | Phạm Công Minh & Nguyễn Thành Đạt | **Done** |
