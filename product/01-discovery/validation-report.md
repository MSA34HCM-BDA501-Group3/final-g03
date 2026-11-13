# Báo Cáo Xác Thực Vấn Đề & Đánh Giá Gate 1 (Problem Validation Report — Gate 1)

## 1. Kết Quả Kiểm Định Dữ Liệu Thực Nghiệm Sơ Bộ (`Preliminary 5-Table EDA Evidence`)

Quá trình `Deepdive EDA` trên 5 bảng dữ liệu gốc trong `raw_data/` đã cung cấp bằng chứng định lượng xác thực hoàn toàn các giả thuyết bài toán:

| Bảng Dữ Liệu | Số Bản Ghi | Số Cột | Dung Lượng | Bằng Chứng Thực Nghiệm Xác Thực Giả Thuyết (`Empirical Validation Findings`) |
| :--- | ---: | ---: | ---: | :--- |
| `nodes.csv` | `577,967` | `3` | `19.44 MB` | `0%` khuyết. Tuy nhiên chỉ có **`52,425` nút (`9.07%`)** là giao lộ hai đầu đoạn đường trong `segments.csv`; **`525,542` điểm (`90.93%`)** là điểm uốn cong trung gian vẽ hình học đường (`shape points` — xác thực **`EC-7`**). |
| `streets.csv` | `5,553` | `5` | `0.20 MB` | Khuyết **`82.23%` (`4,566` dòng)** `max_velocity` và **`28.90%` (`1,605` dòng)** `name` (xác thực **`EC-1`**). |
| `segments.csv` | `84,633` | `11` | `10.29 MB` | Khuyết **`88.34%` (`74,762` dòng)** `max_velocity`; có **`154` đoạn `length == 0m`** và **`427` đoạn mang nhãn dân dụng OSM** (`house`, `fuel`... — xác thực **`EC-1`, `EC-2`**). Tạo thành `Directed Line Graph` gồm `151,929` cạnh chuyển tiếp và **`48` `Dangling Nodes`** (`out_degree = 0`). |
| `segment_status.csv` | `90,938` | `4` | `3.58 MB` | Chứa **`90,938` bản tin GPS thô hợp lệ** (`status_dedup_dropped_pings = 0, status_rejected_pings = 0` trong `pipeline.py`; `151` bản tin trên `427` đoạn `S3_NON_ROAD_TYPE` bị lọc trong `spark_batch.py`). Khi gom theo khung 30 phút **`UTC`** bằng `5-Tuple Commutative Monoid` thì tạo ra đúng **`33,441` cửa sổ** (`33,394` sau lọc đường bộ — xác thực **`EC-4`, `Q01`, `Q03`**). |
| `train.csv` (Reference) | `33,441` | `18` | `4.99 MB` | Khuyết **`85.21%` (`28,495` dòng)** `max_velocity`. Chỉ bao phủ **`10,027` đoạn (`11.85%`)**, để lại **`74,606` đoạn (`88.15%`) `Cold-Start`**. Trung vị chỉ có **`2` quan sát/đoạn** (`79.26%` có $\le 2$ quan sát) và chỉ có **`7,898` cặp liên tiếp đúng `+30 phút`** (`66.27%` cặp quan sát cách nhau $> 30$ phút — xác thực **`EC-5`, `EC-6`, `D01`**). |

---

## 2. Kiểm Chứng Chân Trời `t + 30m` & Chống Rò Rỉ Nhãn (`Exact +30m Horizon & Anti-Leakage Validation`)

- **Hiện tượng phát hiện:** (1) Dịch dòng ngây thơ `shift(-1)` trên dữ liệu thưa gán nhãn qua các khoảng trống hàng giờ/hàng ngày (`66.27%` cặp không liên tiếp); (2) dùng trung bình tĩnh trên tập `TRAIN` gây `Intra-Train Target Leakage` trên các đoạn đường có $\le 2$ quan sát.
- **Kết quả sau khi áp dụng kết nối đúng `slot_ts + 30m`, chia `70/15/15` theo mốc thời gian kèm `Target Cutoff Purge` và `Causal Expanding Empirical-Bayes Priors` ($\tau < t$):** `Random Forest (+Graph PageRank, Causal Priors & K-Means, 23D)` được cổng `Validation Gate` chọn tự động (`VAL PR-AUC = 0.8704, VAL ROC-AUC = 0.9593, VAL 6-Class Accuracy = 0.9529`) và đạt trên tập `TEST`: **`Severe F1 = 0.9409`**, **`PR-AUC = 0.9471`**, **`ROC-AUC = 0.9882`**, **`6-Class Accuracy = 0.9450`**, và **`Macro-F1 = 0.5543`** (dự báo đúng khác 0 trên cả 6 lớp `LOS A–F`: `[944, 4, 3, 7, 6, 153]`), vượt qua `Persistence Baseline` (`PR-AUC = 0.9367, ROC-AUC = 0.9867`) và `Random Forest Local Only` (`ROC-AUC = 0.9866, Macro-F1 = 0.5455, VAL ROC-AUC = 0.9448`).

---

## 3. Quyết Định Cổng Kiểm Soát `Gate 1 — Problem Validation`

- [x] **Tiêu chí 1:** Có bằng chứng định lượng từ dữ liệu thực tế (`577,967` nút, `84,633` đoạn đường, `90,938` bản tin GPS thô, `33,441` cửa sổ `LOS`, `7,898` cặp đúng `+30m`).
- [x] **Tiêu chí 2:** Xác định rõ 4 bài toán con (`P1`–`P4`), 6 `Use Cases` nghiệp vụ (`UC-1`–`UC-6`) và 10 `Edge Cases` thực nghiệm (`EC-1`–`EC-10`).
- [x] **Tiêu chí 3:** Chứng minh các cách tiếp cận ngây thơ (`Naive shift(-1)`, `Static Prior`, `48D K-Means`, `Uncoordinated Lambda`) đều thất bại hoặc gây rò rỉ trên dữ liệu thực tế.
- [x] **Tiêu chí 4:** Định vị rõ bộ dữ liệu công khai `~38.5 MB` đóng vai trò **Mẫu chuẩn kiểm chứng quy mô (`Proof-of-Scale Benchmark`)** cho bài toán toàn TP.HCM (`1.5–3.0 TB/ngày`), đồng thời ghi nhận giới hạn độ hỗ trợ nhỏ (`5–8` cửa sổ) của các Association Rules trễ `+30m`.
- **Kết luận `Gate 1`:** **PASSED (Đạt yêu cầu chuyển sang giai đoạn `Product Strategy & Architecture`).**
