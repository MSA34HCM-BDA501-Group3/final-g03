# Báo Cáo Xác Thực Vấn Đề & Đánh Giá Gate 1 (Problem Validation Report — Gate 1)

## 1. Kết Quả Kiểm Định Dữ Liệu Thực Nghiệm Sơ Bộ (`Preliminary 5-Table EDA Evidence`)

Quá trình `Deepdive EDA` trên 5 bảng dữ liệu gốc trong `raw_data/` đã cung cấp bằng chứng định lượng xác thực hoàn toàn các giả thuyết bài toán:

| Bảng Dữ Liệu | Số Bản Ghi | Số Cột | Dung Lượng | Bằng Chứng Thực Nghiệm Xác Thực Giả Thuyết (`Empirical Validation Findings`) |
| :--- | ---: | ---: | ---: | :--- |
| `nodes.csv` | `577,967` | `3` | `19.44 MB` | `0%` khuyết. Tuy nhiên chỉ có **`52,425` nút (`9.07%`)** tham gia vào cạnh của `segments.csv`; **`525,542` nút (`90.93%`)** là đỉnh hình học mồ côi (`Orphan OSM Vertices` — xác thực **`EC-7`**). |
| `streets.csv` | `5,553` | `5` | `0.20 MB` | Khuyết **`82.23%` (`4,566` dòng)** `max_velocity` và **`28.90%` (`1,605` dòng)** `name` (xác thực **`EC-1`**). |
| `segments.csv` | `84,633` | `11` | `10.29 MB` | Khuyết **`88.34%` (`74,762` dòng)** `max_velocity`; có **`154` đoạn `length == 0m`** và **`427` đoạn mang nhãn dân dụng OSM** (`house`, `fuel`... — xác thực **`EC-1`, `EC-2`**). Tạo thành `Directed Line Graph` gồm `151,929` cạnh chuyển tiếp và **`48` `Dangling Nodes`** (`out_degree = 0`). |
| `segment_status.csv` | `90,938` | `4` | `3.58 MB` | Vận tốc tức thời `0..200 km/h` (trung vị `17.0 km/h`). Khi gom theo khung 30 phút **`UTC`** thì khớp **`100.00%` (`33,441 / 33,441`)** với `train.csv` (nếu dùng `UTC+7` chỉ khớp `0.53%` — xác thực **`EC-4`**). |
| `train.csv` | `33,441` | `18` | `4.99 MB` | Khuyết **`85.21%` (`28,495` dòng)** `max_velocity`. Chỉ bao phủ **`10,027` đoạn (`11.85%`)**, để lại **`74,606` đoạn (`88.15%`) `Cold-Start`**. Trung vị chỉ có **`2` quan sát/đoạn** (`79.26%` có $\le 2$ quan sát) và **`66.27%` cặp liên tiếp cách nhau $> 30$ phút** (xác thực **`EC-5`, `EC-6`**). |

---

## 2. Kiểm Chứng Bẫy Rò Rỉ Nhãn Nội Tập (`Intra-Train Target Leakage Validation`)

- **Hiện tượng phát hiện:** Khi dùng đặc trưng trung bình tĩnh `segment_train_prior_los` tính trên toàn bộ tập `TRAIN`, vì `79.26%` đoạn đường chỉ có $\le 2$ quan sát $(t_1, t_2)$, giá trị trung bình tại $t_1$ chứa chính nhãn mục tiêu tương lai $y_{t_2}$ (`target_next_los_ordinal`). Hệ số tương quan với nhãn mục tiêu trên tập `TRAIN` bị thổi phồng lên $r_{\text{train}} = 0.8256$ nhưng sụp đổ trên tập `TEST` ($r_{\text{test}} = 0.4347$), khiến mô hình `Random Forest (+Graph)` bị suy giảm `Severe F1` xuống chỉ còn `0.5758`.
- **Kết quả sau khi áp dụng `Causal Expanding Empirical-Bayes Priors` ($\tau \le t$):** `Random Forest (+Graph PageRank, Causal Priors & K-Means)` đạt **`Severe F1 = 0.6670`** và **`PR-AUC = 0.7568`**, vượt qua cả `Persistence Baseline` (`0.6609`) lẫn `Random Forest Local Only` (`0.6570`).

---

## 3. Quyết Định Cổng Kiểm Soát `Gate 1 — Problem Validation`

- [x] **Tiêu chí 1:** Có bằng chứng định lượng từ dữ liệu thực tế (`577,967` nút, `84,633` đoạn đường, `90,938` bản tin GPS, `33,441` cửa sổ `LOS`).
- [x] **Tiêu chí 2:** Xác định rõ 4 bài toán con (`P1`–`P4`), 6 kịch bản nghiệp vụ (`UC-1`–`UC-6`) và 10 trường hợp biên thực nghiệm (`EC-1`–`EC-10`).
- [x] **Tiêu chí 3:** Chứng minh các cách tiếp cận ngây thơ (`Naive Imputation`, `Static Prior`, `48D K-Means`, `Uncoordinated Lambda`) đều thất bại trên dữ liệu thực tế.
- **Kết luận `Gate 1`:** **PASSED (Đạt yêu cầu chuyển sang giai đoạn `Product Strategy & Architecture`).**
