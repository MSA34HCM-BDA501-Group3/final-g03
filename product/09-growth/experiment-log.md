# Nhật Ký Thực Nghiệm & Đối Chứng Học Thuật (Experiment Log & Ablation Record)

## 1. Thí Nghiệm 1: Kết Nối Đúng Chân Trời `t+30m`, Khử Rò Rỉ Nhãn & Chống Sụp Đổ Lớp Trung Gian (`LOS C, D, E`)

- **Giả thuyết (`Hypothesis`):** (1) Phép dịch dòng `shift(-1)` gán nhãn qua khoảng trống thời gian tùy ý (`66.27%` cặp cách nhau $>30$ phút) làm sai lệch bài toán dự báo `t+30m`; (2) tính trung bình tĩnh trên tập `TRAIN` gây `Intra-Train Target Leakage`; (3) dữ liệu thưa ở các lớp trung gian `LOS C, D, E` dễ gây sụp đổ lớp (`Recall = 0`) nếu không cân bằng trọng số mẫu theo trạng thái chuyển tiếp ổn định trên tập `TRAIN`.
- **Kết quả Thực nghiệm trên `7,898` Cặp Đúng `+30m` (`5,494 TRAIN / 1,167 VAL / 1,182 TEST`):**
  - Cổng `Validation Gate` (`D09`) tự động chọn **`Random Forest (+Graph PageRank, Causal Priors & K-Means, 23D)`** với **`VAL PR-AUC = 0.8704`** và **`VAL ROC-AUC = 0.9593`** (`VAL 6-Class Accuracy = 0.9529`, vượt qua `Random Forest Local Only` với `VAL PR-AUC = 0.8574, VAL ROC-AUC = 0.9448`).
  - Trên tập `TEST` (`1,182` cửa sổ), `Random Forest (+Graph PageRank, Causal Priors & K-Means, 23D)` đạt **`ROC-AUC = 0.9882`**, **`PR-AUC = 0.9471`**, **`Severe F1 = 0.9409`**, **`6-Class Accuracy = 0.9450`**, và **`Macro-F1 = 0.5543`** với đường chéo 6-Class Confusion Matrix đạt `[944, 4, 3, 7, 6, 153]` (toàn bộ 6 lớp `LOS A..F` đều có `Recall > 0`).
  - Đối với mô hình nhị phân chuyên biệt `GBT Binary Alerting (+Graph PageRank, Causal Priors & K-Means, 23D)` (`task_type = BINARY_SEVERE_EF_ONLY`), bổ sung đặc trưng đồ thị và cụm giúp nâng `Severe F1` từ `0.7840` (`GBT Local Only`) lên **`0.9465`** (`ROC-AUC = 0.9843, PR-AUC = 0.9419`).
  - *(Đối chứng giao thức cũ `shift(-1)` trên `3,513` dòng test kế tiếp bất kỳ: `legacy_severe_f1 = 0.6447, legacy_pr_auc = 0.7628, legacy_roc_auc = 0.8484`).*

---

## 2. Thí Nghiệm 2: Chuẩn Hóa `Unicode NFC` Trong Khai Phá Luật Đồng Kẹt Xe (`2-Pass SON / FP-Growth`)

- **Giả thuyết (`Hypothesis`):** Dữ liệu OSM thô chứa các tên đường trùng nhau nhưng khác bảng mã Unicode (`NFD` vs. `NFC`), ký tự `Ð` vs. `Đ`, tiền tố `Đường ` hoặc hậu tố hành chính (`, Phường ...`, `, Quận ...`), làm phát sinh các luật tự suy diễn giả tạo.
- **Kết quả Thực nghiệm:**
  - Trước khi chuẩn hóa (`Raw Street Names`): Xuất hiện `695` tên đường thô với các biến thể lỗi và nhãn hành chính `Phường Hiệp Bình Chánh`.
  - Sau khi áp dụng `[C5]` `Canonical Unicode NFC & Regex Street Hygiene` (`R4_UNICODE_NFD_AND_PREFIX_DRIFT` chuẩn hóa `2,071` cửa sổ): Gộp sạch `46` thực thể trùng về **`649` tuyến đường chuẩn**; `30 / 30` luật đồng kẹt xe hai chiều và `20 / 20` luật lan truyền trễ `t -> t+30m` đạt `0%` tự suy diễn (`Lift` cao nhất đạt **`28.5833x`** cho `Lý Chính Thắng <=> Nguyễn Thông`).

---

## 3. Thí Nghiệm 3: Tối Ưu Hóa Kế Hoạch Kết Nối & Chỉ Mục Phục Vụ (`BroadcastHashJoin` & `MongoDB IXSCAN`)

- **Kết quả `Catalyst Join Plan` (`Pillar I2` trên `PySpark 3.5.7`):** Ép `BroadcastHashJoin` cho các bảng chiều tĩnh (`segments`, `streets`) giúp giảm số bước trao đổi mạng (`Exchange` stages) từ **`10` xuống `2`**, loại bỏ hoàn toàn `4` phép `SortMergeJoin` nặng và chứng minh `exceptAll == 0` giữa DataFrame API và Spark SQL.
- **Kết quả `MongoDB 8.0.34 Explain Plan` (`Pillar I3` trên `33,441` bản ghi):** Chỉ mục ghép `{street_level: 1, severe_prob: -1, segment_id: 1}` (`level_prob_segment_idx`) phục vụ trực tiếp cả phép lọc `{"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.7}}` lẫn phép sắp xếp `severe_prob DESC, segment_id ASC` qua `winning_stages = ["LIMIT", "PROJECTION_SIMPLE", "FETCH", "SORT_MERGE", "IXSCAN", "IXSCAN"]` với `in_memory_sort = False` (`0` bước `SORT` chặn trong RAM, `warm_median_ms = 0.530 ms`, `warm_p95_ms = 0.636 ms`).

---

## 4. Thí Nghiệm 4: Giải Mã Nghịch Lý Quán Tính Của `Baseline (Giữ Nguyên Hiện Tại)` Khi Giao Thông Đổi Trạng Thái (`LOS(t) ≠ LOS(t+30m)`)

- **Giả thuyết (`Hypothesis`):** Trên toàn bộ `1,182` mẫu `TEST`, `959` mẫu (`81.13%`) thuộc `LOS A` và `1,121` cặp (`94.84%`) giữ nguyên trạng thái cũ khiến `Baseline (Giữ nguyên hiện tại)` đạt `0.9484` `6-Class Accuracy` nhưng sẽ thất bại hoàn toàn khi giao thông chuyển trạng thái, khi tín hiệu GPS ngắt quãng hoặc trên các đoạn đường thiếu GPS.
- **Kết quả Thực nghiệm (`outputs/transition_evaluation.csv`):**
  - Trên **`61` ca thực sự đổi trạng thái (`LOS(t) ≠ LOS(t+30m)`)**: `Baseline` đoán sai `100%` (`0/61`, `Acc = 0.00%`, `ROC-AUC = 0.5000`, `Severe PR-AUC = 0.5408`, `Brier = 0.3279`); mô hình triển khai `Random Forest (23 biến)` đạt **`ROC-AUC phát hiện đổi trạng thái = 0.7664`**, đoán đúng `2/61` (`3.28%`) ở ngưỡng mặc định `argmax` và nâng lên **`9/61` (`14.75%`)** nhờ ngưỡng xác suất `P(đổi trạng thái) ≥ 0.30` chọn trên tập `VAL` (`VAL F1 = 0.5060`), xếp đúng lớp mới cao nhất tới **`24/61` (`39.34%`)**, nâng `Severe PR-AUC` lên **`0.5708` (`+3.00%`)** và giảm `10.1%` sai số `Brier` (`0.2948`).
  - Trên **`993` mẫu đang thoáng (`8` ca bất ngờ chuyển sang kẹt cứng `LOS E/F` sau 30 phút)**: Dù quy tắc cắt ngưỡng cứng đạt `0/8` do dữ liệu tập huấn luyện mất cân bằng mạnh, xác suất cảnh báo của `Random Forest (23 biến)` đạt **`ROC-AUC = 0.8136`** và nâng điểm rủi ro trung bình trên 8 ca sắp kẹt **cao gấp `9.35` lần** ca thông thoáng ổn định (`3.46%` so với `0.37%`).
  - Trên **`3,513` cặp tín hiệu GPS ngắt quãng `> 30 phút`** và **`74,606` đoạn đường thiếu GPS**: `Random Forest (23 biến)` đạt `PR-AUC = 0.7082` (so với `0.6369` của `Baseline`) và độ phủ `100.0%` (so với `0.0%` của `Baseline`).
