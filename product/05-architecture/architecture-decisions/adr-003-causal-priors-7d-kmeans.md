# ADR-003: Không Gian Hành Vi 7 Chiều & Tiên Nghiệm Nhân Quả Mở Rộng (7D K-Means & Causal Priors)

- **Trạng thái (`Status`):** `Accepted & Implemented`
- **Bối cảnh (`Context`):** Trong dữ liệu giao thông TP.HCM, trung vị mỗi đoạn đường chỉ có `2` quan sát trong 10 tháng (`79.26%` đoạn đường trong tập `TRAIN` có $\le 2$ quan sát).
  1. Nếu gom cụm `K-Means` trên vector 48 khung giờ trong ngày ($\mathbb{R}^{48}$), ma trận đặc trưng sẽ bị rỗng tới `95.07%` số ô.
  2. Nếu tính trung bình tĩnh `mean(los_ordinal)` trên toàn bộ tập `TRAIN` làm đặc trưng cho mỗi đoạn đường, ở các đoạn đường có 2 quan sát $(t_1, t_2)$, giá trị trung bình tại $t_1$ chứa chính nhãn mục tiêu tương lai $y_{t_2}$ (`Intra-Train Target Leakage`), làm mô hình học vẹt trên `TRAIN` ($r=0.8256$) nhưng sụp đổ trên `TEST` (`Severe F1 = 0.5758`).
- **Quyết định (`Decision`):**
  1. Thay thế ma trận $\mathbb{R}^{48}$ bằng **Không gian Hành vi 7 Chiều (`7D Behavioral Space`)** gồm: `mean_los_ordinal`, `severe_rate`, `mean_velocity`, `velocity_ratio`, `length_m`, `max_velocity`, `pagerank_score`, huấn luyện `K-Means` ($k=6$) chỉ trên tập `TRAIN` (`Silhouette = 0.3158`, `Davies-Bouldin = 1.1462`).
  2. Thay thế trung bình tĩnh bằng **`Causal Expanding Empirical-Bayes Priors`** chỉ cộng dồn các quan sát quá khứ $\tau \le t$ với hằng số làm trơn $m_{\text{seg}}=5$ và $m_{\text{street}}=10$.
- **Hệ quả (`Consequences`):** Khôi phục hoàn toàn tính tổng quát hóa trên tập kiểm thử tương lai (`15% TEST`), nâng `Severe F1` của `Random Forest (+Graph PageRank, Causal Priors & K-Means)` từ `0.5758` lên **`0.6670`** và đạt **`PR-AUC = 0.7568`**.
