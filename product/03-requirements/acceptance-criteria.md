# Tiêu Chí Nghiệm Thu Hệ Thống (Acceptance Criteria — Gate 3 Checklist)

## 1. Tiêu Chí Nghiệm Thu Cho 4 Bài Toán Con (`P1`–`P4`)

- **`AC-P1` (`Descriptive Windowing & Quality Funnel`):**
  - [x] Khớp `100.0%` (`33,441 / 33,441`) cửa sổ 30 phút theo giờ `UTC` giữa `segment_status.csv` (`90,938` bản tin) và `train.csv`.
  - [x] `3-Tier Hierarchical Velocity Imputation` điền khuyết `100.0%` cho `28,495` cửa sổ (`85.21%`) và `74,762` đoạn đường (`88.34%`) thiếu `max_velocity`.
  - [x] Phép gộp `Commutative Monoid` $\mathbf{m} = (\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt})$ khớp tuyệt đối (`0` sai lệch) so với trình kiểm chứng độc lập `baseline.py`.
- **`AC-P2` (`Diagnostic Graph, Rule & Cluster Mining`):**
  - [x] Lọc bỏ chính xác `525,542` điểm uốn cong trung gian (`90.93%`), giữ `52,425` nút giao hoạt động; dựng `Directed Line Graph` gồm `84,633` đỉnh đoạn đường, `151,929` cạnh chuyển tiếp và `48` `Dangling Nodes`; tổng điểm `PageRank` bảo toàn $\sum_{s=1}^{84{,}633} \text{PR}(s) = 84{,}633.0$.
  - [x] Khai phá `30` luật đồng kẹt xe `SON / FPGrowth` trên tên đường đã chuẩn hóa `Unicode NFC` với `0` luật tự suy diễn (`antecedent != consequent`).
  - [x] Phân cụm `Train-Only 7D Behavioral K-Means` ($k=6$) trên `6,926` đoạn đường tập `TRAIN` (timestamp-purged) đạt `Silhouette = 0.2853` và `Davies-Bouldin = 1.1181`.
- **`AC-P3` (`Predictive Leak-Free Causal ML`):**
  - [x] Chia tập theo thứ tự thời gian nghiêm ngặt theo `slot_ts` trên `7,898` cặp chuyển tiếp `t+30m` chính xác: `70% TRAIN (5,494, purged 50) / 15% VAL (1,167, purged 5) / 15% TEST (1,182)`.
  - [x] `Causal Expanding Empirical-Bayes Priors` chỉ sử dụng các quan sát $\tau < t$; mô hình `Random Forest (+Graph PageRank, Causal Priors & K-Means, 23D)` đạt trên `TEST`: **`Severe F1 = 0.9409`**, **`Severe PR-AUC = 0.9471`**, **`Severe ROC-AUC = 0.9882`**, **`6-Class Accuracy = 0.9450`**, **`6-Class Macro-F1 = 0.5543`** (và đối chiếu phụ `next-observed shift(-1)` trên `23,414` cặp đạt `F1 = 0.6447`, `PR-AUC = 0.7628`, `ROC-AUC = 0.8484`).
- **`AC-P4` (`Prescriptive Stream Sketching & Two-Tier Reconciliation`):**
  - [x] `32 KB Bloom Filter` ($262{,}144$ bits, `uint8[32768]`, $k=4$) chặn `334 / 334` (`100%`) bản tin trùng lặp (`FPR = 0.616%`); `1 KB HyperLogLog++` (`1,024` thanh ghi `uint8[1024]`) ước lượng `9,627.9` đoạn đường (sai số `3.98%` so với `10,027`).
  - [x] `Tier 1 Reconciliation` đạt `100.0%` khớp trong `Watermark` (`exceptAll == 0`); `Tier 2 Reconciliation` bù trừ `501` sự kiện cửa sổ đoạn đường trễ trên `219` khung `(date, period)`, hàn gắn `3,953` cửa sổ trễ bậc xuôi dòng và chấm điểm lại `377` cửa sổ, nâng độ đầy đủ từ `98.50%` lên `100.0%`.
  - [x] `MongoDB 8.0` (`bda501.traffic_scores`) xác nhận `Unique Index` `{event_id: 1}` (`event_id_unique`) và `Compound Index` `{street_level: 1, severe_prob: -1, segment_id: 1}` (`level_prob_segment_idx`) đạt `IXSCAN` với `in_memory_sort = False` (`0` bước `SORT`, `warm_p95_ms = 0.636 ms`).

