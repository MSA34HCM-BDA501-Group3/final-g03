# Nhật Ký Thực Nghiệm & Đối Chứng Học Thuật (Experiment Log & Ablation Record)

## 1. Thí Nghiệm 1: Khử Rò Rỉ Nhãn Nội Tập (`Static Train Prior` vs. `Causal Expanding Empirical-Bayes Prior`)

- **Giả thuyết (`Hypothesis`):** Với `79.26%` đoạn đường trong tập `TRAIN` có $\le 2$ quan sát, việc tính trung bình tĩnh `mean(los_ordinal)` trên toàn bộ tập `TRAIN` gây rò rỉ nhãn tương lai $y_{t_2}$ vào đặc trưng của $t_1$, làm mô hình quá khớp (`overfit`) vào biến rò rỉ và suy giảm hiệu năng trên tập `TEST`.
- **Kết quả Thực nghiệm trên `3,513` Cửa sổ `TEST`:**
  - Khi dùng `Static Leaked Prior`: `Random Forest (+Graph)` bị tụt `Severe F1` xuống **`0.5758`** và `Decision Tree (+Graph)` tụt `Accuracy` xuống **`0.3330`**.
  - Khi thay bằng **`Causal Expanding Empirical-Bayes Prior` ($\tau \le t, m_{\text{seg}}=5, m_{\text{street}}=10$)**: `Random Forest (+Graph PageRank, Causal Priors & K-Means)` tăng vọt `Severe F1` lên **`0.6670` (`+9.12%`)** và đạt **`Severe PR-AUC = 0.7568`** (cao nhất toàn bảng), vượt qua cả `Persistence Baseline` (`0.6609`) và `Random Forest Local Only` (`0.6570`).

---

## 2. Thí Nghiệm 2: Chuẩn Hóa `Unicode NFC` Trong Khai Phá Luật Đồng Kẹt Xe (`SON / FPGrowth`)

- **Giả thuyết (`Hypothesis`):** Dữ liệu OSM thô chứa các tên đường trùng nhau nhưng khác bảng mã Unicode (`NFD` vs. `NFC`), ký tự `Ð` vs. `Đ`, hoặc tiền tố `Đường `, làm phát sinh các luật tự suy diễn giả tạo.
- **Kết quả Thực nghiệm:**
  - Trước khi chuẩn hóa (`Raw Street Names`): Xuất hiện các luật lỗi như `Xa Lộ Hà Nội => Xa lộ Hà Nội` và `Phạm Văn Đồng => Phạm Văn Đồng` cùng các nhãn hành chính `Phường Hiệp Bình Chánh`.
  - Sau khi áp dụng `[C5]` `Canonical Unicode NFC & Regex Street Hygiene`: Số tên đường thực tế trong `train.csv` được gộp sạch về `649` tuyến đường chuẩn; `30 / 30` luật kết hợp đạt `0%` tự suy diễn (`Lift` cao nhất đạt **`28.5833x`** cho `Lý Chính Thắng => Nguyễn Thông`).

---

## 3. Thí Nghiệm 3: Tối Ưu Hóa Kế Hoạch Kết Nối & Chỉ Mục Phục Vụ (`BroadcastHashJoin` & `MongoDB IXSCAN`)

- **Kết quả `Catalyst Join Plan` (`Pillar I2`):** Ép `BroadcastHashJoin` cho các bảng chiều tĩnh (`segments`, `streets`) giúp giảm số bước trao đổi mạng (`Exchange` stages) từ **`7` xuống `3`**, loại bỏ hoàn toàn bước sắp xếp bảng sự kiện lớn.
- **Kết quả `MongoDB 8.0 Explain Plan` (`Pillar I3`):** Chỉ mục ghép `{street_level: 1, severe_prob: -1, segment_id: 1}` phục vụ trực tiếp cả phép lọc `street_level IN [1,2]`, `severe_prob >= 0.70` lẫn phép sắp xếp `severe_prob DESC, segment_id ASC` qua `IXSCAN` với `in_memory_sort = False` (`0` bước `SORT`).
