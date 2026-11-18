# Báo Cáo Xác Thực Vấn Đề & Đánh Giá Gate 1 (Problem Validation Report — Gate 1)

## 1. Kết Quả Kiểm Định Dữ Liệu Thực Nghiệm Sơ Bộ (`Preliminary 5-Table EDA Evidence`)

Quá trình `Deepdive EDA` trên 5 bảng dữ liệu gốc trong `raw_data/` kết hợp phễu làm sạch không rò rỉ đã cung cấp bằng chứng định lượng xác thực các giả thuyết bài toán:

| Bảng Dữ Liệu | Số Bản Ghi | Số Cột | Dung Lượng | Bằng Chứng Thực Nghiệm Xác Thực Giả Thuyết (Leak-Free Ground Truth) |
| :--- | ---: | ---: | ---: | :--- |
| `nodes.csv` | `577,967` | `3` | `19.44 MB` | `0%` khuyết. Sau khi lọc `427` đoạn phi đường bộ và tỉa **`525,786` điểm (`90.97%`)** uốn cong trung gian, giữ đúng **`52,181` giao lộ hoạt động** (`EC-7`). |
| `streets.csv` | `5,553` | `5` | `0.20 MB` | Khuyết **`82.23%` (`4,566` dòng)** `max_velocity`; `695` tên đường thô được chuẩn hóa `Unicode NFC` về **`641` tên chuẩn** (`54` biến thể gộp trên `2,247` cửa sổ — `EC-1`, `EC-3`). |
| `segments.csv` | `84,633` | `11` | `10.29 MB` | Khuyết **`88.34%` (`74,762` dòng)** `max_velocity`; `154` đoạn `length <= 0m` (`R1`) và `427` đoạn phi đường bộ (`R2`, `580` bản ghi cách ly). Mạng cơ giới sạch gồm **`84,206` đoạn**, tạo `Line Graph` (bỏ U-turn) **`92,453` cạnh** và **`314` `Dangling Nodes`**. |
| `segment_status.csv` | `90,938` | `4` | `3.58 MB` | Sau khi cách ly **`151` bản tin rơi ngoài đường cơ giới (`R6`)**, còn **`90,787` bản tin GPS hợp lệ** (`status_accounting_balanced = true`). Gom theo khung `30m UTC` bằng Pure 5-Tuple Monoid tạo đúng **`33,394` cửa sổ** (`0` sai lệch vs `baseline.py`). |
| `train.csv` (Reference) | `33,394` cơ giới *(+`47` phi đường bộ)* | `18` | `4.99 MB` | Bảng tham chiếu độc lập (loại đúng `47` cửa sổ phi đường bộ theo `R2`). Đối chiếu `BkTraffic urban 5 km/h LOS` (`derive_urban_los`) khớp **`89.19%`**. Trên `84,206` đoạn cơ giới: **`9,995` đoạn (`11.87%`)** có GPS, **`74,211` đoạn (`88.13%`) `Cold-Start`**. Chỉ có `7,885` cặp đúng `+30m`, đồng thời phát hiện **`7,192` cửa sổ burst tổng hợp** (`2020-08-02 23:30` & `2020-08-03 00:00`). |

---

## 2. Kiểm Chứng Chân Trời `t + 30m`, Lọc Burst & Chống Rò Rỉ Nhãn (`Leak-Free Validation`)

- **Hiện tượng phát hiện:** (1) Dịch dòng `shift(-1)` bỏ qua khoảng trống nhiều giờ/ngày; (2) dùng trung bình tĩnh trên `TRAIN` gây `Intra-Train Target Leakage`; (3) hai slot burst tổng hợp tháng 08/2020 (`3,596` cặp trong `TRAIN`) và trọng số `sample_weight` nhân tạo có thể làm sai lệch đánh giá thực tế.
- **Kết quả sau khi lọc `3,596` cặp burst, giữ `1,885` cặp `TRAIN` hữu cơ / `1,167` `VAL` / `1,182` `TEST` và huấn luyện sạch (`class_weight` lũy thừa `0.65`, không `sample_weight` nhân tạo hay fallback bypass):**
  - Cổng `Validation Gate` trên `VAL` với tiêu chí `(val_accuracy_6class + val_pr_auc + val_roc_auc) / 3` chọn **`Random Forest (23D)`** (**`0.8882`** vs **`0.8812`** của `DT 23D`; `τ_severe = 0.25`, `τ_change = 0.40`, `τ_onset = 0.25`).
  - Trên `TEST` (`1,182` mẫu): **`ROC-AUC = 0.9894`**, **`PR-AUC = 0.9543`**, **`6-Class Acc = 0.9255`**, **`Weighted-F1 = 0.9297`**, **`Severe F1 = 0.9362`** (`Prec = 0.9215, Rec = 0.9514`), **`Macro-F1 = 0.4253`** *(phản ánh trung thực mất cân bằng lớp trung gian `B=10, C=15, D=13, E=20`)*, đường chéo `[944, 3, 1, 3, 5, 138]`.
  - Vượt `Persistence` trên nhóm đổi trạng thái (`10 / 61 = 16.39%`, `McNemar p = 0.001953`, `b=10, c=0`; hiệu chỉnh `VAL` đạt `14 / 61 = 22.95%`, tổng `90.19%`), `Detect ROC-AUC = 0.8822` (`PR-AUC = 0.4009`), `Onset risk lift = 3.26x` (`0 / 8` ngưỡng cứng trên `8 / 993` ca) và `Masked-GPS Spatial Cold-Start Proxy` đạt `Acc = 58.63%`, `ROC-AUC = 0.7000`, `PR-AUC = 0.5152` (phủ `100%` `84,206` đoạn).

---

## 3. Quyết Định Cổng Kiểm Soát `Gate 1 — Problem Validation`

- [x] **Tiêu chí 1:** Có bằng chứng định lượng đầy đủ từ dữ liệu thực tế (`577,967` nút $\to$ `52,181` active, `84,206` đoạn cơ giới sạch, `90,787` pings hợp lệ, `33,394` cửa sổ `LOS`, `7,885` cặp đúng `+30m`, `7,192` cửa sổ burst đã nhận diện).
- [x] **Tiêu chí 2:** Xác định rõ 4 bài toán con (`P1`–`P4`), 6 `Use Cases` (`UC-1`–`UC-6`) và 10 `Edge Cases` (`EC-1`–`EC-10`).
- [x] **Tiêu chí 3:** Chứng minh các cách tiếp cận ngây thơ (`Naive shift(-1)`, `Static Prior`, `48D K-Means`, huấn luyện nhiễm burst) đều gây sai lệch hoặc rò rỉ.
- [x] **Tiêu chí 4:** Định vị rõ bộ dữ liệu `~38.5 MB` đóng vai trò **Mẫu chuẩn `Proof-of-Scale`** và ghi nhận trung thực hạn chế cỡ mẫu nhỏ (`5–8` khung) của luật trễ `+30m` cũng như mất cân bằng lớp `B/C/D/E` (`Macro-F1 = 0.4253`).
- **Kết luận `Gate 1`:** **PASSED.**
