# Báo Cáo Xác Thực Vấn Đề & Đánh Giá Gate 1 (Problem Validation Report — Gate 1)

## 1. Kết Quả Kiểm Định Dữ Liệu Thực Nghiệm Sơ Bộ (`Preliminary 5-Table EDA Evidence`)

Quá trình `Deepdive EDA` trên 5 bảng dữ liệu gốc trong `raw_data/` kết hợp phễu làm sạch Wave 2 đã cung cấp bằng chứng định lượng xác thực các giả thuyết bài toán:

| Bảng Dữ Liệu | Số Bản Ghi | Số Cột | Dung Lượng | Bằng Chứng Thực Nghiệm Xác Thực Giả Thuyết (`Wave 2 Verified`) |
| :--- | ---: | ---: | ---: | :--- |
| `nodes.csv` | `577,967` | `3` | `19.44 MB` | `0%` khuyết. Sau khi lọc `427` đoạn phi đường bộ và tỉa **`525,786` điểm (`90.97%`)** uốn cong trung gian/nút mồ côi, giữ đúng **`52,181` giao lộ hoạt động** (`EC-7`). |
| `streets.csv` | `5,553` | `5` | `0.20 MB` | Khuyết **`82.23%` (`4,566` dòng)** `max_velocity`; `695` tên đường thô được chuẩn hóa `Unicode NFC` về **`641` tên chuẩn** (`54` biến thể gộp trên `2,247` cửa sổ — `EC-1`, `EC-3`). |
| `segments.csv` | `84,633` | `11` | `10.29 MB` | Khuyết **`88.34%` (`74,762` dòng)** `max_velocity`; `154` đoạn `length <= 0m` (`R1`) và `427` đoạn phi đường bộ (`R2`, `580` bản ghi cách ly). Mạng cơ giới sạch gồm **`84,206` đoạn**, tạo `Line Graph` (bỏ U-turn) **`92,453` cạnh** và **`314` `Dangling Nodes`**. |
| `segment_status.csv` | `90,938` | `4` | `3.58 MB` | Sau khi cách ly **`151` bản tin rơi ngoài đường cơ giới (`R6`)**, còn **`90,787` bản tin GPS hợp lệ** (`status_accounting_balanced = true`). Gom theo khung `30m UTC` bằng Pure 5-Tuple Monoid tạo đúng **`33,394` cửa sổ** (`0` sai lệch vs `baseline.py`). |
| `train.csv` (Reference) | `33,441` | `18` | `4.99 MB` | Chứa `47` cửa sổ trên đoạn phi đường bộ (`33,441 - 47 = 33,394` cơ giới). Đối chiếu `BkTraffic urban 5 km/h LOS` (`derive_urban_los`) khớp **`89.19%`**. Trên `84,206` đoạn cơ giới: **`9,995` đoạn (`11.87%`)** có GPS, **`74,211` đoạn (`88.13%`) `Cold-Start`**. Chỉ có `7,885` cặp đúng `+30m`, đồng thời phát hiện **`7,192` cửa sổ burst tổng hợp** (`2020-08-02 23:30` & `2020-08-03 00:00`). |

---

## 2. Kiểm Chứng Chân Trời `t + 30m`, Lọc Burst & Chống Rò Rỉ Nhãn (`Wave 2 Clean Validation`)

- **Hiện tượng phát hiện:** (1) Dịch dòng `shift(-1)` bỏ qua khoảng trống nhiều giờ/ngày; (2) dùng trung bình tĩnh trên `TRAIN` gây `Intra-Train Target Leakage`; (3) hai slot burst tổng hợp tháng 08/2020 (`3,596` cặp trong `TRAIN`) và trọng số `sample_weight` nhân tạo có thể làm sai lệch đánh giá thực tế.
- **Kết quả sau khi lọc `3,596` cặp burst, giữ `1,885` cặp `TRAIN` hữu cơ / `1,167` `VAL` / `1,182` `TEST` và huấn luyện sạch (`class_weight` lũy thừa `0.65`, không `sample_weight` nhân tạo hay fallback bypass):**
  - Cổng `Validation Gate` trên `VAL` chọn **`Random Forest (23D)`** (`VAL composite = 0.8990` vs `0.5343` `RF 14D`; `VAL 6-Class Acc = 0.9023`).
  - Trên `TEST` (`1,182` mẫu): **`ROC-AUC = 0.9900`**, **`PR-AUC = 0.9595`**, **`6-Class Acc = 0.9289`**, **`Severe F1 = 0.9301`** (`Prec = 0.9251, Rec = 0.9351`), **`Macro-F1 = 0.4475`** *(phản ánh trung thực mất cân bằng lớp trung gian `B=10, C=15, D=13, E=20`)*, đường chéo `[937, 4, 1, 7, 2, 147]`.
  - Vượt `Persistence` trên nhóm chuyển trạng thái (`McNemar p = 0.015625`, `7 vs 0`), `Transition Detect ROC-AUC = 0.9146`, `Onset risk lift = 3.32x` và phủ `100%` kiểm thử `Masked-GPS Spatial Cold-Start Proxy`.

---

## 3. Quyết Định Cổng Kiểm Soát `Gate 1 — Problem Validation`

- [x] **Tiêu chí 1:** Có bằng chứng định lượng đầy đủ từ dữ liệu thực tế (`577,967` nút $\to$ `52,181` active, `84,206` đoạn cơ giới sạch, `90,787` pings hợp lệ, `33,394` cửa sổ `LOS`, `7,885` cặp đúng `+30m`, `7,192` cửa sổ burst đã nhận diện).
- [x] **Tiêu chí 2:** Xác định rõ 4 bài toán con (`P1`–`P4`), 6 `Use Cases` (`UC-1`–`UC-6`) và 10 `Edge Cases` (`EC-1`–`EC-10`).
- [x] **Tiêu chí 3:** Chứng minh các cách tiếp cận ngây thơ (`Naive shift(-1)`, `Static Prior`, `48D K-Means`, huấn luyện nhiễm burst) đều gây sai lệch hoặc rò rỉ.
- [x] **Tiêu chí 4:** Định vị rõ bộ dữ liệu `~38.5 MB` đóng vai trò **Mẫu chuẩn `Proof-of-Scale`** và ghi nhận trung thực hạn chế cỡ mẫu nhỏ (`5–8` khung) của luật trễ `+30m` cũng như mất cân bằng lớp `B/C/D/E` (`Macro-F1 = 0.4475`).
- **Kết luận `Gate 1`:** **PASSED.**
