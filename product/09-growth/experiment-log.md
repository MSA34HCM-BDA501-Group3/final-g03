# Nhật Ký Thử Nghiệm Kỹ Thuật (Experiment Log — Wave 2 Verified Results)

## Experiment 1: Cô lập Synthetic Burst & Huấn luyện Sạch (`class_weight^0.65`, Không `sample_weight` Nhân tạo)
- **Giả thuyết:** Giữ `3,596` cặp synthetic burst (`2020-08-02 23:30` & `2020-08-03 00:00`, tổng `7,192` cửa sổ sạch) trong `TRAIN` hoặc dùng `sample_weight` nhân tạo sẽ thổi phồng giả tạo hiệu năng trên mô hình.
- **Thiết kế:** Loại bỏ toàn bộ `3,596` cặp burst khỏi `TRAIN`, giữ `1,885` cặp `TRAIN` hữu cơ (`50` purge) / `1,167` `VAL` (`5` purge) / `1,182` `TEST` (`185` severe E/F; `A=959, B=10, C=15, D=13, E=20, F=165`). Dùng `class_weight` sạch với exponent `0.65`.
- **Kết quả Ablation Burst:** Có burst (`TEST Acc = 0.9391, Severe F1 = 0.9372`) vs Hữu cơ sạch (`TEST Acc = 0.9289, Severe F1 = 0.9301`).
- **Kết quả `VAL` Gate (`0.5*Acc + 0.5*PR-AUC`):** `RF 23D` thắng (`0.8990` so với `0.5343` của `RF 14D`; `VAL Acc = 0.9023, VAL PR-AUC = 0.8957, VAL ROC-AUC = 0.9611, th = 0.25`).
- **Bảng đối chứng trên `TEST` (`1,182` cặp hữu cơ — `outputs/ablation_results.csv`):**

| Mô hình | 6-Class Acc | Macro-F1 | Weighted-F1 | Severe Prec | Severe Rec | Severe F1 | ROC-AUC | PR-AUC |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `Persistence (t -> t+30m)` | `0.9484` | `0.5654` | `0.9461` | `0.9365` | `0.9568` | `0.9465` | `0.9867` | `0.9367` |
| `DT 14D` | `0.8832` | `0.3767` | `0.9058` | `0.9467` | `0.8649` | `0.9040` | `0.9833` | `0.9296` |
| `DT 23D` | `0.8748` | `0.3817` | `0.9006` | `0.9459` | `0.7568` | `0.8408` | `0.9799` | `0.9263` |
| `RF 14D` | `0.9239` | `0.4372` | `0.9290` | `0.9429` | `0.8919` | `0.9167` | `0.9898` | `0.9557` |
| **`RF 23D (Winner)`** | **`0.9289`** | **`0.4475`** | **`0.9304`** | **`0.9251`** | **`0.9351`** | **`0.9301`** | **`0.9900`** | **`0.9595`** |
| `GBT 14D` (Binary Severe) | `—` | `—` | `—` | `0.9362` | `0.9514` | `0.9437` | `0.9849` | `0.9534` |
| `GBT 23D` (Binary Severe) | `—` | `—` | `—` | `0.9259` | `0.9459` | `0.9358` | `0.9822` | `0.9454` |

- **Chi tiết `RF 23D`:** Đường chéo ma trận nhầm lẫn `[937, 4, 1, 7, 2, 147]` (đủ 6 lớp); Paired Bootstrap 95% CI: `ROC-AUC [0.9836, 0.9953]`, `Severe F1 [0.9012, 0.9547]`. `Macro-F1 = 0.4475` phản ánh trung thực độ hiếm của các lớp trung gian `B/C/D/E` (`10/15/13/20`).

## Experiment 2: Đánh giá Chuyển Pha (`61` Transitions) & Masked-GPS Spatial Cold-Start Proxy
- **Chuyển pha trên `TEST` (`61 / 1,182 = 5.16%`):** `Persistence` bắt `0/61`; `RF 23D` bắt `7/61` (`11.48%`, `McNemar p = 0.015625`, `7 vs 0`), tại ngưỡng `P >= 0.40` đạt `19.67%` (`12/61`, overall `0.8849`), Conditional top-1 `31.15%`; **`Transition Detect ROC-AUC = 0.9146`** (so với heuristic `0.7921`, DT `0.7996`), `Onset ROC-AUC = 0.8539`, **`Onset risk lift = 3.32x`** (`0.0823 vs 0.0248`).
- **Gap Regime (`3,605` dòng, `49.21%` transitions):** `RF PR-AUC = 0.6293` vs `Persistence = 0.5732`.
- **Masked-GPS Spatial Cold-Start Proxy (`74,211 / 84,206 = 88.13%` cold-start):** Khi che lịch sử GPS trực tiếp, proxy đạt `Acc = 0.5271, ROC-AUC = 0.6175, PR-AUC = 0.4282, Brier = 0.1168`.

## Experiment 3: Chuẩn hóa Unicode NFC & Kiểm định BkTraffic Urban `5 km/h` LOS
- **Unicode NFC:** Chuẩn hóa `2,247` cửa sổ (`695` tên đường thô -> `641` tên chuẩn hóa, gộp `54` biến thể; `640` tên phân biệt trong `data/dataset.csv`).
- **BkTraffic Urban LOS (`derive_urban_los`):** Đạt **`89.19%` exact match** đối chiếu trực tiếp với `train.csv` (phân biệt rõ với hàm tiện ích tỷ lệ `derive_los`).

## Experiment 4: Spark `3.5.7` Catalyst Join, Monoid Salting & MongoDB `8.0.34` Latency
- **Catalyst Join (`hcmc-traffic-spark:3.5.7`):** `SortMergeJoin (5 Exchange, 145.24 ms, IQR 12.83)` -> **`BroadcastHashJoin (1 Exchange, 120.45 ms, IQR 4.59)`**, nhanh hơn **`17.07%`**, `exceptAll_diff = 0`.
- **Pure 5-Tuple Monoid & Salting:** Pure Monoid khớp `100.0%` (`0` sai lệch, `0.0 km/h` diff), Spark Monoid `147.11 ms` (IQR `31.29`) vs 8-bucket salted `222.60 ms` (IQR `11.27`, `59,714` nhóm Stage-1, `0 exceptAll diff`); giảm độ lệch khóa từ `684.95x` xuống `1.0197x` (khung giờ đỉnh) và `salted_partition_skew_ratio = 1.0123` (`CV = 0.0059`).
- **MongoDB `8.0.34` (`33,394` docs):** `IXSCAN` ấm đạt **`0.389 ms` median / `0.692 ms` p95** so với `COLLSCAN 9.161 ms / 9.906 ms` (**`23.55x`**); atomic `$status_version` pipeline guard chặn `25/25` bản tin `v1` cũ.
