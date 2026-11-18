# Nhật Ký Thử Nghiệm Kỹ Thuật (Experiment Log — Leak-Free Ground Truth)

## Experiment 1: Cô lập Synthetic Burst & Huấn luyện Sạch (`class_weight^0.65`, Không `sample_weight` Nhân tạo)
- **Giả thuyết:** Giữ `3,596` cặp synthetic burst (`2020-08-02 23:30` & `2020-08-03 00:00`, tổng `7,192` cửa sổ sạch) trong `TRAIN` hoặc dùng `sample_weight` nhân tạo sẽ thổi phồng giả tạo hiệu năng trên mô hình.
- **Thiết kế:** Loại bỏ toàn bộ `3,596` cặp burst khỏi `TRAIN`, giữ `1,885` cặp `TRAIN` hữu cơ (`50` purge) / `1,167` `VAL` (`5` purge) / `1,182` `TEST` (`185` severe E/F; `A=959, B=10, C=15, D=13, E=20, F=165`). Dùng `class_weight` sạch với exponent `0.65`.
- **Kết quả Ablation Burst:** Có burst (`TEST Acc = 0.9399, Severe F1 = 0.9344`) vs Hữu cơ sạch (`TEST Acc = 0.9255, Severe F1 = 0.9362`).
- **Kết quả `VAL` Gate (`(val_accuracy_6class + val_pr_auc + val_roc_auc) / 3`):** `RF 23D` thắng (**`0.8882`** so với **`0.8812`** của `DT 23D` và `0.6794` của `RF 14D`; `VAL Acc = 0.8166, VAL PR-AUC = 0.8912, VAL ROC-AUC = 0.9567, τ_severe = 0.25, τ_change = 0.40, τ_onset = 0.25`).
- **Bảng đối chứng trên `TEST` (`1,182` cặp hữu cơ — `outputs/ablation_results.csv`):**

| Mô hình | 6-Class Acc | Macro-F1 | Weighted-F1 | Severe Prec | Severe Rec | Severe F1 | ROC-AUC | PR-AUC |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `Persistence (t -> t+30m)` | `0.9484` | `0.5654` | `0.9461` | `0.9365` | `0.9568` | `0.9465` | `0.9867` | `0.9367` |
| `DT 14D` | `0.8697` | `0.4226` | `0.9019` | `0.7106` | `0.9027` | `0.7952` | `0.9767` | `0.9198` |
| `DT 23D` | `0.8790` | `0.3856` | `0.9035` | `0.7479` | `0.9459` | `0.8353` | `0.9853` | `0.9479` |
| `RF 14D` | `0.9222` | `0.3976` | `0.9261` | `0.9355` | `0.9405` | `0.9380` | `0.9881` | `0.9554` |
| **`RF 23D (Winner)`** | **`0.9255`** | **`0.4253`** | **`0.9297`** | **`0.9215`** | **`0.9514`** | **`0.9362`** | **`0.9894`** | **`0.9543`** |
| `GBT 14D` (Binary Severe) | `—` | `—` | `—` | `0.9267` | `0.9568` | `0.9415` | `0.9751` | `0.9488` |
| `GBT 23D` (Binary Severe) | `—` | `—` | `—` | `0.9227` | `0.9676` | `0.9446` | `0.9830` | `0.9498` |

- **Chi tiết `RF 23D`:** Đường chéo `[944, 3, 1, 3, 5, 138]` (đủ 6 lớp); Paired Bootstrap 95% CI: `ROC-AUC [0.9824, 0.9955]`, `Severe F1 [0.9091, 0.9602]`, `PR-AUC diff [-0.0029, 0.0444]`. `Macro-F1 = 0.4253` phản ánh trung thực độ hiếm của các lớp trung gian `B/C/D/E` (`10/15/13/20`).

## Experiment 2: Đánh giá Đổi Trạng Thái (`61` Ca) & Masked-GPS Spatial Cold-Start Proxy
- **Đổi trạng thái trên `TEST` (`61 / 1,182 = 5.16%`):** `Persistence` bắt `0 / 61`; `RF 23D` mặc định bắt **`10 / 61` (`16.39%`, `McNemar p = 0.001953`, `b=10, c=0`)**, hiệu chỉnh `VAL` (`τ_change = 0.40`) đạt **`14 / 61` (`22.95%`, overall `90.19%`)**, Conditional top-1 `31.15%`; **`Detect ROC-AUC = 0.8822`**, **`PR-AUC = 0.4009`** (so với heuristic `0.7921`, DT `0.8511`), `Brier = 0.2811` vs Persistence `0.3279`; `Onset` trên `993` mẫu thoáng (`8 / 993`): `0 / 8` ở ngưỡng cứng `τ_onset = 0.25`, **`Onset ROC-AUC = 0.8438`**, **`Onset risk lift = 3.26x`** (`0.0940 vs 0.0288`).
- **Gap Regime (`3,605` dòng, `49.21%` transitions):** `RF PR-AUC = 0.6405` vs `Persistence = 0.5732` (`GBT 23D = 0.6175`, `RF Acc = 0.4910`).
- **Masked-GPS Spatial Cold-Start Proxy (`74,211 / 84,206 = 88.13%` cold-start):** Khi che lịch sử GPS trực tiếp, proxy đạt **`Acc = 58.63% (0.5863), ROC-AUC = 0.7000, PR-AUC = 0.5152`**, `Brier = 0.1123`.

## Experiment 3: Chuẩn hóa Unicode NFC, Leak-Free Vmax & Kiểm định BkTraffic Urban `5 km/h` LOS
- **Unicode NFC:** Chuẩn hóa `2,247` cửa sổ (`695` tên đường thô -> `641` tên chuẩn hóa, gộp `54` biến thể; `640` tên phân biệt trong `data/dataset.csv`).
- **Leak-Free Vmax Tiers:** Windows `4,946 / 7,650 / 19,601 / 1,197` (`33,394`); Segments `9,869 / 472 / 54,259 / 19,606` (`84,206`).
- **BkTraffic Urban LOS (`derive_urban_los`):** Đạt **`89.19%` exact match** đối chiếu trực tiếp với `train.csv` (phân biệt rõ với hàm tiện ích tỷ lệ `derive_los`).

## Experiment 4: Spark `3.5.7` Catalyst Join, Monoid Salting, Sketches & MongoDB `8.0.34` Latency
- **Catalyst Join (`hcmc-traffic-spark:3.5.7`):** `SortMergeJoin (5 Exchange, 166.18 ms, IQR 23.71)` -> **`BroadcastHashJoin (1 Exchange, 123.37 ms, IQR 4.57)`**, nhanh hơn **`25.76%`**, `exceptAll_diff = 0`.
- **Pure 5-Tuple Monoid & Salting:** Pure Monoid `(cnt, sum_v, min_v, max_v, severe_cnt)` khớp `100.0%` (`0` sai lệch, `0.0 km/h` diff), Spark Monoid `97.92 ms` (IQR `2.13`) vs 8-bucket salted `185.78 ms` (IQR `3.68`, `59,714` nhóm Stage-1, `0 exceptAll diff`); Spark MLlib organic TRAIN: `FPGrowth = 32`, `KMeans = 3,609`, `RF = 1,885 (test_acc = 0.9416, roc_auc = 0.9903)`.
- **Sketches & Reconciliation:** `32.0 KiB RotatingBloomFilter` (`333` dups, `13` FPs, `0.0389%` empirical vs `0.0792%` theoretical), `1.0 KiB HLL++` (`9,620.2` vs `9,995`, `3.75%`); Tier 1 `0` diff (`100%`), Tier 2 `226` slots / `500` segment-windows / `609` lag1 / `384` rescored (`213` prob changed), `98.5% -> 100.0%`.
- **MongoDB `8.0.34` (`33,394` docs, `1,234` arterial alerts):** `IXSCAN` ấm đạt **`0.360 ms` median / `0.603 ms` p95** so với `COLLSCAN 11.289 ms / 12.217 ms` (**`31.36x`**); atomic `$status_version` pipeline guard chặn `25/25` bản tin `v1` cũ.
