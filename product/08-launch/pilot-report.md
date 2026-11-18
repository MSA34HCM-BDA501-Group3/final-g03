# Báo Cáo Thí Điểm & Nghiệm Thu Kỹ Thuật (Pilot & Verification Report — Wave 2)

## 1. Tóm tắt Kết quả Thí điểm Wave 2
Hệ thống đã hoàn tất chạy lại toàn bộ pipeline end-to-end với dữ liệu làm sạch thống nhất, loại bỏ hoàn toàn các nguồn phóng đại hiệu năng nhân tạo (`synthetic burst`, `sample_weight` nhân tạo) và xác thực trên cả Python Lakehouse lẫn Spark `3.5.7` / MongoDB `8.0.34`.

## 2. Chỉ số Xác thực Chính (Verified On-Disk Metrics)
| Lĩnh vực | Chỉ số Wave 2 xác thực trên `outputs/` & `data/` |
|---|---|
| **Đồ thị & Làm sạch** | `84,633 -> 84,206` đoạn đường cơ giới sạch (`427` non-road loại, `580` GIS quarantine); `577,967 -> 52,181` nút hoạt động (`525,786 = 90.97%` tỉa bỏ); `92,453` cạnh đồ thị đường (không U-turn); `314` nút cụt; `#1 PageRank = 21969 Nguyễn Kiệm (PR = 2.7799)` |
| **GPS & Chuẩn hóa** | `90,938 -> 90,787` pings hợp lệ (`151` non-road cách ly); `33,394 x 60` cửa sổ UTC 30m (`4,305` ICT midnight rollover); `2,247` NFC (`695 -> 641`); `89.19%` BkTraffic urban `5 km/h` LOS exact match; Pure 5-tuple Monoid `100.0%` (`0` sai lệch, `0.0 km/h`) |
| **Cặp `+30m` & Burst** | `7,885` cặp `+30m` chính xác; cô lập `7,192` cửa sổ burst (`2020-08-02 23:30` & `2020-08-03 00:00`) -> loại `3,596` cặp burst khi train; giữ `1,885` organic `TRAIN` / `1,167` `VAL` / `1,182` `TEST` |
| **Mô hình `RF 23D`** | Thắng `VAL` (`0.8990` vs `0.5343`); `TEST`: `ROC-AUC = 0.9900`, `PR-AUC = 0.9595`, `Acc = 0.9289`, `Severe F1 = 0.9301` (`Prec = 0.9251, Rec = 0.9351`), `Macro-F1 = 0.4475`, diag `[937, 4, 1, 7, 2, 147]`; `McNemar p = 0.015625` (`7/61` vs `0/61`), `Detect ROC-AUC = 0.9146`, `Onset lift = 3.32x` |
| **Luật & Phân cụm** | K-Means `k=6` trên `3,609` đoạn đường hữu cơ (`Silhouette = 0.2197, DBI = 1.3592`); `84,206 x 23` static store (`74,211 = 88.13%` cold-start); `12` co-rules (`Nguyễn Thị Minh Khai <-> Cống Quỳnh`, Lift `15.2308`) + `14` lagged rules (`Lê Quang Định -> Trần Quang Khải`, Lift `24.0`, `persistence_controlled = True`) |
| **Spark 3.5.7 & Healing** | Catalyst `SortMergeJoin (5 Exchange, 145.24 ms) -> BroadcastHashJoin (1 Exchange, 120.45 ms, 17.07% speedup, 0 exceptAll diff)`; Monoid `147.11 ms` vs salted `222.60 ms` (`59,714` nhóm); `tier1_exceptAll_diff = 0` (`100.0%`); bù `500` out-WM (`226` slots), chữa lành `609` lag, chấm lại `384` (`185` đổi) |
| **MongoDB 8.0 & UI** | `33,394` docs (`69` batches), atomic `$status_version` guard (`25/25` stale v1 rejected), `IXSCAN 0.389 / 0.692 ms` (`23.55x`), `6` Docker services (`Web TOC :8000` + `Streamlit :8501`) |

## 3. Nhận xét Nghiệm thu
Nền tảng sẵn sàng cho trình diễn vận hành và báo cáo học thuật với mức độ minh bạch cao về cả thành tựu kỹ thuật lẫn giới hạn mẫu lớp hiếm (`Macro-F1 = 0.4475`, `Masked-GPS Cold-Start Proxy Acc = 0.5271`), đồng thời ghi nhận rõ hạng mục cập nhật assertion trong `tests/test_platform_v3.py`.
