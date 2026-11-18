# Phân Tích Sản Phẩm & Dữ Liệu (Product Analytics — Wave 2 Ground Truth)

## 1. Cấu trúc Đồ thị & Phủ Sóng Mạng Lưới TP.HCM
- **Đoạn đường (`Segments`):** `84,633` thô -> **`84,206`** đoạn đường cơ giới sạch (loại `427` POI không phải đường bộ; cách ly GIS `580` bản ghi gồm `154 R1 + 426 R2`).
- **Nút giao (`Nodes`):** `577,967` nút gốc -> **`52,181`** nút hoạt động sau khi tỉa **`525,786` (`90.97%`)** nút cô lập/không cơ giới.
- **Đồ thị chuyển hướng (`Line Graph`):** **`92,453`** cạnh chuyển hướng hợp lệ (đã loại bỏ U-turn trên đoạn đường hai chiều) và **`314`** nút cụt (`dangling nodes`).
- **Độ phủ quan sát:** `9,995` đoạn đường có GPS (`11.87%`) và `74,211` đoạn đường Cold-Start (`88.13%`) trong bảng `segment_static_features.parquet` (`84,206 x 23`).
- **Điểm nghẽn Top PageRank:** `#1` **`21969` — Nguyễn Kiệm** (`level=2, in=3, out=1, PR=2.7799`), tiếp theo `#2 37345` Cầu vượt Ngã tư Gò Dưa (`2.7196`), `#3 34723` Quốc lộ 1 (`2.6894`), `#4 40948` Quốc lộ 1 (`2.6516`), `#5 81815` Song Hành XLHN (`2.6508`).

## 2. Phân bố Dữ liệu & Trạng thái `LOS` (`33,394` Cửa sổ UTC 30m)
- **Nguồn đầu vào:** `90,938` raw pings -> `90,787` accepted pings (`151` non-road quarantined) -> `33,394` cửa sổ chuẩn hóa (`4,305` bản ghi lật ngày ICT `12.89%`).
- **Phân bố nhãn `LOS` (`A–F` trên `33,394` cửa sổ):**
  - `A`: `13,267` | `B`: `4,694` | `C`: `3,827` | `D`: `3,701` | `E`: `3,821` | `F`: `4,084`
- **Tính toàn vẹn phục vụ (`outputs/serving_traffic_scores.parquet`):** `32,405 BATCH_VERIFIED (v2)` + `500 BATCH_RECONCILED (v2)` + `489 STREAM_PROVISIONAL (v1)` = `33,394` tài liệu.

## 3. Phân Cụm Hành Vi `K-Means 7D` (`3,609` Đoạn Hữu Cơ Trong `TRAIN`)
- **Chỉ số chất lượng cụm (`k=6`, `Silhouette = 0.2197`, `Davies-Bouldin = 1.3592`, `CH = 767.5843`, `Inertia = 12,232.7036`):**

| Cụm | Số đoạn trong `TRAIN` (`3,609`) | Số đoạn chiếu toàn mạng lưới (`84,206`) |
|---|---:|---:|
| `Cluster 0` | `361` | `3,701` |
| `Cluster 1` | `1,147` | `37,215` |
| `Cluster 2` | `1,017` | `5,372` |
| `Cluster 3` | `142` | `2,890` |
| `Cluster 4` | `711` | `32,376` |
| `Cluster 5` | `231` | `2,652` |

## 4. Tầm quan trọng Đặc trưng (`RF 23D`)
- **Top 5 đặc trưng:** `mean_velocity` (`0.4675`), `current_los_ordinal` (`0.1713`), `velocity_ratio` (`0.0690`), `pagerank_score` (`0.0482`), `period_sin` (`0.0362`).
- **Đóng góp theo nhóm đặc trưng:** Động học cục bộ & độ trễ (`76.20%`), Cấu trúc đồ thị / Causal Priors / K-Means (`12.57%`), Thời gian ICT & Hình học (`11.23%`).
