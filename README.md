# Hệ Thống Giám Sát Hiện Trạng & Dự Báo Ùn Tắc Giao Thông Đô Thị TP.HCM

> **Bộ tài liệu & sản phẩm chính thức của đồ án (`BDA501 — Big Data Analytics`, Leak-Free Verified):**
> - **Báo cáo & Đặc tả Kỹ thuật Markdown:** [`TECHNICAL_NOTE.md`](./TECHNICAL_NOTE.md) · [`product/00-proposal/project-proposal.md`](./product/00-proposal/project-proposal.md) · [`product/README.md`](./product/README.md)
> - **Hướng dẫn Thiết lập Dữ liệu & Nhánh Spark Data Engineering:** [`docs/DATA_SETUP.md`](./docs/DATA_SETUP.md) · [`docs/QUYEN_DATA_ENGINEERING.md`](./docs/QUYEN_DATA_ENGINEERING.md) · [`docs/HUONG_DAN_BRANCH_QUYEN_VI.md`](./docs/HUONG_DAN_BRANCH_QUYEN_VI.md)
> - **Gói Mẫu Chạy Nhanh & Giao diện Điều hành Duy nhất:** [`data/demo_sample/README.md`](./data/demo_sample/README.md) · **Web TOC Command Center `ReactJS + FastAPI` (`http://localhost:8000`)**
> - **Báo cáo PDF, Slide Thuyết Trình & Hồ Sơ Kiểm Định Session 9:** [`HCMC_Smart_Traffic_Final_Report.pdf`](./HCMC_Smart_Traffic_Final_Report.pdf), [`HCMC_Smart_Traffic_Final_Presentation.pptx`](./HCMC_Smart_Traffic_Final_Presentation.pptx), [`HCMC_Smart_Traffic_Final_Slides.pdf`](./HCMC_Smart_Traffic_Final_Slides.pdf), [`PRESENTATION_SCRIPT.md`](./PRESENTATION_SCRIPT.md), [`SESSION9_TECHNICAL_AUDIT_REPORT.md`](./SESSION9_TECHNICAL_AUDIT_REPORT.md), [`SESSION9_LIVE_DEMO_RUNBOOK.md`](./SESSION9_LIVE_DEMO_RUNBOOK.md), [`evidence/session9/I_readiness.md`](./evidence/session9/I_readiness.md), [`BDA501_Session9_Lab_Manual.docx`](./BDA501_Session9_Lab_Manual.docx)

---

## 1. Bức Tranh Tổng Thể Trong 4 Bước Nối Tiếp Nhau

Mục tiêu của hệ thống là **giám sát hiện trạng và dự báo trước `30 phút` nguy cơ kẹt xe trên toàn mạng lưới `84,206` đoạn đường cơ giới một chiều tại TP.HCM** (làm sạch từ `84,633` đoạn đường thô) từ dữ liệu hành trình GPS của xe cộ (ví dụ: lúc `07:08` sáng trên đoạn đường Lý Thường Kiệt mã `47407` tại Quận 10, vận tốc đo được ở mức `4 km/h`). Hệ thống đối chiếu song song thang đo **BkTraffic Urban 5 km/h LOS** (`derive_urban_los`, khớp **`89.19%`** với nhãn tham chiếu `train.csv`) và hàm tiện ích theo tỷ lệ vận tốc `derive_los` (`0.80/0.65/0.50/0.35/0.20`), chuẩn hóa 6 mức phục vụ giao thông (`LOS A–F`): `LOS A–B` (thông thoáng), `LOS C–D` (đông đúc) và `LOS E–F` (ùn ứ nghiêm trọng đến kẹt cứng).

Để vượt qua 4 rào cản lớn của dữ liệu thực tế (**`88.13%` đoạn đường nhánh không có GPS & thiếu tốc độ tối đa**, **tín hiệu gửi đứt quãng**, **đợt burst tổng hợp `7,192` cửa sổ tháng 08/2020**, và **bản tin trùng/trễ mạng**), toàn bộ hệ thống vận hành qua **4 bước nhân quả khép kín**:

1. **Bước 1 — Chuẩn hóa bức tranh hiện trạng (`Spark Batch ETL` & `Monoid`):**
   - Lọc bỏ `427` thực thể phi đường cơ giới (`R2_NON_ROAD_OSM_TAG`) khỏi `84,633` đoạn thô để còn **`84,206` đoạn đường cơ giới sạch** (`580` bản ghi GIS cách ly theo lỗi đầu tiên vào `quarantine_events.parquet`, sửa trung vị `27.0m` cho `154` đoạn `length <= 0`).
   - Tiếp nhận **`90,938` bản tin GPS thô** $\to$ **`90,787` bản tin hợp lệ** (cách ly `151` bản tin rơi vào đoạn phi đường bộ, `0` mất mát ngầm), nội suy `max_velocity` không rò rỉ chỉ học trên `TRAIN` hữu cơ (`4,946 / 7,650 / 19,601 / 1,197` cửa sổ; `9,869 / 472 / 54,259 / 19,606` đoạn), gom bằng **Pure 5-Tuple Monoid** `(cnt, sum_v, min_v, max_v, severe_cnt)` kết hợp chia `8` giỏ khóa (`684.95x` lệch tải khung giờ $\to$ `1.0197x` giỏ cao điểm; `salted_partition_skew_ratio = 1.0123x`, `CV = 0.0059`) thành **`33,394` cửa sổ 30 phút UTC** khớp **`100.0%`** [`baseline.py`](./baseline.py) (`0` sai lệch trên cả 5 đại lượng + vận tốc trung bình, `max_abs_diff = 0.0 km/h`).
   - Thực thi trên **`Apache Spark 3.5.7 (hcmc-traffic-spark:3.5.7)`**: `Catalyst SortMergeJoin` (`5` Exchange, `166.18 ms`) $\to$ **`BroadcastHashJoin` (`1` Exchange, `123.37 ms`, `25.76%` nhanh hơn, `0 exceptAll diff`)**; `Monoid` `97.92 ms` vs `Salted` `185.78 ms` (`59,714` nhóm Stage-1, `0 exceptAll diff`); Spark MLlib huấn luyện thuần trên `TRAIN` hữu cơ (`FPGrowth = 32` luật, `KMeans = 3,609` đoạn, `Spark RF = 1,885` dòng `TRAIN` đạt `test_acc = 0.9416`, `roc_auc = 0.9903`).
2. **Bước 2 — Lấp đầy "vùng mù" `88.13%` đường thiếu GPS (`PageRank`, `Rules` & `K-Means`):**
   - Lọc bỏ **`525,786` điểm uốn cong trung gian (`90.97%`)** từ `577,967` nút, giữ **`52,181` giao lộ hoạt động**; dựng đồ thị nối đoạn đường có hướng (đã loại cung quay đầu U-turn trên đoạn hai chiều) gồm **`92,453` cạnh chuyển tiếp** và **`314` đoạn cụt (`dangling nodes`)**. Nút cổ chai `#1 PageRank` ($\beta = 0.85$): **`21969 — Nguyễn Kiệm (level=2, PR=2.7799)`**.
   - Khai phá luật kết hợp **chỉ trên tập `TRAIN` hữu cơ (non-burst)** kèm kiểm chứng hold-out: **`12` luật đồng kẹt xe** (`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.2308`, `Tô Hiến Thành ↔ Lý Thường Kiệt` lặp lại `20` khung hold-out) và **`14` luật lan truyền trễ `+30m`** kiểm soát tính dai dẳng (`persistence_controlled = True`, đứng đầu `Lê Quang Định → Trần Quang Khải`, `Lift = 24.0`).
   - Phân nhóm **Unified `K-Means` ($k=6$)** học trên **`3,609` đoạn `TRAIN` hữu cơ** (`Silhouette = 0.2241`, `DBI = 1.3284`, `CH = 806.49`, kích thước cụm `846 / 924 / 708 / 136 / 840 / 155`) để phủ kín **`74,211` đoạn đường thiếu GPS (`88.13%`)** trên tổng số **`84,206` đoạn** (`100%` độ phủ).
3. **Bước 3 — Xử lý Burst tổng hợp & Dự báo trung thực `+30 phút` (`Random Forest 23D`):**
   - Phát hiện **`7,192` cửa sổ burst tổng hợp** tại đúng 2 khung (`2020-08-02 23:30` và `2020-08-03 00:00`, mỗi khung `3,609` đoạn thô / `3,596` đoạn cơ giới sạch) và **loại bỏ `3,596` cặp burst** khi huấn luyện ML, giữ **`1,885` cặp `TRAIN` hữu cơ** (`50` mẫu biên bị cắt), **`1,167` `VAL`** (`5` cắt biên) và **`1,182` `TEST`** (`185` mẫu kẹt nặng `E/F`), tổng **`7,885` cặp đúng `+30 phút`** (`train_cutoff = 2020-12-12T08:00:00Z`, `val_cutoff = 2020-12-25T03:30:00Z`).
   - Huấn luyện sạch **không dùng `sample_weight` nhân tạo hay `fallback bypass`** (`class_weight` lũy thừa `0.65`). Trên `VAL` với tiêu chí `(val_accuracy_6class + val_pr_auc + val_roc_auc) / 3`, **`Random Forest 23D`** thắng cổng chọn mô hình (**`0.8882`** vs `DT 23D = 0.8812`, `τ_severe = 0.25`, `τ_change = 0.40`, `τ_onset = 0.25`).
   - Trên **`TEST` (`1,182` mẫu)**: **`ROC-AUC = 0.9894`** (`95% CI [0.9824, 0.9955]`), **`PR-AUC = 0.9543`**, **`Acc = 0.9255`**, **`Macro-F1 = 0.4253`** *(phản ánh trung thực mất cân bằng lớp trung gian `B=10, C=15, D=13, E=20`)*, **`Weighted-F1 = 0.9297`**, **`Severe F1 = 0.9362`** (`Prec = 0.9215, Rec = 0.9514`, `95% CI [0.9091, 0.9602]`), đường chéo bảng đối chiếu 6 lớp **`[944, 3, 1, 3, 5, 138]`** (đủ 6 lớp `> 0`). Khi đổi trạng thái (`61` mẫu, `5.16%`), vượt `Persistence` (`10 / 61 = 16.39%`, `McNemar p = 0.001953`, `b=10, c=0`; hiệu chỉnh `VAL` đạt `14 / 61 = 22.95%`, tổng `90.19%`), `Detect ROC-AUC = 0.8822` (`PR-AUC = 0.4009`), `Brier = 0.2811` vs `0.3279`, `Onset ROC-AUC = 0.8438` (`3.26x` lift, `0 / 8` ngưỡng cứng trên `8 / 993` ca khởi phát), và `Masked-GPS Spatial Cold-Start Proxy` đạt `Acc = 58.63%`, `ROC-AUC = 0.7000`, `PR-AUC = 0.5152` (phủ `100%` `84,206` đoạn).
4. **Bước 4 — Cảnh báo trực tuyến (`33 KiB`), Tự động bù tin trễ & Serving dưới `1 ms`:**
   - Tiếp nhận qua **`Apache Kafka 4.1.0 KRaft`** (`3` phân vùng), lọc trùng & đếm đường bằng **`32.0 KiB RotatingBloomFilter` (`2 × 16 KiB`, `333` trùng, `13` FP, `0.0389%` thực nghiệm vs `0.0792%` lý thuyết) + `1.0 KiB 64-bit HLL++` (`p=10`, `9,620.2` vs `9,995`, sai số `3.75%`)** + **`EWMA`** ($\alpha = 0.15$), tự hồi phục sự cố với **`WAL = 73.7 KB`** (`73,714 B`, `0` nhân đôi khi restart).
   - Lưu **`Medallion Lakehouse`** (`bronze_raw_events` `90,787` pings, `stream_replay_windows` `33,727`, `silver_cleaned_windows` `33,394`, `curated_gold/year_month=*` `10` phân vùng đọc sạch bởi cả `PyArrow/Pandas` và `Spark 3.5.7`), kiểm chứng **Spark Streaming Sink 2** có watermark thực `2020-12-01T12:30:00Z`.
   - Đối soát 2 tầng: **`tier1_exceptAll_diff = 0` (`100.0%` trong watermark)**, bù **`500`** cửa sổ đoạn đường trễ (`226` khung giờ, hoàn thiện `98.5% -> 100.0%`), chữa lành **`609` cửa sổ hạ lưu (`healed_downstream_lag_windows`)**, chấm điểm lại **`384`** cửa sổ (**`213`** đổi xác suất).
   - Phục vụ trên **`MongoDB 8.0.34`** (`33,394` docs, `69` lô, `1,234` cảnh báo trục chính, atomic `$status_version` pipeline guard chặn `25/25` bản ghi `v1` cũ, **`IXSCAN warm median = 0.360 ms / p95 = 0.603 ms`** vs `COLLSCAN median = 11.289 ms`, nhanh hơn **`31.36x`**) cùng ứng dụng web hợp nhất duy nhất **ReactJS + FastAPI TOC Command Center (`http://localhost:8000`)**.

---

## 2. Nhóm Thực hiện, Giảng viên Hướng dẫn & Phân công Chuyên trách

- **Đơn vị & Học phần:** `FSB · FPT University | MSE AI | BDA501 – Big Data Analytics` (`Fall 2026`)
- **Giảng viên hướng dẫn (Instructor):** **Đặng Trần Khánh**

| Thành viên | MSSV | Mảng Chuyên trách trong Dây chuyền 4 Bước | Mã nguồn Cốt lõi |
|---|---|---|---|
| **Nguyễn Tạ Minh Quyền** | `25MS23280` | Bước 1: Làm sạch dữ liệu, ước lượng tốc độ tối đa & xử lý phân tán Spark | `spark_batch.py`, `traffic_data/spark_etl.py`, `baseline.py`, `compose.quyen.yml` |
| **Nguyễn Thành Đạt** | `25MS23306` | Bước 2 & Bước 3: Đồ thị `PageRank`, luật `SON`, phân nhóm `K-Means`, lọc burst & dự báo `+30 phút` | `pipeline.py`, `spark_engine.py`, `tests/` |
| **Phạm Công Minh** | `25MS23310` | Bước 4: Luồng `Kafka`, bộ đếm `33 KiB`, đối soát tin trễ, `MongoDB 8.0` & **Web TOC `ReactJS + FastAPI (:8000)`** | `producer.py`, `starter_stream.py`, `live_kafka_stream.py`, `toc_backend/`, `toc_frontend/`, `docker-compose.yml` |

---

## 3. Cấu Trúc Thư Mục Thực Tế & Năm Bảng Dữ Liệu Nguồn (`raw_data/`)

### 3.1. Cấu trúc Thư mục Chính của Repository
```text
groupexe-knowledgetest/
├── README.md, TECHNICAL_NOTE.md          # Tài liệu tổng quan & đặc tả toán học (đồng bộ Leak-Free Verified)
├── pipeline.py, spark_engine.py          # Pipeline chính (Zones A–F) & kiểm chứng runtime Spark 3.5.7 / MongoDB 8.0
├── spark_batch.py, baseline.py           # Đường Spark Batch độc lập (Quyền) & đối chứng Monoid thuần Python
├── producer.py, starter_stream.py        # Kafka producer, WAL Checkpoint crash-recovery & RotatingBloom/64-bit HLL++
├── live_kafka_stream.py                  # Consumer Kafka liên tục ghi trực tiếp MongoDB 8.0
├── toc_backend/, toc_frontend/           # Ứng dụng Web hợp nhất duy nhất: FastAPI (:8000) + ReactJS/TypeScript + Leaflet
├── traffic_data/                         # Module ETL dùng chung PySpark 3.5.7 / 4.2.0 (`spark_etl.py`)
├── docker-compose.yml, compose.quyen.yml # Cụm 5 dịch vụ mặc định (11.0 GB RAM) + 2 profile batch/stream; Compose batch riêng
├── Dockerfile, Dockerfile.spark35        # Image app/TOC Python & Spark 3.5.7 (`hcmc-traffic-spark:3.5.7`)
├── Dockerfile.spark                      # Image PySpark + OpenJDK 17 (`python:3.11-slim-bookworm`) cho `compose.quyen.yml`
├── docs/                                 # Hướng dẫn thiết lập dữ liệu & bàn giao Data Engineering
├── product/                              # Hệ thống quản trị sản phẩm 10 pha (`00-proposal/` .. `09-growth/`)
├── tests/                                # `test_platform_v3.py` & `test_quyen_spark.py` (Gate 3 PASS, đồng bộ đầy đủ)
├── scripts/, evidence/session9/          # Script dựng báo cáo/slide & hồ sơ kiểm định Session 9 đã tái sinh
├── data/                                 # `dataset.csv` (33,394 × 60), `lakehouse/`, và gói mẫu `demo_sample/`
└── outputs/                              # `metrics.json`, `model.joblib`, `mongo_verify.json`, `spark_runtime_evidence.json`, ...
```

### 3.2. Năm Bảng Dữ liệu Nguồn (`raw_data/`) & Vai trò `Proof-of-Scale`

> **Vai trò mẫu chuẩn kiểm chứng quy mô (`Proof-of-Scale`):** Bộ dữ liệu công khai 5 bảng (`~38.50 MB`) đóng vai trò mẫu chuẩn để đối chứng sai số toán học bằng `0.0` trước khi mở rộng ngang ra luồng GPS toàn TP.HCM (`> 1` triệu xe phát tín hiệu mỗi `5–10` giây $\approx$ `8–17` tỷ bản tin/ngày, `1.5–3.0 TB/ngày`).

| Tệp nguồn | Số bản ghi | Số cột | Dung lượng | Vai trò & Số liệu Thực tế sau Làm sạch |
|---|---:|---:|---:|---|
| `nodes.csv` | `577,967` | `3` | `19.44 MB` | Tọa độ `(long, lat)`; loại bỏ **`525,786` điểm uốn cong trung gian (`90.97%`)**, giữ **`52,181` giao lộ hoạt động** |
| `streets.csv` | `5,553` | `5` | `0.20 MB` | Danh mục tuyến đường (`name`, `level` 1–4, `type`, `max_velocity`); chuẩn hóa `Unicode NFC` (`695 → 641` tên, gộp `54` biến thể) |
| `segments.csv` | `84,633` | `11` | `10.29 MB` | Lọc `427` đoạn phi đường bộ (`R2`) $\to$ **`84,206` đoạn cơ giới sạch** (`92,453` cạnh đồ thị, `314` đoạn cụt, `74,211` Cold-Start = `88.13%`) |
| `segment_status.csv` | `90,938` | `4` | `3.58 MB` | Bản tin GPS gốc $\to$ **`90,787` bản tin hợp lệ** (`151` cách ly phi đường bộ) $\to$ **`33,394` cửa sổ 30m UTC** |
| `train.csv` | `33,394` cơ giới *(+`47` phi đường bộ)* | `18` | `4.99 MB` | Bảng đối chứng tham chiếu độc lập (khớp **`89.19%`** với `BkTraffic urban 5 km/h LOS` và `100.0%` khóa cơ giới `33,394` cửa sổ) |

---

## 4. Hướng dẫn Chuẩn bị Dữ liệu, Khởi chạy Cụm Docker, Live Kafka Demo & Kiểm thử

> **Chuẩn bị dữ liệu (`raw_data/` & `data/demo_sample/`):** Để giữ kho mã nguồn gọn nhẹ, 5 tệp CSV gốc (`~38.50 MB`) đặt tại `raw_data/` theo hướng dẫn chi tiết trong [`docs/DATA_SETUP.md`](./docs/DATA_SETUP.md). Đồng thời, kho mã nguồn **đã đính kèm sẵn gói mẫu thực nghiệm [`data/demo_sample/`](./data/demo_sample/README.md)** để máy checkout mới có thể chạy ngay **Live Kafka Demo** và giao diện Web TOC mà không cần tải trước `38.50 MB`.

### 4.1. Cụm 5 Dịch vụ Docker Mặc định & Web TOC Hợp nhất (`ReactJS + FastAPI :8000`)
`docker-compose.yml` cung cấp **5 dịch vụ mặc định** (`kafka 1536M`, `mongo 2048M`, `spark 6144M`, `toc-web 1024M :8000`, `live-consumer 512M` — tổng hạn mức RAM tối đa **`11.0 GB / 16.0 GB`**, dư `5.0 GB` cho hệ điều hành) cùng 2 dịch vụ chạy theo profile (`pipeline` `[batch]` và `stream` `[stream]`):
- **Ứng dụng Web Điều hành Hợp nhất (`ReactJS + TypeScript + FastAPI + Leaflet`) tại <http://localhost:8000> (`toc-web` = `toc_backend/main.py` + `toc_frontend/dist/`):** Phục vụ trọn vẹn **6 tab điều hành & phân tích** (`01 · Điều Hành & Dự Báo +30p`, `02 · Live Kafka & Sketches 33 KiB` kèm bản đồ Live Streaming riêng, `03 · PageRank & Luật Đồng Kẹt`, `04 · 6 Cụm K-Means & Cold-Start`, `05 · Spark 3.5.7 & Ablation`, `06 · Luồng Thực Thi Pipeline (Live)` theo kiến trúc Lambda chuẩn Zones A–F), 4 kịch bản thực tế trên thanh Đồng hồ TOC (riêng cho Tab 01) và phát **`🎬 Phát Story qua Kafka`** / **`⚡ Stress-Test 200 tin`** ngay tại Tab 02 & Tab 06 qua `Kafka 4.1`.

```powershell
# Khởi chạy trọn bộ 5 dịch vụ mặc định (kafka, mongo, spark, toc-web :8000, live-consumer):
docker compose up -d --build

# Chạy kiểm thử PySpark 3.5.7 trực tiếp bên trong container Spark (lưu ý: máy Windows host không cần cài Java/PySpark):
docker compose exec -T spark python3 -m pytest -v tests/test_quyen_spark.py

# Chạy tác vụ Spark Batch độc lập từ dữ liệu thô trên PySpark 4.2.0 / Java 21:
$env:TRAFFIC_FIT_CUTOFF = '2020-08-01T00:00:00Z'
docker compose -f compose.quyen.yml run --build --rm batch
```

### 4.2. Demo Luồng Thời Gian Thực Đầu–Cuối & Sổ Tay Chạy Live Session 9
> 👉 Xem hướng dẫn copy-paste đầy đủ 8 bước kiểm chứng live (Spark 3.5.7 Query Plan, MongoDB 8.0 IXSCAN, Kafka 4.1, WAL Recovery, Pytest) tại **[`SESSION9_LIVE_DEMO_RUNBOOK.md`](./SESSION9_LIVE_DEMO_RUNBOOK.md)**.

1. Mở **Web TOC Command Center** tại <http://localhost:8000> → **Tab 01** mặc định mở *Kịch bản 3 TEST — Điện Biên Phủ `#50026` (`2021-01-04 07:00 ICT`, dự báo trúng pha `E ➔ F`, có nút Kiểm chứng mù ẩn/hiện thực tế)*.
2. Chuyển sang **Tab 02 (`📡 02 · Live Kafka & Sketches 33 KiB`)** → nhấn **`🎬 Phát Story qua Kafka`** hoặc **`⚡ Stress-Test 200 tin`** (hoặc Terminal: `python producer.py --mode story --scenario morning_rush_dien_bien_phu --bootstrap-servers localhost:9092`): từng bản tin đi thật qua `Kafka 4.1 (3 Partitions)` $\to$ `live-consumer (32 KiB RotatingBloom + 1 KiB 64-bit HLL++ + Frozen 23D RF)` $\to$ `MongoDB 8.0` và cập nhật **Bản đồ Live Streaming độc lập** tại Tab 02 mà không ghi đè Tab 01.
3. Mở **Tab 06 (`⚡ 06 · Luồng Thực Thi Pipeline (Live)`)** để trực quan hóa **Kiến trúc Lambda 2 làn** (Batch `v2` song song Speed/Stream `v1` + cầu nối Read-Only MLOps ➔ hội tụ Serving MongoDB 8.0) và bấm **`⚡ Bơm 200 tin Kafka Live`** ngay trên sơ đồ.

### 4.3. Chạy Trọn vẹn Pipeline 4 Bước, Kiểm chứng Hồi phục `WAL` & Kiểm thử Tự động
```powershell
python -m pip install -r requirements.txt
python pipeline.py
python baseline.py
python starter_stream.py --kill-after-batch 1
uvicorn toc_backend.main:app --host 127.0.0.1 --port 8000
python -m pytest -v
python -m ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
```
> *Trạng thái kiểm thử (`Gate 3 PASS`):* Toàn bộ bộ kiểm thử tự động `pytest` (`tests/test_platform_v3.py` và `tests/test_quyen_spark.py`) cùng hồ sơ kiểm định Session 9 (`evidence/session9/`) đã được đồng bộ hoàn toàn với chuẩn thực nghiệm sạch không rò rỉ dữ liệu (`84,206` đoạn cơ giới, `33,394` cửa sổ, `1,885` cặp `TRAIN` hữu cơ, `RF 23D ROC-AUC = 0.9894`, `IXSCAN = 0.360 / 0.603 ms`, ứng dụng web duy nhất `:8000`).

---

## 5. Danh mục Tệp Kết quả Chính (`data/` & `outputs/`)

| Tệp kết quả | Mô-đun | Nội dung Thực tế (Leak-Free Ground Truth) |
|---|---|---|
| `data/dataset.csv` | `[C14]` Hồ dữ liệu | Bảng chuẩn hóa (**`33,394` dòng × `60` cột**, có `is_synthetic_burst`), kèm `PageRank`, `K-Means`, priors $\tau < t$ và nhãn `target_t30m_*` |
| `data/lakehouse/bronze_raw_events/events.parquet` | `[C14]` Bronze Raw | **`90,787` bản tin GPS gốc sạch** (`4` cột) đọc thông suốt bởi cả PyArrow/Pandas và Spark 3.5.7 |
| `data/lakehouse/stream_replay_windows/replay.parquet` | `[C14]` Replay Buffer | **`33,727` bản ghi cửa sổ phát luồng** (`45` cột, gồm `333` bản trùng và `500` bản trễ ngoài watermark) |
| `data/lakehouse/curated_gold/year_month=*` | `[C14]` Gold Phân vùng | `10` phân vùng `Parquet` theo tháng (`2020-07` đến `2021-04`, tổng `33,394` dòng × `86` cột; `2020-08` có `7,268` dòng) |
| `data/lakehouse/segment_static_features.parquet` | `[C13b]` Đặc trưng bản đồ | Kho tĩnh phủ kín **`84,206` đoạn đường cơ giới** (`23` cột: `9,995` có GPS + `74,211` Cold-Start `88.13%`; `SHA-256 = 75328ac2d5e3...`; mẫu gọn `f26ebdf7d396...`) |
| `data/lakehouse/quarantine_events.parquet` | `[C15]` Cách ly lỗi | **`580` bản ghi GIS** vi phạm quy tắc chất lượng kèm mã lỗi đầu tiên `first_failing_rule` (`9` cột) |
| `outputs/model.joblib` & `model_registry_manifest.json` | `[C13a]` Kho mô hình | Mô hình `Random Forest (23D)` (`rf_full_23d_v3`) khóa cố định từ `Pha A` kèm chữ ký `SHA-256` (`model = a2f411846508...`, `vmax = ee7f692daad8...`) |
| `outputs/ablation_results.csv` & `transition_evaluation.csv` | `[C11]` Học máy `+30p` | Bảng thử nghiệm 7 cấu hình (`ROC-AUC = 0.9894`, `PR-AUC = 0.9543`, `Severe F1 = 0.9362`) & kiểm chứng chuyển trạng thái + Masked-GPS Cold-Start (`58.63%`) |
| `outputs/road_pagerank_top.csv`, `co/lagged_congestion_rules.csv` | `[C10]` Đồ thị & Luật | Top-50 PageRank (`#1 21969 Nguyễn Kiệm PR = 2.7799`), `12` luật đồng kẹt (`Lift 15.23`) & `14` luật trễ (`Lift 24.0`, `persistence_controlled = True`) |
| `outputs/spark_runtime_evidence.json` & `quyen_validation/*` | `[C9]` Spark 3.5.7 | `BroadcastHashJoin` (`5 → 1` Exchange, `166.18 → 123.37 ms`, `25.76%` nhanh hơn, `0 exceptAll diff`), Monoid `97.92 vs 185.78 ms`, MLlib organic (`32 / 3,609 / 1,885`), Sink 2 watermark `2020-12-01T12:30:00Z` |
| `outputs/mongo_verify.json` & `query_progress.json` | `[C16]` MongoDB & WAL | `33,394` docs (`1,234` cảnh báo trục chính), atomic `$status_version` guard (`25/25` chặn `v1`), `IXSCAN warm median = 0.360 ms / p95 = 0.603 ms` (`31.36x` vs COLLSCAN) & `WAL = 73.7 KB` |
