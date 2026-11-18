# Hệ Thống Giám Sát Hiện Trạng & Dự Báo Ùn Tắc Giao Thông Đô Thị TP.HCM

> **Bộ tài liệu & hiện vật chính thức của đồ án (`BDA501 — Big Data Analytics`, Wave 2 Verified):**
> - **Báo cáo & Đặc tả Kỹ thuật Markdown (Đồng bộ Wave 2):** [`TECHNICAL_NOTE.md`](./TECHNICAL_NOTE.md) · [`product/00-proposal/project-proposal.md`](./product/00-proposal/project-proposal.md) · [`product/README.md`](./product/README.md)
> - **Hướng dẫn Thiết lập Dữ liệu & Nhánh Spark Data Engineering:** [`docs/DATA_SETUP.md`](./docs/DATA_SETUP.md) · [`docs/QUYEN_DATA_ENGINEERING.md`](./docs/QUYEN_DATA_ENGINEERING.md) · [`docs/HUONG_DAN_BRANCH_QUYEN_VI.md`](./docs/HUONG_DAN_BRANCH_QUYEN_VI.md)
> - **Gói Mẫu Chạy Nhanh & Giao diện Điều hành:** [`data/demo_sample/README.md`](./data/demo_sample/README.md) · Web TOC `ReactJS + FastAPI (:8000)` · Dashboard `Streamlit (:8501)`
> - **Báo cáo PDF, Slide Thuyết Trình & Hồ Sơ Kiểm Định Session 9 (Đồng bộ Wave 2):** [`HCMC_Smart_Traffic_Final_Report.pdf`](./HCMC_Smart_Traffic_Final_Report.pdf), [`HCMC_Smart_Traffic_Final_Presentation.pptx`](./HCMC_Smart_Traffic_Final_Presentation.pptx), [`HCMC_Smart_Traffic_Final_Slides.pdf`](./HCMC_Smart_Traffic_Final_Slides.pdf), [`PRESENTATION_SCRIPT.md`](./PRESENTATION_SCRIPT.md), [`SESSION9_TECHNICAL_AUDIT_REPORT.md`](./SESSION9_TECHNICAL_AUDIT_REPORT.md), [`evidence/session9/I_readiness.md`](./evidence/session9/I_readiness.md), [`BDA501_Session9_Lab_Manual.docx`](./BDA501_Session9_Lab_Manual.docx)

---

## 1. Bức Tranh Tổng Thể Trong 4 Bước Nối Tiếp Nhau (Wave 2 Verified)

Mục tiêu của hệ thống là **giám sát hiện trạng và dự báo trước `30 phút` nguy cơ kẹt xe trên toàn mạng lưới `84,206` đoạn đường cơ giới một chiều tại TP.HCM** (làm sạch từ `84,633` đoạn đường thô) từ dữ liệu hành trình GPS của xe cộ (ví dụ: lúc `07:08` sáng trên đoạn đường Lý Thường Kiệt mã `47407` tại Quận 10, vận tốc đo được ở mức `4 km/h`). Hệ thống đối chiếu song song thang đo **BkTraffic Urban 5 km/h LOS** (`derive_urban_los`, khớp **`89.19%`** với nhãn tham chiếu `train.csv`) và hàm tiện ích theo tỷ lệ vận tốc `derive_los` (`0.80/0.65/0.50/0.35/0.20`), chuẩn hóa 6 mức phục vụ giao thông (`LOS A–F`): `LOS A–B` (thông thoáng), `LOS C–D` (đông đúc) và `LOS E–F` (ùn ứ nghiêm trọng đến kẹt cứng).

Để vượt qua 4 rào cản lớn của dữ liệu thực tế (**`88.13%` đoạn đường nhánh không có GPS & thiếu tốc độ tối đa**, **tín hiệu gửi đứt quãng**, **đợt burst tổng hợp `7,192` cửa sổ tháng 08/2020**, và **bản tin trùng/trễ mạng**), toàn bộ hệ thống vận hành qua **4 bước nhân quả khép kín**:

1. **Bước 1 — Chuẩn hóa bức tranh hiện trạng (`Spark Batch ETL` & `Monoid`):**
   - Lọc bỏ `427` thực thể phi đường cơ giới (`R2_NON_ROAD_OSM_TAG`) khỏi `84,633` đoạn thô để còn **`84,206` đoạn đường cơ giới sạch** (`580` bản ghi GIS cách ly theo lỗi đầu tiên vào `quarantine_events.parquet`, sửa trung vị `27.0m` cho `154` đoạn `length <= 0`).
   - Tiếp nhận **`90,938` bản tin GPS thô** $\to$ **`90,787` bản tin hợp lệ** (cách ly `151` bản tin rơi vào đoạn phi đường bộ, `0` mất mát ngầm), gom bằng **Pure 5-Tuple Monoid** `(cnt, sum_v, min_v, max_v, severe_cnt)` kết hợp chia `8` giỏ khóa (`684.95x` lệch tải khung giờ $\to$ `1.0197x` giỏ cao điểm; `salted_partition_skew_ratio = 1.0123x`, `CV = 0.0059`) thành **`33,394` cửa sổ 30 phút UTC** khớp **`100.0%`** [`baseline.py`](./baseline.py) (`0` sai lệch trên cả 5 đại lượng + vận tốc trung bình, `max_abs_diff = 0.0 km/h`).
   - Thực thi trên **`Apache Spark 3.5.7 (hcmc-traffic-spark:3.5.7)`**: `Catalyst SortMergeJoin` (`5` Exchange, `145.24 ms`) $\to$ **`BroadcastHashJoin` (`1` Exchange, `120.45 ms`, `17.07%` nhanh hơn, `0 exceptAll diff`)**; `Monoid` `147.11 ms` vs `Salted` `222.60 ms` (`59,714` nhóm Stage-1, `0 exceptAll diff`).
2. **Bước 2 — Lấp đầy "vùng mù" `88.13%` đường thiếu GPS (`PageRank`, `Rules` & `K-Means`):**
   - Lọc bỏ **`525,786` điểm uốn cong trung gian (`90.97%`)** từ `577,967` nút, giữ **`52,181` giao lộ hoạt động**; dựng đồ thị nối đoạn đường có hướng (đã loại cung quay đầu U-turn trên đoạn hai chiều) gồm **`92,453` cạnh chuyển tiếp** và **`314` đoạn cụt (`dangling nodes`)**. Nút cổ chai `#1 PageRank` ($\beta = 0.85$): **`21969 — Nguyễn Kiệm (level=2, PR=2.7799)`**.
   - Khai phá luật kết hợp **chỉ trên tập `TRAIN` hữu cơ (non-burst)** kèm kiểm chứng hold-out: **`12` luật đồng kẹt xe** (`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.2308`, `Tô Hiến Thành ↔ Lý Thường Kiệt` lặp lại `20` khung hold-out) và **`14` luật lan truyền trễ `+30m`** kiểm soát tính dai dẳng (`persistence_controlled = True`, đứng đầu `Lê Quang Định → Trần Quang Khải`, `Lift = 24.0`).
   - Phân nhóm **`K-Means` ($k=6$)** học trên **`3,609` đoạn `TRAIN` hữu cơ** (`Silhouette = 0.2197, DBI = 1.3592`) để phủ kín **`74,211` đoạn đường thiếu GPS (`88.13%`)** trên tổng số **`84,206` đoạn** (`100%` độ phủ).
3. **Bước 3 — Xử lý Burst tổng hợp & Dự báo trung thực `+30 phút` (`Random Forest 23D`):**
   - Phát hiện **`7,192` cửa sổ burst tổng hợp** tại đúng 2 khung (`2020-08-02 23:30` và `2020-08-03 00:00`, mỗi khung `3,609` đoạn thô / `3,596` đoạn cơ giới sạch) và **loại bỏ `3,596` cặp burst** khi huấn luyện ML, giữ **`1,885` cặp `TRAIN` hữu cơ** (`50` mẫu biên bị cắt), **`1,167` `VAL`** (`5` cắt biên) và **`1,182` `TEST`** (`185` mẫu kẹt nặng `E/F`), tổng **`7,885` cặp đúng `+30 phút`**.
   - Huấn luyện sạch **không dùng `sample_weight` nhân tạo hay `fallback bypass`** (`class_weight` lũy thừa `0.65`). Trên `VAL`, **`Random Forest 23D`** thắng cổng chọn mô hình (`VAL composite = 0.8990` so với `0.5343` của `14D`; `VAL acc = 0.9023`).
   - Trên **`TEST` (`1,182` mẫu)**: **`ROC-AUC = 0.9900`** (`95% CI [0.9836, 0.9953]`), **`PR-AUC = 0.9595`**, **`Acc = 0.9289`**, **`Macro-F1 = 0.4475`** *(phản ánh trung thực mất cân bằng lớp trung gian `B=10, C=15, D=13, E=20`)*, **`Severe F1 = 0.9301`** (`Prec = 0.9251, Rec = 0.9351`, `95% CI [0.9012, 0.9547]`), đường chéo ma trận nhầm lẫn 6 lớp **`[937, 4, 1, 7, 2, 147]`** (đủ 6 lớp `> 0`). Khi đổi trạng thái (`61` mẫu), vượt `Persistence` (`McNemar p = 0.015625`, `7 vs 0`), `Transition Detect ROC-AUC = 0.9146` (vs heuristic `0.7921`), `Onset risk lift = 3.32x`, và phủ `100%` kiểm thử `Masked-GPS Spatial Cold-Start Proxy` (`84,206` tổng / `74,211` cold-start).
4. **Bước 4 — Cảnh báo trực tuyến (`33 KiB`), Tự động bù tin trễ & Serving dưới `1 ms`:**
   - Tiếp nhận qua **`Apache Kafka 4.1.0 KRaft`** (`3` phân vùng), lọc trùng & đếm đường bằng **`32.0 KiB RotatingBloom` (`2 × 16 KiB`) + `1.0 KiB 64-bit HyperLogLog` (`p=10`)** + **`EWMA`** ($\alpha = 0.15$), tự hồi phục sự cố với **`WAL = 73.7 KB`** (`73,713 B`, `0` nhân đôi khi restart).
   - Lưu **`Medallion Lakehouse`** (`bronze_raw_events` `90,787` pings, `stream_replay_windows` `33,727`, `silver_cleaned_windows` `33,394`, `curated_gold/year_month=*` `10` phân vùng đọc sạch bởi cả `PyArrow/Pandas` và `Spark 3.5.7`), kiểm chứng **Spark Streaming Sink 2** có watermark thực `2020-12-01T12:30:00Z`.
   - Đối soát 2 tầng: **`tier1_exceptAll_diff = 0` (`100.0%` trong watermark)**, bù **`500`** cửa sổ đoạn đường trễ (`226` khung giờ, hoàn thiện `98.5% -> 100.0%`), chữa lành **`609` cửa sổ hạ lưu (`healed_downstream_lag_windows`)**, chấm điểm lại **`384`** cửa sổ (`185` đổi xác suất).
   - Phục vụ trên **`MongoDB 8.0.34`** (`33,394` docs, atomic `$status_version` pipeline guard chặn `25/25` bản ghi `v1` cũ, **`IXSCAN warm median = 0.389 ms / p95 = 0.692 ms`** vs `COLLSCAN median = 9.161 ms`, nhanh hơn **`23.55x`**) cùng **`Web TOC ReactJS + FastAPI (:8000)`** và **`Streamlit 5-Tab UI (:8501)`**.

---

## 2. Nhóm Thực hiện, Giảng viên Hướng dẫn & Phân công Chuyên trách

- **Đơn vị & Học phần:** `FSB · FPT University | MSE AI | BDA501 – Big Data Analytics` (`Fall 2026`)
- **Giảng viên hướng dẫn (Instructor):** **Đặng Trần Khánh**

| Thành viên | MSSV | Mảng Chuyên trách trong Dây chuyền 4 Bước | Mã nguồn Cốt lõi |
|---|---|---|---|
| **Nguyễn Tạ Minh Quyền** | `25MS23280` | Bước 1: Làm sạch dữ liệu, ước lượng tốc độ tối đa & xử lý phân tán Spark | `spark_batch.py`, `traffic_data/spark_etl.py`, `baseline.py`, `compose.quyen.yml` |
| **Nguyễn Thành Đạt** | `25MS23306` | Bước 2 & Bước 3: Đồ thị `PageRank`, luật `SON`, phân nhóm `K-Means`, lọc burst & dự báo `+30 phút` | `pipeline.py`, `spark_engine.py`, `tests/` |
| **Phạm Công Minh** | `25MS23310` | Bước 4: Luồng `Kafka`, bộ đếm `33 KiB`, đối soát tin trễ, `MongoDB 8.0`, `Web TOC (:8000)` & `Streamlit (:8501)` | `producer.py`, `starter_stream.py`, `live_kafka_stream.py`, `toc_backend/`, `toc_frontend/`, `app.py`, `docker-compose.yml` |

---

## 3. Cấu Trúc Thư Mục Thực Tế & Năm Bảng Dữ Liệu Nguồn (`raw_data/`)

### 3.1. Cấu trúc Thư mục Chính của Repository
```text
groupexe-knowledgetest/
├── README.md, TECHNICAL_NOTE.md          # Tài liệu tổng quan & đặc tả toán học (đồng bộ Wave 2)
├── pipeline.py, spark_engine.py          # Pipeline V3 (Zones A–F) & kiểm chứng runtime Spark 3.5.7 / MongoDB 8.0
├── spark_batch.py, baseline.py           # Đường Spark Batch độc lập (Quyền) & đối chứng Monoid thuần Python
├── producer.py, starter_stream.py        # Kafka producer, WAL Checkpoint crash-recovery & RotatingBloom/64-bit HLL
├── live_kafka_stream.py, app.py          # Consumer Kafka liên tục & Dashboard Streamlit 5-Tab (:8501)
├── toc_backend/, toc_frontend/           # Web TOC thế hệ mới: FastAPI (:8000) + ReactJS/TypeScript + Leaflet
├── traffic_data/                         # Module ETL dùng chung PySpark 3.5.7 / 4.2.0 (`spark_etl.py`)
├── docker-compose.yml, compose.quyen.yml # Cụm 6 dịch vụ mặc định + 2 profile batch/stream; Compose batch riêng
├── Dockerfile, Dockerfile.spark35        # Image app/TOC Python & Spark 3.5.7 (`hcmc-traffic-spark:3.5.7`)
├── Dockerfile.spark                      # Image PySpark + OpenJDK 17 (`python:3.11-slim-bookworm`) cho `compose.quyen.yml`
├── docs/                                 # Hướng dẫn thiết lập dữ liệu & bàn giao Data Engineering
├── product/                              # Hệ thống quản trị sản phẩm 10 pha (`00-proposal/` .. `09-growth/`)
├── tests/                                # `test_platform_v3.py` (35 hàm) & `test_quyen_spark.py` (5 hàm) = 40 bài kiểm thử
├── scripts/, evidence/session9/          # Script dựng báo cáo/slide & hồ sơ kiểm định Session 9
├── data/                                 # `dataset.csv` (33,394 × 60), `lakehouse/`, và gói mẫu `demo_sample/`
└── outputs/                              # `metrics.json`, `model.joblib`, `mongo_verify.json`, `spark_runtime_evidence.json`, ...
```

### 3.2. Năm Bảng Dữ liệu Nguồn (`raw_data/`) & Vai trò `Proof-of-Scale`

> **Vai trò mẫu chuẩn kiểm chứng quy mô (`Proof-of-Scale`):** Bộ dữ liệu công khai 5 bảng (`~38.50 MB`) đóng vai trò mẫu chuẩn để đối chứng sai số toán học bằng `0.0` trước khi mở rộng ngang ra luồng GPS toàn TP.HCM (`> 1` triệu xe phát tín hiệu mỗi `5–10` giây $\approx$ `8–17` tỷ bản tin/ngày, `1.5–3.0 TB/ngày`).

| Tệp nguồn | Số bản ghi | Số cột | Dung lượng | Vai trò & Số liệu Thực tế sau Làm sạch Wave 2 |
|---|---:|---:|---:|---|
| `nodes.csv` | `577,967` | `3` | `19.44 MB` | Tọa độ `(long, lat)`; loại bỏ **`525,786` nút uốn cong/mồ côi (`90.97%`)**, giữ **`52,181` giao lộ hoạt động** |
| `streets.csv` | `5,553` | `5` | `0.20 MB` | Danh mục tuyến đường (`name`, `level` 1–4, `type`, `max_velocity`); chuẩn hóa `Unicode NFC` (`695 → 641` tên, gộp `54` biến thể) |
| `segments.csv` | `84,633` | `11` | `10.29 MB` | Lọc `427` đoạn phi đường bộ (`R2`) $\to$ **`84,206` đoạn cơ giới sạch** (`92,453` cạnh đồ thị, `314` đoạn cụt, `74,211` Cold-Start = `88.13%`) |
| `segment_status.csv` | `90,938` | `4` | `3.58 MB` | Bản tin GPS gốc $\to$ **`90,787` bản tin hợp lệ** (`151` cách ly phi đường bộ) $\to$ **`33,394` cửa sổ 30m UTC** |
| `train.csv` | `33,441` | `18` | `4.99 MB` | Bảng đối chứng tham chiếu độc lập (khớp **`89.19%`** với `BkTraffic urban 5 km/h LOS` và `100.0%` khóa cơ giới `33,394` cửa sổ) |

---

## 4. Hướng dẫn Chuẩn bị Dữ liệu, Khởi chạy Cụm Docker, Live Kafka Demo & Kiểm thử

> **Chuẩn bị dữ liệu (`raw_data/` & `data/demo_sample/`):** Để giữ kho mã nguồn gọn nhẹ, 5 tệp CSV gốc (`~38.50 MB`) đặt tại `raw_data/` theo hướng dẫn chi tiết trong [`docs/DATA_SETUP.md`](./docs/DATA_SETUP.md). Đồng thời, kho mã nguồn **đã đính kèm sẵn gói mẫu thực nghiệm [`data/demo_sample/`](./data/demo_sample/README.md)** để máy checkout mới có thể chạy ngay **Live Kafka Demo** và giao diện mà không cần tải trước `38.50 MB`.

### 4.1. Cụm 6 Dịch vụ Docker Mặc định, Web TOC (`ReactJS + FastAPI :8000`) & Streamlit (`:8501`)
`docker-compose.yml` cung cấp **6 dịch vụ mặc định** (`kafka`, `mongo`, `spark`, `toc-web`, `app`, `live-consumer` với tổng hạn mức RAM **`12.0 GB / 16.0 GB`**) cùng 2 dịch vụ chạy theo profile (`pipeline` `[batch]` và `stream` `[stream]`):
- **Cổng Điều hành Chính Web TOC (`ReactJS + TypeScript + FastAPI + Leaflet + Biểu đồ GPS thô ➔ t+30p`) tại <http://localhost:8000>:** Phục vụ 5 tab điều hành (`01 Điều Hành & Dự Báo +30p`, `02 Live Kafka & Sketches 33 KiB`, `03 PageRank & Luật Đồng Kẹt`, `04 6 Cụm K-Means & Cold-Start`, `05 Spark 3.5.7, Ablation & Đối Soát`), 4 kịch bản thực tế trên thanh Đồng hồ TOC và phát **`🎬 Phát Story qua Kafka`** / **`⚡ Stress-Test 200 tin`** trực tiếp qua `Kafka 4.1`.
- **Dashboard Kỹ thuật `Streamlit 5-Tab` tại <http://localhost:8501>:** Chạy song song phục vụ tra cứu và kiểm chứng đối soát chi tiết từ `data/` & `outputs/`.

```powershell
# Khởi chạy trọn bộ 6 dịch vụ mặc định (kafka, mongo, spark, toc-web :8000, app :8501, live-consumer):
docker compose up -d --build

# Chạy tác vụ Spark Batch độc lập từ dữ liệu thô trên PySpark 4.2.0 / Java 21:
$env:TRAFFIC_FIT_CUTOFF = '2020-08-01T00:00:00Z'
docker compose -f compose.quyen.yml run --build --rm batch
```

### 4.2. Demo Luồng Thời Gian Thực Đầu–Cuối (`🎬 Story Replay` & `⚡ Stress-Test 200 tin` qua `Kafka 4.1` → `MongoDB 8.0`)
1. Mở **Web TOC** tại <http://localhost:8000> → chọn kịch bản thực tế trên thanh Đồng hồ TOC (mặc định: *Kịch bản 1 TEST — Nguyễn Bỉnh Khiêm `#21262` & Trương Định `#21263` chuyển `E ➔ F`*).
2. Nhấn nút **`🎬 Phát Story qua Kafka`** ngay trên thanh đầu trang (hoặc chạy Terminal: `python producer.py --mode story --scenario noon_transition --bootstrap-servers localhost:9092`): từng khung 30 phút đi thật qua `Kafka 4.1 (3 Partitions)` $\to$ `live-consumer (32 KiB RotatingBloom + 1 KiB 64-bit HLL + Frozen 23D RF)` $\to$ `MongoDB 8.0`, cập nhật Đồng hồ TOC và bản đồ theo thời gian thực.
3. Nhấn nút **`⚡ Stress-Test 200 tin`** (hoặc chạy `python producer.py --mode window --bootstrap-servers localhost:9092 --topic hcmc-traffic-events --max-messages 200 --replay-speed 0.2`) để kiểm chứng đa phân vùng, lọc trùng `Bloom` và `Watermark 30m`.

### 4.3. Chạy Trọn vẹn Pipeline 4 Bước, Kiểm chứng Hồi phục `WAL` & Kiểm tra Mã nguồn
```powershell
python -m pip install -r requirements.txt
python pipeline.py
python baseline.py
python starter_stream.py --kill-after-batch 1
python -m ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
streamlit run app.py
```
> *Lưu ý trung thực về bộ kiểm thử (`tests/`):* `tests/test_quyen_spark.py` (`5` hàm kiểm thử Spark) và `tests/test_platform_v3.py` (`23` hàm kiểm thử nền tảng) được quản lý ở nhánh kiểm thử riêng; sau đợt hiệu chỉnh dữ liệu sạch Wave 2 (`84,206` đoạn cơ giới, `33,394` cửa sổ, lọc burst `7,192`, bỏ `sample_weight`), một số assertion trong `tests/test_platform_v3.py` đang giữ ngưỡng Wave 1 cũ nên chỉ trích dẫn kết quả `pytest` đầy đủ sau khi track kiểm thử đồng bộ xong.

---

## 5. Danh mục Tệp Kết quả Chính (`data/` & `outputs/`)

| Tệp kết quả | Mô-đun | Nội dung Thực tế (Wave 2 Verified) |
|---|---|---|
| `data/dataset.csv` | `[C14]` Hồ dữ liệu | Bảng chuẩn hóa (**`33,394` dòng × `60` cột**, có `is_synthetic_burst`), kèm `PageRank`, `K-Means`, priors $\tau < t$ và nhãn `target_t30m_*` |
| `data/lakehouse/bronze_raw_events/events.parquet` | `[C14]` Bronze Raw | **`90,787` bản tin GPS gốc sạch** (`4` cột) đọc thông suốt bởi cả PyArrow/Pandas và Spark 3.5.7 |
| `data/lakehouse/stream_replay_windows/replay.parquet` | `[C14]` Replay Buffer | **`33,727` bản ghi cửa sổ phát luồng** (`45` cột, gồm `333` bản trùng và `500` bản trễ ngoài watermark) |
| `data/lakehouse/curated_gold/year_month=*` | `[C14]` Gold Phân vùng | `10` phân vùng `Parquet` theo tháng (`2020-07` đến `2021-04`, tổng `33,394` dòng × `86` cột; `2020-08` có `7,268` dòng) |
| `data/lakehouse/segment_static_features.parquet` | `[C13b]` Đặc trưng bản đồ | Kho tĩnh phủ kín **`84,206` đoạn đường cơ giới** (`23` cột: `9,995` có GPS + `74,211` Cold-Start `88.13%`) |
| `data/lakehouse/quarantine_events.parquet` | `[C15]` Cách ly lỗi | **`580` bản ghi GIS** vi phạm quy tắc chất lượng kèm mã lỗi đầu tiên `first_failing_rule` (`9` cột) |
| `outputs/model.joblib` & `model_registry_manifest.json` | `[C13a]` Kho mô hình | Mô hình `Random Forest (23D)` (`rf_full_23d_v3`) khóa cố định từ `Pha A` kèm chữ ký `SHA-256` (`a2e12b7c032a...`) |
| `outputs/ablation_results.csv` & `transition_evaluation.csv` | `[C11]` Học máy `+30p` | Bảng thử nghiệm 7 cấu hình (`ROC-AUC=0.9900`, `PR-AUC=0.9595`) & kiểm chứng chuyển pha + Masked-GPS Cold-Start |
| `outputs/road_pagerank_top.csv`, `co/lagged_congestion_rules.csv` | `[C10]` Đồ thị & Luật | Top-50 PageRank (`#1 21969 Nguyễn Kiệm PR=2.7799`), `12` luật đồng kẹt (`Lift 15.23`) & `14` luật trễ (`Lift 24.0`, `persistence_controlled=True`) |
| `outputs/spark_runtime_evidence.json` & `quyen_validation/*` | `[C9]` Spark 3.5.7 | `BroadcastHashJoin` (`5 → 1` Exchange, `145.24 → 120.45 ms`, `0 exceptAll diff`), Monoid `147.11 vs 222.60 ms`, Sink 2 watermark `2020-12-01T12:30:00Z` |
| `outputs/mongo_verify.json` & `query_progress.json` | `[C16]` MongoDB & WAL | `33,394` docs, atomic `$status_version` guard, `IXSCAN warm median=0.389 ms / p95=0.692 ms` (`23.55x` vs COLLSCAN) & `WAL=73.7 KB` |
