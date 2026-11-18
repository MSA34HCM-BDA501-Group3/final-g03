# Hệ Thống Giám Sát Hiện Trạng & Dự Báo Ùn Tắc Giao Thông Đô Thị TP.HCM

> **Bộ tài liệu chính thức của đồ án (`BDA501 — Big Data Analytics`):**
> - **Báo cáo Tổng kết Đồ án & Đặc tả Kỹ thuật Chi tiết:** [`HCMC_Smart_Traffic_Final_Report.pdf`](./HCMC_Smart_Traffic_Final_Report.pdf) & [`product/00-proposal/project-proposal.md`](./product/00-proposal/project-proposal.md)
> - **Bộ Slide Thuyết trình Trực quan (`17 Slides`):** [`HCMC_Smart_Traffic_Final_Presentation.pptx`](./HCMC_Smart_Traffic_Final_Presentation.pptx) & [`HCMC_Smart_Traffic_Final_Slides.pdf`](./HCMC_Smart_Traffic_Final_Slides.pdf)
> - **Kịch bản Thuyết trình, Live Demo & Cẩm nang Phản biện Q&A:** [`PRESENTATION_SCRIPT.md`](./PRESENTATION_SCRIPT.md)
> - **Hồ sơ Kiểm định Kỹ thuật Session 9 (`A–I` & `Card B`):** [`SESSION9_TECHNICAL_AUDIT_REPORT.md`](./SESSION9_TECHNICAL_AUDIT_REPORT.md), [`evidence/session9/I_readiness.md`](./evidence/session9/I_readiness.md) & [`BDA501_Session9_Lab_Manual_Completed.docx`](./BDA501_Session9_Lab_Manual_Completed.docx)
> - **Hướng dẫn Chuẩn bị Dữ liệu & Giao diện Tĩnh 5-Tab:** [`docs/DATA_SETUP.md`](./docs/DATA_SETUP.md), [`data/demo_sample/README.md`](./data/demo_sample/README.md) & [`ui-demo.html`](./ui-demo.html)
> - **Đặc tả Toán học & Kết quả Kiểm chứng Chuyên sâu:** [`TECHNICAL_NOTE.md`](./TECHNICAL_NOTE.md) & [`docs/QUYEN_DATA_ENGINEERING.md`](./docs/QUYEN_DATA_ENGINEERING.md)

## 1. Bức Tranh Tổng Thể Trong 4 Bước Nối Tiếp Nhau

Mục tiêu của hệ thống là **giám sát hiện trạng và dự báo trước `30 phút` nguy cơ kẹt xe trên toàn mạng lưới `84,633` đoạn đường một chiều tại TP.HCM** từ dữ liệu hành trình GPS của xe cộ (ví dụ: lúc `07:08` sáng trên đoạn đường Lý Thường Kiệt số `47407` tại Quận 10, một chiếc xe gửi về tín hiệu báo vận tốc đang nhích ở mức `4 km/h`). Hệ thống chuẩn hóa trạng thái đường phố theo **6 mức phục vụ giao thông (`LOS A–F`)**: `LOS A–B` (`≥ 65%` tốc độ tối đa: thông thoáng), `LOS C–D` (`35%–65%`: đông đúc) và `LOS E–F` (`< 35%`: kẹt cứng).

Để vượt qua 3 rào cản lớn của dữ liệu thực tế (**`88.15%` đoạn đường nhánh không có GPS & thiếu tốc độ tối đa**, **tín hiệu gửi đứt quãng**, và **bản tin trùng/trễ mạng**), toàn bộ hệ thống vận hành qua **4 bước nhân quả khép kín**:
1. **Bước 1 — Chuẩn hóa bức tranh hiện trạng (`Spark Batch ETL`):** Làm sạch `90,938` bản tin GPS thô, tự động ước lượng tốc độ tối đa cho từng loại đường (chỉ học trên tập `TRAIN`), và gom song song thành `33,441` cửa sổ 30 phút bằng bộ gộp 5 đại lượng kết hợp chia `8` giỏ khóa chống lệch tải (`687.43x → 1.023x`) trên **`Apache Spark 3.5.7 (Java 17)`** (khớp `100%` [`baseline.py`](./baseline.py)).
2. **Bước 2 — Lấp đầy "vùng mù" bằng cấu trúc bản đồ (`PageRank` & `K-Means`):** Lọc bỏ `90.93%` điểm uốn cong trung gian trên bản đồ, xếp hạng nút cổ chai giao lộ bằng **`PageRank`** (`84,633` đoạn đường), tìm các cặp đường hay kẹt cùng lúc (`Spark FPGrowth` & `2-Pass SON`), và gom nhóm các đoạn đường có đặc điểm giống nhau bằng **`K-Means` ($k=6$)** phủ kín **`74,606` đoạn đường thiếu GPS (`88.15%`)** — vừa phục vụ Sở GTVT, vừa truyền làm biến đầu vào cho Bước 3.
3. **Bước 3 — Dự báo trước `30 phút` (`Random Forest` 23 biến):** Ghép cặp quan sát cách nhau đúng `30 phút` (`7,898` cặp) kèm cắt sạch mẫu biên thời gian, kết hợp tốc độ hiện tại, khung giờ Việt Nam và đặc trưng bản đồ ở Bước 2 để huấn luyện mô hình duy nhất **`Random Forest (23 biến)`** đạt **`ROC-AUC = 0.9882`**, **`PR-AUC = 0.9471`**, phát hiện sớm tín hiệu đổi trạng thái (`ROC-AUC = 0.7664`) và chấm điểm đủ `100%` mạng lưới thành phố.
4. **Bước 4 — Cảnh báo trực tuyến & Tự động bù dữ liệu trễ (`Kafka Streaming`, `MongoDB` & `Dashboard`):** Tiếp nhận luồng qua **`Apache Kafka 4.1.0 KRaft`**, lọc trùng và theo dõi điểm nóng kẹt dai dẳng hiện tại (`EWMA`) chỉ trong **`33.0 KiB` bộ nhớ cố định** (`32.0 KiB Bloom` + `1.0 KiB HyperLogLog++`), phản hồi cảnh báo dưới `1 ms` (**`0.636 ms`**) trên **`MongoDB 8.0.34`** & **`Streamlit 5-Tab UI`** (`:8501`), đồng thời cuối ngày tự động bù đủ `501` cửa sổ đến trễ để cập nhật lịch sử chính xác **`100%`**.

---

## 2. Nhóm Thực hiện, Giảng viên Hướng dẫn & Phân công Chuyên trách

- **Đơn vị & Học phần:** `FSB · FPT University | MSE AI | BDA501 – Big Data Analytics` (`Fall 2026`)
- **Giảng viên hướng dẫn (Instructor):** **Đặng Trần Khánh**

| Thành viên | MSSV | Mảng Chuyên trách trong Dây chuyền 4 Bước | Mã nguồn Cốt lõi |
|---|---|---|---|
| **Nguyễn Tạ Minh Quyền** | `25MS23280` | Bước 1: Làm sạch dữ liệu, ước lượng tốc độ tối đa & xử lý phân tán Spark | `spark_batch.py`, `traffic_data/spark_etl.py`, `baseline.py`, `compose.quyen.yml` |
| **Nguyễn Thành Đạt** | `25MS23306` | Bước 2 & Bước 3: Đồ thị `PageRank`, luật `SON`, phân nhóm `K-Means` & dự báo `+30 phút` | `pipeline.py`, `spark_engine.py`, `tests/` |
| **Phạm Công Minh** | `25MS23310` | Bước 4: Luồng `Kafka`, bộ đếm `33 KiB`, đối soát tin trễ, `MongoDB 8.0` & giao diện `Streamlit` | `producer.py`, `starter_stream.py`, `app.py`, `docker-compose.yml`, `scripts/` |

---

## 3. Năm Bảng Dữ liệu Nguồn (`raw_data/`) & Vai trò Mẫu Chuẩn `Proof-of-Scale`

> **Vai trò mẫu chuẩn kiểm chứng quy mô (`Proof-of-Scale`):** Bộ dữ liệu công khai 5 bảng (`~38.50 MB`) đóng vai trò mẫu chuẩn để đối chứng sai số toán học bằng `0.0` trước khi mở rộng ngang ra luồng GPS toàn TP.HCM (`> 1` triệu xe phát tín hiệu mỗi `5–10` giây $\approx$ `8–17` tỷ bản tin/ngày, `1.5–3.0 TB/ngày`).

| Tệp nguồn | Số bản ghi | Số cột | Dung lượng | Vai trò trong Hệ thống |
|---|---:|---:|---:|---|
| `nodes.csv` | 577,967 | 3 | 19.44 MB | Tọa độ `(long, lat)`; lọc bỏ `525,542` điểm uốn cong trung gian (`90.93%`), giữ `52,425` giao lộ thực tế |
| `streets.csv` | 5,553 | 5 | 0.20 MB | Danh mục tuyến đường (`name`, `level` 1–4, `type`, `max_velocity`) dùng cho `BroadcastHashJoin` |
| `segments.csv` | 84,633 | 11 | 10.29 MB | Mạng lưới `84,633` đoạn đường một chiều (`151,929` hướng rẽ hợp lệ, `48` đoạn đường cụt) |
| `segment_status.csv` | 90,938 | 4 | 3.58 MB | Bản tin vận tốc GPS thực tế dùng để gom `33,441` cửa sổ 30 phút (`33,394` đường cơ giới) & phát luồng |
| `train.csv` | 33,441 | 18 | 4.99 MB | Bảng đối chứng độc lập dùng để kiểm tra độ khớp `100.0%` khóa cửa sổ `(segment_id, date, period)` |

---

## 4. Hướng dẫn Chuẩn bị Dữ liệu, Khởi chạy, Live Kafka Demo & Kiểm thử Tự động

> **Chuẩn bị dữ liệu (`raw_data/` & `data/demo_sample/`):** Để giữ kho mã nguồn gọn nhẹ, 5 tệp CSV gốc (`~38.50 MB`) đặt tại `raw_data/` theo hướng dẫn chi tiết trong [`docs/DATA_SETUP.md`](./docs/DATA_SETUP.md). Đồng thời, kho mã nguồn **đã đính kèm sẵn gói mẫu thực nghiệm [`data/demo_sample/`](./data/demo_sample/README.md) (`238 KB`)** để bất kỳ máy checkout mới nào cũng chạy ngay được **Live Kafka Demo**, kiểm định **`Card B (B1/B2/B3)`** và giao diện mà không cần tải trước `38.50 MB`.

### 4.1. Giao diện Tĩnh 5-Tab Không Cần Cài Đặt (`ui-demo.html`) & Streamlit Dashboard (`:8501`)
- **Xem nhanh tức thì không cần cài Python/Docker:** Mở trực tiếp tệp [`ui-demo.html`](./ui-demo.html) bằng trình duyệt để trải nghiệm giao diện 5 tab có bộ lọc tương tác cơ bản.
- **Khởi chạy cụm Docker đầy đủ kèm Consumer Kafka thời gian thực (`10.5 / 16.0 GB RAM`):**
```powershell
docker compose --profile live up -d

# Chạy tác vụ Spark Batch độc lập từ dữ liệu thô trên Apache Spark 3.5.7 (Java 17):
$env:TRAFFIC_FIT_CUTOFF = '2020-08-01T00:00:00Z'
docker compose -f compose.quyen.yml run --build --rm batch
```

### 4.2. Demo Luồng Thời Gian Thực Đầu–Cuối (`producer.py` → `Kafka 4.1` → `Consumer` → `MongoDB 8.0` → `Streamlit :8501`)
1. Đảm bảo cụm container đang chạy (`docker compose --profile live up -d`) và mở <http://localhost:8501> → chuyển sang **`Tab 2: Dự báo +30 Phút & MongoDB`** → bật ô **`⏱️ Tự động cập nhật mỗi 2 giây`** tại mục **`2B. 📡 Live Kafka Stream Monitor`**.
2. Mở một cửa sổ Terminal và phát trực tiếp `200` sự kiện lịch sử (`event-time replay`) qua broker `Kafka 4.1.0 KRaft` (`3 partitions`):
```powershell
python producer.py --mode window --bootstrap-servers localhost:9092 --topic hcmc-traffic-events --max-messages 200 --replay-speed 0.2
```
3. Quan sát bộ đếm trên `Streamlit :8501` cập nhật trực tiếp từ `MongoDB 8.0` (`bda501.live_stream_scores` & `bda501.live_stream_telemetry`): `200` bản tin tiêu thụ, `4` bản tin phát trùng bị chặn bởi `32.0 KiB Bloom Filter`, `4` bản tin trễ `> 30m` chuyển sang `Tier-2 Reconciliation`, và `192` dự báo `STREAM_PROVISIONAL (v1)` được `Random Forest (23 biến)` chấm điểm trực tuyến. *(Lưu ý trình bày trung thực: đây là phát lại sự kiện GPS lịch sử qua Kafka theo thời gian thực để kiểm chứng luồng đầu–cuối, không phải nguồn viễn thông trực tiếp).*

### 4.3. Chạy Trọn vẹn Dây chuyền 4 Bước, Kiểm chứng Hồi phục `WAL` & Kiểm thử Tự động (`27/27 Passed`)
```powershell
python -m pip install -r requirements.txt
python pipeline.py
python baseline.py
python starter_stream.py --kill-after-batch 1
python scripts/run_session9_audit.py
python -m pytest -v
python -m ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
streamlit run app.py
```

Sau khi khởi chạy, mở trình duyệt tại <http://localhost:8501> để sử dụng giao diện điều hành 5 tab.

---

## 5. Danh mục Tệp Kết quả Chính (`data/` & `outputs/`)

| Tệp kết quả | Mô-đun | Nội dung |
|---|---|---|
| `data/dataset.csv` | `[C14]` Hồ dữ liệu | Bảng dữ liệu chuẩn hóa (`33,441` dòng × `59` cột) kèm `PageRank`, `K-Means`, biến quá khứ và nhãn `target_t30m_*` |
| `data/lakehouse/curated_gold/year_month=*` | `[C14]` Phân vùng tháng | `10` phân vùng `Parquet` theo tháng (`2020-07` đến `2021-04`, tháng `2020-08` có `7,294` dòng) |
| `data/lakehouse/segment_static_features.parquet` | `[C13b]` Đặc trưng bản đồ | Kho đặc trưng bản đồ tĩnh phủ kín `84,633` đoạn đường (`10,027` có GPS + `74,606` đoạn thiếu GPS) |
| `data/lakehouse/quarantine_events.parquet` | `[C15]` Cách ly lỗi | Bản ghi vi phạm quy tắc chất lượng kèm mã lỗi đầu tiên `first_failing_rule` |
| `outputs/model.joblib` & `model_registry_manifest.json` | `[C13a]` Kho mô hình | Mô hình `Random Forest (23 biến)` khóa cố định từ `Pha A` kèm chữ ký `SHA-256` |
| `outputs/ablation_results.csv` & `transition_evaluation.csv` | `[C11]` Học máy `+30p` | Bảng thử nghiệm 7 cấu hình và đối chứng trực diện khi giao thông đổi trạng thái |
| `outputs/spark_runtime_evidence.json` & `quyen_validation/*` | `[C9]` Spark 3.5.7 | Bằng chứng `BroadcastHashJoin`, `exceptAll == 0`, cắt tỉa `Parquet`, `FPGrowth` & `KMeans` |
| `outputs/mongo_verify.json` | `[C16]` MongoDB 8.0 | Bằng chứng ghi cập nhật 69 lô và truy vấn chỉ mục `IXSCAN` (`warm_p95 = 0.636 ms`) |
| `outputs/slide_charts/*` | Biểu đồ & Bản đồ | Bản đồ TP.HCM có sông Sài Gòn & tên quận, biểu đồ đối chứng trực diện & ảnh cận cảnh giao diện |
