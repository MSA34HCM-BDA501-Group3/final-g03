# Kịch Bản Thuyết Trình Song Song Live Demo (17 Slides + 4 Chặng Live Demo = 21 Phút Chuẩn)

- **Tên đề tài:** Hệ thống Giám sát Hiện trạng & Dự báo Ùn tắc Giao thông Đô thị TP. Hồ Chí Minh (*HCMC Smart Traffic Big Data Analytics & Congestion Prediction Platform*)
- **Đơn vị & Học phần:** `FSB · FPT University | MSE AI | BDA501 – Big Data Analytics` (`Fall 2026`)
- **Giảng viên hướng dẫn (Instructor):** **Đặng Trần Khánh**
- **Bộ slide trình chiếu đi kèm:** `HCMC_Smart_Traffic_Final_Presentation.pptx` & `HCMC_Smart_Traffic_Final_Slides.pdf` (`17 Slides`: Cấu trúc 12 Phần chuẩn Học thuật + 2 Slide Trực quan hóa Phương pháp + Slide 17 Tổng kết Kiểm định Kỹ thuật)
- **Tra cứu nhanh Báo cáo:** `HCMC_Smart_Traffic_Final_Report.pdf` (33 trang, 48 bookmark mục lục, Danh mục Thuật ngữ Bảng 0.1–0.2, Chương 1–8 + 16 Tài liệu tham khảo), `outputs/metrics.json`, `outputs/spark_runtime_evidence.json`, `outputs/mongo_verify.json`, `outputs/query_progress.json`
- **Cơ chế Trình bày Song song Live Demo (Interleaved Present + Demo · Tổng đúng `21:00` · Đúng 2 Lần Chuyển Mic):**
  Thay vì thuyết trình chay 18 phút rồi cuối giờ quay lại giải thích lặp 3 phút demo, mỗi chặng thuật toán và pipeline được **trình diễn trực tiếp ngay bên cạnh** trên màn hình mới **`Tab 06 · Luồng Thực Thi Pipeline (Live)`** và các tab chuyên biệt của **`ReactJS + FastAPI TOC (:8000)`**. Nhóm phân vai rạch ròi **Presenter (người nói)** và **Driver (người bấm chuột/chuyển cửa sổ hỗ trợ)** để triệt tiêu thời gian chết:
  1. **Chặng 1 — Nguyễn Tạ Minh Quyền (`25MS23280`) · `00:00 – 06:00` (`6m00s`: Slide 01–06 `5m15s` + Live Demo #1 `0m45s` · Driver: Thành Đạt):** Mở đầu, Bài toán thực tế, Khảo sát tài liệu, Kiến trúc tổng thể `Zones A–F`, Kiểm toán Kaggle v6, Phương pháp (1/2) & **Live Demo #1 Luồng P1 (`Tab 06`, Stage 1–3)**. *(Sở hữu kỹ thuật chính: Mục 1, 4, 5 & 7A — Data, Quality Funnel & Spark Batch ETL)*
  2. **Chặng 2 — Nguyễn Thành Đạt (`25MS23306`) · `06:00 – 14:15` (`8m15s`: Slide 07–13 `6m45s` + Live Demo #2 `0m45s` + Live Demo #3 `0m45s` · Driver: Công Minh):** Phương pháp (2/2), Data Pipeline (`R1–R6`), Modeling (`23D Predictive ML`), **Live Demo #2 Luồng P2–P3 (`Tab 06` Stage 4–5, `Tab 03`, `Tab 04`)**, Batch Processing (`Spark 3.5.7`), Stream & Serving (`Kafka 4.1 & MongoDB 8.0`), **Live Demo #3 Luồng P4 Live Kafka 200 tin & Spark Audit (`Tab 06/02`, `Tab 05`)**, và Thực nghiệm (`Ablation 7 mô hình` + `Persistence Defense`). *(Sở hữu kỹ thuật chính: Mục 2, 6, 7B & 9 — Literature, Graph, Modeling & Experiments)*
  3. **Chặng 3 — Phạm Công Minh (`25MS23310`) · `14:15 – 21:00` (`6m45s`: Slide 14–17 `4m45s` + Live Demo #4 `2m00s` · Driver: Thành Đạt):** Demonstration (`#50026 Điện Biên Phủ` & `#21064 Công Trường Quốc Tế`), **Live Demo #4 Điều Hành Thực Địa TOC (:8000) & Kiểm Chứng Mù (`Tab 01` & `Mongo IXSCAN`)**, Limitations & Risks, Nghiệm thu & Lộ trình 3 giai đoạn, và Tổng kết 5 Trụ cột Kiểm định Kỹ thuật (mời Q&A, không chạy demo lặp cuối giờ). *(Sở hữu kỹ thuật chính: Mục 3, 8, 10, 11 & 12 — Architecture, Stream, MongoDB, Web TOC, Limitations & Plan)*

---

## Checklist Chuẩn Bị Trước Giờ G (30 Giây) & Phương Án Dự Phòng 15 Giây

- **Bước 1 — Khởi chạy cụm 5 container mặc định (`11.0 GB RAM`: `kafka`, `mongo`, `spark`, `toc-web`, `live-consumer`):**
  ```powershell
  docker compose up -d --build
  ```
- **Bước 2 — Sắp sẵn 2 cửa sổ cạnh nhau để `Alt+Tab` tức thì:**
  - **Cửa sổ 1:** Slide trình chiếu toàn màn hình (`HCMC_Smart_Traffic_Final_Presentation.pptx` hoặc `HCMC_Smart_Traffic_Final_Slides.pdf`).
  - **Cửa sổ 2:** Trình duyệt mở sẵn `http://localhost:8000` (zoom `100%`, tắt thanh bookmark).
- **Bước 3 — Đặt trạng thái sạch trước khi bấm đồng hồ:**
  - Bấm nút **`🧹 Reset 0`** để đưa toàn bộ bộ đếm Kafka Live (`messages`, `Bloom`, `Watermark`, `HLL++`) về `0`.
  - Trên **Tab 01 (`Điều Hành TOC`)**: chọn sẵn **Kịch bản 3 (`#50026 Điện Biên Phủ`, `2021-01-04`, khung `[07:00–07:30)`, chốt `t = 07:30 ICT`)**.
  - Chuyển sẵn sang **Tab 06 (`Luồng Thực Thi Pipeline`)**, chọn trace **`#50026 Điện Biên Phủ`** để Chặng 1 bật lên là chạy ngay không tốn thời gian chọn.
- **Bước 4 — Chốt phân vai Presenter / Driver:**
  - **Chặng 1 (`00:00 – 06:00`):** **Minh Quyền** nói, **Thành Đạt** làm Driver.
  - **Chặng 2 (`06:00 – 14:15`):** **Thành Đạt** nói, **Công Minh** làm Driver.
  - **Chặng 3 (`14:15 – 21:00`):** **Công Minh** nói, **Thành Đạt** làm Driver.
- **Phương án Dự phòng 15 giây (Backup Plan):** Nếu máy chiếu hoặc Docker container bất ngờ lag/nghẽn cổng `:8000`, Driver chuyển ngay sang thư mục ảnh thực địa đã kết xuất sẵn `outputs/slide_charts/ui_tab*.png` (`ui_tab1_overview_hotspots.png`, `ui_tab2_live_forecast_mongo.png`, `ui_tab4_kmeans_coldstart.png`) kết hợp đối chiếu trực tiếp bộ bằng chứng JSON (`outputs/metrics.json`, `outputs/spark_runtime_evidence.json`, `outputs/mongo_verify.json`, `outputs/query_progress.json`) — giữ nguyên lời thoại và đúng nhịp `21:00`.

---

## Phần 1. Bảng Ánh Xạ & Phân Bổ Thời Gian Liên Tục (`16:45` Slide + `04:15` Live Demo Song Song = `21:00`)

| Slide / Live Demo | Chặng | Mục Cấu Trúc 12 Phần | Nội dung Cốt lõi | Người nói (Driver) | Khung Thời gian | Thời lượng |
|---|---|---|---|---|---|---:|
| **Slide 01** | Chặng 1 | **Mở đầu (Cover)** | Giới thiệu đề tài, học phần, 3 thành viên (1 lần duy nhất) & bố cục song song 4 Live Demo | **Minh Quyền** *(Đạt)* | `00:00 – 00:30` | `0m30s` |
| **Slide 02** | Chặng 1 | **1. Problem Statement** | Ùn tắc lan truyền TP.HCM (`WHY`), 3 điểm nghẽn hiện hữu & 3 mục tiêu + giá trị (`WHAT`) | **Minh Quyền** *(Đạt)* | `00:30 – 01:30` | `1m00s` |
| **Slide 03** | Chặng 1 | **2. Literature Review** | Khảo sát 4 hướng tiếp cận giao thông đô thị, khoảng trống nghiên cứu & logic 12 phần | **Minh Quyền** *(Đạt)* | `01:30 – 02:20` | `0m50s` |
| **Slide 04** | Chặng 1 | **3. System Architecture** | Kiến trúc `Zones A–F`, 4 luồng màu Pipeline, `3-Phase MLOps`, `Predictive ML` + Arch Risks | **Minh Quyền** *(Đạt)* | `02:20 – 03:25` | `1m05s` |
| **Slide 05** | Chặng 1 | **4. Data, EDA & Kaggle v6** | Kiểm toán 5 CSV Kaggle v6 (`41` cột, `38.50 MB`, `46/48` khung), `Proof-of-Scale` & `EC-1..EC-10` | **Minh Quyền** *(Đạt)* | `03:25 – 04:25` | `1m00s` |
| **Slide 06** | Chặng 1 | **Phương pháp (1/2)** | `Train-Only 3-Tier Vmax`, `derive_urban_los` (`89.19%`) & `5-Tuple Monoid + Salting` | **Minh Quyền** *(Đạt)* | `04:25 – 05:15` | `0m50s` |
| **Live Demo #1** | Chặng 1 | **Demo Song Song · P1** | `Tab 06` (`#50026 Điện Biên Phủ`, nút `Slide 06 · P1 Monoid/Vmax`): Stage 1→3 (`R1–R6`, `Vmax`, `Monoid 0.0`) → *Chuyển mic 1* | **Minh Quyền** *(Đạt)* | `05:15 – 06:00` | `0m45s` |
| **Slide 07** | Chặng 2 | **Phương pháp (2/2)** | `Line-Graph PageRank` (`30` vòng lặp), `7D K-Means` (`88.13%`) & `33 KiB O(1) Sketches` | **Thành Đạt** *(Minh)* | `06:00 – 06:50` | `0m50s` |
| **Slide 08** | Chặng 2 | **5. Data Pipeline** | Hành trình `07:08`, `6` quy tắc `R1–R6` (`0` silent drops), `Train-Only` & `Predictive ML` + Pipeline Risks | **Thành Đạt** *(Minh)* | `06:50 – 07:40` | `0m50s` |
| **Slide 09** | Chặng 2 | **6. Modeling** | `Exact t+30m`, tách `7,192` burst (`1,885` organic TRAIN), biểu đồ `23D` & `VAL Gate` + Modeling Risks | **Thành Đạt** *(Minh)* | `07:40 – 08:35` | `0m55s` |
| **Live Demo #2** | Chặng 2 | **Demo Song Song · P2–P3** | `Tab 06` Stage 4→5 (`PageRank`, `Cụm #1`, `23D`, `RF 23D` `LOS F 57.9% / 87.2%`), `Tab 03` (`15.23x`) & `Tab 04` (`#37345`) | **Thành Đạt** *(Minh)* | `08:35 – 09:20` | `0m45s` |
| **Slide 10** | Chặng 2 | **7. Batch Processing** | `Spark 3.5.7`: `BHJ` (`Combined -12.45% / Isolated -25.76%`), `Salting` (`1.0197x`, `+87.92 ms`), `PageRank`, `FPGrowth 32`, `K-Means` | **Thành Đạt** *(Minh)* | `09:20 – 10:20` | `1m00s` |
| **Slide 11** | Chặng 2 | **8. Stream & Serving** | `Kafka 4.1 KRaft`, real watermark, `WAL 73.7 KB`, `HLL++ 9,620.2`, `Tier-2 100%`, `Mongo IXSCAN 0.360 ms / 31.36x` | **Thành Đạt** *(Minh)* | `10:20 – 11:15` | `0m55s` |
| **Live Demo #3** | Chặng 2 | **Demo Song Song · P4** | `Tab 06/02` bơm `⚡ 200 tin Kafka Live` (Stage 6→7, `Bloom > 0`, `Watermark > 0`, `HLL++`, `v1`) & lướt `Tab 05` Audit | **Thành Đạt** *(Minh)* | `11:15 – 12:00` | `0m45s` |
| **Slide 12** | Chặng 2 | **9A. Experiments: Ablation** | `7-Model Ablation`, `6×6 CM`, lý giải vì sao chọn `RF 23D` thay vì `RF 14D` (sập `VAL 0.1697`) | **Thành Đạt** *(Minh)* | `12:00 – 13:10` | `1m10s` |
| **Slide 13** | Chặng 2 | **9B. Experiments: Defense** | Đối chứng `Persistence`, đổi pha (`10/61 → 14/61, p=0.001953`), `Cold-Start` (`88.13%`, `58.63%`) → *Chuyển mic 2* | **Thành Đạt** *(Minh)* | `13:10 – 14:15` | `1m05s` |
| **Slide 14** | Chặng 3 | **10. Demonstration** | 2 ảnh `Tab 01` thật (`#50026 Điện Biên Phủ` & `#21064 Công Trường Quốc Tế + Mongo IXSCAN`) trên `TOC (:8000)` | **Công Minh** *(Đạt)* | `14:15 – 15:10` | `0m55s` |
| **Live Demo #4** | Chặng 3 | **Demo Thực Địa · TOC** | `Tab 01` Kịch bản 3 (`#50026`, chốt `07:30 ICT`, **Kiểm chứng mù** `E ➔ F`), Kịch bản 2 (`#21064`, chốt `12:00 ICT`) & `Mongo IXSCAN` | **Công Minh** *(Đạt)* | `15:10 – 17:10` | `2m00s` |
| **Slide 15** | Chặng 3 | **11. Limitations & Risks** | Tổng hợp giới hạn 5 thành phần giải pháp (`Mục 3, 5, 6, 7, 8`) & 2 thẻ rủi ro/kiểm soát | **Công Minh** *(Đạt)* | `17:10 – 18:25` | `1m15s` |
| **Slide 16** | Chặng 3 | **12. Plan & Roadmap** | Nghiệm thu `100%` (kỹ thuật & 3 chặng) & Lộ trình `Giai đoạn 1 / Giai đoạn 2 / Giai đoạn 3` | **Công Minh** *(Đạt)* | `18:25 – 19:40` | `1m15s` |
| **Slide 17** | Chặng 3 | **Tổng kết Kiểm định** | Khép lại 5 trụ cột kiểm định kỹ thuật (đã chứng minh qua 4 Live Demo) → Mời Hội đồng Q&A | **Công Minh** *(Đạt)* | `19:40 – 21:00` | `1m20s` |

---

## Phần 2. Kịch Bản Lời Thoại Chi Tiết Từng Slide & 4 Chặng Live Demo Song Song (`00:00 – 21:00`)

### Chặng 1 — Nguyễn Tạ Minh Quyền (`25MS23280`) · Slide 01–06 + Live Demo #1 (`00:00 – 06:00`, `6m00s` · Driver: Thành Đạt)

#### Slide 01: Mở Đầu — Trang Bìa Đề Tài, Nhóm Thực Hiện & Bố Cục 3 Chặng (`00:00 – 00:30`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Hướng về tiêu đề chính `HCMC Smart Traffic Big Data Platform` và dải tên 3 thành viên cùng giảng viên hướng dẫn.
- **Lời thoại thuyết trình:**
  > "Kính chào thầy Đặng Trần Khánh và quý thầy cô. Nhóm em gồm Nguyễn Tạ Minh Quyền, Nguyễn Thành Đạt và Phạm Công Minh, xin trình bày đồ án cuối kỳ môn BDA501 — Big Data Analytics tại FSB, Đại học FPT: Nền tảng Phân tích Dữ liệu Lớn và Dự báo Ùn tắc Giao thông TP.HCM. Trong 21 phút tới, nhóm trình bày 17 slide song song bốn chặng Live Demo trực tiếp theo từng khối kỹ thuật."
- **Tra cứu nhanh:** Trang bìa & Chương 1 (`1.1`).

---

#### Slide 02: `1. Problem Statement` — Bài Toán Điều Hành Giao Thông Chủ Động Tại TP.HCM (`00:30 – 01:30`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Quét lần lượt 3 thẻ bên trái (`WHY` → `3 Hạn chế` → `WHAT`) đối chiếu bản đồ TP.HCM bên phải.
- **Lời thoại thuyết trình:**
  > "Mở đầu Phần 1 trên Slide 02 là bài toán thực tế tại TP.HCM. Mật độ xe dày đặc từ Tân Sơn Nhất, Quận 1, Lý Thường Kiệt đến Thủ Đức khiến một điểm nghẽn giờ cao điểm dễ lan thành kẹt dây chuyền, nhưng điều tiết hiện vẫn phản ứng bị động sau sự cố.
  >
  > Công cụ hiện hữu vấp ba hạn chế: camera cố định quá đắt chỉ lắp ở ít nút giao; GPS hành trình lệch đại lộ, bỏ trống vùng mù đường nhánh và hay trễ tin; còn bảng giám sát chỉ báo nơi đang kẹt, chưa dự báo trước 30 phút và tách rời trực ban với quy hoạch.
  >
  > Vì vậy, đồ án giải quyết ba mục tiêu: ước lượng lưu thông toàn đô thị kể cả vùng mù, nhận diện nút cổ chai cấu trúc và dự báo sớm 30 phút cho trung tâm TOC, Sở GTVT và người dân."
- **Tra cứu nhanh:** Chương 1 (`1.1–1.4`).

---

#### Slide 03: `2. Literature Review` — Khảo Sát 4 Hướng Tiếp Cận & Logic 12 Phần (`01:30 – 02:20`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Điểm qua 4 dòng bảng khảo sát phía trên và nhấn mạnh thẻ Khoảng trống nghiên cứu ở phía dưới.
- **Lời thoại thuyết trình:**
  > "Ở Phần 2 trên Slide 03, nhóm khảo sát bốn hướng tiếp cận giao thông đô thị. Thứ nhất, cảm biến cố định SCATS, CCTV đo chính xác tại nút nhưng quá đắt nên bỏ trống đường nhánh. Thứ hai, GPS hành trình của Herrera và Zheng phủ rộng hơn nhưng lệch trục lớn, ngắt quãng và chỉ phản ánh hiện trạng. Thứ ba, dự báo ngắn hạn như ARIMA, DCRNN hay Graph WaveNet mạnh trên cao tốc dày cảm biến nhưng suy biến ở đô thị đứt quãng. Thứ tư, nền tảng TOC thường tách rời mô hình học thuật khỏi luồng GPS có tin trễ, tin trùng.
  >
  > Khoảng trống đó khẳng định cần một nền tảng tích hợp đi theo mạch 12 phần từ bài toán, kiến trúc, xử lý lô–luồng đến thực nghiệm."
- **Tra cứu nhanh:** Chương 1 (`1.5`).

---

#### Slide 04: `3. System Architecture` — Kiến Trúc `Zones A–F`, 4 Luồng Pipeline & `3-Phase MLOps` (`02:20 – 03:25`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Chỉ sơ đồ 6 phân vùng `Zones A–F` với 4 màu mũi tên Pipeline bên trái, thẻ `3-Phase MLOps` góc trên phải, và `Arch Risks` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Sang Phần 3 trên Slide 04 là kiến trúc lai Batch–Speed gồm sáu phân vùng Zones A–F, nối bởi bốn luồng màu: Speed tím, Batch xanh dương, MLOps Registry nét đứt xanh ngọc và Storage & Serving xanh lá. Dữ liệu từ Zone A qua Kafka KRaft và phễu chất lượng Zone B, tách nhánh vào Speed Layer Zone C (Watermark 30 phút) song song Batch Layer Zone D chạy đồ thị, phân cụm và huấn luyện học máy dự báo Predictive ML chỉ dùng dữ liệu quá khứ.
  >
  > Cơ chế 3-Phase MLOps chống xung đột ghi tại Zone E: Phase A học một lần trên 1,885 mẫu organic TRAIN rồi khóa mã băm SHA-256; Phase B trực tuyến chỉ đọc để phát cảnh báo v1; Phase C cuối ngày đối soát tin trễ nâng cấp bản chuẩn v2 tại Zone F gồm Parquet, MongoDB 8.0 và web ReactJS + FastAPI cổng 8000.
  >
  > Hai rủi ro kiến trúc là 5 container chung 11 GB RAM có thể tranh chấp đĩa và tin trễ quá 30 phút phải đợi Phase C."
- **Tra cứu nhanh:** Chương 3 (`3.1, 3.2, 3.5`).

---

#### Slide 05: `4. Data, EDA & Data Problem` — Kiểm Toán Kaggle v6, `Proof-of-Scale` & `EC-1..EC-10` (`03:25 – 04:25`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Chỉ biểu đồ dị thường góc trên trái, thẻ Kaggle v6 & `Proof-of-Scale` góc dưới trái, và bảng `EC-1..EC-10` bên phải.
- **Lời thoại thuyết trình:**
  > "Bước sang Phần 4 trên Slide 05, nhóm kiểm toán trực tiếp nguồn Kaggle v6 từ BkTraffic Bách khoa TP.HCM (giấy phép CC0), gồm 5 CSV, 41 cột, 38.50 MB: 577,967 nút bản đồ cắt 90.97% điểm cong còn 52,181 nút giao thực; 5,553 tuyến đường; 84,633 đoạn còn 84,206 đoạn cơ giới sau khi lọc 427 POI OSM; 90,938 pings còn 90,787 hợp lệ; và 33,441 cửa sổ phủ 46/48 khung giờ còn 33,394 cửa sổ cơ giới. Đây là mẫu chuẩn Proof-of-Scale kiểm chứng sai số 0.0 cho quy mô TP.HCM 8–17 tỷ tin và 1.5–3 TB/ngày.
  >
  > Bảng bên phải định lượng EC-1 đến EC-10: thiếu Vmax 85.21% bảng thô (28,495/33,441) và 85.19% cơ giới (28,448/33,394); 88.13% vùng mù; 66.30% đứt quãng từ 60 phút kèm 7,192 burst sáng 03/08/2020 — một sáng đổ dồn 36,096 pings (39.7% cả 10 tháng); 12.89% lệch ngày UTC+7; lệch tải 684.95x; tin trùng, trễ và sập luồng."
- **Tra cứu nhanh:** Chương 2 (`2.1, 2.2`).

---

#### Slide 06: Trực Quan Hóa Phương Pháp (1/2) — `3-Tier Vmax`, `derive_urban_los` & `5-Tuple Monoid` (`04:25 – 05:15`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Chỉ phễu 4 bậc bên trái cho `3-Tier Vmax` và hộp đối chiếu Đỏ/Xanh bên phải cho `5-Tuple Monoid + Key Salting`.
- **Lời thoại thuyết trình:**
  > "Trên Slide 06 là hai phương pháp nền tảng. Bên trái, để điền khuyết hơn 85% cửa sổ thiếu Vmax không rò rỉ tương lai, hệ thống chỉ học trên tập TRAIN theo bốn bậc: giữ 4,946 cửa sổ có biển gốc; lấy P85 tốc độ thoáng chính đoạn cho 7,650 cửa sổ; mượn trung vị cùng cấp–loại cho 19,601 cửa sổ; và dùng chuẩn 80/60/50/40 km/h cho 1,197 cửa sổ. Hàm derive_urban_los bậc 5 km/h khớp 89.19% nhãn BkTraffic.
  >
  > Bên phải là 5-Tuple Monoid kèm Key Salting: Máy A có 1 xe 10 km/h, Máy B có 3 xe 40 km/h — trung bình thật 32.5, gộp ngây thơ ra 25 (sai 7.5). Giữ 5 đại lượng cộng được giúp chia 8 giỏ song song sai số 0.0. Mời hội đồng xem trực tiếp luồng P1 trên Tab 06."
- **Tra cứu nhanh:** Chương 2 (`2.4`) & Chương 3 (`3.3`).

---

#### Live Demo #1 — Luồng P1 (`R1–R6`, `Vmax` & `Monoid`) (`05:15 – 06:00`, `0m45s`)
- **Presenter:** **Nguyễn Tạ Minh Quyền (`25MS23280`)** · **Driver:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Thao tác Driver (từng cú click):**
  1. `Alt+Tab` sang trình duyệt `http://localhost:8000` (đang mở sẵn **`Tab 06 · Luồng Thực Thi Pipeline`** với trace **`#50026 Điện Biên Phủ`**).
  2. Bấm nút nhảy nhanh **`Slide 06 · P1 Monoid/Vmax`** để chạy nối tiếp **Stage 1 → Stage 2 → Stage 3** trực tiếp trên dữ liệu thật.
  3. Giữ con trỏ chuột lần lượt vào: danh sách `2` ping GPS thô (`17 & 6 km/h`), phễu `R1–R6` + tầng `Vmax 60 km/h`, và thẻ gộp **`5-Tuple Monoid`** (`11.5 km/h`, sai số `0.0`).
  4. Ngay khi Quyền nói *"chuyển mic sang bạn Thành Đạt"*, Driver `Alt+Tab` trở lại cửa sổ Slide (Slide 07).
- **Lời thoại thuyết trình song song (`0m45s`):**
  > "Trên màn hình cổng 8000, Tab 06 — Luồng Thực Thi Pipeline — đang mở sẵn dấu vết đoạn #50026 đại lộ Điện Biên Phủ sáng 4/1/2021. Khi bấm nút Slide 06 · P1 Monoid/Vmax: Stage 1 nạp trực tiếp hai ping GPS thô là 17 và 6 km/h; Stage 2 lọc qua phễu R1–R6 không rớt ngầm, nội suy Vmax 60 km/h theo bảng Train-Only và quy đổi LOS bằng derive_urban_los; đến Stage 3, bộ 5-Tuple Monoid gộp chính xác thành vận tốc 11.5 km/h mức E với sai số toán học 0.0 so với đơn luồng. Em xin chuyển mic sang bạn Thành Đạt tiếp tục Chặng 2."

---

### Chặng 2 — Nguyễn Thành Đạt (`25MS23306`) · Slide 07–13 + Live Demo #2 & #3 (`06:00 – 14:15`, `8m15s` · Driver: Công Minh)

#### Slide 07: Trực Quan Hóa Phương Pháp (2/2) — `Line-Graph PageRank`, `7D K-Means` & `33 KiB Sketches` (`06:00 – 06:50`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ lần lượt 3 cột trực quan từ trái sang phải (`PageRank` → `7D K-Means` → `O(1) Sketches`).
- **Lời thoại thuyết trình:**
  > "Cảm ơn bạn Quyền. Trên Slide 07 là ba cơ chế về đồ thị, phân cụm và bộ đếm luồng O(1). Cột một là Line-Graph PageRank: sau khi cắt 90.97% điểm cong và bỏ cạnh quay đầu U-turn, mỗi đoạn cơ giới thành một đỉnh trên 92,453 cạnh chuyển tiếp, chạy PageRank 30 vòng lặp (hệ số 0.85) tìm nút phễu tự nhiên, đứng đầu Nguyễn Kiệm 2.7799.
  >
  > Cột hai là 7D K-Means: gom 84,206 đoạn vào 6 cụm hình thái–hành vi để 74,211 đoạn vùng mù (88.13%) mượn hồ sơ tuyến cảm biến đồng dạng.
  >
  > Cột ba là Sketches 33 KiB cố định: RotatingBloomFilter 2×16 KiB (k=10) chặn 100% tin trùng (FPR 0.0389%), HyperLogLog++ 1 KiB đếm đoạn hoạt động và EWMA bắt kẹt lì."
- **Tra cứu nhanh:** Chương 4 (`4.1, 4.3`) & Chương 6 (`6.2`).

---

#### Slide 08: `5. Data Pipeline & Processing` — 6 Quy Tắc `R1–R6`, `Train-Only` & `Predictive ML` (`06:50 – 07:40`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ sơ đồ hành trình `07:08` phía trên, thẻ `Quality Funnel R1–R6` bên trái dưới và thẻ `Pipeline Risks` bên phải dưới.
- **Lời thoại thuyết trình:**
  > "Sang Phần 5 trên Slide 08, sơ đồ trên theo dấu tin 07:08 Lý Thường Kiệt 4 km/h xuyên bốn chặng tới cảnh báo dưới 1 ms. Phễu R1–R6 dùng ANSI try_cast đảm bảo 0 sập job, 0 rớt ngầm: R1 bù 27 m cho 154 đoạn dài không dương; R2 lọc 427 POI còn 84,206 đoạn cơ giới; R3 tách 631 tên quận phường; R4 chuẩn hóa NFC gộp 695 về 641 đường; R5 nội suy Train-Only 3-Tier Vmax cho 28,448 cửa sổ; R6 cách ly 151 ping lỗi nhưng giữ 0 km/h xe kẹt đứng. Sau đổi UTC+7 (4,305 cửa sổ), 33,394 cửa sổ phục vụ Predictive ML.
  >
  > Rủi ro là 62.28% cửa sổ dùng Vmax tĩnh chưa bắt ngập nước và khung 30 phút làm mượt dao động ngắn."
- **Tra cứu nhanh:** Chương 2 (`2.3–2.5`).

---

#### Slide 09: `6. Modeling` — `23D` Feature Taxonomy, `Organic 1,885` & `VAL Composite Gate` (`07:40 – 08:35`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ biểu đồ tầm quan trọng 23 biến bên trái và 2 thẻ bên phải (`Exact t+30m & VAL Gate` ở trên, `Modeling Risks` ở dưới).
- **Lời thoại thuyết trình:**
  > "Sang Phần 6 trên Slide 09 là thiết kế dự báo đúng +30 phút không rò rỉ tương lai. Do 66.30% cặp đứt quãng từ 60 phút, nhóm ghép chính xác +30 phút được 7,885 cặp, tách 7,192 burst và 55 mẫu biên, giữ 1,885 organic TRAIN, 1,167 VAL và 1,182 TEST; phép thử Future-Shuffle xác nhận sai lệch 0.0.
  >
  > Biểu đồ bên trái cho thấy 14 biến cục bộ và chu kỳ giờ Việt Nam chiếm 89.50% tầm quan trọng — dẫn đầu là mean_velocity 46.35%, LOS hiện tại 17.56% và velocity_ratio 10.64% — cùng 9 biến đồ thị, cụm và lịch sử quá khứ chiếm 10.50% (PageRank 2.77%, neighbor_los 2.35%). Qua cổng khóa cứng VAL Composite Gate không nhìn trộm TEST, RF 23D thắng ở 0.8882, phủ 100% Cold-Start. Giới hạn là lớp B–E thưa và thiếu biến mưa ngập. Mời hội đồng xem trực tiếp luồng P2–P3."
- **Tra cứu nhanh:** Chương 5 (`5.1–5.3, 5.5`).

---

#### Live Demo #2 — Luồng P2–P3 (`Graph`, `23D RF` & `Cold-Start`) (`08:35 – 09:20`, `0m45s`)
- **Presenter:** **Nguyễn Thành Đạt (`25MS23306`)** · **Driver:** **Phạm Công Minh (`25MS23310`)**
- **Thao tác Driver (từng cú click):**
  1. `Alt+Tab` sang trình duyệt `http://localhost:8000` (vẫn đang ở **Tab 06** tại cuối Stage 3 của `#50026 Điện Biên Phủ`).
  2. Bấm tiếp **Stage 4 → Stage 5**: hiện trực tiếp `PageRank`, `Cụm #1`, vector `23D` thật và **biểu đồ 6 cột xác suất `RF 23D` bật thẳng `LOS F = 57.9%, P(E/F) = 87.2%`**.
  3. **Mẹo click trước + câu dẫn 3 giây:** Ngay khi Đạt đọc câu cầu nối *"Bây giờ nhóm mở rộng ra toàn mạng lưới 84,206 đoạn..."*, Driver bấm ngay sang **Tab 03 (`PageRank & Luật Đồng Kẹt`)** để hiện bảng `#21969 Nguyễn Kiệm` vs `#34019 Trần Quốc Thảo` và cặp đồng kẹt `Nguyễn Thị Minh Khai ↔ Cống Quỳnh (Lift 15.23x)`.
  4. Bấm tiếp sang **Tab 04 (`6 Cụm K-Means & Cold-Start`)** — đoạn vùng mù **`#37345 Cầu vượt Ngã tư Gò Dưa`** đã được chọn sẵn mặc định kèm danh sách các tuyến cảm biến đồng dạng cùng cụm `#4`.
  5. `Alt+Tab` trở lại cửa sổ Slide (Slide 10).
- **Lời thoại thuyết trình song song (`0m45s`):**
  > "Tiếp nối ngay trên Tab 06 của đoạn #50026, bấm Stage 4 để ghép PageRank, Cụm #1 và đủ vector 23D thật; sang Stage 5, biểu đồ sáu cột xác suất RF 23D bật thẳng lên mức LOS F 57.9%, tổng rủi ro P(E/F) đạt 87.2%. Bây giờ nhóm mở rộng ra toàn mạng lưới 84,206 đoạn trên Tab 03: nút phễu #21969 Nguyễn Kiệm chạy thoáng, tách bạch nút kẹt thật #34019 Trần Quốc Thảo, cùng cặp đồng kẹt Nguyễn Thị Minh Khai – Cống Quỳnh đạt Lift 15.23x. Ở Tab 04, đoạn vùng mù #37345 Cầu vượt Ngã tư Gò Dưa được ghép ngay vào Cụm #4 mượn tuyến đồng dạng."

---

#### Slide 10: `7. Batch Processing` — Thực Thi Phân Tán `Spark 3.5.7`, Đồ Thị & Phân Cụm (`09:20 – 10:20`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ sơ đồ hai nhánh phía trên (`Batch ETL` & `Graph/K-Means`), thẻ kết quả góc dưới trái và thẻ `Batch Risks` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Trên Slide 10, Phần 7 trình bày xử lý lô phân tán kiểm chứng trên Apache Spark 3.5.7. Ở nhánh Batch ETL, BroadcastHashJoin ghim hai bảng danh mục nhỏ vào RAM, cắt Exchange từ 5 xuống 1 (-80%), giảm độ trễ Combined 12.45% (213.66 xuống 187.07 ms) và Isolated 25.76% (166.18 xuống 123.37 ms) so với mặc định SortMerge, sai khác bằng 0. Song song, 5-Tuple Monoid kèm 8-Bucket Salting khớp 100% baseline.py, hạ skew 684.95x xuống 1.0197x với đánh đổi +87.92 ms so với V1; Parquet cắt 9/10 phân vùng tháng 8/2020.
  >
  > Nhánh khai phá dưới: PageRank bảo toàn 84,206.0; SON 2 lượt và Spark FPGrowth tìm 32 luật organic TRAIN, 12 luật đồng kẹt (Lift 15.23x); 7D K-Means gom 3,609 đoạn thành 6 cụm (Silhouette 0.2241). Rủi ro: 14 luật trễ có 5–8 cửa sổ nên làm giả thuyết thăm dò."
- **Tra cứu nhanh:** Chương 3 (`3.3–3.5`), Chương 4 (`4.1–4.4`) & `evidence/session9/B_benchmark.csv`.

---

#### Slide 11: `8. Stream Processing & MongoDB Serving` — `Kafka 4.1`, Real Watermark & `IXSCAN` (`10:20 – 11:15`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ đường nhanh `Kafka → Speed Layer → MongoDB` phía trên, nhánh đối soát `Tier-2` ở giữa và 2 thẻ dưới.
- **Lời thoại thuyết trình:**
  > "Sang Phần 8 trên Slide 11 là luồng trực tuyến và kho MongoDB 8.0. GPS đi qua Kafka 4.1 KRaft ba phân vùng vào Speed Layer. Nhóm kiểm chứng Watermark thật trên Spark Structured Streaming mốc 12:30 ngày 1/12/2020 (xả 10 dòng hết hạn), cùng nhật ký ghi trước WAL 73.7 KB hồi phục 0 trùng lặp.
  >
  > Khối Sketches 33 KiB gồm RotatingBloom chặn 333/333 tin trùng (FPR 0.0389%), HyperLogLog++ 1 KiB ước lượng 9,620.2/9,995 đoạn (sai số 3.75%), và EWMA bắt điểm kẹt lì. Cuối ngày, Tier-1 khớp 100%, Tier-2 bù 500 cửa sổ trễ, hàn 609 lag và re-score 384 cửa sổ đạt 100%. Trên MongoDB 8.0, khóa `$status_version` chặn 25/25 tin v1 ghi đè v2; IXSCAN trên chỉ mục B-Tree đạt 0.360 ms, nhanh gấp 31.36x COLLSCAN. Mời hội đồng xem bơm luồng Kafka trực tiếp."
- **Tra cứu nhanh:** Chương 6 (`6.1–6.5`).

---

#### Live Demo #3 — Luồng P4 Live Kafka 200 tin & Spark Audit (`11:15 – 12:00`, `0m45s`)
- **Presenter:** **Nguyễn Thành Đạt (`25MS23306`)** · **Driver:** **Phạm Công Minh (`25MS23310`)**
- **Thao tác Driver (từng cú click):**
  1. `Alt+Tab` sang trình duyệt `http://localhost:8000` → quay về **Tab 06** (hoặc **Tab 02 · Luồng Kafka Live**).
  2. Bấm nút **`⚡ Bơm 200 tin Kafka Live (Stress-Test)`** — chờ ~1–2 giây cho cả 5 thẻ KPI cùng **Stage 6 → Stage 7** sáng đèn trực tiếp với số live thực (`3 partitions P0/P1/P2`, **`RotatingBloomFilter 32.0 KiB` (`2×16 KiB, k=10`) chặn tin trùng > 0**, **Watermark bắt tin trễ >30p > 0**, `HLL++ 1.0 KiB`, `MongoDB v1`).
  3. Trong lúc Đạt nhấn mạnh sự khác biệt giữa mẫu chạy live 200 tin với benchmark full `33,727` tin, Driver chuyển sang **Tab 05 (`Spark 3.5.7, Ablation & Đối Soát`)**, cuộn nhanh bảng `Spark 3.5.7` xác nhận `5 → 1 Exchange`, `Combined -12.4%` (`-12.45%`), `Isolated -25.76%`, `684.95x → 1.0197x`, và đối soát `Tier-2 100%`.
  4. `Alt+Tab` trở lại cửa sổ Slide (Slide 12).
- **Lời thoại thuyết trình song song (`0m45s`):**
  > "Trên Tab 06, khi bấm Bơm 200 tin Kafka Live Stress-Test, cả năm thẻ KPI cùng Stage 6 và Stage 7 sáng đèn trực tiếp qua ba phân vùng P0, P1, P2: RotatingBloomFilter chặn ngay tin trùng lớn hơn 0, Watermark 30 phút tách ngay tin trễ quá hạn lớn hơn 0, HyperLogLog++ đếm đoạn hoạt động và ghi cảnh báo v1 vào MongoDB. Lưu ý rõ đây là mẫu chạy live 200 tin, khác với benchmark toàn tập 33,727 tin đạt 333/333 tin trùng và 9,620.2/9,995 đoạn. Lướt Tab 05 xác nhận khớp Spark 3.5.7: 5 xuống 1 Exchange, Combined -12.45%, Isolated -25.76%, skew 1.0197x và Tier-2 100%."

---

#### Slide 12: `9A. Experiments (Ablation & Confusion Matrix)` — So Sánh 7 Mô Hình & Lý Do Chọn `RF 23D` (`12:00 – 13:10`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ bảng `7-Model Ablation` bên trái (đặc biệt `#4 RF 14D` vs `#5 RF 23D`), ma trận phân loại 6 mức (`Confusion Matrix 6×6`) bên phải và thẻ `VAL Gate` phía dưới trái.
- **Lời thoại thuyết trình:**
  > "Sang Phần 9A trên Slide 12 là bảng Ablation 7 mô hình trên 1,182 mẫu TEST. Trong đó, Persistence Baseline là mốc đối chứng tối giản không dùng AI do chính nhóm tự thiết lập — cứ đoán giữ nguyên mức kẹt hiện tại sau 30 phút. Mô hình số 5 — Random Forest 23D — đạt ROC-AUC cao nhất 0.9894, PR-AUC 0.9543, Severe F1 0.9362 (Recall 95.14%), Weighted-F1 0.9297 và 6-Class Acc 92.55%. Đường chéo ma trận phân loại 6×6 là 944, 3, 1, 3, 5, 138, phân biệt đủ 6 lớp; Macro-F1 là 0.4253 do bốn lớp B–E chỉ có 10–20 mẫu TEST.
  >
  > Vì sao không chọn RF 14D khi trên TEST nó nhỉnh nhẹ ở Severe F1 0.9380 và PR-AUC 0.9554? Vì nguyên tắc không nhìn TEST để chọn mô hình: tuyển chọn khóa chặt trên VAL. Trên tập VAL lệch thời gian, RF 14D thiếu ngữ cảnh đồ thị bị sập 6-Class Accuracy xuống 0.1697 (VAL Gate 0.6794), còn RF 23D giữ vững 0.8166 và thắng rõ rệt ở VAL Gate 0.8882."
- **Tra cứu nhanh:** Chương 5 (`5.3`, Bảng `5.4`).

---

#### Slide 13: `9B. Experiments (Defense)` — Đối Chứng `Persistence`, Đổi Pha & `Cold-Start` (`13:10 – 14:15`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ bảng 4 chế độ kiểm định bên trái, biểu đồ so sánh bên phải và thẻ kết luận góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Ở Phần 9B trên Slide 13, vì sao Persistence Baseline đạt 94.84% trên TEST, cao hơn 92.55% của RF 23D? Vì 94.84% mẫu thực tế đứng yên sau 30 phút nên quy tắc đoán giữ nguyên có lợi thế quán tính.
  >
  > Khi bóc tách bốn chế độ kiểm định: trên 61 ca đổi pha, Persistence mù hoàn toàn 0/61; RF 23D đạt Detect ROC-AUC 0.8822, giảm mạnh sai số xác suất Brier (0.2811 vs 0.3279), bắt đúng 10/61 và nâng lên 14/61 (22.95%) sau hiệu chỉnh ngưỡng trên VAL — kiểm định ghép cặp McNemar cho p = 0.001953. Ở 8/993 ca đột ngột hóa kẹt (ngưỡng cứng 0/8), Onset ROC-AUC đạt 0.8438, Lift 3.26x; trên 3,605 cặp đứt quãng vượt +6.73 PR-AUC. Với 88.13% Cold-Start (phủ 100%), qua phép thử Masked-GPS Proxy che toàn bộ tín hiệu GPS tập TEST thành trống để giả lập vùng mù, độ chính xác đạt 58.63%. Em xin chuyển mic sang bạn Công Minh."
- **Tra cứu nhanh:** Chương 5 (`5.4`, Bảng `5.5`).

---

### Chặng 3 — Phạm Công Minh (`25MS23310`) · Slide 14–17 + Live Demo #4 (`14:15 – 21:00`, `6m45s` · Driver: Thành Đạt)

#### Slide 14: `10. Demonstration` — Trung Tâm Điều Hành `ReactJS + FastAPI TOC (:8000)` & 2 Case Studies (`14:15 – 15:10`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Chỉ dẫn trực quan:** Chỉ 2 ảnh `Tab 01` thật: `#50026 Điện Biên Phủ` bên trái và `#21064 Công Trường Quốc Tế + Mongo IXSCAN` bên phải cùng dải kiến trúc giao diện TOC `:8000` phía dưới.
- **Lời thoại thuyết trình:**
  > "Cảm ơn bạn Đạt. Bước sang Phần 10 trên Slide 14, toàn bộ hệ thống hội tụ vào Trung tâm Điều hành ReactJS, Leaflet và FastAPI cổng 8000, kết nối trực tiếp kho MongoDB 8.0 chứa 33,394 bản ghi. Slide hiển thị hai ảnh Tab 01 thật đối chiếu hai tầng cảnh báo trên tập TEST độc lập.
  >
  > Ảnh trái là cao điểm sáng 4/1/2021 khung thu thập [07:00–07:30), chốt phát lệnh 07:30 ICT với 122 đoạn và 38 cảnh báo sớm: đoạn #50026 đại lộ Điện Biên Phủ dự báo trúng thẳng pha E sang F (P(E/F) 87.2%). Ảnh phải là #21064 Công Trường Quốc Tế khung [11:30–12:00), chốt 12:00 ICT: tầng xác suất vẫn bật cảnh báo P(E/F) 68.7% kèm bảng MongoDB IXSCAN 0.360 ms (31.36x). Mời hội đồng kiểm chứng mù trực tiếp trên Tab 01."
- **Tra cứu nhanh:** Chương 6 (`6.4`).

---

#### Live Demo #4 — Điều Hành Thực Địa TOC (:8000) & Kiểm Chứng Mù (`15:10 – 17:10`, `2m00s`)
- **Presenter:** **Phạm Công Minh (`25MS23310`)** · **Driver:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Thao tác Driver (từng cú click):**
  1. `Alt+Tab` sang trình duyệt `http://localhost:8000` → chuyển thẳng sang **Tab 01 (`Điều Hành TOC`)** (đã mở sẵn **Kịch bản 3 `#50026 Điện Biên Phủ`**, khung thu thập `[07:00–07:30)`, **chốt phát lệnh `t = 07:30 ICT`**, `122` đoạn / `38` cảnh báo sớm).
  2. Bấm nút **`🙈 Ẩn nhãn thực tế t+30p (Chế độ kiểm chứng mù)`** — ẩn sạch đáp án trên CẢ bảng lẫn biểu đồ để Hội đồng chỉ nhìn thấy dữ liệu quá khứ và dự báo `E ➔ F (P(F) = 57.9%, P(E/F) = 87.2%)` của `RF 23D` (so với `Persistence` đoán giữ nguyên `E`).
  3. Sau khi Minh đặt câu hỏi kiểm chứng với Hội đồng, bấm tắt chế độ ẩn để mở **`★ Thực tế: LOS F`** khớp 100%!
  4. **Mẹo click trước + câu dẫn 3 giây:** Trong lúc Minh đọc câu cầu nối *"Không chỉ các ca đổi nhãn trực diện, điều hành viên còn cần cảnh báo xác suất sớm..."*, Driver bấm chọn **Kịch bản 2 `#21064 Công Trường Quốc Tế`** (`2021-04-22`, khung thu thập `[11:30–12:00)`, **chốt phát lệnh `t = 12:00 ICT`**).
  5. Chỉ rõ `4` pings GPS sập `23 → 3 km/h`, nhãn `argmax` thô giữ `E` nhưng cảnh báo xác suất vượt ngưỡng **`P(E/F) = 68.7%` (> `25%`) & `P(chg) = 48.9%` (> `40%`)** trước khi thực tế sập xuống `LOS F`.
  6. Bấm nút **`⚡ Mongo IXSCAN (31.36x)`** — xác nhận trực tiếp trên màn hình `warm median = 0.360 ms / p95 = 0.603 ms` (`0 SORT RAM`, chỉ quét `26` khóa B-Tree).
  7. `Alt+Tab` trở lại cửa sổ Slide (Slide 15).
- **Lời thoại thuyết trình song song (`2m00s`):**
  > "Bây giờ chúng ta vào thẳng phòng điều hành TOC tại cổng 8000 trên Tab 01. Trước hết là thanh Đồng hồ Phòng Điều Hành: hệ thống thu thập GPS trong khung [07:00–07:30), chốt phát lệnh đúng t = 07:30 ICT và dự báo cho khung [07:30–08:00) với 122 đoạn hoạt động và 38 cảnh báo sớm.
  >
  > Để chứng minh trung thực hệ thống không hề nhìn đáp án tương lai, em bật Chế độ Kiểm Chứng Mù — ẩn nhãn thực tế t+30p trên cả bảng lẫn biểu đồ. Tại đoạn #50026 đại lộ Điện Biên Phủ đang mức E (11.5 km/h), Persistence đoán giữ nguyên E, còn Random Forest 23D phát lệnh chuyển thẳng E sang F với P(F) 57.9% và P(E/F) 87.2%. Bây giờ em mở lại đáp án: sao thực tế t+30p hiện đúng LOS F, khớp 100%!
  >
  > Không chỉ các ca đổi nhãn trực diện, điều hành viên còn cần cảnh báo xác suất sớm ở các vòng xoay biến động nhanh. Trên Kịch bản 2 — đoạn #21064 Công Trường Quốc Tế, khung [11:30–12:00), chốt phát lệnh 12:00 ICT — bốn ping GPS vừa sập mạnh từ 23 xuống 3 km/h. Dù nhãn argmax thô bảo thủ giữ mức E, tầng cảnh báo xác suất hiệu chỉnh trên VAL vẫn bật báo động tức thời với P(E/F) 68.7% vượt 25% và xác suất đổi pha 48.9% vượt 40%, cảnh báo sớm trước khi thực tế sập xuống F.
  >
  > Cuối cùng, bấm kiểm định Mongo IXSCAN: câu truy vấn Top điểm nóng trục chính đọc thẳng trên chỉ mục B-Tree ESR với 0 SORT RAM, đạt warm median 0.360 ms và p95 0.603 ms — nhanh gấp 31.36 lần so với quét toàn bảng."

---

#### Slide 15: `11. Limitations & Risks` — Tổng Hợp 5 Thành Phần Giải Pháp & 4 Rủi Ro Toàn Cục (`17:10 – 18:25`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Chỉ dẫn trực quan:** Chỉ bảng 5 thành phần bên trái (`Mục 3, 5, 6, 7, 8`) và 2 thẻ rủi ro/kiểm soát bên phải (`4 Rủi Ro Toàn Cục` ở trên, `Nguyên Tắc Kiểm Soát` ở dưới).
- **Lời thoại thuyết trình:**
  > "Sang Phần 11 trên Slide 15, nhóm tổng hợp minh bạch giới hạn của năm thành phần giải pháp bên trái và hai thẻ rủi ro — kiểm soát bên phải.
  >
  > Xét từng thành phần: ở Kiến trúc, 5 container mặc định dùng chung 11 GB RAM và tin quá trễ đợi đối soát cuối ngày, kiểm soát bằng mem_limit và 3-Phase MLOps. Ở Data Pipeline, 62.28% cửa sổ dùng Vmax tĩnh chưa bắt được ngập nước hay rào chắn, bảo vệ bởi Train-Only và Quarantine. Ở Modeling, bốn lớp B–E thưa kéo Macro-F1 về 0.4253 và thiếu biến mưa hay tai nạn, giảm nhẹ nhờ lọc burst và hiệu chỉnh ngưỡng VAL. Ở Batch, 14 luật trễ chỉ có 5–8 cửa sổ nên gắn nhãn giả thuyết thăm dò. Ở Stream, vòng xoay hai thế hệ Bloom được khóa duy nhất MongoDB và status_version bổ trợ.
  >
  > Bốn rủi ro tầm đô thị gồm: GPS mới phủ 11.87% thiên về ô tô, chưa đại diện hơn 80% xe máy TP.HCM; trôi dạt phân phối mùa mưa ngập; ca khởi phát đột ngột quá hiếm (8/993); và hệ thống chỉ hỗ trợ khuyến nghị cho TOC, chưa đóng vòng đèn SCATS."
- **Tra cứu nhanh:** Chương 7 (`7.5`).

---

#### Slide 16: `12. Plan` — Nghiệm Thu Nhóm & Lộ Trình Phát Triển 3 Giai Đoạn (`18:25 – 19:40`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Chỉ dẫn trực quan:** Chỉ bảng nghiệm thu 3 thành viên phía trên và 2 thẻ `Lộ Trình 3 Giai Đoạn & Quality Gate` phía dưới.
- **Lời thoại thuyết trình:**
  > "Ở Phần 12 trên Slide 16 là nghiệm thu hoàn thành 100% và lộ trình ba giai đoạn. Nhóm phân rạch ròi sở hữu kỹ thuật với ba chặng song song Live Demo: bạn Minh Quyền phụ trách bài toán, kiểm toán dữ liệu, Quality Funnel, Vmax, Spark Batch ETL và Chặng 1 kèm Live Demo #1; bạn Thành Đạt phụ trách khảo sát, PageRank, FPGrowth, 7D K-Means, tách burst, thực nghiệm 23D và Chặng 2 kèm Live Demo #2–#3; còn em phụ trách Docker 5 dịch vụ, Kafka, Sketches 33 KiB, đối soát hai tầng, MongoDB IXSCAN, web ReactJS + FastAPI và Chặng 3 kèm Live Demo #4. Hệ thống đạt 44/44 kiểm thử tự động và 0 lỗi Ruff độ phức tạp dưới 15.
  >
  > Lộ trình mở rộng gồm ba giai đoạn: Giai đoạn 1 áp dụng Focal Loss và ngưỡng động nâng nhạy ca đột ngột hóa kẹt; Giai đoạn 2 nối GPS đa phương tiện, API mưa ngập và tái huấn luyện theo PSI; Giai đoạn 3 nâng cấp Kubernetes đa nút, MongoDB phân mảnh và kết nối API đèn SCATS."
- **Tra cứu nhanh:** Chương 7 (`7.1–7.5`).

---

#### Slide 17: Tổng Kết 5 Trụ Cột Kiểm Định Kỹ Thuật & Mời Hội Đồng Q&A (`19:40 – 21:00`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Chỉ dẫn trực quan:** Điểm lần lượt 5 thẻ trụ cột từ trên xuống dưới trên Slide 17 và kết thúc đúng mốc `21:00` để mời Hội đồng đặt câu hỏi.
- **Lời thoại thuyết trình:**
  > "Khép lại trên Slide 17 là năm trụ cột kiểm định kỹ thuật đồng bộ 100% từ bằng chứng JSON chạy thật trên đĩa.
  >
  > Một là Kiến trúc: tách rành mạch phần đã triển khai — 5 container Docker trong 11 GB RAM, cổng ReactJS + FastAPI :8000 — với đề xuất quy mô đô thị gồm luồng tỷ tin, Kubernetes đa nút và đèn SCATS, chuẩn hóa thuật ngữ Predictive ML.
  >
  > Hai là Hiệu năng: Spark 3.5.7 BroadcastHashJoin cắt 80% Exchange, giảm trễ Combined 12.45% và Isolated 25.76%; 8-Bucket Salting đưa skew về 1.0197x; MongoDB IXSCAN đạt 0.360 ms (31.36x).
  >
  > Ba là Kiểm định mô hình: tách 7,192 burst còn 1,885 organic TRAIN, RF 23D thắng VAL Gate 0.8882, McNemar p = 0.001953, kiểm chứng PageRank số một Nguyễn Kiệm đo nút phễu cấu trúc.
  >
  > Bốn là Vận hành: chạy Watermark thật trên Spark Streaming, WAL 73.7 KB hồi phục 0 trùng, Sketches 33 KiB, Tier-2 100% và status_version chặn 25/25 tin cũ.
  >
  > Năm là Tính minh bạch: nêu rõ 88.13% Cold-Start, Macro-F1 0.4253 và 0/8 khởi phát đột ngột. Cả năm trụ cột đã được chứng minh trực tiếp qua bốn chặng Live Demo tại cổng 8000. Em xin cảm ơn và mời hội đồng đặt câu hỏi."
- **Tra cứu nhanh:** `outputs/metrics.json`, `outputs/spark_runtime_evidence.json`, `outputs/mongo_verify.json`.

---

## Phần 3. Bảng Thao Tác Nhanh Cho Driver (4 Mini-Demos · `4m15s` · `ReactJS + FastAPI TOC :8000`)

| Mini-Demo | Khung giờ (`Thời lượng`) | Presenter / **Driver** | Trình tự Click Chính xác trên `http://localhost:8000` | Thời gian chờ & Mẹo Câu Cầu Nối (`3s`) | Điểm Kiểm Chứng Trên Màn Hình |
|---|---|---|---|---|---|
| **Live Demo #1** *(Luồng P1)* | `05:15 – 06:00` (`0m45s`) | Quyền / **Đạt** | 1. `Alt+Tab` vào **Tab 06** (đã chọn sẵn trace `#50026 Điện Biên Phủ`) → 2. Bấm **`Slide 06 · P1 Monoid/Vmax`** (chạy **Stage 1 → 2 → 3**) → 3. `Alt+Tab` về Slide 07 | Tức thì (`<0.3s`). Quyền dẫn câu chuyển mic 1 khi Driver quay về Slide 07 | `2` ping thô (`17 & 6 km/h`) → phễu `R1–R6` → `Vmax 60 km/h` → `derive_urban_los` → `5-Tuple Monoid` (`11.5 km/h`, sai số `0.0`) |
| **Live Demo #2** *(Luồng P2–P3)* | `08:35 – 09:20` (`0m45s`) | Đạt / **Minh** | 1. `Alt+Tab` về **Tab 06** → bấm **Stage 4 → Stage 5** → 2. Click sang **Tab 03** → 3. Click sang **Tab 04** (`#37345` đã chọn mặc định) → 4. `Alt+Tab` về Slide 10 | Tab 03 load `~0.5s`: **Driver click trước** ngay khi Đạt đọc câu nối 3 giây *"mở rộng ra toàn mạng lưới 84,206 đoạn..."* | Stage 4–5: `PageRank`, `Cụm #1`, vector `23D`, biểu đồ 6 cột `RF 23D` **`LOS F = 57.9%, P(E/F) = 87.2%`**; Tab 03: `#21969` vs `#34019`, `Lift 15.23x`; Tab 04: `#37345 Cầu vượt Ngã tư Gò Dưa` |
| **Live Demo #3** *(Luồng P4)* | `11:15 – 12:00` (`0m45s`) | Đạt / **Minh** | 1. `Alt+Tab` vào **Tab 06** (hoặc **Tab 02**) → 2. Bấm **`⚡ Bơm 200 tin Kafka Live (Stress-Test)`** → 3. Lướt nhanh sang **Tab 05** → 4. `Alt+Tab` về Slide 12 | Kafka 200 tin chạy `~1.5–2s`: trong lúc KPI nhảy, Đạt giải thích rõ mẫu live `200` tin vs benchmark full `33,727` tin | 5 thẻ KPI + **Stage 6–7** sáng đèn (`P0/P1/P2`, **Bloom > 0**, **Watermark > 0**, `HLL++`, `Mongo v1`); Tab 05: `5→1 Exchange`, `Combined -12.45%`, `Isolated -25.76%`, `1.0197x`, `Tier-2 100%` |
| **Live Demo #4** *(TOC Thực Địa)* | `15:10 – 17:10` (`2m00s`) | Minh / **Đạt** | 1. `Alt+Tab` sang **Tab 01** (sẵn **Kịch bản 3 `#50026`**) → 2. Bấm **`🙈 Ẩn nhãn thực tế t+30p`** → 3. Bấm mở **`★ Thực tế: LOS F`** → 4. Đổi **Kịch bản 2 `#21064`** → 5. Bấm **`⚡ Mongo IXSCAN (31.36x)`** → 6. `Alt+Tab` về Slide 15 | Đổi Kịch bản 2 mất `~0.5s`: **Driver click trước** khi Minh dẫn câu cầu nối 3 giây về *"cảnh báo xác suất sớm ở vòng xoay..."* | Kịch bản 3 (`[07:00–07:30)`, chốt `07:30 ICT`, `122/38`): kiểm chứng mù ẩn sạch bảng & biểu đồ → mở `LOS F` khớp 100%; Kịch bản 2 (`[11:30–12:00)`, chốt `12:00 ICT`, `4` pings `23→3`, `68.7% / 48.9%`); `IXSCAN 0.360 ms / p95 0.603 ms` (`0 SORT RAM`) |

> **Backup Plan 15s (nếu mạng cục bộ hoặc Docker lag):** Giữ nguyên đúng khung giờ từng Mini-Demo ở trên, Driver mở ngay bộ ảnh kết xuất sẵn `outputs/slide_charts/ui_tab1_overview_hotspots.png`, `ui_tab2_live_forecast_mongo.png`, `ui_tab4_kmeans_coldstart.png` kèm JSON kiểm chứng (`outputs/spark_runtime_evidence.json`, `outputs/mongo_verify.json`) để Presenter thuyết minh trơn tru không gián đoạn.

---

## Phần 4. Cẩm Nang Phản Biện & Trả Lời Hội Đồng (Mỗi Câu ~30 Giây · Chắc Số Liệu)

### Câu 1: *(Bẫy Hội Đồng #1)* *"Trên tập TEST, `RF 14D` có `Sev-F1 = 0.9380` và `PR-AUC = 0.9554` nhỉnh hơn `RF 23D` (`0.9362 / 0.9543`). Tại sao nhóm vẫn chọn `RF 23D`?"*
- **Người trả lời:** **Thành Đạt** (`Slide 09, 12` · `outputs/metrics.json`).
- **Trả lời 30 giây:**
  > "Trong thực nghiệm khoa học, tuyệt đối không được nhìn kết quả `TEST` để chọn mô hình. Nhóm khóa trước tiêu chí **`VAL Composite Gate = (VAL 6-Class Acc + VAL PR-AUC + VAL ROC-AUC) / 3`** trên tập kiểm định lệch thời gian `VAL`. Trên tập `VAL`, `RF 14D` thiếu 9 đặc trưng đồ thị và lịch sử nên **sập nặng độ chính xác 6 lớp xuống chỉ còn `0.1697` (`16.97%`)**, kéo điểm tổng hợp về **`0.6794`** (đồng thời `VAL Sev-F1 = 0.8854`). Ngược lại, **`RF 23D` giữ vững `VAL 6-Class Acc = 0.8166`, `VAL Sev-F1 = 0.8912` và thắng rõ rệt ở `VAL Gate = 0.8882`**, đồng thời trên `TEST` cũng đạt **`ROC-AUC` (`0.9894`), `6-Class Acc` (`0.9255`) và `Macro-F1` (`0.4253` vs `0.3976`) cao nhất**."

### Câu 2: *(Bẫy Hội Đồng #2)* *"Trong tệp kiểm chứng Spark, `Spark MLlib RF` đạt `acc = 0.9416` (`ROC = 0.9903`), cao hơn mô hình triển khai `sklearn RF 23D` (`acc = 0.9255`, `ROC = 0.9894`). Vì sao không triển khai `Spark MLlib RF`?"*
- **Người trả lời:** **Thành Đạt** (`Slide 10, 12, 17` · `spark_engine.py`, `pipeline.py`).
- **Trả lời 30 giây:**
  > "Hai mô hình khác nhau về mục tiêu và hàm trọng số lớp: `Spark MLlib RF` (`25` cây, `maxDepth = 6`, không cân bằng trọng số lớp, kèm `OneHotEncoder(street_type, period)`) được chạy để kiểm chứng năng lực huấn luyện phân tán, lưu/tải `PipelineModel` (`reload parity = True`) và xử lý hạng mục lạ (`handleInvalid='keep'`). Do không gắn trọng số lớp, `Spark MLlib RF` nghiêng về đoán nguyên trạng lớp chiếm đa số `A/F` giống `Persistence (94.84%)`. Trong khi đó, `sklearn RF 23D` triển khai (`120` cây, `max_depth = 10`, trọng số cân bằng $w_c \propto (N / 6n_c)^{0.65}$) chủ động đánh đổi ~`1.6%` độ chính xác quán tính để bắt trúng các ca đổi pha (`10/61 → 14/61`, `McNemar p = 0.001953`), thắng `VAL Composite Gate` và chấm điểm từng sự kiện trực tuyến dưới `1 ms` trong `live-consumer` không tốn chi phí JVM."

### Câu 3: *(Bẫy Hội Đồng #3)* *"Trên Demonstration, tại sao `#21064 Công Trường Quốc Tế` chỉ hiện nhãn thô `raw = E` (`P(E/F) = 68.7%`) trong khi `#50026 Điện Biên Phủ` dự báo trúng thẳng `E → F` (`P(F) = 57.9%`, `P(E/F) = 87.2%`)?"*
- **Người trả lời:** **Công Minh** (`Slide 14` · `toc_backend/data_store.py`).
- **Trả lời 30 giây:**
  > "Sự khác biệt đến từ bối cảnh hình học – giờ cao điểm và minh họa đúng hai tầng quyết định của hệ thống. Đoạn `#50026 Điện Biên Phủ` là trục chính cấp 2 (`Vmax 60 km/h`) lúc cao điểm sáng (khung `[07:00–07:30)`, chốt `07:30 ICT`, `11.5 km/h`, tỷ số vận tốc `0.192`), kết hợp lịch sử kẹt trục lớn và láng giềng nên xác suất `P(F) = 57.9%` vượt hẳn `P(E) = 29.3%` (`P(E/F) = 87.2%`), cho kết quả đoán thẳng `E → F`. Còn `#21064 Công Trường Quốc Tế` là vòng xoay cấp 4 buổi trưa (khung `[11:30–12:00)`, chốt `12:00 ICT`, `12.0/50 = 0.24`), nhãn `argmax` thô bảo thủ giữ mức `E` (`51.1%`), nhưng **tầng cảnh báo xác suất đã hiệu chỉnh trên `VAL`** vẫn bật báo động tức thời nhờ **`P(E/F) = 68.7%` (> `25%`) và `P(chg) = 48.9%` (> `40%`)**, chứng minh điều hành viên TOC không bị bỏ lọt cảnh báo."

### Câu 4: *(Bẫy Hội Đồng #4)* *"Nhóm kiểm chứng nguồn dữ liệu gốc Kaggle v6 và các con số `85.21%` vs `85.19%`, `46/48` khung giờ và `89.19%` khớp BkTraffic như thế nào?"*
- **Người trả lời:** **Minh Quyền** (`Slide 05, 06, 08` · `raw_data/*.csv`, `pipeline.py`).
- **Trả lời 30 giây:**
  > "Nhóm đối chiếu trực tiếp 5 tệp CSV tải từ Kaggle phiên bản 6 (`CC0 · BkTraffic HCMUT`, tổng đúng `41` cột, `38.50 MB`): bảng `train.csv` gốc có `33,441` dòng chỉ xuất hiện `46/48` khung 30 phút (khuyết 2 khung thấp điểm đêm) và thiếu `max_velocity` ở **`28,495 / 33,441 = 85.21%`**. Sau khi quy tắc `R2` lọc bỏ `427` đoạn phi cơ giới (`47` cửa sổ tương ứng), tập sạch còn `33,394` cửa sổ cơ giới với **`28,448 / 33,394 = 85.19%`** thiếu `max_velocity` gốc. Từ vận tốc trung bình và ngưỡng đường gốc, hàm **`derive_urban_los`** (bậc `5 km/h` chuẩn đô thị có dịch ngưỡng trục nhanh) tái lập khớp **`89.19%`** nhãn LOS tham chiếu của BkTraffic."

### Câu 5: *(Bẫy Hội Đồng #5)* *"Tại sao nhóm đầu tư dựng `ReactJS + FastAPI (:8000)` trong cụm Docker 5 dịch vụ (`11.0 GB RAM`) thay vì dùng một công cụ Python dựng giao diện nhanh như Streamlit?"*
- **Người trả lời:** **Công Minh** (`Slide 04, 14, 17` · `docker-compose.yml`, `toc_backend/`, `toc_frontend/`).
- **Trả lời 30 giây:**
  > "Công cụ dựng mẫu nhanh như Streamlit phải chạy lại toàn bộ kịch bản Python mỗi khi người dùng nhấp bản đồ hay kéo thanh thời gian, dễ giật lag khi hiển thị bản đồ `84,206` đoạn đường và khó quản lý trạng thái luồng bất đồng bộ. Với **`ReactJS + Leaflet` tách rời `FastAPI` trên cổng `:8000`**, giao diện giữ mượt trạng thái bản đồ, chạy trực quan 7 chặng Pipeline trên `Tab 06` và phục vụ đồng thời nhiều truy vấn `MongoDB 8.0` dưới `1 ms` song song với nút kích phát `Kafka Live`, đồng thời gói gọn toàn bộ dịch vụ web trong đúng **`1.0 GB RAM`** (tổng 5 container mặc định `11.0 / 16.0 GB RAM`)."

### Câu 6: *"Tổng 5 tệp gốc chỉ `38.50 MB`, vì sao phải dùng `Spark + Kafka + 8-Bucket Salting` thay vì Pandas?"*
- **Người trả lời:** **Minh Quyền** (`Slide 05, 06, 10` · `evidence/session9/B_benchmark.csv`).
- **Trả lời 30 giây:**
  > "Nếu chỉ xử lý tĩnh `38.50 MB` một lần thì Pandas đơn luồng nhanh hơn. Nhưng trong bài toán `Big Data`, tập Kaggle đóng vai trò **mẫu chuẩn `Proof-of-Scale`** kiểm chứng sai số bằng `0.0` cho quy mô thực tế toàn TP.HCM **`8–17` tỷ tin/ngày (`1.5–3.0 TB/ngày`)** — nơi `set()` lọc trùng sẽ tràn RAM, join không `Broadcast` sẽ nghẽn mạng và độ lệch tải giờ cao điểm **`684.95x`** sẽ làm treo worker. Trên Spark `3.5.7`, `BroadcastHashJoin` cắt `5 → 1` Exchange (**`Combined -12.45%`: `213.66 → 187.07 ms`** và **`Isolated -25.76%`: `166.18 → 123.37 ms`**) và `8-Bucket Salting` đưa lệch tải về **`1.0197x`** (`diff = 0`)."

### Câu 7: *"Tại sao không dùng Deep Learning trên đồ thị (`DCRNN`, `Graph WaveNet`) thay vì `Random Forest 23D`?"*
- **Người trả lời:** **Thành Đạt** (`Slide 03, 09`).
- **Trả lời 30 giây:**
  > "Các mạng GNN chuỗi thời gian như `DCRNN` hay `Graph WaveNet` đòi hỏi chuỗi quan sát liên tục trên đa số đỉnh đồ thị. Tại TP.HCM, **`88.13%` đoạn cơ giới (`74,211` đoạn) không có bất kỳ bản tin GPS nào** và **`66.30%` cặp quan sát đứt quãng $\ge 60$ phút**, làm phép tích chập thời gian trên đồ thị bị suy biến. `Random Forest 23D` kết hợp `PageRank` và `7D K-Means` xử lý hiệu quả dữ liệu bảng đứt quãng, phủ `100%` mạng lưới và chạy nhẹ không cần GPU."

### Câu 8: *"Tại sao phải dùng `5-Tuple Monoid` khi chia `8` giỏ `Key Salting`? Đánh đổi độ trễ của `Salting` trên tập nhỏ ra sao?"*
- **Người trả lời:** **Minh Quyền** (`Slide 06, 10` · `evidence/session9/B_benchmark.csv`).
- **Trả lời 30 giây:**
  > "Phép lấy trung bình không có tính kết hợp khi các phân vùng có số xe khác nhau — phép gộp `avg` lồng nhau gây sai số tới **`11.11 km/h`** trên phân vùng `[1, 2, 3]`. Đóng gói thành bộ 5 đại lượng cộng được `(n, sum_v, min_v, max_v, severe_cnt)` giúp `8-Bucket Salting` đạt **sai số `0.0`** và hạ skew từ `684.95x → 1.0197x` (`CV = 0.0059`). Trên tập mẫu nhỏ `90.8k` pings, 2 nhịp shuffle tăng độ trễ cô lập `97.92 → 185.78 ms` (**`+87.92 ms` so với V1** trên benchmark 3 nhánh: `187.07 → 274.99 ms`), một đánh đổi hoàn toàn hợp lý để tránh treo máy ở quy mô hàng tỷ tin."

### Câu 9: *"Làm sao chứng minh mô hình hoàn toàn không rò rỉ dữ liệu tương lai (`Zero Future Leakage`)?"*
- **Người trả lời:** **Thành Đạt** (`Slide 06, 08, 09`).
- **Trả lời 30 giây:**
  > "Nhóm khóa đủ 5 chốt chặn: **(1)** bảng `3-Tier Vmax` chỉ học trên tập `TRAIN`; **(2)** bộ chuẩn hóa và tâm cụm `7D K-Means` chỉ học trên `3,609` đoạn `organic TRAIN`; **(3)** mọi biến lịch sử quá khứ (`past-only priors`) tích lũy nghiêm ngặt tại $\tau < t$; **(4)** tách sạch `7,192` cửa sổ burst khỏi `TRAIN` và cắt `55` mẫu biên `Boundary Purge`; **(5)** phép thử `Future-Shuffle Falsification` xáo trộn dữ liệu ở $\tau > t$ xác nhận ma trận đầu vào $X_t$ có sai lệch **`0.0`**."

### Câu 10: *"PageRank số 1 (`#21969 Nguyễn Kiệm = 2.7799`) nhưng thực tế chạy thoáng (`LOS A`, `45.41 km/h`) — vậy PageRank có bị sai không?"*
- **Người trả lời:** **Thành Đạt** (`Slide 07, 10, 17` · `Mini-Test B2`).
- **Trả lời 30 giây:**
  > "Không sai — đây chính là phát hiện kiểm chứng rất giá trị của nhóm: **`Line-Graph PageRank` đo độ nhạy nút phễu hình học** (nơi nhiều nhánh đổ dồn vào một đoạn), không phải đo mức kẹt hiện tại. Nguyễn Kiệm có mặt cắt rộng nhiều làn nên hấp thụ tốt lưu lượng (`45.41 km/h`, `LOS A`), nhưng nếu xảy ra sự cố hay rào chắn tại đây thì ảnh hưởng lan tỏa sẽ lớn nhất. Khi kết hợp `PageRank` cao với lịch sử kẹt thực tế (`severe prior`), hệ thống tách bạch chính xác các nút cổ chai kẹt thật như `#34019 Trần Quốc Thảo` (`7.25 km/h`) hay `#63450 Lý Thường Kiệt` (`8.54 km/h`)."

### Câu 11: *"Tại sao cần đồng thời cả `Random Forest 23D` lẫn `EWMA Hotspot`? Khác nhau thế nào?"*
- **Người trả lời:** **Công Minh** (`Slide 07, 11`).
- **Trả lời 30 giây:**
  > "`Random Forest 23D` trả lời câu hỏi **tương lai 30 phút tới** (*đoạn nào SẮP kẹt cứng?* — giúp chỉnh pha đèn phòng ngừa), còn **`EWMA Hotspot` ($\alpha = 0.15$)** đo **độ lì hiện tại** (*đoạn nào ĐANG kẹt lặp lại nhiều khung liên tiếp chưa giải tỏa?* — như `#47407 Lý Thường Kiệt = 6.6652`) để điều động lực lượng giải phóng nút nghẽn ngay tức thì. Hai tín hiệu bổ trợ nhau hoàn hảo trên bảng điều hành TOC."

### Câu 12: *"Cơ chế nào giúp `MongoDB 8.0` đạt `0.360 ms / p95 0.603 ms` (`31.36x`) và chống bản tin cũ `v1` ghi đè bản chuẩn `v2`?"*
- **Người trả lời:** **Công Minh** (`Slide 11, 14` · `outputs/mongo_verify.json`).
- **Trả lời 30 giây:**
  > "Nhóm áp dụng quy tắc **`ESR` (`Equality – Sort – Range`)** tạo chỉ mục phức hợp `{street_level: 1, severe_prob: -1, segment_id: 1}`. Khi TOC truy vấn `1,234` cảnh báo trục chính (`street_level` 1–2, `severe_prob >= 0.70`) sắp giảm dần, `MongoDB 8.0` đọc thẳng trên cây `B-Tree` (`IXSCAN`, `in_memory_sort = false`, chỉ quét `26` khóa cho `25` kết quả), đạt **`0.360 ms` trung vị** (**`31.36x`** so với `COLLSCAN 11.289 ms`). Đồng thời, lệnh `Bulk UpdateOne` gắn chốt điều kiện nguyên tử **`$status_version`** chặn **`25/25`** bản tin cũ `v1` đến muộn không thể ghi đè bản chuẩn `v2`."

---

### Bảng Giải Nghĩa Nhanh Thuật Ngữ Trọng Tâm (Phòng Vệ Hội Đồng)

| Thuật ngữ / Ký hiệu | Giải nghĩa ngắn gọn (chuẩn xác khi trả lời Hội đồng) |
|---|---|
| **`Persistence Baseline`** | Mốc đối chứng tối giản **không dùng AI** do chính nhóm tự thiết lập — chỉ đoán giữ nguyên mức kẹt hiện tại sau 30 phút ($\hat{y}_{t+30\text{m}} = \text{LOS}_t$). Mạnh ở pha ổn định (`94.84%`) nhưng mù `0/61` ở ca đổi pha và `0%` độ phủ vùng mù `Cold-Start`. |
| **`Masked-GPS Proxy`** | Phép thử định lượng độ chính xác cho vùng mù: nhóm **cố tình che toàn bộ tín hiệu GPS hiện tại của tập `TEST` thành trống** để giả lập đoạn không cảm biến (chỉ còn GIS tĩnh, `PageRank`, cụm `7D K-Means` và lịch sử tuyến quá khứ), đo lại đạt `58.63%` 6-class Acc (`ROC-AUC 0.7000`, `Brier 0.1123`). |
| **`Burst tổng hợp / Organic TRAIN`** | Sáng `03/08/2020` (UTC `2020-08-02 23:30` & `2020-08-03 00:00`) riêng một sáng đổ dồn `36,096` pings (`39.7%` dữ liệu cả 10 tháng), tạo ra **`7,192` cửa sổ burst tổng hợp**. Tách sạch khối này (cùng `55` mẫu biên `Boundary Purge`) giúp tập **`Organic TRAIN` (`1,885` mẫu)** phản ánh đúng nhịp giao thông thực. |
| **`street_causal_prior_*`** | Biến **tiên nghiệm quá khứ** tích lũy mở rộng nghiêm ngặt tại $\tau < t$ (và đóng băng sau mốc cắt `TRAIN`) theo chuẩn *time-causal* trong xử lý chuỗi thời gian — **không phải suy luận nhân quả can thiệp** (*do-calculus / counterfactual*). |
| **`v1` vs `v2` (`$status_version`)** | **`v1` (`STREAM_PROVISIONAL`)**: cảnh báo phát tức thời trên Speed Layer (có thể còn thiếu tin trễ quá Watermark 30 phút). **`v2` (`BATCH_RECONCILED` / `BATCH_VERIFIED`)**: bản chuẩn sau đối soát cuối ngày `Tier-2`. Khóa nguyên tử `$status_version` trong MongoDB chặn `25/25` tin `v1` đến muộn ghi đè ngược `v2`. |
| **`[I]` vs `[P]`** | **`[I]` (Implemented)**: phần đã chạy thật và đo kiểm chứng có bằng chứng JSON/pytest trong cụm `5` container Docker (`11.0 GB RAM`, `Spark 3.5.7`, `Kafka 4.1`, `MongoDB 8.0`, `ReactJS + FastAPI :8000`). **`[P]` (Proposed)**: kiến trúc đề xuất mở rộng quy mô toàn TP.HCM (`8–17` tỷ tin/ngày, Kubernetes đa nút, MongoDB phân mảnh, kết nối đèn SCATS). |
| **3 nghĩa của `"Baseline"`** | Nhóm phân tách rạch ròi 3 mốc đối chứng khác nhau để không gây nhầm lẫn: **(1) `Persistence Baseline`** = quy tắc đối chứng ML đoán giữ nguyên `LOS(t)`; **(2) Cấu hình Spark mặc định `SortMerge`** = mốc đo hiệu năng Catalyst trước khi bật `BroadcastHashJoin`; **(3) Mã tham chiếu đơn máy `baseline.py`** = bộ kiểm toán `5-Tuple Monoid` đơn luồng đối chiếu sai số `0.0` cho Spark `8-Bucket Salting`. |
