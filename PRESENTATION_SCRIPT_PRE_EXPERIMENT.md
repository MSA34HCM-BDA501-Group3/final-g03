# Kịch Bản Thuyết Trình Đề Xuất Thiết Kế — Bản Trước Thực Nghiệm (13 Slides · 15 Phút Chuẩn)

- **Tên đề tài:** Hệ thống Giám sát Hiện trạng & Dự báo Ùn tắc Giao thông Đô thị TP. Hồ Chí Minh (*HCMC Smart Traffic Big Data Analytics & Congestion Prediction Platform*)
- **Đơn vị & Học phần:** `FSB · FPT University | MSE AI | BDA501 – Big Data Analytics` (`Fall 2026`)
- **Giảng viên hướng dẫn (Instructor):** **Đặng Trần Khánh**
- **Cột mốc báo cáo:** **Design & Methodology Report (Pre-Experiment Milestone)** — Hoàn tất kiểm toán dữ liệu gốc Kaggle v6, nghiên cứu mô hình và thiết kế toàn bộ luồng xử lý từ **Mục 1 đến Mục 8**, tiếp nối bởi **Mục 9 (`Limitations & Risks`)** và **Mục 10 (`Plan & Next Actions`)** trước khi bước vào giai đoạn huấn luyện thực nghiệm và đo kiểm runtime.
- **Bộ slide trình chiếu đi kèm (`13 Slides`):** `HCMC_Smart_Traffic_PreExperiment_Presentation.pptx` & `HCMC_Smart_Traffic_PreExperiment_Slides.pdf`
- **Cơ chế Tiếp sức 3 Chặng Liên Mạch (Đúng 2 Lần Chuyển Mic · Cân Bằng ~33% Mỗi Thành Viên · Không Giới Thiệu Lặp · Đồng Bộ Slide 13):**
  1. **Chặng 1 — Nguyễn Tạ Minh Quyền (`25MS23280`) · Slide 01–05 (`00:00 – 05:15`, `5m15s` · `35.0%`, ~`850` từ):** Mở đầu, Bài toán thực tế, Khảo sát tài liệu, Thiết kế Kiến trúc tổng thể `Zones A–F` & Kiểm toán dữ liệu gốc Kaggle v6 (`EC-1..EC-10`). *(Sở hữu thiết kế kỹ thuật: Mục 1, 4, 5 & 7A — Data, Quality Funnel & Batch ETL)*
  2. **Chặng 2 — Nguyễn Thành Đạt (`25MS23306`) · Slide 06–09 (`05:15 – 10:15`, `5m00s` · `33.3%`, ~`750` từ):** Trực quan hóa Phương pháp (1/2 & 2/2), Thiết kế Data Pipeline (`6` quy tắc `R1–R6` & `derive_urban_los`) & Thiết kế Modeling (`Exact t+30m`, `23D Predictive ML`, kế hoạch `Ablation 7 mô hình` & `VAL Gate`). *(Sở hữu thiết kế kỹ thuật: Mục 2, 6 & 7B — Literature, Graph, K-Means & Modeling)*
  3. **Chặng 3 — Phạm Công Minh (`25MS23310`) · Slide 10–13 (`10:15 – 15:00`, `4m45s` · `31.7%`, ~`745` từ):** Thiết kế Batch Processing (`Spark`), Thiết kế Stream Processing & MongoDB Serving (`Kafka`, `33 KiB Sketches`, `IXSCAN SLA < 10 ms`, `ReactJS + FastAPI TOC :8000`), Tổng hợp Limitations & Risks, và Kế hoạch Thực nghiệm (`Sprint 1 & Sprint 2`, `Target KPIs`). *(Sở hữu thiết kế kỹ thuật: Mục 3, 8, 9 & 10 — Architecture, Stream, MongoDB, Web TOC & Plan)*

---

## Phần 1. Bảng Ánh Xạ & Phân Bổ Thời Gian Liên Tục (`15:00` Chuẩn — Đúng 2 Lần Chuyển Mic)

| Slide | Chặng | Mục Báo Cáo Thiết Kế | Nội dung Cốt lõi (Chỉ dùng EDA đầu vào & Mục tiêu Thiết kế) | Người nói | Khung Thời gian | Thời lượng |
|---|---|---|---|---|---|---:|
| **Slide 01** | Chặng 1 | **Mở đầu (Cover)** | Giới thiệu cột mốc thiết kế, đề tài, 3 thành viên (1 lần duy nhất) & bố cục 3 chặng | **Minh Quyền** | `00:00 – 00:35` | `0m35s` |
| **Slide 02** | Chặng 1 | **1. Problem Statement** | Bối cảnh ùn tắc lan truyền TP.HCM (`WHY`), 3 hạn chế hiện hữu & bài toán + giá trị (`WHAT`) | **Minh Quyền** | `00:35 – 01:45` | `1m10s` |
| **Slide 03** | Chặng 1 | **2. Literature Review** | Khảo sát 4 hướng tiếp cận giao thông đô thị, khoảng trống nghiên cứu & mạch thiết kế thuận chiều | **Minh Quyền** | `01:45 – 02:50` | `1m05s` |
| **Slide 04** | Chặng 1 | **3. System Architecture** | Thiết kế `Zones A–F`, 4 luồng Pipeline, `3-Phase MLOps`, `Predictive ML`, Docker `11.0 GB` + Arch Risks | **Minh Quyền** | `02:50 – 04:05` | `1m15s` |
| **Slide 05** | Chặng 1 | **4. Data, EDA & Kaggle v6** | Kiểm toán 5 CSV Kaggle v6 (`41` cột, `38.50 MB`, `46/48` khung, `85.21% / 85.19%`), `Proof-of-Scale` → *Chuyển mic 1* | **Minh Quyền** | `04:05 – 05:15` | `1m10s` |
| **Slide 06** | Chặng 2 | **Phương pháp (1/2)** | Trực quan hóa `Train-Only 3-Tier Vmax`, thiết kế `derive_urban_los` & `5-Tuple Monoid + Key Salting` | **Thành Đạt** | `05:15 – 06:30` | `1m15s` |
| **Slide 07** | Chặng 2 | **Phương pháp (2/2)** | Trực quan hóa `Line-Graph PageRank` (`30` vòng lặp), `7D K-Means` (`88.13%`) & `33 KiB O(1) Sketches` | **Thành Đạt** | `06:30 – 07:45` | `1m15s` |
| **Slide 08** | Chặng 2 | **5. Data Pipeline** | Hành trình `P1 → P4`, `6` quy tắc `R1–R6` (`0` silent drops), `derive_urban_los`, `Predictive ML` + Pipeline Risks | **Thành Đạt** | `07:45 – 09:00` | `1m15s` |
| **Slide 09** | Chặng 2 | **6. Modeling** | Nhãn `Exact t+30m`, tách `7,192` burst, vector `23D` ($\tau < t$), đề xuất `RF` + `Ablation` & `VAL Gate` → *Chuyển mic 2* | **Thành Đạt** | `09:00 – 10:15` | `1m15s` |
| **Slide 10** | Chặng 3 | **7. Batch Processing** | Thiết kế `Spark Batch ETL` (`BroadcastJoin`, `8-Bucket Salting`, `Parquet`) & `Graph / SON / 7D K-Means` + Batch Risks | **Công Minh** | `10:15 – 11:30` | `1m15s` |
| **Slide 11** | Chặng 3 | **8. Stream & Serving** | Thiết kế `Kafka 4.1`, `Watermark 30m`, `33 KiB Sketches`, `WAL`, `Tier-2 Recon`, `Mongo IXSCAN < 10 ms` & `TOC :8000` | **Công Minh** | `11:30 – 12:45` | `1m15s` |
| **Slide 12** | Chặng 3 | **9. Limitations & Risks** | Tổng hợp giới hạn thiết kế 5 thành phần (`Mục 3, 5, 6, 7, 8`) & 4 rủi ro chung cấp hệ thống | **Công Minh** | `12:45 – 13:55` | `1m10s` |
| **Slide 13** | Chặng 3 | **10. Plan & Next Actions** | Tiến độ hoàn thành thiết kế, lộ trình thực nghiệm `Sprint 1 & Sprint 2` & `Target KPIs` nghiệm thu | **Công Minh** | `13:55 – 15:00` | `1m05s` |

---

## Phần 2. Kịch Bản Lời Thoại Chi Tiết Từng Slide (`Slide 01` – `Slide 13`)

### Chặng 1 — Nguyễn Tạ Minh Quyền (`25MS23280`) · Slide 01 – 05 (`00:00 – 05:15`, `5m15s`)

#### Slide 01: Mở Đầu — Trang Bìa Báo Cáo Thiết Kế & Phương Pháp Luận (`00:00 – 00:35`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Hướng về nhãn `DESIGN & METHODOLOGY REPORT`, tiêu đề `HCMC Smart Traffic Big Data Platform` và dải tên 3 thành viên cùng giảng viên hướng dẫn.
- **Lời thoại thuyết trình:**
  > "Kính chào thầy Đặng Trần Khánh và quý thầy cô. Nhóm chúng em gồm em là Nguyễn Tạ Minh Quyền, bạn Nguyễn Thành Đạt và bạn Phạm Công Minh, xin trình bày báo cáo thiết kế và phương pháp luận đồ án môn BDA501 — Big Data Analytics: Nền tảng Phân tích Dữ liệu Lớn và Dự báo Ùn tắc Giao thông TP.HCM. Nhóm đã hoàn tất kiểm toán dữ liệu gốc và thiết kế từ Mục 1 đến Mục 8, trình bày qua ba chặng trong 15 phút."
- **Đối chiếu Thiết kế:** Trang bìa (`Slide 01`).

---

#### Slide 02: `1. Problem Statement` — Bài Toán Điều Hành Giao Thông Chủ Động Tại TP.HCM (`00:35 – 01:45`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Chỉ lần lượt 3 thẻ bên trái (`WHY`, `3 Hạn chế`, `WHAT`) và đối chiếu bản đồ mạng lưới đường TP.HCM bên phải.
- **Lời thoại thuyết trình:**
  > "Bước vào Phần 1 trên Slide 02 là bối cảnh thực tế và mục tiêu bài toán tại TP.HCM. Mật độ phương tiện dày đặc trên mạng lưới đan xen từ sân bay Tân Sơn Nhất, Quận 1, Lý Thường Kiệt đến Thủ Đức khiến chỉ một điểm nghẽn giờ cao điểm cũng nhanh chóng lan thành kẹt dây chuyền, nhưng công tác điều tiết hiện chủ yếu phản ứng bị động sau sự cố.
  >
  > Công cụ hiện tại vấp ba hạn chế lớn: camera cố định quá tốn kém nên chỉ lắp ở ít nút giao chính; nguồn GPS hành trình lệch về đại lộ, bỏ trống phần lớn vùng mù đường nhánh màu xám và hay ngắt quãng, trễ tin; còn các bảng giám sát chỉ báo nơi đang kẹt, chưa cảnh báo trước 30 phút và tách rời trực ban với quy hoạch hạ tầng.
  >
  > Do đó, nhóm thiết kế nền tảng dữ liệu lớn tích hợp để ước lượng lưu thông toàn đô thị kể cả vùng mù, nhận diện nút cổ chai cấu trúc và dự báo sớm 30 phút cho trung tâm TOC, Sở GTVT và người dân."
- **Đối chiếu Thiết kế:** `scripts/render_visual_assets.py`.

---

#### Slide 03: `2. Literature Review` — Khảo Sát 4 Hướng Tiếp Cận & Khoảng Trống Nghiên Cứu (`01:45 – 02:50`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Điểm qua 4 dòng bảng khảo sát phía trên và nhấn mạnh thẻ `Khoảng Trống Nghiên Cứu` ở phía dưới.
- **Lời thoại thuyết trình:**
  > "Ở Phần 2 trên Slide 03, nhóm khảo sát bốn hướng tiếp cận chính trong quản lý và dự báo giao thông đô thị. Thứ nhất, cảm biến cố định và đèn thích ứng SCATS, SCOOT, CCTV đo chính xác tại nút nhưng chi phí quá cao nên bỏ trống đường nhánh. Thứ hai, dữ liệu hành trình GPS của Herrera và Zheng mở rộng độ phủ hơn nhưng dữ liệu lệch trục lớn, đứt quãng dài và chỉ phản ánh hiện trạng. Thứ ba, dự báo ngắn hạn như ARIMA, DCRNN hay Graph WaveNet rất mạnh trên cao tốc có cảm biến liên tục dày đặc, nhưng suy giảm mạnh ở đô thị thưa và mất tín hiệu kéo dài. Thứ tư, hệ hỗ trợ TOC thường tách rời mô hình học thuật khỏi luồng GPS có tin trễ, tin trùng.
  >
  > Khoảng trống nghiên cứu này khẳng định cần một nền tảng tích hợp đi đúng mạch thuận chiều: từ bài toán, khảo sát, thiết kế kiến trúc–luồng đến kế hoạch thực nghiệm."
- **Đối chiếu Thiết kế:** Cơ sở lý luận `Mục 1–2`.

---

#### Slide 04: `3. System Architecture` — Thiết Kế Kiến Trúc `Zones A–F`, `3-Phase MLOps` & `Predictive ML` (`02:50 – 04:05`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Chỉ sơ đồ 6 phân vùng `Zones A–F` với 4 luồng màu Pipeline bên trái, thẻ `3-Phase MLOps` góc trên phải, và `Arch Risks` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Sang Phần 3 trên Slide 04 là thiết kế kiến trúc lai Batch–Speed gồm sáu phân vùng Zones A–F với bốn luồng màu rõ ràng: Speed màu tím, Batch màu xanh dương, MLOps Registry nét đứt xanh ngọc và Storage & Serving màu xanh lá. Dữ liệu từ Zone A đi qua Apache Kafka KRaft và phễu chất lượng ở Zone B, tách hai nhánh trong Apache Spark: Speed Layer Zone C xử lý luồng trực tuyến với Watermark 30 phút, còn Batch Layer Zone D chạy đồ thị, phân cụm và huấn luyện học máy dự báo Predictive ML chỉ dùng dữ liệu quá khứ.
  >
  > Thiết kế 3-Phase MLOps chống hoàn toàn xung đột ghi vào Zone E: Phase A huấn luyện một lần trên tập TRAIN rồi khóa mã băm SHA-256; Phase B trực tuyến chỉ đọc phát cảnh báo v1; Phase C cuối ngày đối soát tin trễ nâng cấp lên v2 tại Zone F gồm Parquet, MongoDB 8.0 và web ReactJS + FastAPI :8000, gói gọn 5 container trong 11/16 GB RAM.
  >
  > Hai rủi ro sớm là tranh chấp đĩa trên một máy chủ và độ trễ biến lịch sử của tin trễ ngoài Watermark."
- **Đối chiếu Thiết kế:** `pipeline.py`, `starter_stream.py`, `docker-compose.yml`.

---

#### Slide 05: `4. Data, EDA & Data Problem` — Kiểm Toán Kaggle v6, `Proof-of-Scale` & `EC-1..EC-10` (`04:05 – 05:15`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Chỉ dẫn trực quan:** Chỉ biểu đồ dị thường góc trên trái, thẻ Kaggle v6 & `Proof-of-Scale` góc dưới trái, và bảng `EC-1..EC-10` bên phải.
- **Lời thoại thuyết trình:**
  > "Khép lại Chặng 1 ở Phần 4 trên Slide 05 là kết quả kiểm toán 5 tệp CSV gốc Kaggle v6 từ BkTraffic Bách khoa TP.HCM, giấy phép mở CC0, gồm 41 cột, 38.50 MB: 577,967 nút bản đồ cắt 90.97% điểm cong còn 52,181 nút giao thực; 5,553 tuyến đường; 84,633 đoạn còn 84,206 đoạn cơ giới sau khi lọc 427 POI — địa điểm, công trình từ OSM; 90,938 pings GPS; và 33,441 cửa sổ phủ 46/48 khung giờ còn 33,394 cửa sổ cơ giới. Đây là mẫu chuẩn Proof-of-Scale kiểm chứng sai số 0.0 cho quy mô toàn TP.HCM từ 8 đến 17 tỷ bản tin và 1.5–3 TB mỗi ngày.
  >
  > Kết quả EDA bóc tách 10 vấn đề thực tế EC-1 đến EC-10: thiếu Vmax 85.21% bảng thô (28,495/33,441) và 85.19% cơ giới (28,448/33,394); 88.13% hay 74,211 đoạn vùng mù không GPS; 66.30% đứt quãng từ 60 phút kèm 7,192 cửa sổ burst tổng hợp sáng 03/08/2020 — khi riêng một sáng đổ dồn 36,096 pings, chiếm 39.7% dữ liệu; 12.89% lệch ngày UTC+7; cùng lệch tải cao điểm, tin trùng và tin trễ. Em xin chuyển mic sang bạn Thành Đạt."
- **Đối chiếu Thiết kế:** `pipeline.py` (`inspect_raw_tables`), `raw_data/*.csv`.

---

### Chặng 2 — Nguyễn Thành Đạt (`25MS23306`) · Slide 06 – 09 (`05:15 – 10:15`, `5m00s`)

#### Slide 06: Trực Quan Hóa Phương Pháp (1/2) — `3-Tier Vmax`, `derive_urban_los` & `5-Tuple Monoid` (`05:15 – 06:30`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ phễu bậc thang 4 tầng bên trái cho `3-Tier Vmax` và hộp đối chiếu Đỏ/Xanh bên phải cho `5-Tuple Monoid + Key Salting`.
- **Lời thoại thuyết trình:**
  > "Cảm ơn bạn Quyền. Bước sang Chặng 2 trên Slide 06 là nguyên lý ba phương pháp xử lý dữ liệu nền tảng. Bên trái, khi hơn 85% cửa sổ thiếu Vmax, tính trung bình toàn tập sẽ gây rò rỉ tương lai. Nhóm thiết kế bảng tra ba tầng chỉ học trên tập TRAIN từ riêng đến chung: giữ nguyên đoạn có biển hợp lệ; thiếu thì lấy phân vị P85 tốc độ thoáng chính đoạn; ít lịch sử thì mượn trung vị cùng cấp–loại; cuối cùng dùng chuẩn 80/60/50/40 km/h. Kèm theo là thiết kế derive_urban_los chia bậc 5 km/h đô thị có dịch ngưỡng trục nhanh để chuẩn hóa 6 mức A–F và đối chứng ở Sprint 1.
  >
  > Bên phải là bộ gộp 5-Tuple Monoid đi kèm 8-Bucket Key Salting. Máy A có 1 xe 10 km/h, Máy B có 3 xe 40 km/h: trung bình thật 32.5, gộp ngây thơ ra 25 (sai 7.5 km/h). Giữ 5 đại lượng cộng được giúp chia 8 giỏ song song mà sai số 0.0."
- **Đối chiếu Thiết kế:** `pipeline.py` (`fit_train_vmax_lookup`, `derive_urban_los`), `traffic_data/spark_etl.py`.

---

#### Slide 07: Trực Quan Hóa Phương Pháp (2/2) — `Line-Graph PageRank`, `7D K-Means` & `33 KiB Sketches` (`06:30 – 07:45`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ lần lượt 3 cột trực quan từ trái sang phải (`Line-Graph PageRank`, `7D K-Means Cold-Start`, `O(1) Stream Sketches`).
- **Lời thoại thuyết trình:**
  > "Tiếp nối trên Slide 07 là nguyên lý ba cơ chế đồ thị, phân cụm vùng mù và tóm tắt luồng bộ nhớ cố định. Cột một là Line-Graph PageRank: cắt 90.97% điểm vẽ cong trung gian và loại cạnh quay đầu U-turn, đảo mỗi đoạn cơ giới thành một đỉnh trên 92,453 cạnh, chạy PageRank 30 vòng lặp (damping 0.85) và bù khối lượng 314 nút cụt để phát hiện nút phễu tự nhiên ngay cả nơi chưa lắp cảm biến.
  >
  > Cột hai là 7D K-Means: gom 84,206 đoạn vào 6 cụm hành vi–hình thái chỉ học trên TRAIN, giúp 74,211 đoạn vùng mù (88.13%) mượn đặc trưng các tuyến cảm biến đồng dạng phủ kín 100% đô thị.
  >
  > Cột ba là bộ ba O(1) cố định 33 KiB RAM chống tràn bộ nhớ: RotatingBloomFilter 2×16 KiB (k=10) xoay vòng 2 thế hệ lọc tin trùng; HyperLogLog++ 1 KiB (1,024 thanh ghi) đếm số đoạn hoạt động với sai số lý thuyết 3.25%; và trung bình trượt hàm mũ EWMA hệ số 0.15 tách điểm kẹt lì với nhiễu đèn đỏ."
- **Đối chiếu Thiết kế:** `pipeline.py`, `starter_stream.py`.

---

#### Slide 08: `5. Data Pipeline & Processing` — Thiết Kế 6 Quy Tắc `R1–R6`, `derive_urban_los` & `Predictive ML` (`07:45 – 09:00`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ sơ đồ hành trình `P1 → P4` phía trên, thẻ `Quality Funnel R1–R6` góc dưới trái và thẻ `Pipeline Risks` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Sang Phần 5 trên Slide 08, sơ đồ trên nối bốn chặng thiết kế từ bản tin GPS thô qua làm sạch, gộp cửa sổ, chấm điểm đến kho phục vụ. Phễu chất lượng chạy tuần tự 6 quy tắc R1–R6 với ANSI try_cast và Quarantine, bảo đảm 0 sập job và 0 rớt ngầm: R1 bù trung vị cho 154 đoạn dài không dương; R2 cách ly 427 POI — địa điểm, công trình không phải đường — giữ 84,206 đoạn cơ giới; R3 gắn cờ 631 tên quận phường khỏi xếp hạng đường; R4 chuẩn hóa Unicode NFC gộp 695 về 641 đường; R5 nội suy Train-Only 3-Tier Vmax cho 28,448 cửa sổ cơ giới; và R6 cô lập ping lỗi nhưng giữ nguyên 0 km/h xe kẹt đứng. Sau đổi UTC+7 cho 4,305 cửa sổ, derive_urban_los gán 6 mức A–F phục vụ Predictive ML chỉ dùng quá khứ.
  >
  > Ba rủi ro thiết kế là Vmax tĩnh chưa bắt ngập nước hay rào chắn, nguy cơ tăng Quarantine khi đổi schema, và khung 30 phút làm mượt dao động ngắn."
- **Đối chiếu Thiết kế:** `pipeline.py` (`validate_raw_records_ordered`, `derive_urban_los`), `traffic_data/spark_etl.py`.

---

#### Slide 09: `6. Modeling` — Thiết Kế Nhãn `Exact t+30m`, Vector `23D` & Kế Hoạch `Ablation` (`09:00 – 10:15`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Chỉ dẫn trực quan:** Chỉ thẻ `Thiết Kế Nhãn Exact t+30m` góc trên trái, bảng 3 nhóm `23D Predictive ML` góc dưới trái, thẻ `Kiến Trúc Mô Hình & VAL Gate` góc trên phải và `Modeling Risks` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Khép lại Chặng 2 ở Phần 6 trên Slide 09 là thiết kế Predictive ML dự báo trước 30 phút không rò rỉ tương lai. Thứ nhất, vì 66.30% cặp đứt quãng từ 60 phút, nhóm chỉ ghép nhãn đúng mốc +30 phút (7,885 cặp từ EDA), tách sạch 7,192 burst windows khỏi TRAIN, chia thời gian 70/15/15 kèm Boundary Purge và kiểm thử Future-Shuffle xác nhận đầu vào bất biến.
  >
  > Thứ hai, vector 23D gồm 14 biến cục bộ, độ trễ và chu kỳ giờ Việt Nam sin/cos, kết hợp 9 biến đồ thị PageRank, cụm 7D K-Means, mức kẹt láng giềng và thống kê quá khứ tích lũy nghiêm ngặt trước mốc dự báo.
  >
  > Thứ ba, nhóm đề xuất Random Forest 23D (120 cây, độ sâu 10, cân bằng trọng số lớp), đối chiếu Ablation 7 cấu hình cùng Persistence Baseline — quy tắc đối chứng không dùng AI do nhóm tự thiết lập, đoán giữ nguyên mức LOS hiện tại sau 30 phút — chọn khách quan qua cổng VAL Composite Gate để chống nhìn trộm tập TEST và phủ 100% Cold-Start. Rủi ro là quán tính 30 phút làm ngưỡng mặc định bảo thủ và thiếu biến mưa hay tai nạn. Em xin chuyển mic sang bạn Công Minh."
- **Đối chiếu Thiết kế:** `pipeline.py` (`compute_chronological_split_boundaries`, `add_temporal_and_neighbor_lags`).

---

### Chặng 3 — Phạm Công Minh (`25MS23310`) · Slide 10 – 13 (`10:15 – 15:00`, `4m45s`)

#### Slide 10: `7. Batch Processing` — Thiết Kế Luồng `Spark Batch ETL`, Đồ Thị & Phân Cụm (`10:15 – 11:30`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Chỉ dẫn trực quan:** Chỉ sơ đồ hai nhánh phía trên (`Batch ETL → Curated Gold Parquet` và `Graph/K-Means → Static Feature Store`), thẻ thiết kế góc dưới trái và thẻ `Batch Risks` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Cảm ơn bạn Đạt. Bước vào Chặng 3 ở Phần 7 trên Slide 10 là thiết kế xử lý lô phân tán trên Apache Spark theo hai nhánh song song. Ở nhánh Batch ETL trên, chiến lược BroadcastHashJoin ghim hai bảng danh mục nhỏ segments và streets vào RAM các worker để triệt tiêu cấu hình Spark mặc định SortMergeJoin. Tiếp theo là 2-Stage Key Salting chia 8 giỏ kết hợp 5-Tuple Monoid san phẳng tải các trục nóng giờ cao điểm, đối chiếu khớp 100% sai số 0.0 với mã tham chiếu đơn máy baseline.py, trước khi ghi Curated Gold Parquet phân vùng theo tháng hỗ trợ cắt tỉa dữ liệu đọc.
  >
  > Ở nhánh khai phá dưới, khối cắt tỉa dựng đồ thị Line-Graph PageRank bỏ U-turn và bù nút cụt, kết hợp tập TRAIN chạy thuật toán SON phân vùng 2 lượt cùng Spark FPGrowth tìm luật đồng kẹt và 7D K-Means xuất kho Static Feature Store phủ đủ 84,206 đoạn cơ giới.
  >
  > Giới hạn thiết kế là GPS mới phủ 11.87% nên luật trễ 30 phút dễ có cỡ mẫu nhỏ, chỉ dùng làm giả thuyết thăm dò; còn phân cụm tĩnh chưa phản ánh họp chợ tự phát."
- **Đối chiếu Thiết kế:** `spark_batch.py`, `traffic_data/spark_etl.py`, `pipeline.py`.

---

#### Slide 11: `8. Stream Processing & MongoDB Serving` — Thiết Kế Luồng `Kafka`, `33 KiB Sketches` & `IXSCAN` (`11:30 – 12:45`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Chỉ dẫn trực quan:** Chỉ đường nhanh màu tím (`Kafka → Speed Layer → MongoDB`), đường đối soát xanh dương (`Tier-2 Recon`) và 2 thẻ chi tiết phía dưới.
- **Lời thoại thuyết trình:**
  > "Sang Phần 8 trên Slide 11 là thiết kế luồng trực tuyến và kho phục vụ TOC. Gói tin GPS đi vào Apache Kafka 4.1 chế độ KRaft không ZooKeeper, chia ba phân vùng theo mã đoạn giữ đúng thứ tự. Vào Speed Layer, Dual-Sink ghi 100% sự kiện thô kèm nhật ký ghi trước WAL Checkpoint xuống Bronze chống mất tin và chống trùng khi sập luồng, song song áp Watermark 30 phút cho đường cảnh báo nhanh.
  >
  > Trên đường nhanh này, bộ ba Sketches O(1) 33 KiB gồm RotatingBloomFilter, HyperLogLog++ và trung bình trượt hàm mũ EWMA lọc trùng, đếm đoạn hoạt động và chấm điểm kẹt lì trước khi Online Scorer đọc chỉ-đọc mô hình đóng băng phát cảnh báo tạm thời v1. Tin trễ quá 30 phút được Two-Tier Reconciliation cuối ngày gom bù thành bản chuẩn đối soát v2. Cả hai luồng ghi vào MongoDB 8.0 kèm khóa `$status_version` chặn bản tạm v1 ghi đè bản chuẩn v2, cùng chỉ mục ESR giúp quét chỉ mục IXSCAN thay vì quét toàn bảng, hướng tới SLA dưới 10 ms cho web 5-Tab ReactJS + FastAPI :8000. Rủi ro là tin trùng sau hai thế hệ Bloom cần khóa duy nhất MongoDB."
- **Đối chiếu Thiết kế:** `producer.py`, `starter_stream.py`, `toc_backend/*`, `toc_frontend/*`.

---

#### Slide 12: `9. Limitations & Risks` — Tổng Hợp Giới Hạn Thiết Kế 5 Thành Phần & 4 Rủi Ro Chung (`12:45 – 13:55`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Chỉ dẫn trực quan:** Chỉ bảng tổng hợp 5 thành phần bên trái và 2 thẻ bên phải (`4 Rủi Ro Chung` ở trên, `Nguyên Tắc Kiểm Soát` ở dưới).
- **Lời thoại thuyết trình:**
  > "Ở Phần 9 trên Slide 12, nhóm tổng hợp minh bạch mọi giới hạn thiết kế trước thực nghiệm. Bảng bên trái đặt năm thành phần cạnh chốt chặn kiểm soát: Kiến trúc có 5 container dùng chung 11 GB RAM và độ trễ tin ngoài Watermark, kiểm soát bằng mem_limit và 3-Phase MLOps; Data Pipeline có Vmax tĩnh và lệch schema, kiểm soát bằng Train-Only và Quarantine; Modeling có ngưỡng mặc định bảo thủ và lớp trung gian thưa, kiểm soát bằng tách burst, VAL Gate và hiệu chỉnh ngưỡng; Batch có cỡ mẫu luật trễ nhỏ và phân cụm tĩnh; còn Stream có vòng xoay hai thế hệ Bloom và tin quá trễ, kiểm soát bằng khóa duy nhất, đối soát hai tầng và status_version.
  >
  > Thẻ bên phải nêu bốn rủi ro cấp đô thị: GPS mới phủ 11.87% thiên về ô tô; trôi dạt phân phối mùa mưa ngập; tính cực hiếm của pha đột ngột hóa kẹt; và phạm vi Proof-of-Scale khuyến nghị cho TOC, chưa đóng vòng đèn SCATS."
- **Đối chiếu Thiết kế:** Quản trị rủi ro thiết kế `Mục 3–8`.

---

#### Slide 13: `10. Plan & Next Actions` — Kế Hoạch Thực Nghiệm `Sprint 1 & 2` & `Target KPIs` (`13:55 – 15:00`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Chỉ dẫn trực quan:** Chỉ bảng phân công kỹ thuật & Next Actions phía trên, thẻ `Sprint 1 & Sprint 2` góc dưới trái và thẻ `Target KPIs` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Khép lại ở Phần 10 trên Slide 13 là lộ trình thực nghiệm hai Sprint và bộ chỉ tiêu nghiệm thu. Bảng trên phân công rõ sở hữu kỹ thuật: bạn Minh Quyền thực thi Quality Funnel, kiểm chứng 5-Tuple Monoid với chuẩn đơn luồng, đo 8-Bucket Salting, BroadcastHashJoin và Parquet Pruning; bạn Thành Đạt chạy PageRank, FPGrowth, 7D K-Means, tách burst, huấn luyện Ablation 7 mô hình (14D vs 23D), chọn qua VAL Gate, kiểm định Future-Shuffle, McNemar và hiệu chỉnh ngưỡng; còn em dựng cụm 5 container Docker đo RotatingBloom, HyperLogLog++, diễn tập WAL và Watermark thật, Tier-2 Recon, MongoDB IXSCAN và hoàn thiện web ReactJS + FastAPI :8000.
  >
  > Target KPIs gồm: sai số Monoid và Future-Shuffle bằng 0, vượt trội Persistence Baseline ở ca đổi trạng thái, đối soát 100%, 0 trùng khi hồi phục WAL, MongoDB dưới 10 ms và pass 100% pytest. Nhóm mong nhận góp ý của thầy Đặng Trần Khánh và quý thầy cô. Em xin cảm ơn."
- **Đối chiếu Thiết kế:** Lộ trình `Sprint 1 & Sprint 2` & `tests/`.

---

## Phần 3. Cẩm Nang Phản Biện Tại Cột Mốc Thiết Kế (10 Câu Hỏi Trọng Tâm — ~30 Giây/Câu)

### Câu 1: *"Tổng 5 bảng dữ liệu gốc trên Kaggle v6 chỉ `38.50 MB`, vì sao nhóm thiết kế kiến trúc phân tán `Spark + Kafka + 8 giỏ Salting + 33 KiB Sketches`?"*
- **Người trả lời:** **Minh Quyền** (`Slide 04, 05, 06`).
- **Trả lời 30 giây:**
  > "Nếu chỉ xử lý tĩnh tệp `38.50 MB` một lần thì Pandas đơn luồng là đủ. Tuy nhiên, trong môn `BDA501 — Big Data Analytics`, tập Kaggle v6 đóng vai trò **mẫu chuẩn kiểm chứng thiết kế (`Proof-of-Scale`)**. Ở quy mô toàn TP.HCM với hơn 1 triệu xe phát GPS mỗi 5–10 giây (`8–17` tỷ tin/ngày, `1.5–3.0 TB/ngày`), dùng `set()` lọc trùng sẽ tràn RAM, join không broadcast sẽ nghẽn mạng và các trục nóng giờ cao điểm sẽ làm treo worker. Kiểm chứng trên tập chuẩn cho phép nhóm đối chiếu sai số toán học bằng `0.0` trước khi nhân rộng."

### Câu 2: *"Vì sao nhóm kiểm toán cả hai tỷ lệ thiếu `max_velocity` là `85.21%` và `85.19%`, và thiết kế `derive_urban_los` hoạt động ra sao?"*
- **Người trả lời:** **Minh Quyền** (`Slide 05, 06, 08`).
- **Trả lời 30 giây:**
  > "Sự khác biệt đến từ bước lọc phi cơ giới `R2` trong `Quality Funnel`: bảng `train.csv` gốc có `33,441` dòng (trải trên `46/48` khung giờ) với `28,495` dòng thiếu `max_velocity` (`85.21%`); sau khi tách `427` đoạn phi cơ giới (`47` cửa sổ), tập sạch còn `33,394` cửa sổ cơ giới với `28,448` cửa sổ thiếu `max_velocity` (`85.19%`). Hàm `derive_urban_los` chia thang bậc tốc độ đô thị `5 km/h` (có dịch ngưỡng với trục đường tốc độ cao `>= 60–70 km/h`) theo quy chuẩn BkTraffic và sẽ được kiểm chứng tỷ lệ khớp nhãn gốc ở `Sprint 1`."

### Câu 3: *"Tại sao nhóm chọn `Random Forest (23D)` làm ứng viên chính cho `Predictive ML` thay vì các mô hình Deep Learning đồ thị như `DCRNN` hay `Graph WaveNet`?"*
- **Người trả lời:** **Thành Đạt** (`Slide 03, 09`).
- **Trả lời 30 giây:**
  > "Kết quả EDA trên dữ liệu TP.HCM cho thấy **`88.13%` đoạn cơ giới (`74,211` đoạn) hoàn toàn không có GPS** và **`66.30%` cặp quan sát liên tiếp đứt quãng từ `60` phút trở lên**. Các mô hình `Deep Spatio-Temporal GNN` đòi hỏi ma trận chuỗi thời gian liên tục đều đặn trên đa số đỉnh đồ thị nên dễ suy biến trên dữ liệu quá thưa. Ngược lại, `Random Forest (23D)` kết hợp đặc trưng đồ thị tĩnh (`PageRank`, `7D K-Means`) và lịch sử quá khứ ($\tau < t$) xử lý tốt dữ liệu bảng đứt quãng, phủ được `100%` đoạn vùng mù và vận hành nhẹ trong ngân sách `11.0 GB RAM` không cần GPU."

### Câu 4: *"Vì sao chia `8` giỏ `Key Salting` bắt buộc phải đi kèm `5-Tuple Monoid` thay vì gọi `avg()` từng giỏ?"*
- **Người trả lời:** **Thành Đạt** (`Slide 06, 10`).
- **Trả lời 30 giây:**
  > "Phép trung bình cộng không có tính kết hợp khi số bản tin ở các giỏ không bằng nhau (`avg(avg(A), avg(B)) ≠ avg(A ∪ B)`). Như minh họa trên Slide 06, 1 xe chạy `10 km/h` ở Máy A và 3 xe chạy `40 km/h` ở Máy B có trung bình thật `32.5 km/h`, nhưng lấy trung bình của trung bình ra `25.0 km/h`. Đóng gói thành 5 đại lượng cộng được `(count, sum_v, min_v, max_v, severe_count)` bảo đảm chia bao nhiêu giỏ song song kết quả gộp cuối vẫn sai số bằng `0.0`."

### Câu 5: *"Thiết kế của nhóm ngăn chặn rò rỉ dữ liệu tương lai (`Zero Future Leakage`) bằng những chốt chặn nào?"*
- **Người trả lời:** **Thành Đạt** (`Slide 06, 08, 09`).
- **Trả lời 30 giây:**
  > "Nhóm thiết kế 5 chốt chặn: **(1)** bảng `Train-Only 3-Tier Vmax` chỉ học trên `TRAIN` rồi khóa cố định cho `VAL/TEST`; **(2)** bộ chuẩn hóa và tâm cụm `7D K-Means` chỉ fit trên tập `TRAIN` đã tách `7,192` burst windows; **(3)** các biến lịch sử quá khứ (`past-only priors`) chỉ tích lũy nghiêm ngặt tại $\tau < t$; **(4)** chia thời gian `70/15/15` kèm cắt biên `Boundary Purge`; và **(5)** bài kiểm thử tự động `Future-Shuffle` ở `Sprint 1` xáo trộn tương lai $\tau > t$ để xác nhận $X_t$ sai lệch bằng `0.0`."

### Câu 6: *"Nếu trong thực nghiệm sắp tới `Persistence Baseline` đạt Accuracy rất cao do quán tính 30 phút lớn, nhóm dự kiến đánh giá mô hình thế nào cho công bằng?"*
- **Người trả lời:** **Thành Đạt** (`Slide 09, 13`).
- **Trả lời 30 giây:**
  > "Trước hết, **`Persistence Baseline`** là quy tắc đối chứng tối giản **không dùng AI** do chính nhóm tự thiết lập — cứ đoán giữ nguyên mức `LOS` hiện tại sau 30 phút ($\hat{y}_{t+30\text{m}} = \text{LOS}_t$). Nhóm đã thiết kế khung đánh giá đa chế độ ngay từ đầu: bên cạnh bảng `Ablation 7 mô hình` toàn cục (tuyển chọn khách quan trên `VAL Composite Gate` để chống nhìn trộm `TEST`), nhóm sẽ bóc tách đánh giá riêng trên **nhóm ca giao thông đổi trạng thái (`LOS(t) ≠ LOS(t+30m)`)** — nơi `Persistence Baseline` chắc chắn đạt `0%`, kết hợp kiểm định ghép cặp `McNemar`, sai số xác suất `Brier`, hiệu chỉnh ngưỡng chuyển pha $\tau$ trên `VAL`, đo `ROC-AUC / PR-AUC` cho ca đột ngột hóa kẹt và kiểm thử `Masked-GPS Proxy` (che toàn bộ tín hiệu GPS thành trống để giả lập vùng mù `Cold-Start`)."

### Câu 7: *"Cơ chế hoạt động của 3 cấu trúc `O(1) Stream Sketches` (`33.0 KiB RAM`) trong thiết kế luồng là gì?"*
- **Người trả lời:** **Công Minh** (`Slide 07, 11`).
- **Trả lời 30 giây:**
  > "Cả ba cấu trúc dùng bộ nhớ cố định cực nhỏ dù lưu lượng tăng lên hàng tỷ gói tin: **(1) `RotatingBloomFilter` (`2 × 16 KiB = 32 KiB`, `k = 10` hàm băm)** xoay vòng 2 thế hệ mảng bit để lọc gói tin gửi lặp chống bão hòa; **(2) `HyperLogLog++` (`1.0 KiB = 1,024` thanh ghi)** đếm số đoạn đường hoạt động duy nhất (`COUNT DISTINCT`) với sai số lý thuyết `~3.25%`; và **(3) `EWMA` ($\alpha = 0.15$)** cộng dồn điểm kẹt có phai mờ theo nhịp và tự xóa khỏi RAM khi nhỏ hơn $10^{-4}$ để bắt điểm đang kẹt lì."

### Câu 8: *"Vì sao phải thiết kế cả `3-Phase MLOps` lẫn `Two-Tier Reconciliation` và khóa `$status_version` thay vì cập nhật trực tiếp trên luồng?"*
- **Người trả lời:** **Công Minh** (`Slide 04, 11`).
- **Trả lời 30 giây:**
  > "Vừa chấm điểm trực tuyến vừa ghi đè trọng số mô hình sẽ gây xung đột khóa ghi và mất khả năng tái lập kiểm toán — nên `3-Phase MLOps` tách bạch `Phase A` (huấn luyện & khóa `SHA-256`) với `Phase B/C` (chỉ đọc). Song song, với mạng di động có tin trễ quá `Watermark 30m`, ghi kép `Dual-Sink` và đối soát `Tier-2` cuối ngày kèm khóa nguyên tử `$status_version` trên `MongoDB` giúp vừa cảnh báo tức thì `v1`, vừa bù đủ tin trễ thành bản chuẩn `v2` mà tin cũ `v1` không thể ghi đè ngược."

### Câu 9: *"Cơ sở nào giúp nhóm đặt mục tiêu truy vấn `MongoDB` đạt `SLA < 10 ms` và vì sao chọn `ReactJS + FastAPI (:8000)`?"*
- **Người trả lời:** **Công Minh** (`Slide 04, 11, 13`).
- **Trả lời 30 giây:**
  > "Trên `MongoDB 8.0`, nhóm thiết kế chỉ mục phức hợp `{street_level: 1, severe_prob: -1, segment_id: 1}` tuân thủ quy tắc **`ESR` (`Equality – Sort – Range`)** để truy vấn Top điểm nóng trục chính đọc thẳng theo thứ tự trên cây `B-Tree` (`IXSCAN`), loại bỏ hoàn toàn bước sắp xếp trong RAM (`in_memory_sort = false`), và sẽ kiểm chứng trực tiếp bằng `explain('executionStats')` ở `Sprint 2`. Về giao diện, tách biệt `ReactJS + Leaflet` với `FastAPI (:8000)` giúp render mượt bản đồ `84,206` đoạn, kích phát `Kafka` bất đồng bộ và giữ gọn mức RAM `1.0 GB` trong cụm 5 container (`11.0 GB RAM`)."

### Câu 10: *"Đâu là đường găng (`Critical Path`) quan trọng nhất trong kế hoạch `Sprint 1` và `Sprint 2` sau buổi báo cáo thiết kế hôm nay?"*
- **Người trả lời:** **Công Minh** (`Slide 13`).
- **Trả lời 30 giây:**
  > "Đường găng bám chặt thứ tự phụ thuộc dữ liệu: trong **`Sprint 1`**, nhóm phải chạy xong `Quality Funnel (R1–R6)`, kiểm chứng `derive_urban_los`, `Spark Batch ETL` (`Monoid = 0.0`, `BroadcastHashJoin`, `Salting`), dựng `Static Feature Store (84,206 đoạn)` và hoàn tất `Ablation 7 mô hình` trên tập đã tách burst để khóa `Frozen Registry (SHA-256)`. Ngay khi `Frozen Registry` đóng băng, **`Sprint 2`** khởi chạy cụm `5` container Docker đo kiểm runtime `Kafka → Spark Real Watermark → MongoDB IXSCAN` và tích hợp hoàn chỉnh trung tâm điều hành `ReactJS + FastAPI (:8000)`."

---

### Ghi Chú Giải Nghĩa Nhanh Thuật Ngữ Trọng Tâm (Cột Mốc Thiết Kế)

- **`Persistence Baseline`:** Quy tắc đối chứng tối giản **không dùng AI** do nhóm tự thiết lập — đoán giữ nguyên mức `LOS` hiện tại sau 30 phút ($\hat{y}_{t+30\text{m}} = \text{LOS}_t$). Thuật ngữ `"Baseline"` trong báo cáo chỉ dùng cho mốc ML này; các mốc kỹ thuật khác gọi đích danh là **cấu hình Spark mặc định `SortMergeJoin`** và **mã tham chiếu đơn máy `baseline.py`**.
- **`Burst tổng hợp 03/08/2020` & `Organic TRAIN`:** Kết quả EDA cho thấy riêng một sáng `03/08/2020` đổ dồn `36,096` pings (`39.7%` dữ liệu `90,938` pings), tạo ra `7,192` cửa sổ burst tổng hợp; nhóm tách sạch khối này khỏi tập `TRAIN` để mô hình học đúng nhịp giao thông thực.
- **`street_causal_prior_*`:** Biến **tiên nghiệm quá khứ** tích lũy mở rộng nghiêm ngặt tại $\tau < t$ (chỉ dùng quá khứ theo chuỗi thời gian, không phải suy luận nhân quả can thiệp).
- **`Masked-GPS Proxy`:** Kịch bản kiểm thử vùng mù dự kiến thực hiện ở `Sprint 1` — cố tình che toàn bộ tín hiệu GPS hiện tại thành trống để giả lập đoạn `Cold-Start` và đo độ chính xác suy luận từ GIS tĩnh + cụm `7D K-Means`.
- **`v1` vs `v2` (`$status_version`), `WAL`, `IXSCAN`, `EWMA`, `SON`:** `v1` là cảnh báo tạm thời trên Speed Layer còn `v2` là bản chuẩn đối soát cuối ngày (khóa `$status_version` chặn `v1` ghi đè `v2`); `WAL` là nhật ký ghi trước phục hồi luồng; `IXSCAN` là quét trực tiếp trên chỉ mục `B-Tree` thay vì quét toàn bảng; `EWMA` là trung bình trượt hàm mũ theo dõi điểm kẹt lì; `SON` là thuật toán khai phá tập phổ biến phân vùng 2 lượt trên dữ liệu lớn.
