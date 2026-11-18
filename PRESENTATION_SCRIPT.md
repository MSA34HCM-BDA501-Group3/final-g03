# Kịch Bản Thuyết Trình Chuẩn 12 Phần (Hội Nghị, Báo Cáo Điều Hành & Bảo Vệ Đồ Án Học Phần BDA501 — Big Data Analytics)

- **Tên đề tài:** Hệ thống Giám sát Hiện trạng & Dự báo Ùn tắc Giao thông Đô thị TP. Hồ Chí Minh (*HCMC Smart Traffic Big Data Analytics & Congestion Prediction Platform*)
- **Đơn vị & Học phần:** `FSB · FPT University | MSE AI | BDA501 – Big Data Analytics` (`Fall 2026`)
- **Giảng viên hướng dẫn (Instructor):** **Đặng Trần Khánh**
- **Bộ slide trình chiếu đi kèm:** `HCMC_Smart_Traffic_Final_Presentation.pptx` & `HCMC_Smart_Traffic_Final_Slides.pdf` (`17 Slides` theo cấu trúc **12 Phần chuẩn Học thuật & Hội nghị + 2 Slide Trực quan hóa Phương pháp + Slide 17 Technical Readiness & Wave-2 Verification**)
- **Báo cáo tổng kết & Kiểm định đi kèm:** `HCMC_Smart_Traffic_Final_Report.pdf` & `outputs/HCMC_Smart_Traffic_Final_Report.pdf` (`7 Chương`, `60 cột dataset.csv`)
- **Nhóm thực hiện & Phân công Trình bày theo 17 Slides (3 Thành viên — Mỗi Slide đúng 1 Người nói):**
  1. **Nguyễn Tạ Minh Quyền (`25MS23280`)** — Phụ trách **Slide 01, Slide 02 (`1. Problem Statement`), Slide 04 (`3. System Architecture`), Slide 05 (`4. Data, EDA & Data Problem`), Slide 06 (`Giải thích Phương pháp 1/2: Nội suy Vmax & Monoid`) & Slide 08 (`5. Data Pipeline & Data Processing`)**
  2. **Nguyễn Thành Đạt (`25MS23306`)** — Phụ trách **Slide 03 (`2. Literature Review`), Slide 09 (`6. Modeling`), Slide 10 (`7. Batch Processing`), Slide 12 (`9A. Experiments: Ablation`) & Slide 13 (`9B. Experiments: Persistence Defense`)**
  3. **Phạm Công Minh (`25MS23310`)** — Phụ trách **Slide 07 (`Giải thích Phương pháp 2/2: Line-Graph PageRank, 7D K-Means & O(1) Sketches`), Slide 11 (`8. Stream Processing`), Slide 14 (`10. Demonstration`), Slide 15 (`11. Limitations & Risks — Tổng hợp`), Slide 16 (`12. Plan`), Slide 17 (`13. Technical Readiness & Production Verification`) & Trình diễn `Live Demo` trực tiếp Dual-UI (`:8000` & `:8501`)**

---

## Phần 1. Bảng Ánh Xạ Cấu Trúc 12 Phần + Production Audit Sang 17 Slides (`18 Phút + 3 Phút Live Demo`)

| Thứ tự Slide | Mục trong Cấu trúc 12 Phần | Nội dung Cốt lõi (Chỉ nêu `Limitations & Risks` ở các Phần Giải pháp) | Người Trình bày | Khung Thời gian | Mục Báo cáo |
|---|---|---|---|---|---|
| **Slide 01** | **Mở đầu (Cover)** | Trang bìa tinh gọn 4 cụm: Đơn vị & Học phần, Tên dự án (`Hero Title`), Nhóm thực hiện & Giảng viên hướng dẫn | **Minh Quyền** | `00:00 – 00:35` | Trang bìa & `1.1` |
| **Slide 02** | **1. Problem Statement** | Bối cảnh ùn tắc lan truyền TP.HCM (`WHY`), 3 hạn chế quản lý hiện hữu & bài toán + giá trị thụ hưởng (`WHAT`) | **Minh Quyền** | `00:35 – 01:45` | Chương 1 (`1.1–1.4`) |
| **Slide 03** | **2. Literature Review** | Khảo sát 4 hướng tiếp cận quản lý – dự báo giao thông đô thị, khoảng trống thực tiễn & lý do cần nền tảng tích hợp | **Thành Đạt** | `01:45 – 02:55` | Chương 1 (`1.5`) |
| **Slide 04** | **3. System Architecture** | **[Solution]** Sơ đồ AWS-Style (`Zones A–F`, `6` dịch vụ Docker `12.0/16 GB`), `3-Phase MLOps` + **Arch Risks** | **Minh Quyền** | `02:55 – 04:05` | Chương 3 (`3.1, 3.2, 3.5`) |
| **Slide 05** | **4. Data, EDA & Data Problem** | Kiểm toán 5 bảng gốc (`38.50 MB`), định vị `Proof-of-Scale` & 10 vấn đề dữ liệu (`EC-1..EC-10`, lọc `427` POIs & `7,192` burst) | **Minh Quyền** | `04:05 – 05:15` | Chương 2 (`2.1, 2.2`) |
| **Slide 06** | **Giải thích Phương pháp (1/2)** | **[Visual Explainer]** Trực quan hóa `Train-Only 3-Tier Vmax Imputation` & `5-Tuple Monoid + Key Salting` bằng ví dụ số | **Minh Quyền** | `05:15 – 06:25` | Chương 2 (`2.4`) & Chương 3 (`3.3`) |
| **Slide 07** | **Giải thích Phương pháp (2/2)** | **[Visual Explainer]** Trực quan hóa `Line-Graph PageRank`, `7D K-Means Cold-Start (88.13%)` & `O(1) Sketches (33.0 KiB)` | **Công Minh** | `06:25 – 07:40` | Chương 4 (`4.1, 4.3`) & Chương 6 (`6.2`) |
| **Slide 08** | **5. Data Pipeline & Processing** | **[Solution]** `End-to-End Walkthrough`, `Ordered Quality Funnel`, `3-Tier Vmax`, `LOS A–F (89.19%)` + **Pipeline Risks** | **Minh Quyền** | `07:40 – 08:50` | Chương 2 (`2.3–2.5`) |
| **Slide 09** | **6. Modeling** | **[Solution]** `Exact t+30m (7,885 cặp)`, lọc `7,192` burst (`1,885` organic TRAIN), `23D`, `VAL Gate (0.8990)` + **Modeling Risks** | **Thành Đạt** | `08:50 – 10:00` | Chương 5 (`5.1–5.3, 5.5`) |
| **Slide 10** | **7. Batch Processing** | **[Solution]** `Monoid`, `Salting`, `BroadcastHashJoin (5→1 Exch, +17.07%)`, `PageRank (84,206.0)`, `SON`, `7D K-Means` + **Batch Risks** | **Thành Đạt** | `10:00 – 11:15` | Chương 3 (`3.3–3.5`) & Chương 4 (`4.1–4.4`) |
| **Slide 11** | **8. Stream Processing** | **[Solution]** `Kafka 4.1`, `O(1) Sketches (33.0 KiB)`, `Two-Tier Recon (500 trễ/609 lag/384 rescore)`, `IXSCAN (0.389/0.692 ms)` + **Stream Risks** | **Công Minh** | `11:15 – 12:25` | Chương 6 (`6.1–6.5`) |
| **Slide 12** | **9A. Experiments (Ablation)** | Bảng `7-Model Ablation` (`ROC 0.9900, PR 0.9595, Sev-F1 0.9301, Macro-F1 0.4475`), `CM` & hiệu năng Spark/MongoDB | **Thành Đạt** | `12:25 – 13:30` | Chương 5 (`5.3`) |
| **Slide 13** | **9B. Experiments (Defense)** | Đối chứng `Baseline (94.84%)` vs `RF (23D)` trên 4 chế độ (`Detect AUC 0.9146, McNemar p=0.0156, Lift 3.32x, 100% Cold-Start`) | **Thành Đạt** | `13:30 – 14:45` | Chương 5 (`5.4`) |
| **Slide 14** | **10. Demonstration** | Dual-UI (`TOC Web :8000` & `Streamlit :8501`): `Case Study 1 (21064 E→F 72.4%)` & `Case Study 2 (61781 Cold-Start)` | **Công Minh** | `14:45 – 15:50` | Chương 6 (`6.4`) |
| **Slide 15** | **11. Limitations & Risks** | **Tổng hợp Limitations của 5 Thành phần (`Mục 3, 5, 6, 7, 8`) & 4 Rủi ro Toàn cục (bao gồm `Macro-F1 0.4475`, `0/8 Onset`)** | **Công Minh** | `15:50 – 16:55` | Chương 7 (`7.5`) |
| **Slide 16** | **12. Plan** | **Nghiệm thu Hoàn thành 100% (3 thành viên) & Lộ trình Mở rộng 3 Giai đoạn (`Phase 1–3 Plan` + Quality Gate)** | **Công Minh** | `16:40 – 17:20` | Chương 7 (`7.1–7.5`) |
| **Slide 17** | **13. Readiness & Wave-2 Sync** | **Tổng kết Kiểm định (`[I]` vs `[P]`, Spark `145.24→120.45 ms`, Real Watermark, `$status_version`, `100%` Parity `outputs/*.json`)** | **Công Minh** | `17:20 – 18:00` | `outputs/metrics.json`, `spark_runtime_evidence.json` |
| **Live Demo** | **Trình diễn Dual-UI (`:8000` & `:8501`)** | Trực quan hóa `FastAPI+ReactJS+Leaflet TOC Web (:8000)` & `Streamlit 5-Tab (:8501)` kết nối `Kafka → MongoDB 8.0` | **Công Minh** | `18:00 – 21:00` | `toc_backend/*`, `app.py`, `producer.py` |

---

## Phần 2. Kịch Bản Lời Thoại Chi Tiết Từng Slide (`Slide 01` – `Slide 17`)

#### Slide 01: Mở Đầu — Trang Bìa Đề Tài, Học Phần, Nhóm Thực Hiện & Giảng Viên Hướng Dẫn (`00:00 – 00:35`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Giới thiệu ngắn gọn, trang trọng tên đề tài (`Hero Title`), học phần `BDA501 – Big Data Analytics`, 3 thành viên thực hiện và Giảng viên hướng dẫn.
- **Chỉ dẫn trực quan:** Hướng về tiêu đề trung tâm `HCMC Smart Traffic Big Data Platform` và dải thông tin `Project Team` cùng `Instructor` ở cuối trang bìa.
- **Lời thoại thuyết trình:**
  > "Kính chào thầy **Đặng Trần Khánh** và quý thầy/cô trong hội đồng. Hôm nay nhóm chúng em gồm ba thành viên: em là **Nguyễn Tạ Minh Quyền (`25MS23280`)**, bạn **Nguyễn Thành Đạt (`25MS23306`)** và bạn **Phạm Công Minh (`25MS23310`)**, xin trình bày báo cáo đồ án cuối kỳ học phần `BDA501 – Big Data Analytics` thuộc chương trình `MSE AI` tại `FSB · FPT University`, với đề tài: **HCMC Smart Traffic Big Data Platform — Nền tảng phân tích dữ liệu lớn và dự báo ùn tắc giao thông đô thị TP.HCM**."
- **Tra cứu nhanh Báo cáo:** Trang bìa & Chương 1, Mục `1.1`.

---

#### Slide 02: `1. Problem Statement` — Bài Toán Điều Hành Giao Thông Chủ Động Tại Đô Thị TP.HCM (`00:35 – 01:50`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Trả lời trọn vẹn hai câu hỏi nền tảng **WHY** (vấn đề thực tế của giao thông TP.HCM và vì sao phương pháp hiện tại chưa đủ) và **WHAT** (đồ án giải quyết chính xác bài toán gì và mang lại giá trị thực tiễn cho ai), chưa đi sớm vào thuật toán kỹ thuật.
- **Chỉ dẫn trực quan:** Chỉ lần lượt từ thẻ `1. Vấn Đề Thực Tế Của Giao Thông TP.HCM` xuống thẻ `2. Vì Sao Phương Pháp Hiện Tại Chưa Đủ?` và thẻ `3. Bài Toán Đặt Ra & Giá Trị Thụ Hưởng` bên trái, đối chiếu bản đồ bối cảnh mạng lưới đường TP.HCM bên phải.
- **Lời thoại thuyết trình:**
  > "Mở đầu ở **Phần 1 — Problem Statement** trên Slide 02, nhóm xin làm rõ lý do thực tiễn khiến hệ thống này cần tồn tại:
  > - **Thứ nhất — Vấn đề thực tế tại TP.HCM (`WHY`):** Mật độ phương tiện rất lớn lưu thông trên mạng lưới đường đô thị đan xen dày đặc — từ cửa ngõ sân bay Tân Sơn Nhất, trung tâm Bến Thành Quận 1, trục Lý Thường Kiệt Quận 10 đến hành lang Đông Bắc Thủ Đức — khiến chỉ một điểm nghẽn cục bộ giờ cao điểm cũng nhanh chóng lan truyền thành ùn tắc dây chuyền. Trong khi đó, công tác điều tiết hiện nay chủ yếu **phản ứng bị động sau sự cố** (khi dòng xe đã kẹt cứng mới cử lực lượng giải tỏa), gây lãng phí lớn về thời gian và chi phí vận tải đô thị.
  > - **Thứ hai — Vì sao nguồn dữ liệu và công cụ hiện tại chưa đủ?** Camera CCTV và cảm biến cố định rất đắt đỏ, chỉ đặt được ở số ít nút giao chính; còn nguồn định vị GPS hành trình lại phân bố lệch về đại lộ, bỏ trống phần lớn mạng lưới đường nhánh màu xám trên bản đồ (*vùng mù quan trắc*) và thường xuyên bị ngắt quãng, trễ tin. Đồng thời, các bảng giám sát hiện hữu chỉ cho biết nơi **đang kẹt**, chưa cảnh báo trước 30 phút nơi **sắp kẹt cứng**, và tách rời giữa trực ban sự vụ với quy hoạch hạ tầng dài hạn.
  > - **Thứ ba — Bài toán đặt ra & Giá trị thụ hưởng (`WHAT`):** Đồ án giải quyết bài toán xây dựng nền tảng dữ liệu lớn tích hợp để **(1)** ước lượng trạng thái giao thông toàn đô thị kể cả vùng thiếu cảm biến, **(2)** nhận diện các nút cổ chai cấu trúc, và **(3)** dự báo sớm nguy cơ kẹt cứng trước **30 phút** — tạo giá trị trực tiếp cho **Trung tâm `TOC` & Kỹ sư đèn** điều tiết chủ động, **Sở GTVT & Đội tuần tra** ưu tiên đúng điểm nghẽn, và **Người dân / Ứng dụng dẫn đường** chọn lộ trình né kẹt từ xa."
- **Tra cứu nhanh Báo cáo:** Chương 1, Mục `1.1` – `1.4` & Hình `1.1`, `1.2`.

---

#### Slide 03: `2. Literature Review` — Các Hướng Tiếp Cận Quan Trắc, Dự Báo & Điều Hành Giao Thông (`01:50 – 03:05`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Khảo sát 4 hướng tiếp cận giải quyết bài toán giao thông đô thị trong thực tế và học thuật, chỉ ra khoảng trống nghiên cứu cấp miền ứng dụng (`Domain-Level Research Gap`) và lý giải tại sao bắt buộc phải có một **nền tảng tích hợp** theo đúng quy trình nghiên cứu thuận chiều.
- **Chỉ dẫn trực quan:** Điểm qua 4 dòng trong bảng khảo sát các hệ thống & nghiên cứu giao thông phía trên, sau đó nhấn mạnh vào thẻ `Khoảng Trống Nghiên Cứu & Vì Sao Cần Nền Tảng Tích Hợp HCMC Smart Traffic` ở phía dưới.
- **Lời thoại thuyết trình:**
  > "Tiếp nối bài toán thực tế vừa nêu, ở **Phần 2 — Literature Review** trên Slide 03, nhóm khảo sát 4 hướng tiếp cận chính trong quản lý và dự báo giao thông đô thị hiện nay:
  > 1. **Hệ thống quan trắc cố định và đèn thích ứng (`SCATS`, `SCOOT`, Camera CCTV):** Đo đạc chính xác tại nút giao có thiết bị, nhưng chi phí đầu tư và bảo trì quá cao nên để trống phần lớn mạng lưới đường nhánh đô thị.
  > 2. **Giám sát từ dữ liệu hành trình (`Floating Car / Probe GPS` — `Herrera et al., 2010; Zheng, 2015`):** Mở rộng độ phủ diện rộng không cần lắp cảm biến mặt đường, nhưng dữ liệu lệch về trục lớn, ngắt quãng theo thời gian và mới dừng ở bản đồ phản ánh hiện trạng.
  > 3. **Nghiên cứu dự báo ùn tắc ngắn hạn (`ARIMA`, `DCRNN`, `Graph WaveNet` — `Vlahogianni, 2014; Li, 2018`):** Dự báo trước 15–60 phút rất hiệu quả trên cao tốc có cảm biến dày đặc, liên tục (`PeMS`, `METR-LA`), nhưng suy giảm mạnh khi gặp dữ liệu đô thị thưa và đứt quãng dài.
  > 4. **Các nền tảng hỗ trợ quyết định giao thông (`TOC / Urban DSS` — `Lv et al., 2015; Amini et al., 2017`):** Thường tách rời mô hình dự báo học thuật khỏi luồng dữ liệu di động thực tế có tin trễ, tin trùng và chia cắt giữa trực ban tức thời với quy hoạch dài hạn.
  >
  > Từ đó, thẻ bên dưới đúc kết **khoảng trống nghiên cứu cốt lõi** và lý giải **tại sao không thể triển khai rời rạc từng công nghệ đơn lẻ**: một mô hình dự báo đứng riêng không thể tự chuẩn hóa luồng GPS di động nhiễu/trễ, không suy diễn được vùng mù đường nhánh nếu thiếu phân tích cấu trúc mạng lưới đường, và không phục vụ đồng thời cả trực ban `TOC` lẫn quy hoạch `Sở GTVT`. Theo đúng logic nghiên cứu thuận chiều — **Xác định vấn đề (`Mục 1`) $\rightarrow$ Khảo sát & chỉ ra khoảng trống (`Mục 2`) $\rightarrow$ Đề xuất kiến trúc tích hợp (`Mục 3–8`) $\rightarrow$ Thực nghiệm kiểm chứng (`Mục 9–10`)** — nhóm xin chuyển sang kiến trúc hệ thống ở Phần 3."
- **Tra cứu nhanh Báo cáo:** Chương 1, Mục `1.5`.

---

#### Slide 04: `3. System Architecture` — Kiến Trúc Hybrid Batch–Speed (`Zones A–F`) & `3-Phase MLOps` (`03:10 – 04:20`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Trình bày sơ đồ kiến trúc tổng thể chuẩn AWS (`Zones A–F`, đồng màu theo từng Zone và từng luồng Pipeline), vòng đời `3-Phase MLOps` không xung đột ghi, và nêu rõ **Limitations & Risks của kiến trúc hệ thống**.
- **Chỉ dẫn trực quan:** Chỉ theo các luồng mũi tên màu đi qua `Zone A` đến `Zone F` trên sơ đồ bên trái, thẻ `3-Phase MLOps Lifecycle` góc trên phải, và thẻ `Limitations & Risks (Arch)` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Sang **Phần 3 — System Architecture** trên Slide 04: sơ đồ bên trái trình bày kiến trúc tổng thể gồm **6 phân vùng (`Zones A–F`)**, trong đó các thành phần cùng một Zone mang màu thống nhất và các mũi tên cùng một Pipeline mang màu riêng biệt. Dữ liệu từ `Zone A` đi qua `Zone B` (`Apache Kafka KRaft` & `Quality Funnel`), sau đó tách làm hai nhánh bên trong cụm `Apache Spark`: nhánh tím phía trên là **`Zone C (Speed Layer)`** xử lý luồng trực tuyến trong `Watermark 30m`; nhánh xanh dương phía dưới là **`Zone D (Batch Layer)`** xử lý lô, đồ thị, huấn luyện ML và đối soát.
  >
  > Ở thẻ phía trên bên phải, cơ chế **`Zero-Conflict 3-Phase MLOps`** ngăn chặn hoàn toàn xung đột ghi vào `Zone E (Frozen Registry)`: chỉ học đúng 1 lần ở `Phase A` trên `1,885` mẫu **organic TRAIN** (đã tách `7,192` synthetic burst windows) rồi khóa `SHA-256`; `Phase B` trực tuyến chỉ đọc để phát cảnh báo `v1`; và `Phase C` cuối ngày bù `500` cửa sổ trễ, hàn gắn `609` cửa sổ lag và re-score `384` cửa sổ lên bản ghi chuẩn `v2` tại `Zone F` (`Medallion Lakehouse`, `MongoDB 8.0`, **`FastAPI + ReactJS/Leaflet TOC Web :8000`** và **`Streamlit UI :8501`**).
  >
  > **Về Limitations & Risks của thành phần Kiến trúc (thẻ góc dưới phải):**
  > - *Thứ nhất (`Single-Host Contention`):* Việc chạy chung cả **6 dịch vụ container** (`Kafka 1.5 GB`, `Spark 6.0 GB`, `MongoDB 2.0 GB`, `TOC Web :8000 1.0 GB`, `Streamlit :8501 1.0 GB`, `live-consumer 0.5 GB` = **`12.0 / 16.0 GB RAM`**) trên cùng một máy chủ tiềm ẩn rủi ro tranh chấp băng thông đĩa (`Disk I/O`) khi `Spark Shuffle` đồng thời với `MongoDB` ghi lô.
  > - *Thứ hai (`Eventual Consistency Gap`):* Cảnh báo trực tuyến `v1` phản hồi dưới 1 mili-giây, nhưng việc cập nhật biến trễ lịch sử từ các bản tin đến muộn quá 30 phút phải chờ `Phase C` cuối ngày."
- **Tra cứu nhanh Báo cáo:** Chương 3, Mục `3.1`, `3.2` & `3.5` (Hình `3.1`, `3.2`).

---

#### Slide 05: `4. Data, EDA & Data Problem` — Hồ Sơ 5 Bảng Gốc, `Proof-of-Scale` & 10 Vấn Đề Dữ Liệu (`04:20 – 05:30`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Trình bày hồ sơ kiểm toán 5 bảng gốc (`38.50 MB`), vai trò mẫu chuẩn `Proof-of-Scale` cho quy mô toàn TP.HCM (`8–17 tỷ tin/ngày`), phân bố thực nghiệm và 10 vấn đề dữ liệu (`EC-1..EC-10`).
- **Chỉ dẫn trực quan:** Chỉ biểu đồ tỷ lệ dị thường góc trên trái, thẻ `Hồ Sơ 5 Bảng Gốc & Proof-of-Scale` góc dưới trái, và bảng 10 `Data Problems (EC-1..EC-10)` bên phải.
- **Lời thoại thuyết trình:**
  > "Bước sang **Phần 4 — Data, EDA & Data Problem** trên Slide 05: qua kiểm toán 5 bảng gốc trên Kaggle (`38.50 MB`: gồm `nodes` `577,967` điểm — lọc `525,786` điểm vẽ cong `90.97%` còn `52,181` nút giao thực; `streets` `5,553` đường; `segments` `84,633` thô còn **`84,206` đoạn cơ giới** sau khi cách ly `427` non-road POIs; `segment_status` `90,938` thô còn **`90,787` bản tin hợp lệ** sau cách ly `151` pings lỗi; và `train` `33,441` thô còn **`33,394` cửa sổ cơ giới**, khớp `89.19%` BkTraffic urban LOS), bộ dữ liệu này đóng vai trò **mẫu chuẩn `Proof-of-Scale`** đối chứng sai số bằng `0.0` cho quy mô toàn TP.HCM (**`8–17` tỷ bản tin/ngày, `1.5–3.0 TB/ngày`**). Về phân bố, chỉ có **`9,995 / 84,206` đoạn (`11.87%`) có GPS**, phát hiện mới **`7,192` synthetic burst windows**, và **`94.84%` mẫu `TEST` giữ nguyên trạng thái sau 30 phút**.
  >
  > Đi sâu vào bảng bên phải, nhóm định lượng được **10 vấn đề dữ liệu thực tế (`EC-1` đến `EC-10`)**: nổi bật là **85.21% cửa sổ (`28,495`) & 88.34% đoạn (`74,762`) thiếu `Vmax` (`EC-1`)**, **154 đoạn $\le 0\text{ m}$ (bù trung vị `27.0m`), `427` POIs & `151` pings lỗi (`EC-2`)**, **695 → 641 tên đường Unicode NFC (`EC-3`)**, **4,305 cửa sổ (`12.89%`) lệch ngày giờ `UTC+7` (`EC-4`)**, **66.30% (`15,514/23,399`) đứt quãng $\ge 60$ phút & `7,192` burst (`EC-5`)**, **`88.13%` (`74,211` đoạn) `Cold-Start` (`EC-6`)**, **lệch tải giờ cao điểm `684.95x` (`EC-8`)**, **`333` tin trùng & `500` cửa sổ trễ (`EC-9`)** cùng khóa `$status_version` (`25/25`, `EC-10`)."
- **Tra cứu nhanh Báo cáo:** Chương 2, Mục `2.1`, `2.2` (Hình `2.1`).

---

#### Slide 06: Giải Thích Phương Pháp (1/2) — Nội Suy 3 Tầng `Vmax` & Bộ Gộp `5-Tuple Monoid` (`05:15 – 06:25`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Trực quan hóa bằng hình vẽ và con số cụ thể để người nghe hiểu rõ bản chất của 2 phương pháp nền tảng trước khi đi vào chi tiết Pipeline và Spark: **(1) Nội suy tốc độ tối đa 3 tầng (`Train-Only 3-Tier Vmax Imputation`)** và **(2) Bộ gộp 5 đại lượng cộng được (`5-Tuple Monoid`) kết hợp chia giỏ khóa (`Key Salting`)**.
- **Chỉ dẫn trực quan:** Chỉ sơ đồ phễu bậc thang 4 tầng bên trái cho `3-Tier Vmax`, sau đó chỉ bảng đối chiếu hai hộp Đỏ (`SAI`) và Xanh (`ĐÚNG`) bên phải cho `5-Tuple Monoid + Salting`.
- **Lời thoại thuyết trình:**
  > "Trước khi đi vào chi tiết các thành phần xử lý, trên **Slide 06 và Slide 07**, nhóm xin trực quan hóa ngắn gọn bản chất các phương pháp cốt lõi để quý thầy/cô và người xem dễ hình dung chúng hoạt động thế nào:
  > - **Bên trái — Nội suy tốc độ tối đa 3 tầng (`Train-Only 3-Tier Vmax`):** Khi `85.21%` cửa sổ thiếu biển tốc độ tối đa, nếu lấy trung bình toàn bộ dữ liệu sẽ làm rò rỉ thông tin tương lai. Hệ thống chỉ học trên tập quá khứ `TRAIN` rồi tra cứu theo bậc thang từ riêng đến chung: đường nào có sẵn biển hợp lệ thì giữ nguyên (`4,946` cửa sổ) $\rightarrow$ nếu thiếu thì lấy **phân vị 85% vận tốc lúc đường thoáng của chính đoạn đó** (`Tầng 1`, `9,376` cửa sổ) $\rightarrow$ nếu đoạn mới chưa đủ lịch sử thì mượn **trung vị các đường cùng cấp và loại** (`Tầng 2`, `17,693` cửa sổ) $\rightarrow$ cuối cùng mới dùng quy chuẩn đô thị `80/60/50/40 km/h` (`Tầng 3`, `1,379` cửa sổ).
  > - **Bên phải — Vì sao phải dùng `5-Tuple Monoid` và `Key Salting`?** Giả sử trên cùng đoạn đường có 4 xe nằm ở 2 máy: Máy A có 1 xe chạy `10 km/h`, Máy B có 3 xe chạy `40 km/h` — trung bình thực tế phải là **`32.5 km/h`**. Nếu gộp ngây thơ trung bình của trung bình `(10 + 40) / 2` sẽ ra **`25.0 km/h` — bị sai lệch ngay `7.5 km/h`!** Thay vào đó, mỗi máy giữ **bộ 5 con số cộng được `(số xe, tổng vận tốc, min, max, số xe kẹt)`**: gộp lại ra tổng vận tốc `130` chia `4` xe bằng đúng **`32.5 km/h` (sai số `0.0`)**, nhờ đó hệ thống thoải mái gắn đuôi ngẫu nhiên chia giờ cao điểm ra `8` giỏ song song (`Key Salting`) để chống nghẽn mà kết quả cuối vẫn chính xác tuyệt đối."
- **Tra cứu nhanh Báo cáo:** Chương 2 (`2.4`) & Chương 3 (`3.3`).

---

#### Slide 07: Giải Thích Phương Pháp (2/2) — `Line-Graph PageRank`, `7D K-Means` & Bộ Đếm `O(1)` (`06:25 – 07:40`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Trực quan hóa bằng sơ đồ 3 cột để giải thích dễ hiểu cách hệ thống biến bản đồ thành nút cổ chai (`Line-Graph PageRank`), cách suy diễn cho `88.13%` đường không có xe gửi GPS (`7D K-Means`), và cách lọc trùng hàng tỷ bản tin mà không tràn RAM (`O(1) Sketches`).
- **Chỉ dẫn trực quan:** Chỉ lần lượt từ cột 1 (`Line-Graph PageRank`) sang cột 2 (`7D K-Means Cold-Start`) và cột 3 (`Bloom Filter, HLL++ & EWMA`).
- **Lời thoại thuyết trình:**
  > "Em là **Phạm Công Minh**, xin trình bày trọn vẹn **Slide 07** về ba phương pháp cốt lõi tiếp theo của hệ thống:
  > - **Ở cột đầu tiên — `Line-Graph PageRank`:** Bản đồ gốc có tới `90.97%` điểm chỉ dùng để vẽ độ cong con đường. Hệ thống cắt bỏ các chấm đỏ trung gian này, loại bỏ cạnh quay đầu `U-turn`, và **đảo mỗi đoạn cơ giới thành một nút (`Line-Graph`: `92,453` cạnh chuyển tiếp)**: khi nhiều nhánh đường `A, B, C` cùng đổ dồn xe vào một lối ra hẹp `D`, thuật toán **`PageRank`** sẽ chấm điểm nút cổ chai `D` cao vọt (`#1 #21969 Nguyễn Kiệm = 2.7799`, `#2 #37345 Cầu vượt Ngã tư Gò Dưa = 2.7196`) ngay cả khi chưa lắp cảm biến!
  > - **Ở cột thứ hai — `7D K-Means Cold-Start`:** Với **`74,211` đoạn đường vùng mù (`0` GPS)** trên `84,206` đoạn cơ giới (`88.13%`), thuật toán **`7D K-Means`** đo 7 đặc trưng hình thái bản đồ để gom toàn bộ thành phố vào **`6` nhóm hành vi đường phố**: nhờ đó các đoạn vùng mù (ô vuông đen) mượn được đặc trưng của các đường có GPS tương đồng (chấm tròn) nằm chung nhóm để phủ kín `100%` đô thị.
  > - **Ở cột thứ ba — Bộ đếm luồng `O(1) Sketches`:** Khi mở rộng lên hàng tỷ bản tin GPS mỗi ngày, nếu lưu danh sách mã bản tin vào RAM để kiểm tra trùng lặp thì bộ nhớ sẽ đầy tràn ngay lập tức. Thay vào đó, **`Bloom Filter`** chỉ dùng một dãy ô bit `0/1` vỏn vẹn **`32.0 KiB`** như người gác cổng — hễ cả 4 vị trí băm đều đã bật `1` là chặn ngay bản tin trùng; **`HyperLogLog++` (`1.0 KiB`)** ước lượng nhanh `9,620.2` trên `9,995` đoạn hoạt động (`sai số 3.75%`); còn **`EWMA Hotspot`** cộng dồn có phai mờ để bắt các điểm **ĐANG kẹt lì nhiều giờ** (`#1 #47407 Lý Thường Kiệt = 6.6652`), bổ trợ trực tiếp cho `Random Forest` dự báo điểm **SẮP kẹt sau 30 phút**."
- **Tra cứu nhanh Báo cáo:** Chương 4 (`4.1, 4.3`) & Chương 6 (`6.2`).

---

#### Slide 08: `5. Data Pipeline & Data Processing` — `Ordered Quality Funnel` & `Train-Only Imputation` (`07:40 – 08:50`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Trình bày luồng biến đổi dữ liệu cụ thể đầu–cuối (`End-to-End Walkthrough`), `Ordered Quality Funnel`, `Train-Only 3-Tier Vmax Imputation` và chuẩn hóa thang đo `LOS A–F`, đồng thời chỉ rõ **Limitations & Risks của Data Pipeline**.
- **Chỉ dẫn trực quan:** Chỉ sơ đồ phóng lớn `END-TO-END WALKTHROUGH` phía trên, thẻ `Ordered Quality Funnel & 3-Tier Vmax` bên trái dưới, và thẻ `Limitations & Risks (Data Pipeline)` bên phải dưới.
- **Lời thoại thuyết trình:**
  > "Khi đã nắm rõ nguyên lý, sang **Phần 5 — Data Pipeline & Data Processing** trên Slide 08: sơ đồ phía trên minh họa trọn vẹn hành trình một bản tin GPS thực tế lúc `07:08` sáng trên đường Lý Thường Kiệt (`4 km/h`) đi xuyên qua 4 chặng xử lý đến cảnh báo dưới `1 ms`. Ở thẻ bên trái dưới, **Ordered Quality Funnel 5 bước** dùng `ANSI try_cast` cách ly `580` bản ghi đoạn lỗi & `151` ping lỗi sang `Quarantine` (`0` làm sập job, `0` rớt ngầm); loại `427` non-road POIs còn `84,206` đoạn cơ giới; chuẩn hóa `Unicode NFC` (`695 → 641` tên đường, gộp `54` biến thể) và giờ `UTC+7` (`4,305` cửa sổ). Áp dụng bảng tra `Train-Only 3-Tier Vmax` (`4,946 → 9,376 → 17,693 → 1,379`), toàn bộ **33,394 cửa sổ 30 phút** được gán nhãn 6 mức **`LOS A–F`** (**`89.19%` khớp BkTraffic**).
  >
  > **Về Limitations & Risks của Data Pipeline (thẻ bên phải dưới):**
  > - *Thứ nhất (`Static Vmax Approximation`):* Có `57.11%` cửa sổ (`19,072` cửa sổ) dùng `Vmax` suy diễn theo cấp/prior (`Tier 2 & 3`), chưa tự động giảm khi đoạn đường bị rào chắn thi công hay ngập nước cục bộ.
  > - *Thứ hai (`Quarantine Spike & 30m Slot`):* Nếu thiết bị GPS đổi định dạng hàng loạt sẽ làm tăng tỷ lệ cách ly vào `Quarantine`; đồng thời khung gom 30 phút làm mượt các dao động ngắn dưới 5 phút tại ngã tư."
- **Tra cứu nhanh Báo cáo:** Chương 2, Mục `2.3` – `2.5`.

---

#### Slide 09: `6. Modeling` — Nhãn `Exact t+30m`, Lọc Burst, Vector 23 Biến & Tuyển Chọn `VAL Gate` (`08:50 – 10:00`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Trình bày thiết kế bài toán học máy dự báo `+30m` chống rò rỉ tương lai, phát hiện lọc bỏ `7,192` chuỗi burst tổng hợp khỏi `TRAIN`, cấu trúc 23 biến đầu vào, tuyển chọn `Live VAL Gate`, và nêu rõ **Limitations & Risks của thành phần Modeling**.
- **Chỉ dẫn trực quan:** Chỉ 2 thẻ bên trái (`Exact t+30m Pairing & Burst Filter` và `23D Features`), thẻ góc trên phải (`Live VAL Gate & Cold-Start`), và thẻ góc dưới phải (`Limitations & Risks (Modeling)`).
- **Lời thoại thuyết trình:**
  > "Em xin trình bày **Phần 6 — Modeling** trên Slide 09. Để mô hình học đúng bài toán dự báo trước 30 phút trên quy luật giao thông thật và tuyệt đối không nhìn trộm tương lai, nhóm áp dụng 3 nguyên tắc:
  > 1. **Ghép cặp `Exact t+30m Join` & Tách chuỗi Burst tổng hợp:** Chỉ ghép nhãn ở đúng `t + 30 phút` thu được **7,885 cặp hợp lệ** (loại bỏ `15,514` cặp đứt quãng $\ge 60$ phút = `66.30%`), đồng thời phát hiện và tách **`7,192` cửa sổ synthetic burst** (`3,596` cặp khỏi `TRAIN`).
  > 2. **Chia tập `Chronological 70/15/15` + `Boundary Purge`:** Cắt bỏ `55` mẫu biên (`50` TRAIN, `5` VAL) giữ lại **`1,885` mẫu organic `TRAIN`**, **`1,167` mẫu `VAL`** và **`1,182` mẫu `TEST`**; kiểm thử `Future-Shuffle` xác nhận sai lệch biến đầu vào bằng **`0.0`**.
  > 3. **Tổ hợp `23D Features`, `VAL Composite Gate` & `Cold-Start`:** Kết hợp 14 biến cục bộ/thời gian VN (`87.43%` tầm quan trọng, dẫn đầu bởi `mean_velocity 46.75%`, `current_los_ordinal 17.13%`) và 9 biến đồ thị `PageRank (4.82%)`, `neighbor_los (2.73%)`, `K-Means` & `Priors` (`12.57%`). Huấn luyện với trọng số lớp căn bậc hai sạch **`cls_w = 0.65`**, mô hình `#5 Random Forest (23D)` **thắng Live VAL Composite Gate (`0.8990` vs `RF 14D 0.5343`; `τ = 0.25`)**, khóa `SHA-256` tại `outputs/model.joblib` và phủ đủ **`74,211` đoạn `Cold-Start`** (`100%` trên `84,206` đoạn cơ giới).
  >
  > **Về Limitations & Risks của Modeling (thẻ góc dưới phải):**
  > - *Thứ nhất (`Default Conservatism & Sparse B–E`):* Ở các ca đổi pha, ngưỡng mặc định đạt `7/61 (11.48%)` và hiệu chỉnh trên `VAL (τ = 0.40)` tăng lên **`12/61 (19.67%)`** (**`McNemar p = 0.0156`**); các lớp trung gian `B–E` rất thưa (`10–20` mẫu TEST) khiến `Macro-F1` chỉ đạt **`0.4475`** (dù `Severe F1 = 0.9301`).
  > - *Thứ hai (`Exogenous Blindness`):* Vector 23 biến chưa có dữ liệu mưa ngập hay tai nạn giao thông trực tiếp."
- **Tra cứu nhanh Báo cáo:** Chương 5, Mục `5.1` – `5.3` & `5.5`.

---

#### Slide 10: `7. Batch Processing` — Luồng Xử Lý Lô Phân Tán, Khai Phá Đồ Thị & Phân Cụm (`10:00 – 11:15`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Trình bày rõ luồng dữ liệu & các component xử lý lô từ `Zones A → B → D → F / E`, kết quả thực nghiệm Spark 3.5.7 (`BroadcastHashJoin +17.07%`, `Monoid diff=0.0`, `Salting IQR`) và **Limitations & Risks của Batch Processing**.
- **Chỉ dẫn trực quan:** Chỉ theo các mũi tên trên **Sơ đồ Luồng & Thành phần Batch Processing** phía trên, thẻ kết quả bên trái dưới và thẻ `Limitations & Risks (Batch Processing)` bên phải dưới.
- **Lời thoại thuyết trình:**
  > "Tiếp theo ở **Phần 7 — Batch Processing** trên Slide 10, em xin trình bày luồng xử lý lô trên **Apache Spark 3.5.7**:
  > - **Trên nhánh Spark Batch ETL phía trên:** **`BroadcastHashJoin`** loại bỏ hoàn toàn `SortMergeJoin`, giảm tầng trộn mạng **`Exchange` từ `5` xuống `1` (`-80%`)** và rút ngắn thời gian thực thi từ **`145.24 ms` xuống `120.45 ms` (`+17.07%` speedup, `exceptAll_diff = 0`)**. Song song đó, **`5-Tuple Monoid`** khớp `100.0%` với `baseline.py` (`0` diff, `33,394` cửa sổ) và **`8-Bucket Key Salting`** kéo lệch tải slot từ **`684.95x` xuống `1.020x`**, giảm `IQR` thời gian chạy từ **`31.29 ms` xuống `11.27 ms`** (ở đánh đổi hợp lý `147.11 → 222.60 ms` trên tập nhỏ `90.8k` pings do chi phí 2-stage shuffle); **`Parquet Pruning`** chỉ đọc **`7,268 / 33,394` dòng** cho tháng `2020-08`.
  > - **Trên nhánh Khai phá Đồ thị & Phân cụm phía dưới:** Cắt tỉa `90.97%` điểm vẽ cong, dựng đồ thị bỏ `U-turn` (`92,453` cạnh, `314` nút cụt) tính **`Line-Graph PageRank`** bảo toàn khối lượng **`84,206.0`** (**`#1 #21969 Nguyễn Kiệm = 2.7799`**, `#2 #37345 Cầu vượt Ngã tư Gò Dưa = 2.7196`). Khối **`SON / FPGrowth & 7D K-Means`** tìm `335` luật FPGrowth, `12` luật đồng kẹt (`#1 Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.23x`) và `14` luật trễ đã kiểm soát quán tính, đồng thời phân `6` cụm (`3,609` đoạn organic TRAIN, `Silhouette = 0.2197`) phủ đủ `74,211` đoạn `Cold-Start`.
  >
  > **Về Limitations & Risks của Batch Processing (thẻ góc dưới phải):** `14` luật trễ `+30m` (`Lift` tới `24.0x`) chỉ xuất hiện trong **`5–8` cửa sổ** do GPS thưa → chỉ làm giả thuyết thăm dò; còn `74,211` đoạn `Cold-Start` gán cụm dựa trên hình thái tĩnh."
- **Tra cứu nhanh Báo cáo:** Chương 3 (`3.3–3.5`, Hình `3.3`) & Chương 4 (`4.1–4.4`, Hình `4.1`).

---

#### Slide 11: `8. Stream Processing` — Luồng Cảnh Báo Trực Tuyến, `O(1) Sketches` & Đối Soát Trễ (`11:15 – 12:25`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Trình bày luồng sự kiện Kafka, Spark real watermark, `O(1) Sketches (33.0 KiB)`, `Two-Tier Recon`, MongoDB `$status_version` + `IXSCAN` và **Limitations & Risks của Stream Processing**.
- **Chỉ dẫn trực quan:** Chỉ theo các mũi tên trên sơ đồ phía trên và 2 thẻ bên dưới.
- **Lời thoại thuyết trình:**
  > "Em là **Phạm Công Minh**, xin trình bày **Phần 8 — Stream Processing** trên Slide 11:
  > - **Trên đường nhanh `Kafka 4.1 KRaft → Speed Layer`:** **`Watermark 30m`** được kiểm chứng watermark thật trên Spark Structured Streaming (`2020-12-01T12:30:00Z`, tự động giải phóng `10` bản ghi hết hạn khỏi State Store) kèm `WAL` phục hồi `0` dupes. Khối **`O(1) Stream Sketches` (`33.0 KiB`)** gồm `Bloom Filter (32.0 KiB)` chặn `333/333` tin trùng (`FPR = 0.611%`), `HyperLogLog++ (1.0 KiB)` ước lượng `9,620.2 / 9,995` đoạn (`3.75%` err), và `EWMA Hotspot` ($\alpha = 0.15$, **`#1 #47407 Lý Thường Kiệt = 6.6652`**) trước khi `Online Scorer` xuất bản ghi tạm thời `v1`.
  > - **Đối soát hai tầng & `MongoDB 8.0`:** **`Tier 1`** khớp `100%` (`tier1_exceptAll_diff = 0`); **`Tier 2`** bù đủ **`500` cửa sổ trễ** (`226` khung giờ), hàn gắn **`609` downstream lag windows** và re-score **`384` cửa sổ** (`185` đổi xác suất, nâng độ đầy đủ `98.5% → 100.0%`). Trên `MongoDB 8.0.34` (`33,394` docs), **`atomic $status_version pipeline guard`** chặn **`25/25`** bản tin cũ `v1`; chỉ mục phức hợp `IXSCAN` đạt **`warm median = 0.389 ms / p95 = 0.692 ms`** (**nhanh hơn `23.55x`** so với `COLLSCAN 9.161 ms`) phục vụ Dual-UI `:8000` & `:8501`.
  >
  > **Về Limitations & Risks:** Mảng bit `32.0 KiB` cần `Ring-Buffer Rotation` khi chạy dài ngày ở quy mô TB/ngày; và `500` cửa sổ trễ `> 30m` phải đợi `Tier-2 Reconciliation` cuối ngày."
- **Tra cứu nhanh Báo cáo:** Chương 6, Mục `6.1` – `6.5` & Hình `6.1`.

---

#### Slide 12: `9A. Experiments (Ablation & Benchmarks)` — Thực Nghiệm 7 Mô Hình & Kiểm Chứng Hạ Tầng (`12:25 – 13:30`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Trình bày bảng `Ablation` 7 mô hình sau khi lọc burst (`1,885` organic TRAIN), thắng thế của `RF 23D` trên `Live VAL Gate`, `Confusion Matrix` 6 lớp (kèm báo cáo trung thực `Macro-F1 = 0.4475`) và benchmark Spark/MongoDB.
- **Chỉ dẫn trực quan:** Chỉ dòng in đậm `#5 Random Forest (23D)` bên trái, 2 thẻ bên phải.
- **Lời thoại thuyết trình:**
  > "Sang **Phần 9A — Experiments (Ablation & Benchmarks)** trên Slide 12: trên tập `1,182` mẫu `TEST` độc lập sau khi huấn luyện trên `1,885` mẫu `organic TRAIN` với `cls_w = 0.65`, **`#5 Random Forest (23D)`** được tuyển chọn từ **`Live VAL Composite Gate` (`0.8990` so với `0.5343` của `RF 14D`)** đạt **`ROC-AUC = 0.9900`**, **`PR-AUC = 0.9595`**, **`Severe F1 = 0.9301`** và **`6-Class Acc = 0.9289`**, với đường chéo `Confusion Matrix` **`[937, 4, 1, 7, 2, 147]`** phân biệt đủ cả 6 lớp `A–F`. So với `RF 14D` (`Acc 0.9239, Sev-F1 0.9167, PR-AUC 0.9557`), 9 biến đồ thị và phân cụm cải thiện toàn bộ các chỉ số của `RF`. Nhóm cũng báo cáo thẳng thắn **`Macro-F1 = 0.4475`** do các lớp trung gian `B, C, D, E` trong thực tế `TEST` chỉ có `10–20` mẫu.
  >
  > Song song ở thẻ dưới phải là các thông số đo trực tiếp từ `Spark 3.5.7` và `MongoDB 8.0`: `BroadcastHashJoin` (`5 → 1` Exchange, `145.24 → 120.45 ms`, **`+17.07%`**), `Monoid Salting` (`684.95x → 1.020x`, `IQR 31.29 → 11.27 ms`), `Sketches 33.0 KiB`, và `MongoDB IXSCAN warm median 0.389 ms / p95 0.692 ms` (**`23.55x`**)."
- **Tra cứu nhanh Báo cáo:** Chương 5, Mục `5.3`, Bảng `5.4` & Hình `5.1`.

---

#### Slide 13: `9B. Experiments (Defense)` — Giải Mã Nghịch Lý `Persistence Baseline` Trên 4 Chế Độ (`13:30 – 14:45`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Giải mã trực diện vì sao chọn `Random Forest (23D)` (`Acc = 92.89%`) khi `Persistence Baseline` đạt `94.84%` thông qua đối chứng khách quan trên 4 chế độ giao thông thực tế và kiểm định `McNemar`.
- **Chỉ dẫn trực quan:** Chỉ bảng 4 chế độ kiểm định bên trái đối chiếu biểu đồ bên phải và thẻ kết luận dưới phải.
- **Lời thoại thuyết trình:**
  > "Ở **Phần 9B — Experiments (Scientific Defense)** trên Slide 13, nhóm giải mã câu hỏi phản biện: **Vì sao `Persistence Baseline` đạt `94.84%` trên toàn tập `TEST` trong khi `Random Forest (23D)` đạt `92.89%`?** Bởi `94.84%` mẫu (`1,121 / 1,182`) không đổi nhãn sau 30 phút nên đoán nguyên trạng luôn thắng trên phần đứng yên.
  >
  > Nhưng khi bóc tách sang **4 chế độ kiểm định thực tế**:
  > 1. **Ở `61` ca đổi trạng thái (`LOS(t) ≠ LOS(t+30m)`):** `Baseline` **mù `100%` (`0 / 61`)**, còn `Random Forest (23D)` đạt **`Detect ROC-AUC = 0.9146`** (vượt xa `Decision Tree 0.7996` và mô hình 2 biến `0.7921`), bắt trúng mức mới **`12 / 61` (`19.67%` tại `τ = 0.40` trên `VAL`; mặc định `7 / 61 = 11.48%`)**, `Top-1 hướng mới = 31.15%`, giảm sai số `Brier` từ `0.3279 → 0.2872`, và vượt trội có ý nghĩa thống kê với **kiểm định `McNemar` `p = 0.0156` (`7` vs `0`)**!
  > 2. **Ở `8 / 993` ca đang thoáng (`A–D`) đột ngột hóa kẹt cứng (`E–F`):** Dù ngưỡng cứng `0/8` do quá hiếm (`0.81%`), năng lực xếp hạng đạt **`Onset ROC-AUC = 0.8539`** và đẩy xác suất rủi ro tăng **`3.32x`** (`0.0823` vs `0.0248`).
  > 3. **Ở `3,605` cặp GPS ngắt quãng `> 30 phút` (`49.21%` đổi nhãn) và `74,211` đoạn `Cold-Start`:** `RF 23D` vượt `Baseline` **`+5.61 điểm PR-AUC`** (`0.6293` vs `0.5732`) và nâng độ phủ mạng lưới cơ giới từ **`11.87%` (`0%` Cold-Start) lên `100.0%` (`84,206` đoạn)**."
- **Tra cứu nhanh Báo cáo:** Chương 5, Mục `5.4`, Bảng `5.5` & Hình `5.2`.

---

#### Slide 14: `10. Demonstration` — Kiến Trúc `Dual-UI`: `TOC Web (:8000)` & `Streamlit (:8501)` (`14:45 – 15:50`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Trình chiếu 2 case studies cận cảnh trên `:8501` (`Case Study 1: Tab 2 E → F` và `Case Study 2: Tab 4 Cold-Start`) cùng bức tranh kiến trúc **Dual-UI (`FastAPI :8000 + ReactJS + Leaflet TOC Web` & `Streamlit :8501`)** kết nối `MongoDB 8.0` (`33,394` docs).
- **Chỉ dẫn trực quan:** Chỉ `Case Study 1` bên trái, `Case Study 2` bên phải và dải `Kiến Trúc Dual-UI` phía dưới.
- **Lời thoại thuyết trình:**
  > "Bước sang **Phần 10 — Demonstration** trên Slide 14: hệ thống vận hành đồng thời **2 giao diện trực quan hóa (`Dual-UI`)** kết nối trực tiếp kho **`MongoDB 8.0` (`33,394` documents)**:
  > - **Bên trái (`Case Study 1` trên `Tab 2`):** Tại **đoạn `21064` — Công Trường Quốc Tế** (`2021-04-22 11:30 ICT`, đang ở `LOS E`, `12.0 km/h`), `Baseline` giữ nguyên mức `E` (đoán sai), trong khi **`Random Forest (23D)` dự báo chính xác chuyển sang mức `F` (`severe_prob = 72.4%`, khớp thực tế lúc `12:00` là `F`)**!
  > - **Bên phải (`Case Study 2` trên `Tab 4`):** Khi chọn **đoạn `61781` — Phú Định** (`0` GPS, `PageRank = 2.2141`, `Cluster 5` trong `74,211` đoạn `Cold-Start`; lưu ý `#1` nút cổ chai toàn thành phố là `#21969 Nguyễn Kiệm PR = 2.7799`), hệ thống đối chiếu tức thì các đoạn cảm biến tương đồng nhất trong cùng cụm (`Nguyễn Kiệm`, `Song Hành Xa lộ Hà Nội`).
  > - **Dải bên dưới:** Bên cạnh **5 Tab `Streamlit (:8501)`** phục vụ điều hành và kiểm toán, hệ thống còn tích hợp cổng **`TOC Web` hiện đại (`FastAPI :8000 + ReactJS + Leaflet`)** hiển thị bản đồ hành lang **`84,206` đoạn cơ giới**, phát cảnh báo `< 1 ms` và mô phỏng trực tiếp ngay ở phần Live Demo sắp tới."
- **Tra cứu nhanh Báo cáo:** Chương 6, Mục `6.4` & Hình `6.2`, `6.3`, `6.4`.

---

#### Slide 15: `11. Limitations & Risks` — Tổng Hợp 5 Thành Phần Giải Pháp & 4 Rủi Ro Chung (`15:50 – 16:55`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Tổng hợp (`Roll-Up`) toàn bộ hạn chế của **5 thành phần (`Mục 3, 5, 6, 7, 8`)** và trình bày trung thực **4 Rủi ro Chung Toàn Hệ thống**.
- **Chỉ dẫn trực quan:** Chỉ bảng 5 thành phần bên trái và 4 rủi ro bên phải.
- **Lời thoại thuyết trình:**
  > "Sang **Phần 11 — Limitations & Risks (Tổng hợp)** trên Slide 15: bảng bên trái hệ thống hóa lại giới hạn của 5 thành phần (`6 dịch vụ / 12.0 GB RAM` ở `Arch`, `57.11% Vmax Tier 2/3` ở `Data Pipeline`, `Macro-F1 = 0.4475` và đổi pha bảo thủ ở `Modeling`, `14` luật trễ `5–8` cửa sổ ở `Batch`, và `Bloom 32.0 KiB` + tin trễ `> 30m` ở `Stream`).
  >
  > Thẻ bên phải đúc kết **4 Rủi ro Toàn cục**: **(1)** Độ phủ GPS mới đạt **`11.87%` (`9,995 / 84,206` đoạn)** thiên về ô tô; **(2)** Tập `1,885` organic TRAIN có thể trôi dạt khi gặp mùa mưa ngập, đồng thời lớp trung gian `B–E` thưa (`10–20` mẫu `TEST`) kéo `Macro-F1` về **`0.4475`**; **(3)** Ca thoáng chuyển sang kẹt đột ngột chỉ chiếm **`0.81%` (`8/993`, Hard Recall `0/8`, `Onset ROC = 0.8539`, Lift `3.32x`)**; và **(4)** Cụm Docker đơn `12.0/16 GB RAM` hỗ trợ khuyến nghị cho điều hành viên, chưa đóng vòng đèn `SCATS`."
- **Tra cứu nhanh Báo cáo:** Chương 7, Mục `7.5`.

---

#### Slide 16: `12. Plan` — Nghiệm Thu Kế Hoạch Thực Hiện Nhóm & Lộ Trình Phát Triển 3 Giai Đoạn (`16:40 – 17:20`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Trình bày nghiệm thu 100% của 3 thành viên và lộ trình `Phase 1–3 Plan` khắc phục từng hạn chế ở Phần 11.
- **Chỉ dẫn trực quan:** Chỉ bảng nghiệm thu phía trên và 2 thẻ `Phase 1–3 Plan & Quality Gate` phía dưới.
- **Lời thoại thuyết trình:**
  > "Tiếp theo ở **Phần 12 — Plan** trên Slide 16: cả 3 thành viên — bạn **Minh Quyền** (`Quality Funnel`, `Monoid diff=0.0`, `Key Salting 684.95x→1.020x`, `BroadcastHashJoin +17.07%`), bạn **Thành Đạt** (`PageRank 84,206.0`, `7D K-Means`, tách `7,192` burst → `1,885` organic TRAIN, `RF 23D ROC=0.9900, PR=0.9595, McNemar p=0.0156`), và em **Công Minh** (`Docker 6 dịch vụ 12.0/16 GB`, `Spark Real Watermark`, `Sketches 33.0 KiB`, `Recon 500/609/384`, `MongoDB IXSCAN 0.389/0.692 ms`, `Dual-UI :8000 & :8501`) — đều đã hoàn thành 100%, vượt qua kiểm thử `pytest`, thực thi `Spark 3.5.7 runtime` và đạt `0` lỗi `Ruff C901 < 15`. Lộ trình tiếp theo đi qua **Phase 1** (Focal Loss + Ring-Buffer Bloom), **Phase 2** (GPS đa nguồn + API Mưa ngập + PSI retrain), và **Phase 3** (Kubernetes Multi-Node + Sharded MongoDB + kết nối đèn `SCATS`)."
- **Tra cứu nhanh Báo cáo:** Chương 7, Mục `7.1` – `7.5`.

---

#### Slide 17: `13. Technical Readiness & Production Verification Summary` (`17:20 – 18:00`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Tổng kết đối chiếu `[I]` vs `[P]`, thông số runtime Spark 3.5.7 & MongoDB 8.0 đồng bộ 100% từ các file JSON trên đĩa (`2026-11-18T20:07Z`), và chuyển mượt sang **Live Demo Dual-UI (`:8000` & `:8501`)**.
- **Chỉ dẫn trực quan:** Chỉ 5 thẻ từ trên xuống dưới trên Slide 17.
- **Lời thoại thuyết trình:**
  > "Khép lại bộ slide trên **Slide 17 — Technical Readiness & Production Verification**, toàn bộ con số đều đồng bộ 100% từ nguồn sự thật `outputs/metrics.json`, `spark_runtime_evidence.json` và `mongo_verify.json` (`Wave 2 · 2026-11-18T20:07Z`):
  > 1. **Architecture Truth (`[I]` vs `[P]`):** Đã vận hành đủ **6 container Docker (`12.0/16 GB RAM`)** cùng **`Dual-UI` (`FastAPI + ReactJS/Leaflet TOC Web :8000` & `Streamlit :8501`)** và gói mẫu `data/demo_sample/`.
  > 2. **Spark 3.5.7 Runtime:** `BroadcastHashJoin` giảm `Exchange` **`5 → 1` (`-80%`)**, rút thời gian **`145.24 ms → 120.45 ms` (`+17.07%`, `exceptAll_diff = 0`)**; `8-Bucket Salted Monoid` kéo lệch slot `684.95x → 1.020x` (giảm `IQR` `31.29 → 11.27 ms` ở tradeoff `147.11 → 222.60 ms`) và cắt `9/10` phân vùng Parquet.
  > 3. **ML Validation:** Lọc `7,192` burst → `1,885` organic TRAIN (`cls_w = 0.65`), `RF 23D` thắng `VAL Gate` (`0.8990`), `McNemar p = 0.0156`, và kiểm chứng `#21969 Nguyễn Kiệm` có `PageRank #1 = 2.7799` dù lưu thông thoáng (khẳng định `PageRank` đo nút phễu hình học).
  > 4. **Spark Real Watermark & MongoDB 8.0:** Structured Streaming watermark thật **`2020-12-01T12:30:00Z`** (evict `10` rows), `Bloom 32.0 KiB`, `HLL++ 1.0 KiB`, `MongoDB $status_version` chặn `25/25` `v1` và `IXSCAN warm 0.389 / p95 0.692 ms` (**`23.55x`**).
  > 5. **Honest Limitations:** Báo cáo minh bạch `88.13%` (`74,211`) `Cold-Start`, `Macro-F1 = 0.4475` (`B–E` thưa) và `0/8` ca khởi phát đột ngột (`Onset ROC = 0.8539`, Lift `3.32x`). Sau đây em xin trình diễn trực tiếp trên **`http://localhost:8000` (`TOC Web`)** và **`http://localhost:8501` (`Streamlit`)**!"
- **Tra cứu nhanh Báo cáo:** `outputs/metrics.json`, `outputs/spark_runtime_evidence.json`, `outputs/mongo_verify.json`.

---

## Phần 3. Kịch Bản Thao Tác `Live Demo` 3 Phút (`ReactJS + FastAPI TOC :8000` & `Streamlit :8501`)

> **Chuẩn bị sẵn trước giờ lên bục (1 lệnh duy nhất khởi động đủ 6 container `kafka`, `mongo`, `spark`, `toc-web`, `app`, `live-consumer`):**
> ```powershell
> docker compose --profile live up -d --build
> ```
> Mở sẵn **Cổng Điều Hành TOC (`ReactJS (toc_frontend/) + FastAPI (toc_backend/) + Leaflet` tại `:8000`)** ở `http://localhost:8000` (cùng **Streamlit 5-Tab UI `:8501`** tại `http://localhost:8501`).

1. **Phút 1 (`00:00 – 01:05`) — Kể chuyện Dự báo Theo Trục Thời Gian Động trên `Tab 01 · Phòng Điều Hành (:8000)`:**
   - Chỉ lên thanh **Đồng hồ Phòng Điều Hành TOC** trên đầu trang: giải thích rõ quy ước **`Thu thập GPS [12:00 – 12:30) ➔ Chốt phát lệnh t = 12:30 ICT ➔ Dự báo trước +30p cho [12:30 – 13:00)`**.
   - Nhấp chọn **`Công Trường Quốc Tế (#21064)`** (`2021-04-22 11:30 ICT`):
     - **Trên Bản đồ Leaflet động (`84,206` đoạn cơ giới):** Bản đồ tự động bay tới đúng ngã tư tại TP.HCM, tô sáng đoạn đường đang báo động và bật **`🔥 Hiện 25 Điểm nóng EWMA (UC-1)`** để đối chiếu `#47407 Lý Thường Kiệt (6.6652)`.
     - **Trên Biểu đồ Chuỗi Thời Gian 2 Tầng bên dưới:** Chỉ từng chấm xanh lam GPS thô gom thành điểm chốt `12.0 km/h (LOS E)` tại `11:30 ICT`, sau đó mở nhãn thực tế `t+30m` để thấy `Persistence = E` đoán trượt còn **mũi tên nét đứt đỏ `Random Forest 23D = LOS F (severe_prob = 72.4%)`** bắt trúng thực tế `LOS F`!
2. **Phút 2 (`01:05 – 02:10`) — Phát luồng qua `Kafka` & đối chứng `Sketches` (`:8000` & `:8501`):**
   - Nhấn **`🎬 Phát Story qua Kafka`** trên TOC Web `:8000`: dữ liệu đi thật từ `producer.py --mode story` qua `Kafka 4.1 (3 Partitions)` → `live-consumer (33.0 KiB Sketches + RF 23D)` → `MongoDB 8.0`.
   - Chuyển sang **`Tab 02 · Luồng Kafka Thời Gian Thực`** (`:8000` hoặc `:8501`) để chứng minh `32.0 KiB Bloom Filter` chặn `100%` tin trùng (`333/333`, `FPR 0.611%`), `Watermark 30m` giữ tin trễ cho `Tier-2`, `1.0 KiB HLL++` (`9,620.2 / 9,995`, sai số `3.75%`), và `$status_version` bảo vệ bản ghi trong `MongoDB 8.0`.
3. **Phút 3 (`02:10 – 03:00`) — Lướt nhanh `Tab 03`, `Tab 04` & `Tab 05` (Đủ 6 Use Cases `UC-1..UC-6`):**
   - **`Tab 03 (UC-3, UC-4)`:** Chỉ bảng `Line-Graph PageRank` (`#1 #21969 Nguyễn Kiệm PR = 2.7799` nhưng chạy thoáng vs nút cổ chai kẹt thực tế) và cặp đồng kẹt `SON / FPGrowth` (`Nguyễn Thị Minh Khai ↔ Cống Quỳnh`, `Lift = 15.23x`).
   - **`Tab 04 (UC-5)`:** Chọn trực tiếp 1 đoạn `Cold-Start (0 GPS)` trong **`74,211` đoạn (`88.13%`)**, ví dụ **`#61781 Phú Định (PR = 2.2141, Cluster 5)`**, để xem các đoạn tương đồng có GPS trong cùng cụm `7D K-Means` (`Nguyễn Kiệm`, `Song Hành XLHN`).
   - **`Tab 05 (UC-6)`:** Chỉ bảng `Spark 3.5.7` (**`5 → 1 Exchange`, `145.24 → 120.45 ms (+17.07%)`**, `684.95x → 1.020x` skew), `7-Model Ablation` & đối soát 2 tầng (`500 trễ, 609 lag, 384 rescore`, **`98.5% → 100.0%`**).

---

## Phần 4. Cẩm Nang Phản Biện & Trả Lời Câu Hỏi Hội Đồng (`10 Câu Hỏi Trọng Tâm`)

### Câu 1: *"Tổng dung lượng 5 bảng gốc chỉ khoảng 38.5 MB (bảng vận tốc GPS nặng 3.58 MB), dùng Pandas chạy vài chục mili-giây là xong, tại sao nhóm phải dựng cụm Spark + Kafka + chia 8 giỏ khóa?"*
- **Người trả lời chính:** Minh Quyền (`Slide 05, 06, 10` & `Chương 1 Mục 1.1`, `Chương 3 Mục 3.3`).
- **Ý chính trả lời:**
  1. Nhóm hoàn toàn đồng ý nếu chỉ tính một lần trên tệp tĩnh `3.58 MB` thì `Pandas` đơn luồng là nhanh nhất. Tuy nhiên, trong môn `BDA501 — Big Data Analytics`, bộ dữ liệu công khai của Kaggle đóng vai trò **mẫu chuẩn kiểm chứng quy mô (`Proof-of-Scale`)**.
  2. Thực tế toàn TP.HCM với hơn `1` triệu xe buýt, taxi, xe công nghệ và xe tải phát GPS mỗi `5–10` giây sẽ sinh ra **`8–17` tỷ bản tin/ngày (`~1.5–3.0 TB/ngày`)**. Ở quy mô đó, lưu danh sách mã sự kiện lọc trùng trong RAM sẽ gây tràn bộ nhớ, ghép bảng không dùng `BroadcastHashJoin` sẽ nghẽn mạng, và lệch tải giờ cao điểm **`684.95` lần** sẽ làm treo máy xử lý phân vùng nóng.
  3. Trên **`Spark 3.5.7`**, phép **`5-Tuple Monoid + 8-Bucket Salting`** đạt sai số `0.0` so với `baseline.py` và kéo lệch slot về `1.020x` (giảm `IQR` thời gian chạy từ `31.29 → 11.27 ms`). Đánh đổi hợp lý là ở mẫu nhỏ `90.8k` pings, 2-stage shuffle tốn thêm độ trễ trung vị **`147.11 → 222.60 ms`** (trong khi **`BroadcastHashJoin`** rút **`5 → 1` Exchange**, **`145.24 → 120.45 ms`, `+17.07%` speedup**).

### Câu 2: *"Tại sao trên toàn tập TEST (1,182 mẫu), Persistence Baseline đạt 94.84%, RF 23D đạt Acc = 92.89% và Macro-F1 chỉ 0.4475? Nhóm giải thích việc lọc burst thế nào?"*
- **Người trả lời chính:** Thành Đạt (`Slide 09, 12, 13` & `Chương 5 Mục 5.3–5.4`, `Bảng 5.4, 5.5`).
- **Ý chính trả lời:**
  1. **Trung thực về `Macro-F1 = 0.4475` & Tách chuỗi Burst (`7,192` windows):** Trước đây nếu giữ `5,481` cặp TRAIN (bao gồm chuỗi burst tổng hợp), `RF` đạt `Acc = 0.9391, Sev-F1 = 0.9372` nhưng bị phụ thuộc vào mật độ burst nhân tạo. Nhóm **tách toàn bộ `7,192` burst windows khỏi TRAIN** còn **`1,885` organic TRAIN** (`cls_w = 0.65`) → `RF 23D` thắng `Live VAL Gate` (`0.8990` vs `RF 14D 0.5343`) và đạt TEST **`ROC-AUC = 0.9900, PR-AUC = 0.9595, Severe F1 = 0.9301, Acc = 0.9289`**. Chỉ số **`Macro-F1 = 0.4475`** là hệ quả tự nhiên do tập `TEST` có các lớp trung gian `B, C, D, E` quá thưa (chỉ `10, 15, 13, 20` mẫu so với `959` lớp `A` và `165` lớp `F`).
  2. **So sánh trực diện ở `5.16%` thời điểm đổi trạng thái (`61` ca):** `Baseline` **đoán sai `100%` (`0/61`)**, trong khi `RF 23D` đạt **`Detect ROC-AUC = 0.9146`**, đoán trúng mức mới **`12/61` (`19.67%` tại `τ = 0.40` trên `VAL`; `7/61 = 11.48%` mặc định)**, `Top-1 hướng mới = 31.15%`, và **vượt trội có ý nghĩa thống kê với kiểm định `McNemar p = 0.0156` (`7 vs 0`)**!
  3. **Về `8 / 993` ca đang thoáng (`A–D`) đột ngột hóa kẹt (`E–F`):** Dù `Hard Recall = 0/8` do quá hiếm (`0.81%`), năng lực xếp hạng đạt **`Onset ROC-AUC = 0.8539`** với xác suất rủi ro **cao gấp `3.32x`** (`0.0823` vs `0.0248`), và **`RF 23D`** phủ được **`100%` `84,206` đoạn cơ giới** (bao gồm `88.13%` vùng mù `Cold-Start`).

### Câu 3: *"Vì sao trong phần Literature Review, nhóm không chọn các mô hình Deep Learning trên đồ thị như DCRNN hay Graph WaveNet mà lại chọn Random Forest 23D?"*
- **Người trả lời chính:** Thành Đạt (`Slide 03, 07, 09` & `Chương 1 Mục 1.5`).
- **Ý chính trả lời:**
  Các mô hình `Deep Spatio-Temporal GNN` (`DCRNN`, `ST-GCN`, `Graph WaveNet`) đòi hỏi ma trận chuỗi thời gian liên tục đều đặn trên phần lớn các đỉnh đồ thị. Trong bộ dữ liệu thực tế tại TP.HCM, **88.13% đoạn cơ giới (`74,211` đoạn) không có bất kỳ bản tin GPS nào** và **66.30% cặp quan sát liên tiếp bị đứt quãng $\ge 60$ phút**, khiến phép tích chập thời gian trên đồ thị bị suy biến. Ngược lại, `Random Forest (23D)` kết hợp các đặc trưng đồ thị tĩnh (`Line-Graph PageRank`, `7D K-Means`) xử lý rất hiệu quả dữ liệu bảng đứt quãng trong ngân sách `12.0 GB RAM` mà không cần GPU chuyên dụng.

### Câu 4: *"Ở Phần 7 tính `PageRank` và phân cụm `K-Means`, rồi sang Phần 8 lại tính `EWMA Hotspot` — các phần này liên kết thế nào với mô hình `Random Forest` ở Phần 6?"*
- **Người trả lời chính:** Thành Đạt & Công Minh (`Slide 07, 09, 10, 11` & `Chương 4`, `Chương 6 Mục 6.2`).
- **Ý chính trả lời:**
  1. **`Line-Graph PageRank` và `7D K-Means` (Phần 7 → Phần 6):** Vừa hiển thị cho Sở GTVT quản lý `74,211` đoạn đường thiếu GPS (`Cold-Start`), vừa đóng góp trực tiếp **`12.57%` độ quan trọng** qua nhóm 9 biến không gian (`pagerank_score 4.82%`, `neighbor_los_ordinal 2.73%`, `street_causal_prior_los 1.42%`, `cluster_id`) vào vector `23D` của `Random Forest` (đưa `RF 23D` vượt `RF 14D` trên `VAL Gate`: `0.8990 vs 0.5343`).
  2. **`Random Forest` (Phần 6) vs. `EWMA Hotspot` (Phần 8):** `Random Forest` dự báo **tương lai 30 phút tới** (*"đường nào SẮP kẹt?"* để chỉnh đèn xanh phòng ngừa), còn `EWMA Hotspot` ($\alpha = 0.15$) đo **độ lì hiện tại** (*"đường nào ĐANG kẹt cứng lặp đi lặp lại suốt nhiều giờ chưa thông?"* như `#47407 Lý Thường Kiệt = 6.6652`).

### Câu 5: *"Các luật kẹt nối tiếp sau 30 phút có `Lift = 24.0x` nhưng chỉ xuất hiện trong `5` cửa sổ (`2.5` tiếng trong 10 tháng). Liệu có phải trùng hợp ngẫu nhiên không?"*
- **Người trả lời chính:** Thành Đạt (`Slide 10, 15` & `Chương 4 Mục 4.2, 4.4`).
- **Ý chính trả lời:**
  Nhóm hoàn toàn đồng tình và đã gắn cờ kiểm soát quán tính (`persistence_controlled = True`) cùng liệt kê đây là **Limitation & Risk trực tiếp của Batch Processing** ngay trên Slide `10` và Slide `15`. Do GPS gốc chỉ phủ `11.87%` đoạn cơ giới và gửi ngắt quãng, xác suất hai tuyến đường cùng có xe gửi tin ở hai khung 30 phút liên tiếp rất thấp khiến `14` luật trễ chỉ đạt `5–8` cửa sổ. Với cỡ mẫu nhỏ như vậy, hệ thống chỉ xem luật trễ là **giả thuyết tham khảo (`Exploratory Hypothesis`)** chứ không dùng làm quy luật cứng.

### Câu 6: *"Tại sao khi gom khung trên Spark lại phải cộng gộp bộ 5 đại lượng (`5-Tuple Monoid`), và đánh đổi độ trễ của Salting thế nào?"*
- **Người trả lời chính:** Minh Quyền (`Slide 06, 10` & `Chương 3 Mục 3.3`).
- **Ý chính trả lời:**
  Phép tính trung bình cộng không có tính kết hợp khi các phân vùng chứa số lượng bản tin khác nhau (`avg` lồng nhau gây sai số **`11.111111 km/h`** trên phân vùng `[1, 2, 3]`). Cộng gộp bộ 5 đại lượng `(n, sum_v, min_v, max_v, severe_cnt)` đảm bảo `8-Bucket Key Salting` đạt **sai số `0.0`** (`diff = 0`) so với `baseline.py`. Trên `Spark 3.5.7`, `Salting` cắt lệch tải slot từ `684.95x → 1.020x` và giảm `IQR` từ `31.29 → 11.27 ms` (`-64%` dao động), đánh đổi thêm `+75.5 ms` trung vị (`147.11 → 222.60 ms`) trên tập nhỏ `90.8k` pings do chi phí Shuffle 2 giai đoạn — hoàn toàn đáng giá khi mở rộng sang tỷ bản tin bị lệch phân vùng nóng.

### Câu 7: *"Vì sao báo cáo ghi 84,633 đoạn thô / 84,206 đoạn cơ giới và 33,441 dòng train.csv / 33,394 cửa sổ?"*
- **Người trả lời chính:** Minh Quyền (`Slide 05, 08` & `Chương 2 Mục 2.3`).
- **Ý chính trả lời:**
  Số liệu kế toán khớp chính xác 100%: từ `84,633` đoạn OSM thô, `Ordered Quality Funnel (R2)` lọc bỏ **`427` đoạn phi cơ giới** (lối đi bộ/cầu thang) còn đúng **`84,206` đoạn cơ giới**; tương ứng trên `90,938` bản tin gốc tách bỏ **`151` pings lỗi/phi cơ giới** còn **`90,787` bản tin hợp lệ**, gom thành **`33,394` cửa sổ cơ giới** (giảm đúng `47` cửa sổ phi cơ giới so với `33,441` dòng `train.csv` thô) và khớp **`89.19%`** nhãn LOS chuẩn BkTraffic.

### Câu 8: *"Làm sao chứng minh mô hình dự báo hoàn toàn không bị rò rỉ dữ liệu tương lai (`Zero Future Leakage`)?"*
- **Người trả lời chính:** Thành Đạt (`Slide 06, 08, 09` & `Chương 2 Mục 2.4`, `Chương 5 Mục 5.1–5.2`).
- **Ý chính trả lời:**
  Nhóm khóa chặt cả 5 chốt chặn: **(1)** Bảng `3-Tier Vmax` chỉ học trên `TRAIN`; **(2)** Bộ chuẩn hóa và tâm cụm `7D K-Means` chỉ học trên **`3,609` đoạn `organic TRAIN`**; **(3)** Tách sạch **`7,192` burst windows** khỏi `TRAIN` còn `1,885` mẫu; **(4)** Cắt `55` mẫu biên (`Boundary Purge`); và **(5)** Kiểm thử `Future-Shuffle Falsification` đảo ngẫu nhiên tương lai $> t$ cho sai lệch **`0.0`**.

### Câu 9: *"Những rủi ro lớn nhất khi đưa hệ thống Stream Processing (`32.0 KiB Bloom Filter` & `Watermark 30m`) chạy thực tế nhiều ngày liền là gì và kế hoạch khắc phục ra sao?"*
- **Người trả lời chính:** Công Minh (`Slide 07, 11, 15, 16` & `Chương 6 Mục 6.5`, `Chương 7 Mục 7.5`).
- **Ý chính trả lời:**
  1. **Rủi ro bão hòa mảng bit `Bloom Filter` (`Bit Saturation`):** Mảng cố định `32.0 KiB` (`262,144` bits) rất hiệu quả cho dữ liệu mẫu (`333/333` trùng, `FPR = 0.611%`), nhưng chạy nhiều ngày ở quy mô TB/ngày cần cơ chế **Ring-Buffer Sliding Bloom Filter** tự xoay vòng mỗi 30 phút (`Phase 1 Plan`).
  2. **Rủi ro bản tin trễ quá `Watermark 30m`:** Đã kiểm chứng watermark thật `2020-12-01T12:30:00Z` trên Spark Streaming, kết hợp `Two-Tier Recon` bù đủ **`500` cửa sổ trễ / `609` lag / `384` rescore** và khóa **`atomic $status_version` pipeline guard** trên `MongoDB 8.0` chặn `25/25` bản tin cũ `v1`.

### Câu 10: *"Làm thế nào `MongoDB 8.0` đạt được độ trễ truy vấn `0.389 ms / p95 0.692 ms` (`23.55x` so với COLLSCAN) mà không cần sắp xếp trên RAM?"*
- **Người trả lời chính:** Công Minh (`Slide 11, 14` & `Chương 6 Mục 6.4`).
- **Ý chính trả lời:**
  Nhóm thiết kế chỉ mục phức hợp **`Compound Index {street_level: 1, severe_prob: -1, segment_id: 1}`** tuân thủ quy tắc **`ESR` (`Equality – Sort – Range`)**. Khi Dashboard truy vấn `1,202` cửa sổ trục chính (`street_level` 1–2) có `severe_prob >= 0.70` sắp xếp giảm dần theo `severe_prob`, `MongoDB 8.0.34` đọc thẳng trên cây `B-Tree` (`winning_stages: [LIMIT, PROJECTION_SIMPLE, FETCH, SORT_MERGE, IXSCAN, IXSCAN]`, `in_memory_sort = false`), đạt **`warm median = 0.389 ms`**, **`warm p95 = 0.692 ms`** (**nhanh hơn `23.55x`** so với `COLLSCAN 9.161 ms`).
