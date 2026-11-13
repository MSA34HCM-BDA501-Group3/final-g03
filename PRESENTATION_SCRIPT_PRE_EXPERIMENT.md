# Kịch Bản Thuyết Trình Chi Tiết — Bản Trước Thực Nghiệm (Cột Mốc Nghiên Cứu Mô Hình & Thiết Kế Luồng Xử Lý · BDA501 — Big Data Analytics)

- **Tên đề tài:** Hệ thống Giám sát Hiện trạng & Dự báo Ùn tắc Giao thông Đô thị TP. Hồ Chí Minh (*HCMC Smart Traffic Big Data Analytics & Congestion Prediction Platform*)
- **Đơn vị & Học phần:** `FSB · FPT University | MSE AI | BDA501 – Big Data Analytics` (`Fall 2026`)
- **Giảng viên hướng dẫn (Instructor):** **Đặng Trần Khánh**
- **Cột mốc báo cáo:** **Design & Methodology Report (Pre-Experiment Milestone)** — Hoàn tất nghiên cứu mô hình và thiết kế toàn bộ luồng xử lý từ **Mục 1 đến hết Mục 8**, tiếp nối bởi **Mục 9 (`Limitations & Risks`)** và **Mục 10 (`Plan & Next Actions`)** trước khi bước vào giai đoạn huấn luyện mô hình thực nghiệm và đo kiểm hiệu năng runtime.
- **Bộ slide trình chiếu đi kèm (`13 Slides`):** `HCMC_Smart_Traffic_PreExperiment_Presentation.pptx` & `HCMC_Smart_Traffic_PreExperiment_Slides.pdf`
- **Nguyên tắc phân vai (Mỗi slide chỉ đúng 1 người nói trọn vẹn từ đầu đến cuối):**
  - **Slide 01, 02, 03, 04:** **Nguyễn Thành Đạt (`25MS23306`)**
  - **Slide 05, 06:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
  - **Slide 07, 08, 09:** **Phạm Công Minh (`25MS23310`)**
  - **Slide 10, 11:** **Nguyễn Thành Đạt (`25MS23306`)**
  - **Slide 12, 13:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**

---

## Phần 1. Bảng Ánh Xạ 13 Slides & Phân Công Người Thuyết Trình (`15 Phút` — `1 Slide = 1 Người Nói`)

| Thứ tự Slide | Lượt | Mục Báo Cáo Thiết Kế | Nội dung Cốt lõi | Người Thuyết trình | Khung Thời gian | Đối chiếu Thiết kế |
|---|---|---|---|---|---|---|
| **Slide 01** | Lượt 1 | **Mở đầu (Cover)** | Trang bìa `DESIGN & METHODOLOGY REPORT`, tên dự án, thành viên & Giảng viên hướng dẫn | **Thành Đạt** | `00:00 – 00:35` | Trang bìa |
| **Slide 02** | Lượt 1 | **1. Problem Statement** | Bối cảnh ùn tắc dây chuyền TP.HCM (`WHY`), 3 hạn chế hiện hữu & bài toán + giá trị (`WHAT`) | **Thành Đạt** | `00:35 – 01:45` | `render_visual_assets.py` |
| **Slide 03** | Lượt 1 | **2. Literature Review** | Khảo sát 4 hướng tiếp cận giao thông đô thị, khoảng trống nghiên cứu & lý do cần nền tảng tích hợp | **Thành Đạt** | `01:45 – 02:55` | Cơ sở lý luận `Mục 1–2` |
| **Slide 04** | Lượt 1 | **3. System Architecture** | **[Solution]** Sơ đồ kiến trúc tổng thể (`Zones A–F`), `3-Phase MLOps Lifecycle` + **Arch Limitations & Risks** | **Thành Đạt** | `02:55 – 04:05` | `pipeline.py`, `starter_stream.py` |
| **Slide 05** | Lượt 2 | **4. Data, EDA & Data Problem** | Kiểm toán 5 bảng gốc Kaggle (`38.50 MB`), vai trò `Proof-of-Scale` & 10 vấn đề dữ liệu (`EC-1..EC-10`) | **Minh Quyền** | `04:05 – 05:20` | `pipeline.py` (`inspect_raw_tables`) |
| **Slide 06** | Lượt 2 | **Giải thích Phương pháp (1/2)** | **[Visual Explainer]** Trực quan hóa `Train-Only 3-Tier Vmax Imputation` & `5-Tuple Monoid + Key Salting` | **Minh Quyền** | `05:20 – 06:35` | `traffic_data/spark_etl.py` |
| **Slide 07** | Lượt 3 | **Giải thích Phương pháp (2/2)** | **[Visual Explainer]** Trực quan hóa `Line-Graph PageRank`, `7D K-Means Cold-Start` & `O(1) Sketches (Bloom, HLL++, EWMA)` | **Công Minh** | `06:35 – 07:55` | `pipeline.py`, `starter_stream.py` |
| **Slide 08** | Lượt 3 | **5. Data Pipeline & Processing** | **[Solution]** Hành trình dữ liệu (`P1 → P4`), `Ordered Quality Funnel`, `3-Tier Vmax`, `LOS A–F` + **Pipeline Limitations & Risks** | **Công Minh** | `07:55 – 09:05` | `pipeline.py`, `traffic_data/spark_etl.py` |
| **Slide 09** | Lượt 3 | **6. Modeling** | **[Solution]** Thiết kế nhãn `Exact t+30m (7,898 cặp)`, `Boundary Purge`, tổ hợp `23D Features`, `Random Forest` + **Modeling Limitations & Risks** | **Công Minh** | `09:05 – 10:20` | `pipeline.py` |
| **Slide 10** | Lượt 4 | **7. Batch Processing** | **[Solution]** Sơ đồ luồng `Batch ETL` (`BroadcastJoin`, `Salting`, `Monoid`) & `Graph / K-Means` + **Batch Limitations & Risks** | **Thành Đạt** | `10:20 – 11:35` | `spark_batch.py`, `traffic_data/spark_etl.py` |
| **Slide 11** | Lượt 4 | **8. Stream Processing** | **[Solution]** Sơ đồ luồng `Kafka KRaft`, `Watermark 30m`, `33.0 KiB Sketches`, `Two-Tier Recon`, `MongoDB IXSCAN` + **Stream Limitations & Risks** | **Thành Đạt** | `11:35 – 12:50` | `producer.py`, `starter_stream.py`, `app.py` |
| **Slide 12** | Lượt 5 | **9. Limitations & Risks** | **Bảng Tổng hợp Giới hạn Thiết kế của 5 Thành phần Giải pháp (`Mục 3, 5, 6, 7, 8`) & 4 Rủi ro Chung Cấp Hệ thống** | **Minh Quyền** | `12:50 – 13:55` | Quản trị rủi ro hệ thống |
| **Slide 13** | Lượt 5 | **10. Plan & Next Actions** | **Tiến độ Hoàn thành Thiết kế (`Mục 1–8`), Kế hoạch Thực nghiệm & Đo kiểm Runtime (`Sprint 1 & 2`) + `Target KPIs`** | **Minh Quyền** | `13:55 – 15:00` | Lộ trình thực nghiệm & nghiệm thu |

---

## Phần 2. Kịch Bản Lời Thoại Chi Tiết Từng Slide (`Slide 01` – `Slide 13`)

### Lượt 1 (`Slide 01 – 04`): Nguyễn Thành Đạt (`25MS23306`)

#### Slide 01: Mở Đầu — Trang Bìa Báo Cáo Thiết Kế & Phương Pháp Luận (`00:00 – 00:35`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Giới thiệu trang trọng tên đề tài, học phần `BDA501 – Big Data Analytics`, 3 thành viên nhóm, Giảng viên hướng dẫn và cột mốc hiện tại của dự án (hoàn tất nghiên cứu mô hình & thiết kế luồng xử lý, chuẩn bị bước vào thực nghiệm & đo kiểm runtime).
- **Chỉ dẫn trực quan:** Hướng về nhãn `DESIGN & METHODOLOGY REPORT`, tiêu đề chính `HCMC Smart Traffic Big Data Platform` và dải `Project Team` cùng `Instructor` ở cuối trang bìa.
- **Lời thoại thuyết trình (Thành Đạt):**
  > "Kính chào thầy **Đặng Trần Khánh** và quý thầy/cô. Hôm nay nhóm chúng em gồm ba thành viên: em là **Nguyễn Thành Đạt (`25MS23306`)**, cùng hai bạn **Nguyễn Tạ Minh Quyền (`25MS23280`)** và **Phạm Công Minh (`25MS23310`)**, xin trình bày báo cáo thiết kế và phương pháp luận đồ án học phần `BDA501 – Big Data Analytics` với đề tài: **HCMC Smart Traffic Big Data Platform — Nền tảng phân tích dữ liệu lớn và dự báo ùn tắc giao thông đô thị TP.HCM**.
  >
  > Tính đến cột mốc hiện tại, nhóm đã hoàn tất việc kiểm toán dữ liệu gốc, nghiên cứu mô hình dự báo và thiết kế toàn bộ luồng xử lý từ **Phần 1 đến hết Phần 8**. Mở đầu bài báo cáo, em xin trình bày từ Phần 1 đến Phần 3 để làm rõ bài toán thực tế, khoảng trống nghiên cứu và kiến trúc tổng thể của hệ thống."
- **Đối chiếu Thiết kế:** Trang bìa (`Slide 01`).

---

#### Slide 02: `1. Problem Statement` — Bài Toán Điều Hành Giao Thông Chủ Động Tại Đô Thị TP.HCM (`00:35 – 01:45`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Trả lời rõ hai câu hỏi nền tảng **WHY** (vấn đề thực tế của giao thông TP.HCM và vì sao công cụ hiện tại chưa đáp ứng được) và **WHAT** (đồ án thiết kế giải quyết bài toán gì, tạo ra giá trị cho ai).
- **Chỉ dẫn trực quan:** Chỉ lần lượt 3 thẻ bên trái (`1. Vấn Đề Thực Tế`, `2. Vì Sao Phương Pháp Hiện Tại Chưa Đủ?`, `3. Bài Toán Đặt Ra & Giá Trị Thụ Hưởng`) kết hợp đối chiếu bản đồ mạng lưới đường TP.HCM bên phải.
- **Lời thoại thuyết trình (Thành Đạt):**
  > "Bước vào **Phần 1 — Problem Statement** trên Slide 02, em xin làm rõ ba câu hỏi nền tảng của bài toán:
  > - **Thứ nhất — Vấn đề thực tế tại TP.HCM là gì (`WHY`)?** Mật độ phương tiện rất cao lưu thông trên mạng lưới đường đan xen dày đặc — từ cửa ngõ sân bay Tân Sơn Nhất, khu trung tâm Quận 1 – Quận 10 đến hành lang Thủ Đức — khiến chỉ một điểm nghẽn cục bộ giờ cao điểm cũng nhanh chóng lan truyền thành ùn tắc dây chuyền. Công tác điều tiết hiện nay chủ yếu **phản ứng bị động sau khi đường đã kẹt cứng**, gây lãng phí lớn về thời gian và chi phí vận tải đô thị.
  > - **Thứ hai — Vì sao các công cụ hiện tại chưa đủ?** Camera cố định và vòng từ mặt đường rất đắt đỏ, chỉ đặt được ở một số nút giao chính; còn nguồn GPS hành trình lại phân bố lệch về đại lộ, bỏ trống phần lớn mạng lưới đường nhánh màu xám trên bản đồ (*vùng mù quan trắc*) và thường xuyên bị ngắt quãng, trễ tin. Đồng thời, các bảng giám sát hiện hữu chỉ báo nơi **ĐANG kẹt**, chưa cảnh báo sớm trước **30 phút** nơi **SẮP kẹt**, và tách rời giữa trực ban sự vụ với quy hoạch hạ tầng.
  > - **Thứ ba — Bài toán đặt ra & Giá trị thụ hưởng (`WHAT`):** Nhóm thiết kế một nền tảng dữ liệu lớn tích hợp để **(1)** suy diễn lưu thông toàn đô thị kể cả vùng mù cảm biến, **(2)** tìm nút cổ chai cấu trúc, và **(3)** dự báo nguy cơ kẹt cứng trước **30 phút** — mang lại giá trị trực tiếp cho **Trung tâm điều hành `TOC` & Kỹ sư đèn**, **Sở GTVT & Đội tuần tra**, và **Người dân tham gia giao thông**."
- **Đối chiếu Thiết kế:** `scripts/render_visual_assets.py` (`render_hcmc_problem_context_map`).

---

#### Slide 03: `2. Literature Review` — Các Hướng Tiếp Cận Quan Trắc, Dự Báo & Điều Hành Giao Thông (`01:45 – 02:55`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Khảo sát 4 hướng tiếp cận giải quyết bài toán giao thông đô thị trong thực tế và học thuật, chỉ ra khoảng trống nghiên cứu (`Research Gap`) và chứng minh logic nghiên cứu thuận chiều.
- **Chỉ dẫn trực quan:** Điểm qua 4 dòng trong bảng tổng quan phía trên, sau đó nhấn mạnh vào thẻ `Khoảng Trống Nghiên Cứu & Định Hướng Thiết Kế Nền Tảng Tích Hợp` ở phía dưới.
- **Lời thoại thuyết trình (Thành Đạt):**
  > "Tiếp nối bài toán vừa nêu, ở **Phần 2 — Literature Review** trên Slide 03, em xin điểm qua 4 hướng tiếp cận quản lý và dự báo giao thông đô thị hiện hữu:
  > 1. **Quan trắc cố định và đèn thích ứng (`SCATS`, `SCOOT`, Camera CCTV):** Đo lưu lượng chính xác tại nút có thiết bị, nhưng chi phí lắp đặt và bảo trì cao nên bỏ trống phần lớn mạng lưới đường nhánh.
  > 2. **Giám sát từ dữ liệu hành trình (`Floating Car / Probe GPS` — `Herrera et al., 2010; Zheng, 2015`):** Phủ diện rộng không cần cảm biến mặt đường, nhưng dữ liệu lệch về trục lớn, tín hiệu đứt quãng dài và chỉ mới phản ánh hiện trạng.
  > 3. **Dự báo ùn tắc ngắn hạn 15–60 phút (`ARIMA`, `DCRNN`, `Graph WaveNet` — `Vlahogianni, 2014; Li et al., 2018`):** Dự báo hiệu quả trên cao tốc có cảm biến liên tục dày đặc (`PeMS`, `METR-LA`), nhưng suy giảm mạnh khi gặp mạng lưới đô thị có dữ liệu thưa và ngắt quãng.
  > 4. **Các nền tảng hỗ trợ quyết định điều hành (`TOC / Urban DSS` — `Lv et al., 2015; Amini et al., 2017`):** Thường tách rời mô hình học thuật khỏi luồng vận hành thực tế có tin GPS trễ, tin trùng, và chia cắt giữa trực ban tức thời với quy hoạch dài hạn.
  >
  > Từ khảo sát đó, thẻ bên dưới đúc kết **khoảng trống nghiên cứu cốt lõi**: một mô hình dự báo đơn lẻ không thể tự làm sạch luồng GPS di động nhiễu/trễ, không suy diễn được vùng mù nếu thiếu cấu trúc đồ thị đường phố, và không nối được trực ban `TOC` với quy hoạch `Sở GTVT`. Bám sát quy trình nghiên cứu thuận chiều — **từ bài toán thực tế (`Mục 1`) → xác định khoảng trống (`Mục 2`) → thiết kế mô hình & luồng xử lý (`Mục 3–8`) → lập kế hoạch thực nghiệm (`Mục 9–10`)** — em xin chuyển sang kiến trúc tổng thể ở Slide 04."
- **Đối chiếu Thiết kế:** Cơ sở lý luận & định vị kiến trúc tích hợp.

---

#### Slide 04: `3. System Architecture` — Thiết Kế Kiến Trúc Hybrid Batch–Speed & `3-Phase MLOps` (`02:55 – 04:05`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Trình bày sơ đồ kiến trúc tổng thể 6 phân vùng (`Zones A–F`), thiết kế vòng đời `3-Phase MLOps Lifecycle` chống xung đột ghi trong ngân sách `10.5 / 16 GB RAM`, và nêu rõ **Limitations & Risks của thiết kế Kiến trúc** trước khi mời Minh Quyền tiếp nối.
- **Chỉ dẫn trực quan:** Chỉ theo các luồng mũi tên màu đi qua `Zone A` đến `Zone F` trên sơ đồ kiến trúc bên trái, thẻ `3-Phase MLOps Lifecycle` góc trên phải, và thẻ `Limitations & Risks (Arch)` góc dưới phải.
- **Lời thoại thuyết trình (Thành Đạt):**
  > "Trên **Slide 04 — Phần 3: System Architecture**, sơ đồ bên trái trình bày kiến trúc **Hybrid Batch–Speed** chia thành **6 phân vùng (`Zones A–F`)**. Dữ liệu từ `Zone A` đi qua `Zone B` (`Apache Kafka KRaft` & `Quality Funnel`), sau đó tách làm hai nhánh bên trong `Apache Spark`: nhánh tím phía trên là **`Zone C (Speed Layer)`** xử lý luồng trực tuyến với `Watermark 30m` để phát cảnh báo nhanh; nhánh xanh dương phía dưới là **`Zone D (Batch Layer)`** phụ trách xử lý lô chuẩn xác, khai phá đồ thị, huấn luyện mô hình và đối soát dữ liệu trễ.
  >
  > Ở thẻ góc trên phải, em muốn nhấn mạnh thiết kế **`3-Phase MLOps Lifecycle`** giúp triệt tiêu hoàn toàn xung đột ghi vào `Zone E (Frozen Registry)`:
  > - **`Phase A (Offline Batch Fit)`:** Chỉ huấn luyện mô hình và trích xuất đặc trưng tĩnh một lần trên tập quá khứ `TRAIN`, sau đó khóa chữ ký `SHA-256`.
  > - **`Phase B (Online Speed Layer)`:** Luồng trực tuyến chỉ đọc (`.predict_proba`) để phát cảnh báo tạm thời `v1`, tuyệt đối không ghi đè mô hình.
  > - **`Phase C (Late Reconciliation)`:** Cuối ngày gom các gói tin trễ quá 30 phút để cập nhật bản chuẩn `v2` tại `Zone F` (`Medallion Lakehouse`, `MongoDB` và `Streamlit Dashboard`), gói gọn cả 4 container trong ngân sách **`10.5 / 16 GB RAM`**.
  >
  > **Về Limitations & Risks nhận diện sớm ở cấp Kiến trúc (thẻ góc dưới phải):** thiết kế 4 dịch vụ trên cùng 1 máy chủ tiềm ẩn tranh chấp `Disk I/O` khi `Spark` trộn dữ liệu lớn (`Single-Host Contention`), đồng thời các gói tin trễ quá 30 phút sẽ có độ trễ cập nhật biến trễ đến chu kỳ `Phase C` cuối ngày (`Eventual Consistency Gap`).
  >
  > Tiếp theo, để làm rõ bức tranh dữ liệu thực tế đi vào kiến trúc này, em xin mời bạn **Minh Quyền** trình bày Slide 05 và Slide 06."
- **Đối chiếu Thiết kế:** `pipeline.py`, `starter_stream.py` (`verify_frozen_registry_integrity`).

---

### Lượt 2 (`Slide 05 – 06`): Nguyễn Tạ Minh Quyền (`25MS23280`)

#### Slide 05: `4. Data, EDA & Data Problem` — Hồ Sơ 5 Bảng Gốc & 10 Vấn Đề Dữ Liệu Thực Nghiệm (`04:05 – 05:20`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Trình bày kết quả kiểm toán khám phá (EDA) trên 5 bảng dữ liệu thô ban đầu (`38.50 MB`), giải thích vai trò mẫu chuẩn `Proof-of-Scale` cho quy mô toàn TP.HCM, và ánh xạ 10 dị thường dữ liệu (`EC-1..EC-10`) sang thiết kế kỹ thuật đề xuất.
- **Chỉ dẫn trực quan:** Chỉ biểu đồ tỷ lệ dị thường góc trên trái, thẻ `Hồ Sơ 5 Bảng Gốc & Mục Tiêu Quy Mô` góc dưới trái, và bảng 10 `Data Problems (EC-1..EC-10)` bên phải.
- **Lời thoại thuyết trình (Minh Quyền):**
  > "Cảm ơn bạn Đạt. Em là **Nguyễn Tạ Minh Quyền**, xin trình bày **Phần 4 — Data, EDA & Data Problem** trên Slide 05. Qua kiểm toán 5 bảng dữ liệu thô trên Kaggle (`38.50 MB`) — gồm `nodes` (`577,967` điểm bản đồ), `streets` (`5,553` tuyến đường), `segments` (`84,633` đoạn đường có hướng), `segment_status` (`90,938` bản tin GPS, chẳng hạn lúc `07:08` sáng trên đường Lý Thường Kiệt xe báo vận tốc chỉ `4 km/h`) và bảng đối chứng `train` (`33,441` cửa sổ) — nhóm định vị tập dữ liệu này là **mẫu đối chứng thiết kế (`Proof-of-Scale`)** để kiểm chứng sai số toán học trước khi nhân rộng cho quy mô toàn TP.HCM (ước tính **`8–17` tỷ tin/ngày, `1.5–3.0 TB/ngày`**).
  >
  > Kết quả EDA cho thấy dữ liệu gốc chỉ phủ GPS trên **`11.85%` đoạn đường** và bộc lộ **10 vấn đề dữ liệu thực tế (`EC-1` đến `EC-10`)** ở bảng bên phải:
  > - **Nhóm dị thường chất lượng & thời gian:** `85.21%` cửa sổ (`28,495` dòng) thiếu tốc độ tối đa `Vmax` (`EC-1`); `154` đoạn chiều dài $\le 0\text{ m}$ cùng `427` lối đi bộ phi cơ giới (`EC-2`); `631` dòng Quận và `2,071` dòng lệch chuẩn dấu Unicode (`EC-3`); và `4,320` cửa sổ (`12.92%`) bị lệch ngày khi đổi sang giờ Việt Nam `UTC+7` (`EC-4`).
  > - **Nhóm độ thưa & cấu trúc bản đồ:** `66.27%` (`15,516` cặp) quan sát liên tiếp bị đứt quãng $\ge 60$ phút (`EC-5`); **`88.15%` (`74,606` đoạn) hoàn toàn không có bản tin GPS nào (`EC-6 — Cold-Start`)**; và `90.93%` (`525,542` điểm) trên bản đồ chỉ là điểm vẽ cong kèm `48` ngõ cụt (`EC-7`).
  > - **Nhóm thách thức phân tán (`EC-8, EC-9, EC-10`):** Lệch tải giờ cao điểm và sai số khi gộp `avg()` từng phần, gói tin gửi lặp hoặc trễ mạng quá 30 phút, cùng rủi ro sập luồng và ghi đè phiên bản cũ. Từng vấn đề này đều được đối ứng bằng một thiết kế kỹ thuật ở cột bên phải."
- **Đối chiếu Thiết kế:** `pipeline.py` (`inspect_raw_tables`, `validate_raw_records_ordered`).

---

#### Slide 06: Giải Thích Phương Pháp (1/2) — Nội Suy 3 Tầng `Vmax` & Bộ Gộp `5-Tuple Monoid` (`05:20 – 06:35`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Trực quan hóa bằng sơ đồ và ví dụ số dễ hiểu để người nghe nắm rõ bản chất toán học của 2 phương pháp cốt lõi: **(1) Nội suy tốc độ tối đa 3 tầng chỉ học trên tập `TRAIN` (`Train-Only 3-Tier Vmax`)** và **(2) Bộ gộp 5 đại lượng cộng được (`5-Tuple Monoid`) kết hợp chia giỏ khóa (`Key Salting`)** trước khi mời Công Minh tiếp nối.
- **Chỉ dẫn trực quan:** Chỉ phễu bậc thang 4 mức bên trái cho `3-Tier Vmax`, sau đó chỉ ví dụ đối chiếu hộp Đỏ (`SAI`) và hộp Xanh (`ĐÚNG`) bên phải cho `5-Tuple Monoid + Key Salting`.
- **Lời thoại thuyết trình (Minh Quyền):**
  > "Sang **Slide 06**, em xin giải thích trực quan nguyên lý của hai phương pháp xử lý dữ liệu đầu tiên để người xem dễ hình dung cách chúng giải quyết `EC-1` và `EC-8`:
  > - **Bên trái — Nội suy tốc độ 3 tầng (`Train-Only 3-Tier Vmax`):** Khi `85.21%` cửa sổ thiếu `max_velocity`, nếu lấy vận tốc cực đại trên toàn bộ dữ liệu sẽ gây rò rỉ tương lai (`Data Leakage`). Do đó, hệ thống khóa quy tắc **chỉ học bảng tra từ tập quá khứ `TRAIN`** rồi suy diễn theo bậc thang từ riêng đến chung: đường có sẵn biển tốc độ hợp lệ thì giữ nguyên $\rightarrow$ nếu thiếu thì lấy **phân vị `P85` vận tốc lúc đường thoáng của chính đoạn đó trên `TRAIN`** (`Tầng 1`) $\rightarrow$ nếu đoạn ít lịch sử thì mượn **trung vị nhóm đường cùng cấp và loại** (`Tầng 2`) $\rightarrow$ cuối cùng mới dùng quy chuẩn đô thị `80 / 60 / 50 / 40 km/h` (`Tầng 3`).
  > - **Bên phải — Bộ gộp `5-Tuple Monoid` đi kèm `Key Salting`:** Giả sử trên cùng đoạn đường có 4 xe nằm ở 2 máy phân tán: Máy A có 1 xe chạy `10 km/h`, Máy B có 3 xe chạy `40 km/h` — vận tốc trung bình thật phải là `(10 + 120) / 4 = 32.5 km/h`. Nếu gộp ngây thơ bằng trung bình của trung bình `(10 + 40) / 2` sẽ ra **`25.0 km/h` — sai lệch ngay `7.5 km/h`!** Thay vào đó, mỗi máy giữ **bộ 5 đại lượng cộng được `(số xe, tổng vận tốc, min, max, số xe kẹt)`**. Nhờ phép cộng kết hợp chính xác tuyệt đối này, hệ thống thoải mái chia các trục đường nóng giờ cao điểm ra **`8` giỏ song song (`2-Stage Key Salting`)** để chống nghẽn mà kết quả cuối vẫn chuẩn xác `100%`.
  >
  > Để giải thích tiếp nhóm phương pháp về đồ thị, phân cụm và thiết kế mô hình dự báo, em xin mời bạn **Công Minh** trình bày từ Slide 07 đến Slide 09."
- **Đối chiếu Thiết kế:** `pipeline.py` (`fit_train_vmax_lookup`), `traffic_data/spark_etl.py`.

---

### Lượt 3 (`Slide 07 – 09`): Phạm Công Minh (`25MS23310`)

#### Slide 07: Giải Thích Phương Pháp (2/2) — `Line-Graph PageRank`, `7D K-Means` & Bộ Đếm `O(1)` (`06:35 – 07:55`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Một mình Công Minh trình bày xuyên suốt cả 3 cột phương pháp trên Slide 07: tìm nút cổ chai cấu trúc bằng `Line-Graph PageRank`, suy diễn cho `88.15%` đường vùng mù bằng `7D K-Means`, và giải thích trực quan bộ 3 thuật toán tóm tắt luồng **`Bloom Filter`, `HyperLogLog++` và `EWMA`** chỉ dùng `33.0 KiB RAM`.
- **Chỉ dẫn trực quan:** Chỉ lần lượt từ Cột 1 (`Line-Graph PageRank`) sang Cột 2 (`7D K-Means Cold-Start`) và Cột 3 (`O(1) Stream Sketches: Bloom, HLL++, EWMA`).
- **Lời thoại thuyết trình (Công Minh):**
  > "Cảm ơn bạn Quyền. Em là **Phạm Công Minh**, xin trình bày trọn vẹn **Slide 07** về ba nhóm phương pháp xử lý cốt lõi còn lại:
  > - **Cột thứ nhất — `Line-Graph PageRank`:** Bản đồ gốc có tới `90.93%` điểm chỉ dùng để vẽ độ cong con đường. Thiết kế của nhóm cắt bỏ toàn bộ các chấm trung gian này để giữ lại ngã rẽ thật, sau đó **đảo mỗi đoạn đường thành một nút (`Directed Line-Graph`)**: khi nhiều nhánh đường `A, B, C` cùng dồn lưu lượng vào một lối thoát hẹp `D`, thuật toán **`PageRank`** tự động chấm điểm cấu trúc của `D` cao vọt lên — giúp phát hiện nút cổ chai bẩm sinh ngay cả khi đoạn đó chưa có xe gắn GPS!
  > - **Cột thứ hai — `7D K-Means Cold-Start`:** Để giải quyết **`74,606` đoạn đường vùng mù (`0` GPS)**, thuật toán **`7D K-Means`** gom toàn bộ `84,633` đoạn đường vào các cụm hành vi đồng nhất dựa trên 7 đặc trưng hình thái và đồ thị (chỉ học trên tập `TRAIN`), giúp các đoạn vùng mù (ô vuông đen) mượn được đặc trưng của những tuyến có cảm biến tương đồng (chấm tròn) trong cùng cụm để phủ kín `100%` đô thị.
  > - **Cột thứ ba — Bộ 3 `O(1) Stream Sketches` trong `33.0 KiB` RAM:** Khi xử lý luồng trực tuyến quy mô lớn, lưu danh sách ID bản tin hay lịch sử từng đường vào `set`/`list` sẽ làm tràn RAM. Nhóm thiết kế 3 cấu trúc tóm tắt siêu gọn:
  >   1. **`Bloom Filter (32.0 KiB = 262,144 bits)`:** Kiểm tra nhanh gói tin GPS gửi lặp bằng 4 vị trí băm bit mà không cần lưu chuỗi ID gốc.
  >   2. **`HyperLogLog++ (1.0 KiB = 1,024 thanh ghi)`:** Đếm xem có **bao nhiêu đoạn đường khác nhau (`COUNT DISTINCT segment_id`)** đang hoạt động dựa trên kỷ lục chuỗi bit `0` liên tiếp ở đầu mã băm `SHA-256` trên `1,024` ngăn nhỏ (`1 byte`/ngăn) — sai số lý thuyết chỉ khoảng `3.25%` (`1.04 / √1024`) nhưng tốn cố định đúng `1 KiB` RAM.
  >   3. **`EWMA (Exponentially Weighted Moving Average, α = 0.15)`:** Điểm nóng có trí nhớ phai dần theo công thức $C_s(t) = 0.85 \cdot C_s(t-1) + \text{is\_severe}_t$. Đường kẹt cứng liên tiếp nhiều khung sẽ cộng dồn điểm vọt lên cao; khi đường thông thoáng trở lại (`is_severe = 0`), điểm tự nguội dần `15%` mỗi nhịp và tự xóa khỏi RAM khi xuống dưới `10^-4`, giúp phân biệt điểm **ĐANG kẹt lì thật sự** với nhiễu chờ đèn đỏ tức thời."
- **Đối chiếu Thiết kế:** `pipeline.py` (`compute_road_pagerank`, `fit_train_only_clusters_and_priors`), `starter_stream.py` (`bloom_check_and_add`, `hll_add`, `hll_estimate`, `update_bounded_decay_state`).

---

#### Slide 08: `5. Data Pipeline & Processing` — Thiết Kế `Quality Funnel` & Nội Suy `Train-Only` (`07:55 – 09:05`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Trình bày hành trình xuyên suốt một bản tin GPS đi qua 4 chặng thiết kế (`P1 → P4`), cơ chế `Ordered Quality Funnel` bảo toàn 100% quân số bản ghi, chuẩn hóa nhãn `LOS A–F`, và nêu rõ **Limitations & Risks của Data Pipeline**.
- **Chỉ dẫn trực quan:** Chỉ sơ đồ ngang `END-TO-END DESIGN WALKTHROUGH` phía trên, thẻ `Thiết Kế Ordered Quality Funnel & 3-Tier Vmax` góc dưới trái, và thẻ `Limitations & Risks (Data Pipeline)` góc dưới phải.
- **Lời thoại thuyết trình (Công Minh):**
  > "Tiếp nối các phương pháp vừa giải thích, em xin trình bày **Phần 5 — Data Pipeline & Processing** trên Slide 08. Sơ đồ phía trên kết nối trọn vẹn hành trình thiết kế của một bản tin GPS thực tế (như bản tin lúc `07:08` sáng trên đường Lý Thường Kiệt báo vận tốc `4 km/h`) đi từ dữ liệu thô qua 4 chặng xử lý `P1 → P4` đến kho phục vụ trực tuyến.
  >
  > Ở thẻ góc dưới trái, nhóm thiết kế **Ordered Quality Funnel 5 bước**: dùng `ANSI try_cast` cô lập mọi bản ghi lỗi định dạng sang vùng cách ly `Quarantine` kèm mã quy tắc vi phạm (đảm bảo **không làm sập job và không rơi bản ghi ngầm — `0 Silent Drops`**); tách riêng lối đi bộ phi cơ giới; chuẩn hóa tên đường về `Unicode NFC` và quy đổi dấu thời gian sang múi giờ Việt Nam `UTC+7`. Sau khi điền tốc độ tối đa bằng `Train-Only 3-Tier Vmax`, tỷ lệ `velocity / Vmax` của mỗi cửa sổ 30 phút được chuẩn hóa thành 6 mức phục vụ **`LOS A–F`**: `A–B` ($\ge 65\%$: thông thoáng), `C–D` ($35\%–65\%$: đông đúc) và `E–F` ($< 35\%$: kẹt cứng).
  >
  > **Về Limitations & Risks của Data Pipeline (thẻ góc dưới phải):** các tuyến khuyết biển báo phải dùng `Vmax` suy diễn tĩnh theo cấp đường (`Tier 2 & 3`) nên chưa tự động phản ánh rào chắn thi công hay ngập nước cục bộ; nếu thiết bị GPS đầu nguồn đổi schema hàng loạt sẽ làm tăng tỷ lệ cách ly vào `Quarantine`; và khung gom 30 phút sẽ làm mượt các dao động ngắn dưới 5 phút tại ngã tư."
- **Đối chiếu Thiết kế:** `pipeline.py` (`validate_raw_records_ordered`, `align_utc_to_ict`, `derive_los`).

---

#### Slide 09: `6. Modeling` — Thiết Kế Nhãn `Exact t+30m`, Vector 23 Biến & Mô Hình Dự Báo (`09:05 – 10:20`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Trình bày thiết kế bài toán học máy dự báo sớm `+30 phút` chống rò rỉ dữ liệu tương lai, thiết kế 3 nhóm biến đầu vào (`23D Features`), lựa chọn mô hình `Random Forest` đặt cạnh hệ đối chứng `Ablation`, và nêu rõ **Limitations & Risks của Modeling** trước khi mời Thành Đạt tiếp nối.
- **Chỉ dẫn trực quan:** Chỉ 2 thẻ bên trái (`Thiết Kế Nhãn Exact t+30m & Chống Rò Rỉ`, `Thiết Kế Tổ Hợp 23 Biến Đầu Vào`), thẻ góc trên phải (`Mô Hình Nghiên Cứu & Cơ Chế Cold-Start`), và thẻ góc dưới phải (`Limitations & Risks (Modeling)`).
- **Lời thoại thuyết trình (Công Minh):**
  > "Sang **Phần 6 — Modeling** trên Slide 09, em xin trình bày thiết kế bài toán học máy dự báo trước 30 phút với nguyên tắc xuyên suốt là **không rò rỉ dữ liệu tương lai (`Zero Leakage`)**:
  > 1. **Thiết kế nhãn `Exact (segment_id, slot_ts + 30m) Join` (thẻ trên trái):** Vì `66.27%` cặp GPS liên tiếp đứt quãng $\ge 60$ phút, nhóm không ghép bừa dòng kế tiếp mà chỉ ghép nhãn khi cùng đoạn đường có quan sát ở **đúng khung `t + 30 phút`** (`7,898` cặp hợp lệ từ EDA), chia tập `TRAIN / VAL / TEST` theo trình tự thời gian `70 / 15 / 15` kèm cắt biên **`Boundary Purge`**, và thiết kế bài kiểm chứng **`Future-Shuffle Falsification`** (xáo trộn tương lai $\tau > t$ để xác nhận ma trận đầu vào $X_t$ hoàn toàn bất biến).
  > 2. **Tổ hợp 23 biến đầu vào (`23D Features` — thẻ dưới trái):** Gồm **Nhóm 1 (7 biến cục bộ & độ trễ)** nắm bắt động lượng ngắn hạn; **Nhóm 2 (7 biến chu kỳ giờ Việt Nam `sin/cos` & hình học đường)** mã hóa nhịp cao điểm; và **Nhóm 3 (9 biến đồ thị `PageRank`, cụm `7D K-Means`, mức kẹt láng giềng & `Causal Priors` chỉ tích lũy trên quá khứ $\tau < t$)**.
  > 3. **Mô hình nghiên cứu & kế hoạch đối chứng (thẻ trên phải):** Nhóm đề xuất **`Random Forest (23D)`** (`250` cây, `max_depth = 14`, `class_weight = 'balanced_subsample'`) để học tương tác phi tuyến trên dữ liệu thưa mà không cần chuỗi liên tục như `GNN`, đặt cạnh các ứng viên `Ablation` (`Persistence Baseline`, `Decision Tree`, `Binary GBT` trên `14D` vs. `23D`) và kết hợp `Static Feature Store` để chấm điểm cho cả `74,606` đoạn `Cold-Start`.
  >
  > **Về Limitations & Risks của Modeling (thẻ dưới phải):** do quán tính trạng thái giữa hai khung 30 phút rất lớn, ngưỡng `argmax` mặc định có nguy cơ bảo thủ — cần hiệu chỉnh ngưỡng $\tau$ trên tập `VAL` ở bước thực nghiệm; đồng thời vector 23 biến chưa có dữ liệu mưa ngập hay tai nạn trực tiếp.
  >
  > Tiếp theo, để trình bày thiết kế thực thi phân tán trên `Batch` và `Stream`, em xin mời bạn **Thành Đạt** tiếp tục với Slide 10 và Slide 11."
- **Đối chiếu Thiết kế:** `pipeline.py` (`compute_chronological_split_boundaries`, `add_temporal_and_neighbor_lags`, `score_feature_snapshot`).

---

### Lượt 4 (`Slide 10 – 11`): Nguyễn Thành Đạt (`25MS23306`)

#### Slide 10: `7. Batch Processing` — Thiết Kế Luồng Xử Lý Lô, Khai Phá Đồ Thị & Phân Cụm (`10:20 – 11:35`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Một mình Thành Đạt trình bày trọn vẹn luồng di chuyển dữ liệu và vai trò từng component trong kiến trúc xử lý lô phân tán (`Broadcast Join`, `2-Stage Key Salting & 5-Tuple Monoid`, `Shape-Point Pruner & Line-Graph PageRank`, `SON / FPGrowth & 7D K-Means`), kèm **Limitations & Risks của Batch Processing**.
- **Chỉ dẫn trực quan:** Chỉ theo các mũi tên trên **Sơ đồ Luồng & Thành phần Batch Processing** phía trên (nhánh trên ra `Curated Gold Parquet`; nhánh dưới ra `Static Feature Store`), sau đó đối chiếu thẻ thiết kế bên trái dưới và thẻ `Limitations & Risks (Batch Processing)` bên phải dưới.
- **Lời thoại thuyết trình (Thành Đạt):**
  > "Cảm ơn bạn Minh. Em là **Thành Đạt**, xin trình bày trọn vẹn **Phần 7 — Batch Processing** trên Slide 10. Nhìn vào sơ đồ luồng phía trên, dữ liệu từ `Zone A` đi qua `Quality Funnel (Zone B)` vào `Apache Spark Batch Layer (Zone D)` theo hai nhánh song song:
  > - **Ở nhánh Spark Batch ETL phía trên:** Nhóm thiết kế **`BroadcastHashJoin`** ghim danh mục đường nhỏ gọn vào RAM của mọi worker để triệt tiêu trộn mạng cho bảng lớn; tiếp nối sang **`2-Stage Key Salting (8 giỏ 0..7) & 5-Tuple Monoid`** nhằm phân tán đều các trục đường nóng giờ cao điểm mà vẫn bảo toàn tổng và trung bình chính xác tuyệt đối, trước khi ghi xuống kho **`Curated Gold Parquet (Zone F)`** phân vùng theo `year_month`.
  > - **Ở nhánh Khai phá Đồ thị & Phân cụm phía dưới:** Khối **`Shape-Point Pruner & Line-Graph`** cắt tỉa `90.93%` điểm vẽ cong để dựng đồ thị có hướng `Line-Graph PageRank` ($\beta = 0.85$, có bù khối lượng cho 48 nút cụt `Dangling Sinks`). Kết hợp các cửa sổ `TRAIN` từ nhánh trên, khối **`SON / FPGrowth & 7D K-Means`** quét 2 lượt phân tán tìm tập đường đồng kẹt và gom cụm hình thái 7 chiều để xuất ra kho **`Static Feature Store (Zone E)`** phủ kín toàn bộ `84,633` đoạn đường.
  >
  > **Về Limitations & Risks của Batch Processing (thẻ góc dưới phải):** do GPS gốc chỉ phủ `11.85%` đoạn đường và đứt quãng, các luật lan truyền đồng kẹt trễ `+30m` dễ có `Support` thấp nên chỉ được dùng làm giả thuyết thăm dò (`Exploratory Hypothesis`); ngoài ra phân cụm `Cold-Start` tĩnh chưa phản ánh được các điểm họp chợ tự phát nhỏ lẻ trong hẻm."
- **Đối chiếu Thiết kế:** `spark_batch.py`, `traffic_data/spark_etl.py`, `pipeline.py`.

---

#### Slide 11: `8. Stream Processing` — Thiết Kế Luồng Cảnh Báo, `O(1) Sketches` & Đối Soát Trễ (`11:35 – 12:50`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Làm rõ luồng sự kiện trực tuyến từ `Apache Kafka` qua `Speed Layer` (`Watermark & Dual-Sink`, `O(1) Stream Sketches`, `Online Stream Scorer`), cơ chế chịu lỗi `WAL`, đối soát hai tầng `Two-Tier Reconciliation`, thiết kế chỉ mục `MongoDB IXSCAN` (mục tiêu `SLA < 10 ms`), và **Limitations & Risks của Stream Processing** trước khi mời Minh Quyền kết bài.
- **Chỉ dẫn trực quan:** Chỉ theo các luồng mũi tên tím (`Speed`), xanh ngọc nét đứt (`Read-Only Registry`), xanh dương (`Tier-2 Recon`) và xanh lá (`Serving`) trên sơ đồ phía trên, sau đó chỉ 2 thẻ bên dưới.
- **Lời thoại thuyết trình (Thành Đạt):**
  > "Tiếp nối xử lý lô, em xin trình bày **Phần 8 — Stream Processing** trên Slide 11. Theo sơ đồ phía trên, gói tin GPS đi vào **`Apache Kafka Broker (Zone B)`** ở chế độ `KRaft` được phân vùng theo `segment_id` để giữ đúng thứ tự thời gian từng con đường. Khi vào **`Speed Layer (Zone C)`**, khối **`Watermark & Dual-Sink`** thực hiện ghi kép: vừa ghi `100%` sự kiện thô kèm nhật ký **`WAL Checkpoint`** xuống `Bronze Landing` chống mất tin khi sập luồng, vừa áp ngưỡng `Watermark 30 phút` đưa bản tin đúng hạn đi thẳng vào đường nhanh màu tím.
  >
  > Trên đường nhanh này, khối **`O(1) Stream Sketches (33.0 KiB RAM)`** lọc trùng bằng `Bloom Filter (32 KiB)`, đếm số đường hoạt động bằng `HyperLogLog++ (1 KiB)` và cập nhật điểm nóng kẹt lì bằng `EWMA (α = 0.15)`; ngay sau đó **`Online Stream Scorer`** đọc chỉ-đọc mô hình đóng băng từ `Zone E` để phát cảnh báo sớm `+30m` phiên bản tạm thời `v1`. Với các gói tin trễ quá 30 phút ở nhánh dưới, tiến trình **`Two-Tier Reconciliation (Zone D)`** cuối ngày sẽ gom bù, tính lại biến `lag` và chấm điểm lại thành bản chuẩn `v2`. Cả hai luồng cùng `Bulk Upsert` vào **`MongoDB Serving Store (Zone F)`** kèm khóa **`version_ts`** (chặn tin cũ `v1` ghi đè bản chuẩn `v2`) và chỉ mục phức hợp **`Compound Index IXSCAN`** theo quy tắc `ESR` hướng tới mục tiêu **`SLA < 10 ms`** cho **`Streamlit Dashboard` `5` Tab**.
  >
  > **Về Limitations & Risks của Stream Processing (thẻ góc dưới phải):** mảng `Bloom Filter` `32.0 KiB` nếu chạy liên tục nhiều ngày ở quy mô hàng tỷ tin/ngày mà không xoay vòng (`Ring-Buffer Rotation`) sẽ đầy bit làm tăng báo trùng giả; và các gói tin trễ hơn 30 phút phải đợi chu kỳ `Tier-2 Reconciliation` cuối ngày.
  >
  > Khép lại toàn bộ thiết kế mô hình và luồng xử lý ở Phần 8, em xin mời bạn **Minh Quyền** trình bày hai slide cuối cùng về tổng hợp rủi ro và kế hoạch hành động kế tiếp."
- **Đối chiếu Thiết kế:** `producer.py`, `starter_stream.py`, `app.py`.

---

### Lượt 5 (`Slide 12 – 13`): Nguyễn Tạ Minh Quyền (`25MS23280`)

#### Slide 12: `9. Limitations & Risks` — Tổng Hợp Từng Thành Phần Thiết Kế & 4 Rủi Ro Chung (`12:50 – 13:55`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Tổng hợp có hệ thống toàn bộ giới hạn thiết kế nhận diện sớm của **5 thành phần giải pháp (`Mục 3, 5, 6, 7, 8`)** kèm cơ chế kiểm soát tương ứng trong kiến trúc đề xuất, đồng thời chỉ ra **4 rủi ro tổng thể cấp hệ thống** trước khi bước vào giai đoạn thực nghiệm.
- **Chỉ dẫn trực quan:** Chỉ bảng tổng hợp 5 thành phần giải pháp bên trái, sau đó chuyển sang thẻ `4 Rủi Ro & Giới Hạn Chung Cấp Hệ Thống` màu xanh đậm bên phải.
- **Lời thoại thuyết trình (Minh Quyền):**
  > "Cảm ơn bạn Đạt. Em là **Minh Quyền**, xin trình bày **Phần 9 — Limitations & Risks** trên Slide 12 để tổng hợp minh bạch mọi giới hạn kỹ thuật nhận diện sớm trước khi bước vào thực nghiệm:
  > - **Ở bảng bên trái — Tổng hợp 5 thành phần giải pháp (`Mục 3, 5, 6, 7, 8`):** Nhóm đặt cạnh nhau từng giới hạn thiết kế và chốt chặn kiểm soát tương ứng: từ chia sẻ tài nguyên `10.5 / 16 GB` trên 1 máy Docker ở `System Architecture` (kiểm soát bằng `mem_limit` cứng và tách đọc/ghi `3-Phase MLOps`), suy diễn `Vmax` tĩnh ở `Data Pipeline` (kiểm soát bằng `Train-Only` và `Quarantine`), nguy cơ ngưỡng mặc định bảo thủ ở `Modeling` (kiểm soát bằng hiệu chỉnh ngưỡng $\tau$ trên `VAL`), cỡ mẫu luật đồng kẹt thưa ở `Batch Processing`, cho đến bão hòa bit `Bloom Filter` và độ trễ tin muộn ở `Stream Processing` (kiểm soát bằng `Two-Tier Reconciliation` và khóa `version_ts`).
  > - **Ở thẻ bên phải — 4 Rủi ro chung cấp hệ thống:** Thứ nhất là **thiên lệch độ phủ không gian và đội xe** khi GPS gốc mới phủ `11.85%` đoạn đường và nghiêng về ô tô hơn xe máy; thứ hai là **trôi dạt phân phối (`Concept Drift`)** theo mùa mưa ngập hoặc rào chắn mới; thứ ba là **tỷ lệ rất hiếm của các pha chuyển đột ngột sang kẹt cứng** đòi hỏi đánh giá bằng `PR-AUC`; và thứ tư là **phạm vi `Proof-of-Scale` hỗ trợ quyết định cho điều hành viên**, chưa đấu nối tự động trực tiếp vào tủ đèn `SCATS`."
- **Đối chiếu Thiết kế:** Tổng hợp kiểm soát rủi ro thiết kế `Mục 3–8`.

---

#### Slide 13: `10. Plan & Next Actions` — Kế Hoạch Thực Nghiệm Mô Hình, Đo Kiểm Runtime & Demo (`13:55 – 15:00`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Một mình Minh Quyền đại diện nhóm trình bày trọn vẹn Slide 13: làm rõ ranh giới giữa phần nghiên cứu/thiết kế đã hoàn thành (`Mục 1–8`) với kế hoạch hành động tiếp theo của nhóm trong `Sprint 1` và `Sprint 2`, cùng bộ tiêu chí nghiệm thu định lượng (`Target KPIs`).
- **Chỉ dẫn trực quan:** Chỉ bảng tiến độ & `Next Actions` phía trên, thẻ `Lộ Trình Thực Thi Kế Tiếp (Sprint 1 & Sprint 2)` góc dưới trái và thẻ `Tiêu Chí Nghiệm Thu Thực Nghiệm (Target KPIs)` góc dưới phải.
- **Lời thoại thuyết trình (Minh Quyền):**
  > "Khép lại bài báo cáo ở **Phần 10 — Plan & Next Actions** trên Slide 13, em xin thay mặt nhóm trình bày kế hoạch chuyển từ thiết kế sang chạy thực nghiệm mô hình và đo kiểm runtime:
  > - **Trên bảng phân công phía trên:** Sau khi cả ba thành viên đã hoàn tất toàn bộ khâu nghiên cứu và thiết kế luồng từ Mục 1 đến Mục 8, nhóm chia rõ ba mũi thực nghiệm kế tiếp: **(1)** ở mảng xử lý dữ liệu và `Spark Batch ETL`, em sẽ trực tiếp chạy kiểm chứng sai số gộp `5-Tuple Monoid` đối chiếu với `baseline.py`, đo hệ số giảm lệch tải của `8` giỏ `Key Salting`, số `Shuffle Stages` và hiệu quả cắt tỉa `Parquet`; **(2)** ở mảng đồ thị và học máy, bạn Thành Đạt sẽ huấn luyện bảng đối sánh **`Ablation 7 cấu hình`** trên bộ biến `14D` và `23D`, kiểm định chống rò rỉ `Future-Shuffle`, phân tích nhóm ca đổi trạng thái `LOS(t) ≠ LOS(t+30m)` và hiệu chỉnh ngưỡng $\tau$ trên tập `VAL`; và **(3)** ở mảng luồng trực tuyến và kho phục vụ, bạn Công Minh sẽ khởi chạy cụm Docker đo tỷ lệ báo trùng giả của `Bloom Filter`, sai số `HyperLogLog++`, diễn tập sập luồng kiểm tra hồi phục `WAL`, đo tỷ lệ bù trễ `Tier-2`, độ trễ truy vấn `MongoDB IXSCAN` (`p50 / p95`) và hoàn thiện giao diện `5` Tab.
  > - **Ở hai thẻ bên dưới:** Toàn bộ khối lượng này được gói gọn trong **`Sprint 1` (Thực nghiệm Đồ thị & Học máy)** và **`Sprint 2` (Benchmark Runtime Hạ tầng & Live Demo)**, gắn với các chỉ tiêu nghiệm thu định lượng rõ ràng: sai số `Monoid = 0.0`, sai lệch `Future-Shuffle = 0.0`, vượt trội `Persistence Baseline` trên các ca chuyển trạng thái, độ đầy đủ sau đối soát đạt `100%`, `0` bản ghi trùng khi hồi phục `WAL`, và truy vấn `MongoDB IXSCAN` đạt `SLA < 10 ms`.
  >
  > Nhóm rất mong nhận được ý kiến nhận xét và định hướng của thầy **Đặng Trần Khánh** cùng quý thầy/cô để hoàn thiện thực nghiệm trong giai đoạn tới. Em xin thay mặt nhóm trân trọng cảm ơn!"
- **Đối chiếu Thiết kế:** Lộ trình thực thi `Sprint 1 & Sprint 2` + `tests/test_platform_v3.py`, `tests/test_quyen_spark.py`.

---

## Phần 3. Cẩm Nang Phản Biện Tại Cột Mốc Thiết Kế (`10 Câu Hỏi Trọng Tâm` — Mỗi Câu 1 Người Phụ Trách Chính)

### Câu 1: *"Tổng dung lượng 5 bảng dữ liệu gốc trên Kaggle chỉ `38.50 MB`, tại sao nhóm lại thiết kế kiến trúc phức tạp gồm `Spark + Kafka + 8 giỏ Salting + 33.0 KiB Sketches`?"*
- **Người trả lời chính:** **Minh Quyền** (`Slide 05, 06`).
- **Ý chính trả lời:**
  1. Nếu chỉ xử lý một lần tệp tĩnh `38.50 MB` thì `Pandas` đơn luồng hoàn toàn đủ dùng. Tuy nhiên, trong khuôn khổ môn `BDA501 — Big Data Analytics`, tập dữ liệu Kaggle được nhóm sử dụng làm **mẫu đối chứng thiết kế (`Proof-of-Scale`)**.
  2. Ở quy mô vận hành thực tế toàn TP.HCM với hơn `1` triệu phương tiện phát GPS mỗi `5–10` giây (sinh ra **`8–17` tỷ tin/ngày, `1.5–3.0 TB/ngày`**), dùng `set()` lọc trùng trên luồng sẽ làm tràn RAM, ghép bảng không có `BroadcastHashJoin` sẽ gây nghẽn mạng, và các trục đường trung tâm giờ cao điểm sẽ làm nghẽn cục bộ 1 phân vùng `Spark`.
  3. Thiết kế trên tập mẫu chuẩn `38.50 MB` cho phép nhóm chạy đối chứng ở `Sprint 1` để kiểm tra **sai số toán học bằng `0.0`** giữa thuật toán phân tán (`5-Tuple Monoid + Salting`) và chuẩn đơn luồng (`baseline.py`) trước khi mở rộng quy mô.

### Câu 2: *"Vì sao nhóm đề xuất mô hình `Random Forest (23D)` thay vì chọn các kiến trúc Deep Learning đồ thị hiện đại như `DCRNN` hay `Graph WaveNet`?"*
- **Người trả lời chính:** **Công Minh** (`Slide 07, 09`).
- **Ý chính trả lời:**
  Qua kiểm toán EDA trên dữ liệu thực tế TP.HCM (Slide `05`), có tới **`88.15%` đoạn đường (`74,606` đoạn) không có bất kỳ bản tin GPS nào** và **`66.27%` cặp quan sát liên tiếp bị ngắt quãng từ `60` phút trở lên**. Các mạng `Deep Spatio-Temporal GNN` (`DCRNN`, `Graph WaveNet`) đòi hỏi chuỗi thời gian liên tục đều đặn trên các đỉnh đồ thị nên sẽ bị suy biến nặng hoặc phải nội suy giả tạo phần lớn ma trận. Ngược lại, `Random Forest (23D)` kết hợp các đặc trưng đồ thị tĩnh (`Line-Graph PageRank`, `7D K-Means`) học rất tốt trên dữ liệu bảng đứt quãng, giải thích được độ quan trọng từng nhóm biến, chấm điểm được cho `100%` mạng lưới Cold-Start và chạy nhẹ nhàng trong ngân sách `10.5 / 16 GB RAM` không cần GPU.

### Câu 3: *"Hãy giải thích rõ vai trò và cơ chế của `Bloom Filter`, `HyperLogLog++` và `EWMA` trong khối `Stream Sketches`?"*
- **Người trả lời chính:** **Công Minh** (`Slide 07` & `starter_stream.py`).
- **Ý chính trả lời:**
  Ba cấu trúc này giải quyết 3 bài toán trực tuyến khác nhau với chi phí `O(1)` và bộ nhớ siêu nhỏ:
  1. **`Bloom Filter (32.0 KiB = 262,144 bits)`:** Kiểm tra nhanh gói tin GPS có bị gửi trùng lặp do lỗi mạng di động hay không bằng 4 vị trí băm bit, không cần lưu chuỗi `event_id`.
  2. **`HyperLogLog++ (1.0 KiB = 1,024 thanh ghi)`:** Đếm số lượng **đoạn đường duy nhất (`COUNT DISTINCT segment_id`)** đang hoạt động dựa trên kỷ lục độ dài chuỗi bit `0` liên tiếp ở đầu mã băm `SHA-256` trên `1,024` ngăn nhỏ — sai số lý thuyết chỉ `1.04 / √1024 ≈ 3.25%` nhưng bộ nhớ cố định đúng `1 KiB` dù lưu lượng tăng lên hàng tỷ gói tin.
  3. **`EWMA (α = 0.15)`:** Tính điểm nóng ùn tắc có suy giảm theo thời gian $C_s(t) = 0.85 \cdot C_s(t-1) + \text{is\_severe}_t$. Đường kẹt lặp đi lặp lại sẽ tích lũy điểm cao nổi lên đầu bảng cảnh báo hiện tại (`t`), còn đường chỉ chậm thoáng qua rồi thông ngay sẽ tự nguội dần `15%` mỗi nhịp và được xóa khỏi RAM khi xuống dưới `10^-4`.

### Câu 4: *"Tại sao khi chia `8` giỏ `Key Salting` trên Spark lại bắt buộc phải dùng `5-Tuple Monoid` mà không gọi hàm `avg()` từng giỏ rồi tính trung bình lại?"*
- **Người trả lời chính:** **Minh Quyền** (`Slide 06`).
- **Ý chính trả lời:**
  Vì phép lấy trung bình cộng **không có tính kết hợp** khi số lượng bản tin trong các giỏ khác nhau (`avg(avg(A), avg(B)) ≠ avg(A ∪ B)`). Như ví dụ trên Slide `06`, Giỏ A có 1 xe `10 km/h` và Giỏ B có 3 xe `40 km/h` thì vận tốc trung bình thật là `32.5 km/h`, nhưng lấy trung bình hai giỏ lại ra `25.0 km/h`. Khi đóng gói thành bộ 5 đại lượng cộng được `(count, sum_v, min_v, max_v, severe_count)`, phép gộp từng phần và gộp chung cuối cùng hoàn toàn giao hoán – kết hợp, đảm bảo chia bao nhiêu giỏ song song kết quả cuối vẫn chính xác tuyệt đối.

### Câu 5: *"Thiết kế của nhóm ngăn chặn rò rỉ dữ liệu tương lai (`Data Leakage`) ở những khâu nào trước khi huấn luyện mô hình?"*
- **Người trả lời chính:** **Công Minh** (`Slide 08, 09`).
- **Ý chính trả lời:**
  Nhóm thiết kế **4 chốt chặn chống rò rỉ nghiêm ngặt**:
  1. **Nội suy tốc độ tối đa `Train-Only 3-Tier Vmax`:** Chỉ tính phân vị `P85` và `Median` trên tập `TRAIN`, đóng băng thành `vmax_lookup.json` để áp cố định cho `VAL` và `TEST`.
  2. **Chuẩn hóa cụm & `Causal Priors` nhân quả:** Bộ chuẩn hóa `Z-score`, tâm cụm `7D K-Means` chỉ học trên `TRAIN`; các biến xác suất lịch sử `segment/street_causal_prior` chỉ tích lũy nhân quả trên các thời điểm quá khứ nghiêm ngặt $\tau < t$.
  3. **Chia tập theo thời gian `70/15/15` kèm `Boundary Purge`:** Cắt bỏ các mẫu sát ranh giới có nhãn mục tiêu `t + 30 phút` vắt sang tập kiểm định hoặc kiểm thử.
  4. **Kiểm chứng tự động `Future-Shuffle Falsification`:** Trong `Sprint 1`, nhóm sẽ chạy phép thử xáo trộn ngẫu nhiên toàn bộ dữ liệu ở $\tau > t$ để chứng minh ma trận đặc trưng $X_t$ không thay đổi dù chỉ `10^-12`.

### Câu 6: *"Trong thực nghiệm sắp tới, nếu mô hình ngây thơ `Persistence Baseline` (đoán 30 phút sau giống hệt hiện tại) đạt `Accuracy` tổng thể rất cao do giao thông có quán tính lớn, nhóm sẽ đánh giá mô hình thế nào cho công bằng?"*
- **Người trả lời chính:** **Công Minh** (`Slide 09`).
- **Ý chính trả lời:**
  Nhóm đã lường trước rủi ro này ngay trong phần `Limitations & Risks (Modeling)` ở Slide `09` và đưa vào kế hoạch thực nghiệm ở Slide `13`: bên cạnh bảng `Ablation` toàn cục, nhóm sẽ **bóc tách đánh giá riêng trên nhóm ca giao thông chuyển trạng thái (`LOS(t) ≠ LOS(t+30m)`)** — nơi `Persistence Baseline` chắc chắn đoán sai `100%` (`Accuracy = 0%`). Đồng thời, nhóm sẽ đo năng lực cảnh báo sớm bằng `ROC-AUC`, `PR-AUC`, hiệu chỉnh ngưỡng quyết định $\tau$ trên tập `VAL` (không chạm tập `TEST`), và đánh giá độ phủ trên `74,606` đoạn `Cold-Start` nơi `Baseline` hoàn toàn mù thông tin.

### Câu 7: *"Các kết quả `Line-Graph PageRank` và `7D K-Means` ở Phần 7 liên kết như thế nào với mô hình dự báo ở Phần 6 và luồng trực tuyến ở Phần 8?"*
- **Người trả lời chính:** **Thành Đạt** (`Slide 04, 10, 11`).
- **Ý chính trả lời:**
  Toàn bộ hệ thống gắn kết chặt chẽ qua kho **`Static Feature Store` (`Zone E`)**:
  - Kết quả `Line-Graph PageRank` (`pagerank_score`, `in_degree`, `out_degree`) và nhãn cụm hành vi `7D K-Means` (`cluster_id`) tính ở Phần 7 được xuất thành bảng đặc trưng tĩnh cho đủ `84,633` đoạn đường.
  - Ở Phần 6 (`Phase A`), bảng này cung cấp nhóm biến không gian cho vector `23D` để huấn luyện `Random Forest`.
  - Ở Phần 8 (`Phase B`), `Online Stream Scorer` đọc trực tiếp bảng tĩnh này ghép với vận tốc tức thời từ `Kafka` để dự báo `+30m` dưới mili-giây mà không phải chạy lại thuật toán đồ thị nặng trên luồng.

### Câu 8: *"Tại sao phải thiết kế cả `3-Phase MLOps` lẫn `Two-Tier Reconciliation` có khóa `version_ts` mà không cập nhật thẳng mô hình và dữ liệu ngay trên luồng?"*
- **Người trả lời chính:** **Thành Đạt** (`Slide 04, 11`).
- **Ý chính trả lời:**
  1. Nếu vừa chấm điểm trực tuyến vừa huấn luyện cập nhật trọng số mô hình trên luồng sẽ gây xung đột khóa ghi (`Write Conflict`), trôi dạt không kiểm soát và không thể tái lập kết quả kiểm toán — do đó `3-Phase MLOps` tách bạch hoàn toàn `Phase A` (fit & khóa `SHA-256`) với `Phase B/C` (chỉ đọc).
  2. Trong mạng di động đô thị, xe đi qua hầm hoặc vùng rớt sóng thường gửi gói tin GPS trễ quá `Watermark 30 phút`. Nhờ ghi kép `Dual-Sink` và đối soát `Tier 2` cuối ngày kèm khóa `version_ts` trên `MongoDB`, hệ thống vừa phát được cảnh báo nhanh `v1` tức thời, vừa bù đủ dữ liệu trễ thành bản chuẩn `v2` mà không sợ gói tin `v1` đến muộn ghi đè ngược làm hỏng bản chuẩn.

### Câu 9: *"Cơ sở kỹ thuật nào giúp nhóm đặt mục tiêu truy vấn `MongoDB Serving Store` đạt `SLA < 10 ms` mà không bị nghẽn khi sắp xếp Top điểm nóng?"*
- **Người trả lời chính:** **Thành Đạt** (`Slide 11`).
- **Ý chính trả lời:**
  Nhóm thiết kế chỉ mục phức hợp **`Compound Index {street_level: 1, severe_prob: -1, segment_id: 1}`** tuân thủ nghiêm ngặt quy tắc **`ESR` (`Equality – Sort – Range`)**. Khi Dashboard truy vấn các trục đường chính có rủi ro kẹt cứng cao và sắp xếp giảm dần theo `severe_prob`, `MongoDB` đọc thẳng các nút lá đã được sắp xếp sẵn trên cây `B-Tree` (`IXSCAN`) thay vì phải quét toàn bảng (`COLLSCAN`) hay sắp xếp lại trong RAM (`SORT stage`). Trong `Sprint 2`, nhóm sẽ trích xuất trực tiếp `explain("executionStats")` để nghiệm thu chỉ số này.

### Câu 10: *"Sau buổi báo cáo thiết kế hôm nay, đâu là đường găng (`Critical Path`) quan trọng nhất trong kế hoạch `Next Actions` của nhóm?"*
- **Người trả lời chính:** **Minh Quyền** (`Slide 13`).
- **Ý chính trả lời:**
  Đường găng đi theo đúng thứ tự phụ thuộc dữ liệu qua **2 Sprint**:
  - **Bước mở đầu (`Sprint 1`):** Chạy chuẩn hóa `Quality Funnel`, `3-Tier Vmax` và `Spark Batch Graph / K-Means` trước để sinh kho `Curated Gold Parquet` cùng `Static Feature Store (84,633 đoạn)` và khóa `Frozen Registry` sau khi hoàn tất bảng `Ablation 7 mô hình`.
  - **Bước tích hợp (`Sprint 2`):** Khi đã có `Frozen Registry` đóng băng chữ ký `SHA-256`, nhóm khởi chạy luồng `Kafka → Speed Layer → MongoDB` để đo kiểm runtime (`Bloom FPR`, `HLL++ error`, `WAL Crash-Recovery`, `Tier-2 Recon`, `IXSCAN p50/p95`) và kết nối lên giao diện điều hành `Streamlit` `5` Tab để chuẩn bị cho buổi bảo vệ cuối kỳ.
