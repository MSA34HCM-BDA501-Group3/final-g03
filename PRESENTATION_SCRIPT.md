# Kịch Bản Thuyết Trình Chuẩn 12 Phần (Hội Nghị, Báo Cáo Điều Hành & Bảo Vệ Đồ Án Học Phần BDA501 — Big Data Analytics)

- **Tên đề tài:** Hệ thống Giám sát Hiện trạng & Dự báo Ùn tắc Giao thông Đô thị TP. Hồ Chí Minh (*HCMC Smart Traffic Big Data Analytics & Congestion Prediction Platform*)
- **Đơn vị & Học phần:** `FSB · FPT University | MSE AI | BDA501 – Big Data Analytics` (`Fall 2026`)
- **Giảng viên hướng dẫn (Instructor):** **Đặng Trần Khánh**
- **Bộ slide trình chiếu đi kèm:** `HCMC_Smart_Traffic_Final_Presentation.pptx` & `HCMC_Smart_Traffic_Final_Slides.pdf` (`16 Slides` theo cấu trúc **12 Phần chuẩn Học thuật & Hội nghị + 2 Slide Trực quan hóa Phương pháp**, nêu rõ **`Limitations & Risks` ngay tại từng Solution Component** và tổng hợp **`System-Wide Limitations & Risks + Plan`** ở cuối bài)
- **Báo cáo tổng kết tra cứu đi kèm:** `HCMC_Smart_Traffic_Final_Report.pdf` & `product/00-proposal/project-proposal.md` (`7 Chương`)
- **Nhóm thực hiện & Phân công Trình bày theo 16 Slides (3 Thành viên — Mỗi Slide đúng 1 Người nói):**
  1. **Nguyễn Tạ Minh Quyền (`25MS23280`)** — Phụ trách **Slide 01, Slide 02 (`1. Problem Statement`), Slide 04 (`3. System Architecture`), Slide 05 (`4. Data, EDA & Data Problem`), Slide 06 (`Giải thích Phương pháp 1/2: Nội suy Vmax & Monoid`) & Slide 08 (`5. Data Pipeline & Data Processing`)**
  2. **Nguyễn Thành Đạt (`25MS23306`)** — Phụ trách **Slide 03 (`2. Literature Review`), Slide 09 (`6. Modeling`), Slide 10 (`7. Batch Processing`), Slide 12 (`9A. Experiments: Ablation`) & Slide 13 (`9B. Experiments: Persistence Defense`)**
  3. **Phạm Công Minh (`25MS23310`)** — Phụ trách **Slide 07 (`Giải thích Phương pháp 2/2: Line-Graph PageRank, 7D K-Means & O(1) Sketches`), Slide 11 (`8. Stream Processing`), Slide 14 (`10. Demonstration`), Slide 15 (`11. Limitations & Risks — Tổng hợp`), Slide 16 (`12. Plan`) & Trình diễn `Live Demo` trực tiếp trên `:8501`**

---

## Phần 1. Bảng Ánh Xạ Cấu Trúc 12 Phần Sang 16 Slides & Phân Bổ Thời Lượng (`18 Phút + 3 Phút Live Demo`)

| Thứ tự Slide | Mục trong Cấu trúc 12 Phần | Nội dung Cốt lõi (Chỉ nêu `Limitations & Risks` ở các Phần Giải pháp) | Người Trình bày | Khung Thời gian | Mục Báo cáo |
|---|---|---|---|---|---|
| **Slide 01** | **Mở đầu (Cover)** | Trang bìa tinh gọn 4 cụm: Đơn vị & Học phần, Tên dự án (`Hero Title`), Nhóm thực hiện & Giảng viên hướng dẫn | **Minh Quyền** | `00:00 – 00:35` | Trang bìa & `1.1` |
| **Slide 02** | **1. Problem Statement** | Bối cảnh ùn tắc lan truyền TP.HCM (`WHY`), 3 hạn chế quản lý hiện hữu & bài toán + giá trị thụ hưởng (`WHAT`) | **Minh Quyền** | `00:35 – 01:45` | Chương 1 (`1.1–1.4`) |
| **Slide 03** | **2. Literature Review** | Khảo sát 4 hướng tiếp cận quản lý – dự báo giao thông đô thị, khoảng trống thực tiễn & lý do cần nền tảng tích hợp | **Thành Đạt** | `01:45 – 02:55` | Chương 1 (`1.5`) |
| **Slide 04** | **3. System Architecture** | **[Solution]** Sơ đồ AWS-Style (`Zones A–F`, 4 luồng pipeline đồng màu), `3-Phase MLOps` + **Architecture Limitations & Risks** | **Minh Quyền** | `02:55 – 04:05` | Chương 3 (`3.1, 3.2, 3.5`) |
| **Slide 05** | **4. Data, EDA & Data Problem** | Kiểm toán 5 bảng gốc (`38.50 MB`), định vị `Proof-of-Scale` & 10 vấn đề dữ liệu thực nghiệm (`EC-1..EC-10`) | **Minh Quyền** | `04:05 – 05:15` | Chương 2 (`2.1, 2.2`) |
| **Slide 06** | **Giải thích Phương pháp (1/2)** | **[Visual Explainer]** Trực quan hóa `Train-Only 3-Tier Vmax Imputation` & `5-Tuple Monoid + Key Salting` bằng ví dụ số | **Minh Quyền** | `05:15 – 06:25` | Chương 2 (`2.4`) & Chương 3 (`3.3`) |
| **Slide 07** | **Giải thích Phương pháp (2/2)** | **[Visual Explainer]** Trực quan hóa `Line-Graph PageRank`, `7D K-Means Cold-Start` & `O(1) Stream Sketches (33.0 KiB)` | **Công Minh** | `06:25 – 07:40` | Chương 4 (`4.1, 4.3`) & Chương 6 (`6.2`) |
| **Slide 08** | **5. Data Pipeline & Processing** | **[Solution]** Sơ đồ `End-to-End Walkthrough`, `Ordered Quality Funnel`, `3-Tier Vmax`, `LOS A–F` + **Pipeline Limitations & Risks** | **Minh Quyền** | `07:40 – 08:50` | Chương 2 (`2.3–2.5`) |
| **Slide 09** | **6. Modeling** | **[Solution]** Ghép cặp `Exact t+30m (7,898 cặp)`, `Boundary Purge`, `23D Features`, `Random Forest` + **Modeling Limitations & Risks** | **Thành Đạt** | `08:50 – 10:00` | Chương 5 (`5.1–5.3, 5.5`) |
| **Slide 10** | **7. Batch Processing** | **[Solution]** `5-Tuple Monoid`, `2-Stage Salting`, `BroadcastHashJoin`, `PageRank`, `SON`, `7D K-Means` + **Batch Limitations & Risks** | **Thành Đạt** | `10:00 – 11:15` | Chương 3 (`3.3–3.5`) & Chương 4 (`4.1–4.4`) |
| **Slide 11** | **8. Stream Processing** | **[Solution]** `Kafka 4.1 KRaft`, `O(1) Sketches (33.0 KiB)`, `EWMA vs RF`, `Two-Tier Recon`, `MongoDB IXSCAN` + **Stream Limitations & Risks** | **Công Minh** | `11:15 – 12:25` | Chương 6 (`6.1–6.5`) |
| **Slide 12** | **9A. Experiments (Ablation)** | Bảng thực nghiệm `7-Model Ablation`, `Confusion Matrix` 6 lớp & kiểm chứng hiệu năng hạ tầng đầu–cuối | **Thành Đạt** | `12:25 – 13:30` | Chương 5 (`5.3`) |
| **Slide 13** | **9B. Experiments (Defense)** | Đối chứng 1-đối-1 `Persistence Baseline (94.84%)` vs `Random Forest (23D)` trên 4 chế độ thực tế | **Thành Đạt** | `13:30 – 14:45` | Chương 5 (`5.4`) |
| **Slide 14** | **10. Demonstration** | Cận cảnh giao diện `:8501` (`Case Study 1: Tab 2 E → F`, `Case Study 2: Tab 4 Cold-Start`) & tổng quan 5 tab nghiệp vụ | **Công Minh** | `14:45 – 15:50` | Chương 6 (`6.4`) |
| **Slide 15** | **11. Limitations & Risks** | **Bảng Tổng hợp (Roll-Up) Limitations của 5 Thành phần Giải pháp (Mục 3, 5, 6, 7, 8) & 4 Rủi ro Chung Toàn Hệ thống** | **Công Minh** | `15:50 – 16:55` | Chương 7 (`7.5`) |
| **Slide 16** | **12. Plan** | **Kế hoạch Thực thi Đã Hoàn thành (100% của 3 thành viên) & Lộ trình Mở rộng 3 Giai đoạn (`Phase 1–3 Plan`)** | **Công Minh** | `16:55 – 18:00` | Chương 7 (`7.1–7.5`) |
| **Live Demo** | **Trình diễn Trực tiếp (`:8501`)** | Thao tác trực tiếp 5 tab nghiệp vụ trên trình duyệt (`http://localhost:8501`) kết nối `MongoDB 8.0` | **Công Minh** | `18:00 – 21:00` | Toàn bộ 5 tab `app.py` |

---

## Phần 2. Kịch Bản Lời Thoại Chi Tiết Từng Slide (`Slide 01` – `Slide 16`)

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
  > Ở thẻ phía trên bên phải, cơ chế **`Zero-Conflict 3-Phase MLOps`** ngăn chặn hoàn toàn xung đột ghi vào `Zone E (Frozen Registry)`: chỉ học đúng 1 lần ở `Phase A` trên `5,494` mẫu `TRAIN` rồi khóa `SHA-256`; `Phase B` trực tuyến chỉ đọc để phát cảnh báo `v1`; và `Phase C` cuối ngày bù `501` cửa sổ trễ, nâng cấp lên bản ghi chuẩn `v2` tại `Zone F` (`Medallion Lakehouse`, `MongoDB 8.0` và `Streamlit UI :8501`).
  >
  > **Về Limitations & Risks của thành phần Kiến trúc (thẻ góc dưới phải):**
  > - *Thứ nhất (`Single-Host Contention`):* Việc chạy chung cả 4 container (`Kafka 1.5 GB`, `Spark 6.0 GB`, `MongoDB 2.0 GB`, `UI 1.0 GB` = `10.5 / 16.0 GB RAM`) trên cùng một máy chủ giúp triển khai gọn nhẹ, nhưng tiềm ẩn rủi ro tranh chấp băng thông đĩa (`Disk I/O`) khi `Spark` thực hiện `Shuffle` lớn cùng lúc `MongoDB` ghi lô.
  > - *Thứ hai (`Eventual Consistency Gap`):* Cảnh báo trực tuyến `v1` phản hồi dưới 1 giây, nhưng việc cập nhật biến trễ lịch sử từ các bản tin đến muộn quá 30 phút phải chờ `Phase C` cuối ngày."
- **Tra cứu nhanh Báo cáo:** Chương 3, Mục `3.1`, `3.2` & `3.5` (Hình `3.1`, `3.2`).

---

#### Slide 05: `4. Data, EDA & Data Problem` — Hồ Sơ 5 Bảng Gốc, `Proof-of-Scale` & 10 Vấn Đề Dữ Liệu (`04:20 – 05:30`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Trình bày hồ sơ kiểm toán 5 bảng gốc (`38.50 MB`), vai trò mẫu chuẩn `Proof-of-Scale` cho quy mô toàn TP.HCM (`8–17 tỷ tin/ngày`), phân bố thực nghiệm và 10 vấn đề dữ liệu (`EC-1..EC-10`).
- **Chỉ dẫn trực quan:** Chỉ biểu đồ tỷ lệ dị thường góc trên trái, thẻ `Hồ Sơ 5 Bảng Gốc & Proof-of-Scale` góc dưới trái, và bảng 10 `Data Problems (EC-1..EC-10)` bên phải.
- **Lời thoại thuyết trình:**
  > "Bước sang **Phần 4 — Data, EDA & Data Problem** trên Slide 05: qua kiểm toán 5 bảng gốc trên Kaggle (`38.50 MB`: gồm `nodes` `577,967` điểm, `streets` `5,553` đường, `segments` `84,633` đoạn, `segment_status` `90,938` bản tin GPS — ví dụ lúc `07:08` sáng trên đường Lý Thường Kiệt xe báo vận tốc chỉ `4 km/h` — và bảng đối chứng `train` `33,441` cửa sổ), bộ dữ liệu này đóng vai trò **mẫu chuẩn `Proof-of-Scale`** giúp đối chứng sai số toán học bằng `0.0` trước khi nhân rộng ra quy mô toàn TP.HCM (**`8–17` tỷ bản tin/ngày, `1.5–3.0 TB/ngày`**). Về phân bố, chỉ có **`10,027 / 84,633` đoạn (`11.85%`) có GPS** và **`94.84%` mẫu `TEST` giữ nguyên trạng thái sau 30 phút**.
  >
  > Đi sâu vào bảng bên phải, nhóm định lượng được **10 vấn đề dữ liệu thực tế (`EC-1` đến `EC-10`)**: nổi bật là **85.21% cửa sổ thiếu tốc độ tối đa (`EC-1`)**, **154 đoạn chiều dài $\le 0\text{ m}$ và 427 lối đi bộ (`EC-2`)**, **695 tên đường lệch chuẩn dấu Unicode (`EC-3`)**, **4,320 cửa sổ đổi ngày khi chuyển sang giờ Việt Nam `UTC+7` (`EC-4`)**, **66.27% cặp quan sát ngắt quãng $\ge 60$ phút (`EC-5`)**, **`88.15%` đoạn `Cold-Start` (`EC-6`)**, và **lệch tải giờ cao điểm gấp 687.43 lần (`EC-8`)**."
- **Tra cứu nhanh Báo cáo:** Chương 2, Mục `2.1`, `2.2` (Hình `2.1`).

---

#### Slide 06: Giải Thích Phương Pháp (1/2) — Nội Suy 3 Tầng `Vmax` & Bộ Gộp `5-Tuple Monoid` (`05:15 – 06:25`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Trực quan hóa bằng hình vẽ và con số cụ thể để người nghe hiểu rõ bản chất của 2 phương pháp nền tảng trước khi đi vào chi tiết Pipeline và Spark: **(1) Nội suy tốc độ tối đa 3 tầng (`Train-Only 3-Tier Vmax Imputation`)** và **(2) Bộ gộp 5 đại lượng cộng được (`5-Tuple Monoid`) kết hợp chia giỏ khóa (`Key Salting`)**.
- **Chỉ dẫn trực quan:** Chỉ sơ đồ phễu bậc thang 4 tầng bên trái cho `3-Tier Vmax`, sau đó chỉ bảng đối chiếu hai hộp Đỏ (`SAI`) và Xanh (`ĐÚNG`) bên phải cho `5-Tuple Monoid + Salting`.
- **Lời thoại thuyết trình:**
  > "Trước khi đi vào chi tiết các thành phần xử lý, trên **Slide 06 và Slide 07**, nhóm xin trực quan hóa ngắn gọn bản chất các phương pháp cốt lõi để quý thầy/cô và người xem dễ hình dung chúng hoạt động thế nào:
  > - **Bên trái — Nội suy tốc độ tối đa 3 tầng (`Train-Only 3-Tier Vmax`):** Khi `85.21%` cửa sổ thiếu biển tốc độ tối đa, nếu lấy trung bình toàn bộ dữ liệu sẽ làm rò rỉ thông tin tương lai. Hệ thống chỉ học trên tập quá khứ `TRAIN` rồi tra cứu theo bậc thang từ riêng đến chung: đường nào có sẵn biển hợp lệ thì giữ nguyên (`4,946` cửa sổ, ví dụ Điện Biên Phủ `60 km/h`) $\rightarrow$ nếu thiếu thì lấy **phân vị 85% vận tốc lúc đường thoáng của chính đoạn đó** (`Tầng 1`, `8,164` cửa sổ) $\rightarrow$ nếu đoạn mới chưa đủ lịch sử thì mượn **trung vị các đường cùng cấp và loại** (`Tầng 2`, `20,124` cửa sổ) $\rightarrow$ cuối cùng mới dùng quy chuẩn đô thị `80/60/50/40 km/h` (`Tầng 3`, `207` cửa sổ).
  > - **Bên phải — Vì sao phải dùng `5-Tuple Monoid` và `Key Salting`?** Giả sử trên cùng đoạn đường có 4 xe nằm ở 2 máy: Máy A có 1 xe chạy `10 km/h`, Máy B có 3 xe chạy `40 km/h` — trung bình thực tế phải là **`32.5 km/h`**. Nếu gộp ngây thơ trung bình của trung bình `(10 + 40) / 2` sẽ ra **`25.0 km/h` — bị sai lệch ngay `7.5 km/h`!** Thay vào đó, mỗi máy giữ **bộ 5 con số cộng được `(số xe, tổng vận tốc, min, max, số xe kẹt)`**: gộp lại ra tổng vận tốc `130` chia `4` xe bằng đúng **`32.5 km/h` (sai số `0.0`)**, nhờ đó hệ thống thoải mái gắn đuôi ngẫu nhiên chia giờ cao điểm ra `8` giỏ song song (`Key Salting`) để chống nghẽn máy mà kết quả cuối vẫn chính xác tuyệt đối."
- **Tra cứu nhanh Báo cáo:** Chương 2 (`2.4`) & Chương 3 (`3.3`).

---

#### Slide 07: Giải Thích Phương Pháp (2/2) — `Line-Graph PageRank`, `7D K-Means` & Bộ Đếm `O(1)` (`06:25 – 07:40`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Trực quan hóa bằng sơ đồ 3 cột để giải thích dễ hiểu cách hệ thống biến bản đồ thành nút cổ chai (`Line-Graph PageRank`), cách suy diễn cho `88.15%` đường không có xe gửi GPS (`7D K-Means`), và cách lọc trùng hàng tỷ bản tin mà không tràn RAM (`O(1) Sketches`).
- **Chỉ dẫn trực quan:** Chỉ lần lượt từ cột 1 (`Line-Graph PageRank`) sang cột 2 (`7D K-Means Cold-Start`) và cột 3 (`Bloom Filter, HLL++ & EWMA`).
- **Lời thoại thuyết trình:**
  > "Em là **Phạm Công Minh**, xin trình bày trọn vẹn **Slide 07** về ba phương pháp cốt lõi tiếp theo của hệ thống:
  > - **Ở cột đầu tiên — `Line-Graph PageRank`:** Bản đồ gốc có tới `90.93%` điểm chỉ dùng để vẽ độ cong con đường. Hệ thống cắt bỏ các chấm đỏ trung gian này, sau đó **đảo mỗi đoạn đường thành một nút (`Line-Graph`)**: khi nhiều nhánh đường `A, B, C` cùng đổ dồn xe vào một lối ra hẹp `D`, thuật toán **`PageRank`** sẽ chấm điểm nút cổ chai `D` cao vọt (như cầu Phú Định, Lê Lai) ngay cả khi chưa lắp cảm biến!
  > - **Ở cột thứ hai — `7D K-Means Cold-Start`:** Với **`74,606` đoạn đường vùng mù (`0` GPS)**, thuật toán **`7D K-Means`** đo 7 đặc trưng hình thái bản đồ để gom toàn bộ thành phố vào **`6` nhóm hành vi đường phố**: nhờ đó các đoạn vùng mù (ô vuông đen) mượn được đặc trưng của các đường có GPS tương đồng (chấm tròn) nằm chung nhóm để phủ kín `100%` đô thị.
  > - **Ở cột thứ ba — Bộ đếm luồng `O(1) Sketches`:** Khi mở rộng lên hàng tỷ bản tin GPS mỗi ngày, nếu lưu danh sách mã bản tin vào RAM để kiểm tra trùng lặp thì bộ nhớ sẽ đầy tràn ngay lập tức. Thay vào đó, **`Bloom Filter`** chỉ dùng một dãy ô bit `0/1` vỏn vẹn **`32.0 KiB`** như người gác cổng — hễ cả 4 vị trí băm đều đã bật `1` là chặn ngay bản tin trùng; **`HyperLogLog++` (`1.0 KiB`)** nhìn độ dài chuỗi số `0` đầu mã băm để đếm nhanh hàng chục nghìn đoạn đường hoạt động; còn **`EWMA Hotspot`** cộng dồn có phai mờ để bắt các điểm **ĐANG kẹt lì nhiều giờ**, bổ trợ trực tiếp cho `Random Forest` dự báo điểm **SẮP kẹt sau 30 phút**."
- **Tra cứu nhanh Báo cáo:** Chương 4 (`4.1, 4.3`) & Chương 6 (`6.2`).

---

#### Slide 08: `5. Data Pipeline & Data Processing` — `Ordered Quality Funnel` & `Train-Only Imputation` (`07:40 – 08:50`)
- **Người trình bày:** **Nguyễn Tạ Minh Quyền (`25MS23280`)**
- **Mục tiêu truyền đạt:** Trình bày luồng biến đổi dữ liệu cụ thể đầu–cuối (`End-to-End Walkthrough`), `Ordered Quality Funnel`, `Train-Only 3-Tier Vmax Imputation` và chuẩn hóa thang đo `LOS A–F`, đồng thời chỉ rõ **Limitations & Risks của Data Pipeline**.
- **Chỉ dẫn trực quan:** Chỉ sơ đồ phóng lớn `END-TO-END WALKTHROUGH` phía trên, thẻ `Ordered Quality Funnel & 3-Tier Vmax` bên trái dưới, và thẻ `Limitations & Risks (Data Pipeline)` bên phải dưới.
- **Lời thoại thuyết trình:**
  > "Khi đã nắm rõ nguyên lý, sang **Phần 5 — Data Pipeline & Data Processing** trên Slide 08: sơ đồ phía trên minh họa trọn vẹn hành trình một bản tin GPS thực tế lúc `07:08` sáng trên đường Lý Thường Kiệt (`4 km/h`) đi xuyên qua 4 chặng xử lý đến cảnh báo dưới `1 ms`. Ở thẻ bên trái dưới, **Ordered Quality Funnel 5 bước** dùng `ANSI try_cast` tách bản ghi lỗi sang `Quarantine` (`0` làm sập job, `0` rớt ngầm); lọc `427` lối đi bộ; chuẩn hóa `Unicode NFC` (`695 → 649` tên đường) và giờ `UTC+7` (`4,320` cửa sổ). Áp dụng bảng tra `Train-Only 3-Tier Vmax`, toàn bộ **33,441 cửa sổ 30 phút** được gán nhãn 6 mức **`LOS A–F`**: `A–B` ($\ge 65\%$: thông thoáng), `C–D` ($35\%–65\%$: đông đúc) và `E–F` ($< 35\%$: kẹt cứng).
  >
  > **Về Limitations & Risks của Data Pipeline (thẻ bên phải dưới):**
  > - *Thứ nhất (`Static Vmax Approximation`):* Có `60.79%` cửa sổ (`20,331` cửa sổ) dùng `Vmax` suy diễn tĩnh theo cấp đường (`Tier 2 & 3`), chưa tự động giảm khi đoạn đường bị rào chắn thi công hay ngập nước cục bộ.
  > - *Thứ hai (`Quarantine Spike & 30m Slot`):* Nếu thiết bị GPS đổi định dạng hàng loạt sẽ làm tăng tỷ lệ cách ly vào `Quarantine`; đồng thời khung gom 30 phút làm mượt các dao động ngắn dưới 5 phút tại ngã tư."
- **Tra cứu nhanh Báo cáo:** Chương 2, Mục `2.3` – `2.5`.

---

#### Slide 09: `6. Modeling` — Thiết Kế Nhãn `Exact t+30m`, Vector 23 Biến & Mô Hình `Random Forest` (`08:50 – 10:00`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Trình bày thiết kế bài toán học máy dự báo `+30m` chống rò rỉ tương lai, cấu trúc 23 biến đầu vào, cơ chế chấm điểm vùng mù `Cold-Start`, và nêu rõ **Limitations & Risks của thành phần Modeling**.
- **Chỉ dẫn trực quan:** Chỉ 2 thẻ bên trái (`Exact t+30m Pairing` và `23D Features`), thẻ góc trên phải (`Kiến Trúc Random Forest & Cold-Start`), và thẻ góc dưới phải (`Limitations & Risks (Modeling)`).
- **Lời thoại thuyết trình:**
  > "Em xin trình bày **Phần 6 — Modeling** trên Slide 09. Để mô hình học đúng bài toán dự báo trước 30 phút và tuyệt đối không nhìn trộm tương lai, nhóm áp dụng 3 nguyên tắc:
  > 1. **Ghép cặp `Exact t+30m Join`:** Chỉ ghép nhãn khi cùng đoạn đường có quan sát ở đúng `t + 30 phút`, thu được **7,898 cặp hợp lệ** (loại bỏ `15,516` cặp ngắt quãng $\ge 60$ phút).
  > 2. **Chia tập `Chronological 70/15/15` kèm `Boundary Purge`:** Cắt bỏ 55 mẫu sát biên để giữ lại **5,494 mẫu `TRAIN`**, **1,167 mẫu `VAL`** và **1,182 mẫu `TEST`** hoàn toàn độc lập; bài kiểm thử `Future-Shuffle Falsification` xác nhận sai lệch biến đầu vào bằng **`0.0`**.
  > 3. **Tổ hợp `23D Spatio-Temporal Features` và `Cold-Start Scoring`:** Kết hợp 7 biến trạng thái/độ trễ cục bộ (`79.82%` độ quan trọng), 7 biến chu kỳ giờ Việt Nam/hình học (`12.18%`), và 9 biến đồ thị `PageRank`, cụm `K-Means` cùng `Causal Priors` (`8.00%`). Mô hình `#5 Random Forest (23D)` được khóa cố định tại `outputs/model.joblib` và hỗ trợ chấm điểm cho cả **74,606 đoạn `Cold-Start`** (phủ `100%` `84,633` đoạn đường).
  >
  > **Về Limitations & Risks của Modeling (thẻ góc dưới phải):**
  > - *Thứ nhất (`Default Threshold Conservatism`):* Do `94.84%` mẫu huấn luyện đứng yên, ngưỡng `argmax` mặc định bị bảo thủ trên các pha đổi trạng thái (chỉ bắt được `2/61` ca ở mặc định, cần hiệu chỉnh ngưỡng $\tau = 0.30$ trên tập `VAL` để tăng lên `9/61` ca).
  > - *Thứ hai (`Exogenous Blindness`):* Vector 23 biến chưa có dữ liệu ngoại sinh thời gian thực như lượng mưa hay tai nạn giao thông, nên chỉ phát hiện bất thường sau khi vận tốc xe bắt đầu giảm."
- **Tra cứu nhanh Báo cáo:** Chương 5, Mục `5.1` – `5.3` & `5.5`.

---

#### Slide 10: `7. Batch Processing` — Luồng Xử Lý Lô Phân Tán, Khai Phá Đồ Thị & Phân Cụm (`10:00 – 11:15`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Trình bày rõ luồng dữ liệu & các component xử lý lô từ `Zones A → B → D → F / E` (`Broadcast Join & 3-Tier Vmax`, `2-Stage Key Salting & 5-Tuple Monoid`, `Shape-Point Pruner & Line-Graph`, `SON / FPGrowth & 7D K-Means`), đối chiếu kết quả định lượng và nêu rõ **Limitations & Risks của Batch Processing**.
- **Chỉ dẫn trực quan:** Chỉ theo các mũi tên trên **Sơ đồ Luồng & Thành phần Batch Processing** phía trên (nhánh trên: `ETL → Curated Gold Parquet`; nhánh dưới: `Graph & K-Means → Static Feature Store`), sau đó chỉ thẻ `Kết Quả Thực Thi Batch ETL, Đồ Thị & Phân Cụm` bên trái dưới và thẻ `Limitations & Risks (Batch Processing)` bên phải dưới.
- **Lời thoại thuyết trình:**
  > "Tiếp theo ở **Phần 7 — Batch Processing** trên Slide 10, em xin trình bày trọn vẹn luồng xử lý lô đi từ `GPS Logs & OSM Topology (Zone A)` qua `Quality Funnel (Zone B)` vào `Apache Spark Batch Layer (Zone D)` theo hai nhánh song song:
  > - **Trên nhánh Spark Batch ETL phía trên:** **`Broadcast Join & 3-Tier Vmax`** giảm trộn mạng từ `10` xuống `2 Shuffle Stages`, tiếp nối sang **`2-Stage Key Salting (8 giỏ) & 5-Tuple Monoid`** giúp giảm lệch tải giờ cao điểm từ **`687.43x` xuống `1.023x`** và khớp `100%` với `baseline.py` (**sai số `= 0.0`**), trước khi ghi xuống **`Curated Gold Parquet (Zone F)`** (nơi cơ chế cắt tỉa phân vùng chỉ đọc đúng **`7,294 / 33,441` dòng** cho tháng `2020-08`).
  > - **Trên nhánh Khai phá Đồ thị & Phân cụm phía dưới:** **`Shape-Point Pruner & Line-Graph`** cắt tỉa `90.93%` điểm uốn cong, biến mỗi đoạn đường một chiều thành 1 đỉnh để tính **`Line-Graph PageRank`** bảo toàn khối lượng `84,633.0` (`#1 Phú Định = 3.7515`, `#2 Lê Lai = 3.4650`). Kết hợp các cửa sổ `TRAIN` từ nhánh trên, khối **`SON / FPGrowth & 7D K-Means`** khai phá `335` luật đồng kẹt (`Lift = 28.58x`), gom 6 cụm hành vi (`Silhouette = 0.2853`) và khóa vào **`Static Feature Store (Zone E)`** phủ đủ `74,606` đoạn `Cold-Start`.
  >
  > **Về Limitations & Risks của Batch Processing (thẻ góc dưới phải):**
  > 1. *Cỡ mẫu nhỏ ở luật lan truyền trễ `SON / FPGrowth`:* Các luật trễ `+30m` (`Lift = 32.92x`) chỉ xuất hiện trong **5 đến 8 cửa sổ (`2.5–4.0` giờ quan sát)** do GPS thưa — nhóm chỉ gắn nhãn giả thuyết thăm dò (`Exploratory Hypothesis`).
  > 2. *Phân cụm `Cold-Start` tĩnh:* `74,606` đoạn thiếu GPS được gán cụm theo hình thái đồ thị và tốc độ cấp đường, chưa phản ánh được các điểm họp chợ tự phát trong hẻm nhỏ."
- **Tra cứu nhanh Báo cáo:** Chương 3 (`3.3–3.5`, Hình `3.3`) & Chương 4 (`4.1–4.4`, Hình `4.1`).

---

#### Slide 11: `8. Stream Processing` — Luồng Cảnh Báo Trực Tuyến, `O(1) Sketches` & Đối Soát Trễ (`11:15 – 12:25`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Trình bày rõ luồng sự kiện và các component xử lý thời gian thực qua `Zones B → C / E / D → F` (`Apache Kafka Broker`, `Watermark & Dual-Sink`, `O(1) Stream Sketches`, `Online Stream Scorer`, `WAL & Bronze Landing`, `Two-Tier Reconciliation`, `MongoDB Serving Store`, `Streamlit Dashboard`), kết quả định lượng và **Limitations & Risks của Stream Processing**.
- **Chỉ dẫn trực quan:** Chỉ theo các mũi tên màu tím (`Speed Pipeline`), xanh ngọc nét đứt (`MLOps Read-Only`), xanh dương (`Tier-2 Recon`) và xanh lá (`Serving`) trên **Sơ đồ Luồng & Thành phần Stream Processing** phía trên, rồi đối chiếu thẻ kết quả góc dưới trái và thẻ `Limitations & Risks (Stream Processing)` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Em là **Phạm Công Minh**, xin trình bày **Phần 8 — Stream Processing** trên Slide 11. Nhìn trên sơ đồ luồng phía trên: sự kiện từ **`Apache Kafka Broker (Zone B)`** đi vào **`Speed Layer (Zone C)`** qua khối **`Watermark & Dual-Sink`** (vừa ghi `WAL & Bronze Landing` chống mất tin, vừa phân tách tin đúng hạn `≤ 30m` và tin trễ `> 30m`). Trên đường nhanh màu tím, khối **`O(1) Stream Sketches`** chỉ tốn **`33.0 KiB` RAM cố định** — gồm `Bloom Filter (32.0 KiB)` chặn `334/334` tin trùng (`FPR = 0.616%`), `HyperLogLog++ (1.0 KiB)` đếm đoạn hoạt động (sai số `3.98%`), và `EWMA Hotspot` ($\alpha = 0.15$, `#1 Lý Thường Kiệt = 6.67`) theo dõi điểm ĐANG kẹt lì, trước khi **`Online Stream Scorer`** đọc chỉ-đọc mô hình đóng băng từ `Zone E` để phát cảnh báo tạm thời `v1` sang **`MongoDB Serving Store`**.
  >
  > Với các gói tin đến trễ `> 30m` nằm ở đáy sơ đồ, tiến trình **`Two-Tier Reconciliation (Zone D)`** cuối ngày đối soát `32,940` cửa sổ đúng hạn khớp `100%` (`Tier 1`), tự động bù **`501` cửa sổ trễ**, cập nhật **`3,953` cửa sổ lag**, re-score **`377` cửa sổ** thành bản chuẩn `v2` (`Tier 2`, nâng độ đầy đủ `98.50% → 100%`) rồi `Bulk Upsert` vào **`MongoDB`** (khóa `version_ts` chặn `25/25` tin cũ `v1`, truy vấn `IXSCAN` đạt **`warm_p95 = 0.636 ms`**) cấp cho **`Streamlit Dashboard`**.
  >
  > **Về Limitations & Risks của Stream Processing (thẻ góc dưới phải):**
  > - *Thứ nhất (`Bloom Bit Saturation`):* Mảng cố định `32.0 KiB` (`262,144` bits) nếu chạy liên tục nhiều ngày ở quy mô `1.5–3.0 TB/ngày` mà không xoay vòng (`Ring-Buffer Rotation`) theo từng cửa sổ sẽ bị đầy bit và tăng tỷ lệ báo trùng giả.
  > - *Thứ hai (`Beyond-Watermark Delay`):* Các gói tin trễ quá `Watermark 30m` do rớt mạng di động phải đợi tiến trình `Tier-2 Reconciliation` cuối ngày."
- **Tra cứu nhanh Báo cáo:** Chương 6, Mục `6.1` – `6.5` & Hình `6.1`.

---

#### Slide 12: `9A. Experiments (Ablation & Benchmarks)` — Thực Nghiệm 7 Mô Hình & Kiểm Chứng Hạ Tầng (`12:25 – 13:30`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Trình bày kết quả thực nghiệm đối sánh 7 cấu hình mô hình (`Ablation Study`), ma trận phân loại 6 lớp `LOS A–F` và các chỉ số kiểm chứng hiệu năng hạ tầng đầu–cuối.
- **Chỉ dẫn trực quan:** Chỉ dòng in đậm `#5 Random Forest (23D)` trong bảng 7 mô hình bên trái, thẻ `Confusion Matrix & Giá Trị Biến Đồ Thị` góc trên phải, và thẻ `Kiểm Chứng Hiệu Năng Hạ Tầng Đầu–Cuối` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Sang **Phần 9A — Experiments (Ablation & Benchmarks)** trên Slide 12: bảng bên trái trình bày kết quả thực nghiệm 7 cấu hình trên `1,182` mẫu `TEST` độc lập. Khi bổ sung 9 biến đồ thị `PageRank`, cụm `7D K-Means` và `Causal Priors` (nâng từ 14 biến lên **23 biến**), `Random Forest (#5)` tăng `Macro-F1` từ `0.5455` lên **`0.5543`** (và ở `Binary GBT` nâng `Severe F1` từ `0.7840` lên **`0.9465`**). Mô hình vận hành chính `#5 Random Forest (23D)` đạt **`6-Class Acc = 0.9450`**, **`ROC-AUC = 0.9882`**, **`PR-AUC = 0.9471`**, với đường chéo `Confusion Matrix` phân biệt đủ cả 6 mức `[944, 4, 3, 7, 6, 153]`.
  >
  > Song song đó ở thẻ góc dưới phải, toàn bộ dây chuyền hạ tầng đều đạt chuẩn thực nghiệm: `5-Tuple Monoid` đạt sai số bằng **`0.0`**, `2-Stage Salting` giảm lệch tải còn **`1.023x`**, `Parquet Pruning` chỉ đọc **`7,294 / 33,441` dòng** cho tháng `2020-08`, bộ nhớ luồng giữ cố định ở **`33.0 KiB`**, và truy vấn `MongoDB 8.0 IXSCAN` đạt **`p95 = 0.636 ms`**."
- **Tra cứu nhanh Báo cáo:** Chương 5, Mục `5.3`, Bảng `5.4` & Hình `5.1`.

---

#### Slide 13: `9B. Experiments (Defense)` — Giải Mã Nghịch Lý `Persistence Baseline` Trên 4 Chế Độ (`13:30 – 14:45`)
- **Người trình bày:** **Nguyễn Thành Đạt (`25MS23306`)**
- **Mục tiêu truyền đạt:** Giải mã trực diện vì sao chọn `Random Forest (23D)` (`Acc = 94.50%`) khi `Persistence Baseline` đạt `94.84%` thông qua đối chứng khách quan trên 4 chế độ giao thông thực tế.
- **Chỉ dẫn trực quan:** Chỉ từng dòng trong bảng 4 chế độ kiểm định bên trái đối chiếu biểu đồ góc trên phải, rồi nhấn mạnh vào thẻ `Đúc Kết Đối Chứng Khoa Học (1-on-1 Defense)` góc dưới phải.
- **Lời thoại thuyết trình:**
  > "Ở **Phần 9B — Experiments (Scientific Defense)** trên Slide 13, nhóm giải mã câu hỏi phản biện cốt lõi: **Vì sao trên toàn tập `TEST`, `Persistence Baseline` đạt `94.84%` cao hơn `94.50%` của `Random Forest (23D)`?** Bởi vì `94.84%` thời gian (`1,121 / 1,182` cặp) đường giữ nguyên mức cũ sau 30 phút, nên đoán 'hiện tại sao thì 30 phút nữa vậy' nghiễm nhiên trúng trên phần tĩnh.
  >
  > Nhưng khi bóc tách sang **4 chế độ vận hành thực tế** ở bảng bên trái:
  > 1. **Ở 61 ca đổi trạng thái (`LOS(t) ≠ LOS(t+30m)`):** `Baseline` **đoán sai 100% (`0 / 61`)**, trong khi `Random Forest (23D)` đạt **`Detect ROC-AUC = 0.7664`**, bắt trúng mức mới **`9 / 61` ca (`14.75%`)** tại ngưỡng chọn trên `VAL` ($\tau = 0.30$; mặc định đạt `2 / 61 = 3.28%`), và xếp đúng hướng biến động số 1 ở **`24 / 61` ca (`39.34%`)**.
  > 2. **Ở 8 / 993 ca đường đang thoáng (`A–D`) đột ngột hóa kẹt cứng (`E–F`):** Dù cỡ mẫu cực hiếm (`0.81%`) khiến ngưỡng cứng mặc định đạt `0 / 8`, xét theo xác suất cảnh báo sớm `Random Forest` đạt **`ROC-AUC = 0.8136`** và đẩy xác suất rủi ro trung bình **cao gấp `9.35x`**.
  > 3. **Ở 3,513 cặp tín hiệu GPS ngắt quãng `> 30 phút` và 74,606 đoạn vùng mù `Cold-Start`:** `Random Forest` vượt xa `Baseline` **`+7.13%` về `Severe PR-AUC`** (`0.7082` vs `0.6369`) và nâng độ phủ mạng lưới từ **`0%` lên `100%` (`84,633` đoạn)**."
- **Tra cứu nhanh Báo cáo:** Chương 5, Mục `5.4`, Bảng `5.5` & Hình `5.2`.

---

#### Slide 14: `10. Demonstration` — Cổng Điều Hành `Streamlit 5 Tab (:8501)` & 2 Case Studies Thực Tế (`14:45 – 15:50`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Trình chiếu 2 ảnh chụp cận cảnh từ giao diện `:8501` đang kết nối `MongoDB 8.0` (`Case Study 1` bắt đúng pha chuyển `E → F` trên `Tab 2` và `Case Study 2` tra cứu vùng `Cold-Start` trên `Tab 4`), kèm bức tranh tổng quan 5 tab điều hành.
- **Chỉ dẫn trực quan:** Chỉ ảnh chụp cận cảnh `Case Study 1 (Tab 2)` bên trái, `Case Study 2 (Tab 4)` bên phải, và dải tổng hợp `Toàn Cảnh 5 Tab Nghiệp Vụ Trên Cổng Điều Hành (:8501 ↔ MongoDB 8.0)` ở phía dưới.
- **Lời thoại thuyết trình:**
  > "Bước sang **Phần 10 — Demonstration** trên Slide 14: hai ảnh chụp cận cảnh từ cổng điều hành `Streamlit (:8501)` kết nối trực tiếp `MongoDB 8.0` (`33,441` bản ghi) minh chứng giá trị thực tế của hệ thống:
  > - **Bên trái (`Case Study 1` trên `Tab 2`):** Tại **đoạn `21064` — Công Trường Quốc Tế** lúc 11:30 trưa (đang ở `LOS E`, `12 km/h`), `Baseline` giữ nguyên mức `E` (đoán sai), trong khi **`Random Forest (23D)` dự báo chính xác sau 30 phút đường chuyển sang kẹt cứng mức `F`** (xác suất kẹt cứng `59.6%`, xác suất đổi trạng thái `73.0%`), khớp hoàn toàn với thực tế `LOS(t+30m) = F`!
  > - **Bên phải (`Case Study 2` trên `Tab 4`):** Khi chọn **đoạn `61781` — Phú Định** (`PageRank #1 = 3.7515`, `0` bản tin GPS), hệ thống lập tức đối chiếu các đoạn có cảm biến tương đồng nhất trong cùng cụm `K-Means` (`Châu Văn Liêm`, `Nguyễn Kiệm`).
  > - **Dải bên dưới** tóm tắt trọn vẹn **5 Tab nghiệp vụ** gắn liền với 6 kịch bản `UC-1` đến `UC-6` mà nhóm sẽ thao tác trực tiếp trong phần `Live Demo`."
- **Tra cứu nhanh Báo cáo:** Chương 6, Mục `6.4` & Hình `6.2`, `6.3`, `6.4`.

---

#### Slide 15: `11. Limitations & Risks` — Tổng Hợp 5 Thành Phần Giải Pháp & 4 Rủi Ro Chung (`15:50 – 16:55`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Tổng hợp (`Roll-Up`) toàn bộ các hạn chế của **5 thành phần giải pháp (`Mục 3, 5, 6, 7, 8`)** vào bảng đối chiếu kèm biện pháp kiểm soát, đồng thời chỉ ra **4 Rủi ro & Hạn chế Chung ở cấp độ toàn hệ thống (`System-Wide Limitations & Risks`)**.
- **Chỉ dẫn trực quan:** Chỉ bảng tổng hợp 5 thành phần giải pháp (`Solution Components`) bên trái, sau đó nhấn mạnh vào 4 điểm trong thẻ `4 Rủi Ro & Hạn Chế Chung Toàn Hệ Thống` màu xanh đậm bên phải.
- **Lời thoại thuyết trình:**
  > "Sang **Phần 11 — Limitations & Risks (Tổng hợp)** trên Slide 15: bảng bên trái hệ thống hóa lại toàn bộ các giới hạn kỹ thuật của **5 thành phần giải pháp (`Mục 3, 5, 6, 7, 8`)** mà nhóm đã nêu trực tiếp khi trình bày từng thành phần — gồm tranh chấp tài nguyên trên Docker đơn (`Mục 3: System Architecture`), giả định $v_{\max}$ tĩnh ở Tier 2/3 (`Mục 5: Data Pipeline`), ngưỡng mặc định bảo thủ và thiếu biến ngoại sinh mưa/tai nạn (`Mục 6: Modeling`), cỡ mẫu nhỏ `5–8` cửa sổ của luật trễ `SON` (`Mục 7: Batch Processing`), cùng nguy cơ đầy bit `Bloom Filter` nếu chạy dài ngày không xoay vòng (`Mục 8: Stream Processing`).
  >
  > Nhìn rộng ra cấp độ toàn đô thị ở thẻ bên phải, nhóm đúc kết **4 Hạn chế & Rủi ro Chung Toàn Hệ thống (`System-Wide Risks`)**:
  > 1. **Thiên lệch độ phủ & đại diện phương tiện (`Spatial & Fleet Representation Bias`):** Nguồn GPS cộng đồng chỉ phủ `11.85%` đoạn đường (chủ yếu từ ô tô trục chính), chưa đại diện đầy đủ cho luồng xe máy chiếm trên `80%` lưu lượng TP.HCM.
  > 2. **Trôi dạt phân phối & mùa vụ (`Open-World Concept Drift & Seasonality`):** Mô hình đóng băng ở `Phase A` và tập `TEST` mùa khô (`12/2020–03/2021`) sẽ suy giảm khi gặp mùa mưa ngập lịch sử hoặc lô cốt thi công mới.
  > 3. **Khan hiếm mẫu chuyển pha đột ngột (`Rare Sudden-Onset Scarcity`):** Ca chuyển đột ngột từ thoáng sang kẹt cứng chỉ chiếm `0.81%` (`8 / 993` ca `TEST`, `Hard Recall = 0/8`), đòi hỏi hàm phạt chi phí chuyên biệt.
  > 4. **Ranh giới `Proof-of-Scale` & điều khiển vòng hở (`Human-in-the-Loop Ceiling`):** Hạ tầng Docker đơn (`38.5 MB` mẫu `Proof-of-Scale`) và giao diện `:8501` hiện dừng ở khuyến nghị điều hành, chưa nối tự động vòng kín vào đèn `SCATS`."
- **Tra cứu nhanh Báo cáo:** Chương 7, Mục `7.5`.

---

#### Slide 16: `12. Plan` — Nghiệm Thu Kế Hoạch Thực Hiện Nhóm & Lộ Trình Phát Triển 3 Giai Đoạn (`16:55 – 18:00`)
- **Người trình bày:** **Phạm Công Minh (`25MS23310`)**
- **Mục tiêu truyền đạt:** Trình bày bảng nghiệm thu tiến độ thực hiện của 3 thành viên (`100% Hoàn thành`) gắn với 12 phần, và đưa ra **Kế hoạch hành động 3 giai đoạn (`Phase 1–3 Plan`)** giải quyết trực tiếp từng hạn chế đã nêu ở Phần 11 trước khi mở `Live Demo`.
- **Chỉ dẫn trực quan:** Chỉ bảng nghiệm thu 3 thành viên phía trên, thẻ `Kế Hoạch Ngắn & Trung Hạn (Phase 1 & 2)` bên trái dưới và thẻ `Kế Hoạch Dài Hạn (Phase 3) & Quality Gate` bên phải dưới.
- **Lời thoại thuyết trình:**
  > "Khép lại bộ slide ở **Phần 12 — Plan** trên trang 16:
  > - **Về phân công thực thi đồ án (bảng trên):** Cả 3 thành viên — bạn **Minh Quyền** (`Mục 1, 4, 5, 7A`), bạn **Thành Đạt** (`Mục 2, 6, 7B, 9`) và em **Công Minh** (`Mục 3, 8, 10, 11, 12`) — phụ trách các phần tương ứng. Số test/pass và kết quả `Ruff` cần được lấy từ lần chạy gần nhất trước khi trình bày; số liệu `22/22` trong bản kịch bản cũ không được xem là trạng thái xác nhận hiện tại.
  > - **Về kế hoạch phát triển tiếp theo (`Phase 1–3 Plan` ở 2 thẻ dưới) nhằm giải quyết triệt để các hạn chế ở Phần 11:**
  >   - **`Phase 1 (Ngắn hạn — Khắc phục hạn chế Modeling & Sketch)`:** Áp dụng **Cost-Sensitive Focal Loss** để bắt nhạy các ca đột ngột hóa kẹt (`A–D → E–F`), đồng thời triển khai **Ring-Buffer Sliding Bloom Filter** tự xoay vòng bit mỗi 30 phút.
  >   - **`Phase 2 (Trung hạn — Đa nguồn dữ liệu & Chống Concept Drift)`:** Đấu nối luồng GPS xe buýt/xe công nghệ toàn TP.HCM (`~1.5–3.0 TB/ngày`) kèm API Thời tiết/Mưa ngập, và tự động kích hoạt huấn luyện lại `Phase A` hàng tuần khi chỉ số `PSI` báo hiệu trôi dạt phân phối.
  >   - **`Phase 3 (Dài hạn — Hạ tầng Đa nút & Đóng vòng SCATS)`:** Nâng cấp lên cụm **Kubernetes Multi-Node + Sharded MongoDB + Vector-Tile GIS** và tích hợp trực tiếp API dự báo `+30m` vào hệ thống điều khiển đèn tín hiệu `SCATS`. Ngay sau đây, em xin phép mở trình duyệt tại `http://localhost:8501` để **Live Demo** trực tiếp hệ thống ạ!"
- **Tra cứu nhanh Báo cáo:** Chương 7, Mục `7.1` – `7.5`.

---

## Phần 3. Kịch Bản Thao Tác `Live Demo` 3 Phút (`http://localhost:8501`)

1. **Phút 1 (`00:00 – 00:45`) — `Tab 1: Bản đồ Điểm nóng & Dữ liệu`:**
   - Chỉ dải 5 thông số tổng quan trên đầu trang (`90,938` bản tin GPS, `33,441` cửa sổ, `84,633` đoạn đường, `ROC-AUC = 0.9882`, `0.636 ms`), sau đó chỉ vào bản đồ TP.HCM có sông Sài Gòn và bảng xếp hạng điểm nóng `EWMA` (dẫn đầu là **Đoạn 47407 — Lý Thường Kiệt**, điểm `= 6.6652`, mức `F`).
2. **Phút 2 (`00:45 – 01:55`) — `Tab 2: Dự báo +30 Phút & MongoDB`:**
   - Giữ đoạn mặc định `Đoạn 21064 · Công Trường Quốc Tế` → chỉ rõ 5 ô kết quả: `Hiện tại = E`, `Đoán theo Hiện tại = E` (sai), **`Random Forest (23 biến) = F (59.6%, đổi trạng thái 73.0%)`**, `Thực tế = F` (đúng!).
   - Nhấn nút **`🔄 Làm mới dữ liệu trực tuyến (5s)`** → chỉ trạng thái kết nối trực tiếp `MongoDB 8.0` (`33,441` bản ghi), rồi cuộn xuống biểu đồ so sánh trực diện giữa `Persistence Baseline` và `Random Forest`.
3. **Phút 3 (`01:55 – 03:00`) — Lướt nhanh `Tab 3`, `Tab 4` & `Tab 5`:**
   - **`Tab 3`:** Chỉ bảng nút cổ chai `Line-Graph PageRank` (`Phú Định`, `Lê Lai`, `Nguyễn Kiệm`) và lưu ý cỡ mẫu nhỏ (`5–8` khung) của luật lan truyền trễ.
   - **`Tab 4`:** Chỉ bản đồ 6 cụm đường phố `7D K-Means` và công cụ tìm các đoạn đường có cảm biến tương đồng cho `74,606` đoạn thiếu GPS (`Cold-Start`).
   - **`Tab 5`:** Chỉ bộ nhớ lọc trùng cố định `32.0 KiB` + `1.0 KiB`, độ đầy đủ đối soát hai tầng `98.50% -> 100.00%` và bằng chứng truy vấn `MongoDB IXSCAN` (`0.636 ms`).

---

## Phần 4. Cẩm Nang Phản Biện & Trả Lời Câu Hỏi Hội Đồng (`10 Câu Hỏi Trọng Tâm`)

### Câu 1: *"Tổng dung lượng 5 bảng gốc chỉ khoảng 38.5 MB (bảng vận tốc GPS nặng 3.58 MB), dùng Pandas chạy vài chục mili-giây là xong, tại sao nhóm phải dựng cụm Spark + Kafka + chia 8 giỏ khóa?"*
- **Người trả lời chính:** Minh Quyền (`Slide 05, 06, 10` & `Chương 1 Mục 1.1`, `Chương 3 Mục 3.3`).
- **Ý chính trả lời:**
  1. Nhóm hoàn toàn đồng ý nếu chỉ tính một lần trên tệp tĩnh `3.58 MB` thì `Pandas` đơn luồng là nhanh nhất. Tuy nhiên, trong môn `BDA501 — Big Data Analytics`, bộ dữ liệu công khai của Kaggle đóng vai trò **mẫu chuẩn kiểm chứng quy mô (`Proof-of-Scale`)**.
  2. Thực tế toàn TP.HCM với hơn `1` triệu xe buýt, taxi, xe công nghệ và xe tải phát GPS mỗi `5–10` giây sẽ sinh ra **`8–17` tỷ bản tin/ngày (`~1.5–3.0 TB/ngày`)**. Ở quy mô đó, lưu danh sách mã sự kiện lọc trùng trong RAM sẽ gây tràn bộ nhớ, ghép bảng không dùng `BroadcastHashJoin` sẽ nghẽn mạng, và lệch tải giờ cao điểm **`687.43` lần** sẽ làm treo máy xử lý phân vùng nóng.
  3. Chạy trên mẫu chuẩn `90,938` bản tin giúp nhóm đối chiếu từng phép tính với `baseline.py` để chứng minh **sai số toán học bằng `0.0`** trước khi nhân rộng ra cụm máy chủ lớn.

### Câu 2: *"Tại sao trên toàn tập kiểm thử (1,182 mẫu), cách đoán mò theo hiện tại (`Persistence Baseline` = 94.84%) lại cao hơn một chút so với `Random Forest` (94.50%)? Và với các ca chuyển từ thoáng sang kẹt cứng, mô hình bắt được bao nhiêu ca?"*
- **Người trả lời chính:** Thành Đạt (`Slide 12, 13` & `Chương 5 Mục 5.4`, `Bảng 5.5`, `Hình 5.2`).
- **Ý chính trả lời:**
  1. **Vì sao tổng thể `Baseline` cao?** Do **94.84% thời gian (`1,121 / 1,182` mẫu) đường giữ nguyên mức cũ sau 30 phút** (và 81.13% mẫu là đường thông thoáng mức A), nên giữ nguyên nhãn hiện tại nghiễm nhiên đúng trên phần đứng yên.
  2. **So sánh trực diện ở 5.16% thời điểm đổi trạng thái (`61` ca):** `Baseline` **đoán sai 100% (`0/61`)**, trong khi `Random Forest (23D)` phát hiện sớm tín hiệu đổi trạng thái với **`ROC-AUC = 0.7664`**, đoán trúng mức mới **`9/61` ca (`14.75%`)** khi dùng ngưỡng chọn trên tập `VAL` ($\tau = 0.30$; mặc định thận trọng đạt `2/61 = 3.28%`) và xếp đúng hướng biến động số 1 ở **`24/61` ca (`39.34%`)**.
  3. **Trả lời thẳng thắn về các ca đang thoáng (`A–D`) đột ngột hóa kẹt cứng (`E–F`):** Cả tập `TEST` chỉ có đúng **`8 / 993` ca** (`0.81%`). Do cỡ mẫu quá hiếm, ở ngưỡng phân lớp cứng mặc định mô hình bắt trúng **`0/8` ca**, nhưng xét theo xác suất cảnh báo rủi ro, mô hình đạt **`ROC-AUC = 0.8136`** và đẩy xác suất rủi ro trung bình trên 8 ca sắp kẹt **cao gấp `9.35x`** đường thoáng ổn định (`3.46%` so với `0.37%`). Đồng thời, chỉ có `Random Forest` mới chấm điểm được cho **100% mạng lưới (kể cả 88.15% đoạn đường thiếu GPS)**.

### Câu 3: *"Vì sao trong phần Literature Review, nhóm không chọn các mô hình Deep Learning trên đồ thị như DCRNN hay Graph WaveNet mà lại chọn Random Forest 23D?"*
- **Người trả lời chính:** Thành Đạt (`Slide 03, 07, 09` & `Chương 1 Mục 1.5`).
- **Ý chính trả lời:**
  Các mô hình `Deep Spatio-Temporal GNN` (`DCRNN`, `ST-GCN`, `Graph WaveNet`) đòi hỏi ma trận chuỗi thời gian liên tục đều đặn trên phần lớn các đỉnh đồ thị. Trong bộ dữ liệu thực tế tại TP.HCM, **88.15% đoạn đường (`74,606` đoạn) không có bất kỳ bản tin GPS nào** và **66.27% cặp quan sát liên tiếp bị đứt quãng $\ge 60$ phút**, khiến phép tích chập thời gian trên đồ thị bị suy biến. Ngược lại, `Random Forest (23D)` kết hợp các đặc trưng đồ thị tĩnh (`Line-Graph PageRank`, `7D K-Means`) xử lý rất hiệu quả dữ liệu bảng đứt quãng trong ngân sách `10.5 GB RAM` mà không cần GPU chuyên dụng.

### Câu 4: *"Ở Phần 7 tính `PageRank` và phân cụm `K-Means`, rồi sang Phần 8 lại tính `EWMA Hotspot` — các phần này liên kết thế nào với mô hình `Random Forest` ở Phần 6?"*
- **Người trả lời chính:** Thành Đạt & Công Minh (`Slide 07, 09, 10, 11` & `Chương 4`, `Chương 6 Mục 6.2`).
- **Ý chính trả lời:**
  1. **`Line-Graph PageRank` và `7D K-Means` (Phần 7 → Phần 6):** Vừa hiển thị cho Sở GTVT quản lý 74,606 đoạn đường thiếu GPS (`Cold-Start`), vừa trích xuất trực tiếp thành các biến không gian (`pagerank_score`, `in_degree`, `out_degree`, `cluster_id`) đưa vào vector 23 biến của `Random Forest` ở Phần 6.
  2. **`Random Forest` (Phần 6) vs. `EWMA Hotspot` (Phần 8):** `Random Forest` dự báo **tương lai 30 phút tới** (*"đường nào SẮP kẹt?"* để chỉnh đèn xanh phòng ngừa), còn `EWMA Hotspot` ($\alpha = 0.15$) đo **độ lì hiện tại** (*"đường nào ĐANG kẹt cứng lặp đi lặp lại suốt nhiều giờ chưa thông?"* như Lý Thường Kiệt để cử CSGT ra giải tỏa).

### Câu 5: *"Các luật kẹt nối tiếp sau 30 phút có `Lift = 32.92x` nhưng chỉ xuất hiện trong `5` cửa sổ (`2.5` tiếng trong 10 tháng). Liệu có phải trùng hợp ngẫu nhiên không?"*
- **Người trả lời chính:** Thành Đạt (`Slide 10, 15` & `Chương 4 Mục 4.2, 4.4`).
- **Ý chính trả lời:**
  Nhóm hoàn toàn đồng tình và đã liệt kê đây là **Limitation & Risk trực tiếp của Batch Processing** ngay trên Slide `10` và Slide `15`. Do GPS gốc chỉ phủ `11.85%` đoạn đường và gửi ngắt quãng, xác suất hai tuyến đường cùng có xe gửi tin ở hai khung 30 phút liên tiếp rất thấp khiến số cửa sổ chung chỉ đạt `5–8` khung. Với cỡ mẫu nhỏ như vậy, hệ thống chỉ xem luật trễ là **giả thuyết tham khảo (`Exploratory Hypothesis`)** chứ không dùng làm quy luật cứng.

### Câu 6: *"Tại sao khi gom khung trên Spark lại phải cộng gộp bộ 5 đại lượng (`5-Tuple Monoid`) mà không dùng hàm `avg()` từng phần rồi gộp lại?"*
- **Người trả lời chính:** Minh Quyền (`Slide 06, 10` & `Chương 3 Mục 3.3`).
- **Ý chính trả lời:**
  Phép tính trung bình cộng không có tính kết hợp khi các phân vùng chứa số lượng bản tin khác nhau. Như đã minh họa bằng con số cụ thể trên Slide `06` và kiểm thử thực tế trên 3 phân vùng lệch kích thước `[1, 2, 3]`, lấy trung bình của trung bình gây sai số tới **`11.11 km/h`**. Ngược lại, cộng gộp bộ 5 đại lượng `(n, sum_v, min_v, max_v, severe_cnt)` đảm bảo chia 8 giỏ khóa (`2-Stage Key Salting`) gộp song song vẫn đạt **sai số bằng `0.0`** tuyệt đối so với `baseline.py`.

### Câu 7: *"Vì sao báo cáo có chỗ ghi `33,441` cửa sổ, còn trong tác vụ `spark_batch.py` lại ghi `33,394` cửa sổ?"*
- **Người trả lời chính:** Minh Quyền (`Slide 08` & `Chương 2 Mục 2.3`).
- **Ý chính trả lời:**
  Hai con số khớp nhau đến từng bản tin: trên toàn bộ `90,938` bản tin gốc, luồng chính tạo đúng `33,441` cửa sổ (khớp `100%` khóa với bảng đối chứng `train.csv`). Khi bật bộ lọc tách riêng `427` đoạn lối đi bộ/nội bộ phi cơ giới trong `spark_batch.py`, có đúng `151` bản tin thuộc các lối đi bộ này được tách ra (`90,938 - 151 = 90,787` bản tin đường cơ giới), tương ứng giảm đúng **`47` cửa sổ lối đi bộ** (`33,441 - 47 = 33,394`).

### Câu 8: *"Làm sao chứng minh mô hình dự báo hoàn toàn không bị rò rỉ dữ liệu tương lai (`Zero Future Leakage`)?"*
- **Người trả lời chính:** Thành Đạt (`Slide 06, 08, 09` & `Chương 2 Mục 2.4`, `Chương 5 Mục 5.1–5.2`).
- **Ý chính trả lời:**
  Nhóm khóa chặt cả 4 kênh dễ gây rò rỉ: **(1)** Bảng ước lượng tốc độ tối đa 3 tầng chỉ học trên tập `TRAIN`; **(2)** Bộ chuẩn hóa `Z-score` và tâm cụm `7D K-Means` chỉ học trên `6,926` đoạn thuộc `TRAIN`; **(3)** Khi chia `70/15/15` theo thời gian, cắt bỏ `55` mẫu sát ranh giới (`Boundary Purge`) có nhãn $t + 30\text{m}$ vắt sang tập sau; và **(4)** Kiểm thử tự động `Future-Shuffle Falsification` đảo ngẫu nhiên toàn bộ tương lai $> t$ xác nhận ma trận 23 biến tại thời điểm $t$ thay đổi đúng bằng **`0.0`**.

### Câu 9: *"Những rủi ro lớn nhất khi đưa hệ thống Stream Processing (`32.0 KiB Bloom Filter` & `Watermark 30m`) chạy thực tế nhiều ngày liền là gì và kế hoạch khắc phục ra sao?"*
- **Người trả lời chính:** Công Minh (`Slide 07, 11, 15, 16` & `Chương 6 Mục 6.5`, `Chương 7 Mục 7.5`).
- **Ý chính trả lời:**
  1. **Rủi ro bão hòa mảng bit `Bloom Filter` (`Bit Saturation`):** Mảng cố định `32.0 KiB` (`262,144` bits) rất hiệu quả cho `90,938` bản tin mẫu (`FPR = 0.616%`), nhưng nếu chạy liên tục nhiều ngày ở quy mô hàng tỷ bản tin mà không làm mới, các bit sẽ dần chuyển hết sang `1`. Trong **Phase 1 Plan (Slide 16)**, nhóm giải quyết bằng cơ chế **Ring-Buffer Sliding Bloom Filter** tự xoay vòng mảng bit mới sau mỗi cửa sổ 30 phút.
  2. **Rủi ro bản tin trễ quá `Watermark 30m`:** Được khắc phục bằng kiến trúc `Dual Sink` lưu 100% sự kiện thô kết hợp **`Two-Tier Reconciliation`** bù đủ `501` cửa sổ trễ và khóa phiên bản `version_ts` trên `MongoDB 8.0`.

### Câu 10: *"Làm thế nào `MongoDB 8.0` đạt được độ trễ truy vấn `0.636 ms` mà không cần sắp xếp trên RAM (`0 in-memory SORT`)?"*
- **Người trả lời chính:** Công Minh (`Slide 11, 14` & `Chương 6 Mục 6.4`).
- **Ý chính trả lời:**
  Nhóm thiết kế chỉ mục phức hợp **`Compound Index {street_level: 1, severe_prob: -1, segment_id: 1}`** tuân thủ quy tắc **`ESR` (`Equality – Sort – Range`)**. Khi Dashboard truy vấn 25 trục đường chính (`street_level` thuộc cấp 1–2) có xác suất kẹt cứng `severe_prob >= 0.70` sắp xếp giảm dần theo `severe_prob`, bộ tối ưu truy vấn của `MongoDB 8.0.34` đọc thẳng các nút lá đã được sắp xếp sẵn trên cây `B-Tree` (`winningPlan = IXSCAN`, `hasSortStage = false`), nhờ đó đạt **`warm_median = 0.530 ms`** và **`warm_p95 = 0.636 ms`**.
