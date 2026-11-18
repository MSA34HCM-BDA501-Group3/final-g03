<!-- session9-current-status -->
> **Session 9 implementation status, 18 November 2026:** The current handoff is
> `../../evidence/session9/README.md`, with commands, provenance and gate limitations. Historical
> replay now has a Python Kafka consumer, journaled Mongo ingress, exact deduplication,
> immutable forecast revisions and a run-scoped dashboard. The prior file-stream,
> local replay and broker publish checks remain separate historical experiments.
>
> All measurements and screenshots in the older narrative below retain their original
> provenance. The 687.43x and 1.023x statistics compare different populations and do
> not demonstrate reduced task skew. Warm Mongo query latency is separate from
> end-to-end latency. Next-slot prediction does not establish 30 minutes of advance
> warning before target start. Coverage of cold-start roads does not establish accuracy.
> Historical point-in-time availability remains UNVERIFIED. The existing DOCX, PDF
> and PPTX have not been regenerated or render-verified for this implementation and
> must not be used to claim all gates Ready or a verified 95/100 score.
<!-- /session9-current-status -->

# Báo Cáo Tổng Kết Đồ Án & Đặc Tả Kỹ Thuật Chi Tiết

> **Cập nhật riêng ngày 18/11/2026:** Chữ ký artifact đã đồng bộ với manifest của lần chạy lại. Các số đo hiệu năng và phiên bản trong phần báo cáo bên dưới vẫn là kết quả trước lần chạy lại. Kết quả mới, log và giới hạn kiểm chứng: [Session 9 rerun](../../evidence/session9_rerun/README.md). Bản sửa này chưa xác nhận hoàn tất mọi yêu cầu trong lab manual.

- **Tên đề tài:** Hệ thống Giám sát Hiện trạng & Dự báo Ùn tắc Giao thông Đô thị TP. Hồ Chí Minh (*HCMC Smart Traffic Big Data Analytics & Congestion Prediction Platform*)
- **Học phần:** `BDA501 — Big Data Analytics` (Chương trình Thạc sĩ MSE AI — FPT School of Business & Technology · `Fall 2026`)
- **Giảng viên hướng dẫn (Instructor):** **Đặng Trần Khánh**
- **Nhóm thực hiện (3 thành viên):**
  1. **Nguyễn Tạ Minh Quyền (`25MS23280`)** — *Bước 1: Làm sạch dữ liệu & Xử lý phân tán Apache Spark*
  2. **Nguyễn Thành Đạt (`25MS23306`)** — *Bước 2 & Bước 3: Đồ thị giao lộ, Phân cụm & Dự báo trước 30 phút*
  3. **Phạm Công Minh (`25MS23310`)** — *Bước 4: Luồng trực tuyến Kafka, Đối soát tin trễ, MongoDB & Giao diện điều hành*
- **Loại tài liệu:** Báo cáo tổng kết nghiệm thu cuối kỳ và hồ sơ đặc tả kỹ thuật toàn diện của hệ thống vận hành hoàn chỉnh `100%` trên hạ tầng container.

---

### Tóm Tắt Cốt Lõi (Toàn Cảnh Bài Toán & Lời Giải Trong 4 Bước)

> **Mục tiêu của hệ thống** là giám sát hiện trạng và dự báo trước `30 phút` nguy cơ kẹt xe trên toàn mạng lưới **`84,633` đoạn đường một chiều** tại TP.HCM từ dữ liệu hành trình GPS của xe cộ (chẳng hạn lúc `07:08` sáng trên đoạn đường Lý Thường Kiệt mã `47407` tại Quận 10, một chiếc xe gửi về tín hiệu báo đang di chuyển chậm ở vận tốc `4 km/h`).
>
> Tuy nhiên, dữ liệu thực tế ngoài đường gặp **3 rào cản lớn**:
> 1. **Thiếu hụt thông tin:** Gần **`88.15%`** đoạn đường (`74,606 / 84,633` đoạn, chủ yếu là đường nhánh và hẻm dân cư) không có xe gửi GPS về, và đa số đường bị thiếu thông tin tốc độ tối đa cho phép;
> 2. **Đứt quãng thời gian:** Xe gửi tín hiệu không đều, lúc cách nhau `30 phút`, lúc ngắt quãng nhiều giờ đến nhiều ngày;
> 3. **Nhiễu đường truyền:** Bản tin gửi về theo thời gian thực thường bị trùng lặp hoặc đến trễ quá hạn.
>
> Để giải quyết trọn vẹn, hệ thống vận hành qua **4 bước nối tiếp nhau**:
> - **Bước 1 – Chuẩn hóa bức tranh hiện trạng (Spark Batch ETL):** Làm sạch `90,938` bản tin GPS thô, tự động ước lượng tốc độ tối đa cho từng loại đường, và gom dữ liệu theo từng khung `30 phút` (`33,441` cửa sổ) để xác định đường đang Thông thoáng (mức `A–B`), Đông đúc (`C–D`) hay Kẹt cứng (`E–F`).
> - **Bước 2 – Lấp đầy "vùng mù" bằng cấu trúc bản đồ (Đồ thị & Phân cụm):** Với `88.15%` đoạn đường không có GPS, hệ thống phân tích cấu trúc giao lộ (`PageRank`) để tìm các "nút cổ chai" tự nhiên, đồng thời gom nhóm các đoạn đường có đặc điểm giống nhau (`K-Means`) để suy đoán hành vi giao thông cho những nơi thiếu cảm biến và làm biến đầu vào cho Bước 3.
> - **Bước 3 – Dự báo trước 30 phút (Machine Learning):** Kết hợp trạng thái hiện tại trên đường, khung giờ trong ngày và đặc trưng nút cổ chai ở Bước 2 để huấn luyện mô hình `Random Forest` dự báo xác suất một đoạn đường sẽ bị kẹt cứng sau đúng `30 phút` nữa (đạt **`ROC-AUC = 0.9882`**, **`PR-AUC = 0.9471`**).
> - **Bước 4 – Cảnh báo trực tuyến & Tự động bù dữ liệu trễ (Kafka Streaming & Dashboard):** Đưa mô hình vào chấm điểm trực tiếp luồng dữ liệu GPS đổ về liên tục để hiển thị cảnh báo tức thì dưới `1 mili-giây` (**`0.636 ms`**) trên Dashboard điều hành; đồng thời lưu trữ các bản tin đến muộn để cuối ngày đối soát và cập nhật lại lịch sử chính xác **`100%`**.

---

## Chương 1. Câu Chuyện Thực Tế Từ Dòng Tín Hiệu GPS, Coupled Sub-Problems (P1–P4), Operational Use Cases (UC-1–UC-6) & System Boundary

### 1.1. Bối Cảnh Bài Toán Điều Hành Giao Thông TP.HCM, Dòng Dữ Liệu Thực Tế & Vai Trò Mẫu Chuẩn Proof-of-Scale

Tại siêu đô thị TP. Hồ Chí Minh, mật độ phương tiện rất lớn lưu thông trên mạng lưới hàng chục nghìn đoạn đường và ngã tư đan xen chặt chẽ (từ cửa ngõ sân bay Tân Sơn Nhất, đầu mối trung tâm Bến Thành – Quận 1, trục xuyên tâm Lý Thường Kiệt – Quận 10 đến hành lang Đông Bắc Thủ Đức và vành đai Quốc lộ 1). Chỉ một điểm ùn ứ cục bộ vào giờ cao điểm cũng nhanh chóng lan truyền thành kẹt xe dây chuyền. Trong khi đó, công tác điều tiết hiện nay còn gặp **ba khoảng trống thực tiễn**: **(1)** hệ thống camera CCTV và vòng từ cố định có chi phí đầu tư/bảo trì cao nên chỉ đặt được ở một số giao lộ lớn, còn nguồn định vị GPS hành trình lại lệch về đại lộ, bỏ trống phần lớn đường nhánh (*vùng mù quan trắc*) và thường xuyên ngắt quãng, trễ tin; **(2)** các bảng giám sát hiện hữu chỉ phản ánh nơi **đang kẹt ở hiện tại**, thiếu cảnh báo sớm trước `30 phút` nơi **sắp chuyển sang kẹt cứng** để điều chỉnh đèn và phân luồng chủ động; và **(3)** công tác trực ban xử lý sự vụ tức thời tại Trung tâm Điều hành (`TOC`) vẫn tách rời khỏi bài toán phân tích nút cổ chai quy hoạch dài hạn của `Sở GTVT`.

Để thấy rõ thách thức từ dữ liệu thực địa trước khi đi vào giải pháp, hãy nhìn vào một dòng dữ liệu gốc trong bảng `segment_status.csv`:
- Vào lúc **`07:08:28` sáng giờ Việt Nam** ngày `03/08/2020` (bản ghi mã `15281`), một chiếc xe đi ngang qua **đoạn đường Lý Thường Kiệt số `47407`** (dài `16m` tại Quận 10) gửi về vận tốc tức thời chỉ **`4 km/h`**; hoặc lúc **`11:30` trưa** tại **Công Trường Quốc Tế (đoạn `21064`, Quận 3)**, xe báo vận tốc **`12 km/h`**.

Để biến hàng chục nghìn con số vận tốc rời rạc như vậy thành bức tranh điều hành thống nhất trên toàn mạng lưới **`84,633` đoạn đường một chiều**, hệ thống quy đổi tỷ lệ giữa vận tốc lưu thông thực tế so với tốc độ tối đa cho phép của đoạn đường sang **6 mức phục vụ giao thông (`Level of Service` — `LOS` từ `A` đến `F`)**:
- **Mức `LOS A` và `LOS B` (vận tốc đạt từ `65%` tốc độ tối đa trở lên):** Đường thông thoáng, xe cộ lưu thông tự do.
- **Mức `LOS C` và `LOS D` (vận tốc đạt từ `35%` đến dưới `65%` tốc độ tối đa):** Đường đông đúc ổn định vào khung giờ cao điểm.
- **Mức `LOS E` và `LOS F` (vận tốc tụt xuống dưới `35%` tốc độ tối đa, riêng mức `F` dưới `20%` hoặc xe dừng hẳn):** Đường ùn ứ nghiêm trọng đến kẹt cứng tê liệt.

![Hình 1.1. Bản đồ giao thông TP.HCM 84,633 đoạn đường, vùng Cold-Start 88.15% và các điểm nóng kẹt xe](../../outputs/slide_charts/map_hcmc_hotspots_coldstart.png)

*Hình 1.1. Bản đồ giao thông TP.HCM tái tạo từ `84,633` đoạn đường thực tế trên nền sông Sài Gòn, kênh Tàu Hủ – Bến Nghé và các quận trung tâm: phần nền xám nhạt là `74,606` đoạn đường nhánh không có xe gửi GPS (`88.15% Cold-Start`), các trục đỏ là điểm nóng thường xuyên kẹt cứng (Lý Thường Kiệt, Lê Lai – Bến Thành, Nguyễn Kiệm, Phú Định, Quốc lộ 1, Kha Vạn Cân).*

> **Vai trò mẫu chuẩn kiểm chứng quy mô (`Proof-of-Scale` — Tại sao xây dựng kiến trúc `Spark + Kafka` trên bộ dữ liệu mẫu `~38.5 MB`?):**
> Năm bảng công khai của bộ dữ liệu giao thông TP.HCM trên Kaggle có tổng dung lượng **`38.50 MB`** (riêng bảng vận tốc `segment_status.csv` nặng **`3.58 MB`** với `90,938` bản tin GPS). Nếu chỉ tính toán một lần trên tệp tĩnh này, thư viện `Pandas` trên máy cá nhân chỉ mất vài chục mili-giây. Tuy nhiên, trong khuôn khổ môn học `BDA501 — Big Data Analytics`, bộ dữ liệu này đóng vai trò **mẫu chuẩn kiểm chứng quy mô (`Proof-of-Scale`)**:
> 1. **Quy mô thực tế toàn đô thị TP.HCM:** Khi kết nối luồng định vị thực tế từ hơn `1` triệu xe buýt, taxi, xe công nghệ và xe tải phát tín hiệu mỗi `5–10` giây, hệ thống sẽ tiếp nhận **`8–17` tỷ bản tin/ngày (`~1.5–3.0 TB/ngày`)**. Ở quy mô đó, hiện tượng lệch tải giờ cao điểm (gấp `687.43` lần trung vị) và bộ nhớ lọc bản tin trùng không giới hạn sẽ làm tràn RAM máy đơn.
> 2. **Kiểm chứng sai số toán học bằng `0.0` trước khi nhân rộng:** Thử nghiệm kiến trúc phân tán trên mẫu chuẩn `90,938` bản tin cho phép đối chiếu từng con số với bộ đối chứng độc lập `baseline.py` để chứng minh **sai số bằng `0.0`** trước khi mở rộng ngang ra cụm máy chủ lớn.

### 1.2. Sự Liên Kết Nhân Quả Giữa Bốn Bài Toán Trong Cùng Một Hệ Thống (Coupled Sub-Problems P1 → P4)

Bốn bài toán `P1`, `P2`, `P3`, `P4` không phải là bốn đồ án nhỏ tách rời, mà là **bốn mắt xích nối tiếp nhau trong một dây chuyền thống nhất**: kết quả chuẩn hóa của `P1` nuôi dữ liệu cho `P2`; đặc trưng nút cổ chai và nhóm đường phố ở `P2` truyền thẳng làm biến đầu vào cho mô hình dự báo ở `P3`; và mô hình `P3` được đưa vào chấm điểm trực tuyến kết hợp theo dõi điểm nóng ở `P4`:

| Mắt xích Hệ thống | Câu hỏi Thực tiễn Cần Giải quyết | Dữ liệu Đầu vào Thực tế | Kết quả Sinh ra & Mối Nối Sang Bước Sau |
|---|---|---|---|
| **`P1: Descriptive` (Batch Layer)** | **Hiện tại và trong lịch sử, đoạn đường nào đang thông thoáng hay kẹt cứng?** | `90,938` bản tin GPS thô gửi không đều; `88.34%` đoạn đường thiếu tốc độ tối đa. | `33,441` cửa sổ `30 phút` gắn mức `LOS A–F` chuẩn xác (sai số `0.0` so với đối chứng), tạo nền dữ liệu sạch cho `P2` và `P3`. |
| **`P2: Diagnostic` (Graph & Clustering)** | **Làm sao hiểu các nút cổ chai và `88.15%` đoạn đường nhánh `Cold-Start` không có xe gửi GPS?** | Bản đồ giao lộ `84,633` đoạn đường một chiều và lịch sử các khung giờ kẹt cứng. | Điểm nút cổ chai `Line-Graph PageRank`, luật `SON / FPGrowth` và `6` cụm hành vi `7D K-Means` — vừa phục vụ Sở GTVT, vừa truyền làm biến không gian cho `P3`. |
| **`P3: Predictive` (+30m Forecasting)** | **Đúng 30 phút nữa, đoạn đường nào sẽ chuyển sang kẹt cứng?** | `7,898` cặp quan sát cách nhau đúng `30 phút`, giờ Việt Nam và đặc trưng bản đồ từ `P2`. | Mô hình `Random Forest` (`23D`) dự báo mức `LOS` và xác suất kẹt cứng sau `30 phút` (**`ROC-AUC = 0.9882`**, **`PR-AUC = 0.9471`**), bàn giao cho `P4`. |
| **`P4: Prescriptive` (Speed & Serving Layer)** | **Làm sao vừa cảnh báo dưới `1 ms` trên màn hình trực ban, vừa không mất tin đến trễ?** | Luồng bản tin GPS đổ về liên tục có lẫn `1.0%` tin trùng và `1.5%` tin trễ quá `30 phút`. | `O(1) Stream Sketches` (`33.0 KiB`), cảnh báo `MongoDB IXSCAN` dưới `1 ms` (`0.636 ms`) trên Dashboard và `Two-Tier Reconciliation` bù đủ `501` cửa sổ trễ vào cuối ngày. |

### 1.3. Sáu Kịch Bản Vận Hành Thực Tế (Operational Use Cases UC-1–UC-6)

| Use Case | Người Sử dụng (Actor) | Thông tin Nhận được trên Màn hình | Quyết định Xử lý Ngoài Đời thực |
|---|---|---|---|
| **`UC-1` — EWMA Hotspot Alerting** | Cán bộ Trung tâm Điều hành (`TOC`) | Danh sách `25` đoạn đường đang kẹt cứng liên tiếp nhiều khung giờ chưa giải tỏa (tiêu biểu: Lý Thường Kiệt, Trương Định). | Phát cảnh báo sớm lên bảng điện tử ngoài đường và điều lực lượng cảnh sát giao thông ra giải tỏa tại nút giao. |
| **`UC-2` — Proactive Signal Priority** | Kỹ sư Tín hiệu Đèn giao thông | Danh sách các trục đại lộ cấp 1–2 được dự báo có xác suất kẹt cứng `≥ 70%` trong `30 phút` tới, phản hồi dưới `1 ms` (`IXSCAN`). | Chủ động kéo dài pha đèn xanh cho hướng trục chính trước khi dòng xe kịp dồn ứ khóa chặt ngã tư. |
| **`UC-3` — Bottleneck & Sensor Planning** | Chuyên viên Quy hoạch (`Sở GTVT`) | Bản đồ xếp hạng nút cổ chai `Line-Graph PageRank` (Phú Định, Lê Lai, Châu Văn Liêm, Nguyễn Kiệm) ngay cả nơi chưa có xe gửi GPS. | Xác định đúng vị trí trọng yếu để ưu tiên lắp mới camera giám sát và biển báo phân luồng tải trọng theo giờ. |
| **`UC-4` — Spillover Detour Routing** | Ứng dụng Bản đồ & Dẫn đường | Cảnh báo các cặp tuyến đường thường kẹt đồng thời (`Cộng Hòa ↔ Trường Chinh`, `Cao Thắng ↔ Cống Quỳnh`) hoặc lan sau `30 phút`. | Gợi ý người lái xe rẽ sang lộ trình thay thế từ các ngã tư phía xa trước khi đi vào hành lang đang ùn tắc. |
| **`UC-5` — Cluster-Based Patrol Dispatch** | Đội Tuần tra & Điều tiết Giao thông | Bản đồ chia toàn bộ `84,633` đoạn đường thành `6` cụm hành vi `7D K-Means` (giao lộ nội đô kẹt mãn tính, đại lộ cửa ngõ, phố thương mại...). | Tập trung lực lượng chốt trực đúng nhóm `2,352` giao lộ nội đô nghẽn mãn tính vào giờ cao điểm thay vì rải đều. |
| **`UC-6` — Late-Data Reconciliation** | Kỹ sư Dữ liệu & Vận hành hệ thống | Báo cáo `Two-Tier Reconciliation` cuối ngày: `501` cửa sổ đến muộn đã backfill và `377` dự báo lịch sử đã re-score. | Đảm bảo `100%` số liệu báo cáo tổng kết cuối ngày của Sở GTVT và kho dữ liệu huấn luyện đạt độ chính xác tuyệt đối. |

### 1.4. System Boundary (2 Nguồn Dữ Liệu Đầu Vào → 4 Nhóm Kết Quả Đầu Ra)

- **Hai nguồn dữ liệu đầu vào thực tế (`External Inputs`):**
  1. **Luồng bản tin vận tốc GPS (`producer.py`, `seed = 42`):** Phát `90,938` bản tin GPS thực tế từ `raw_data/segment_status.csv` vào hệ thống hàng đợi `Apache Kafka 4.1.0 KRaft`, tái hiện môi trường mạng di động có `1.0%` bản tin gửi trùng và `5.0%` bản tin đến trễ.
  2. **Bản đồ số đường bộ TP.HCM (`GIS Topology`):** Gồm 3 bảng địa lý `nodes.csv` (`577,967` điểm tọa độ), `streets.csv` (`5,553` tuyến đường) và `segments.csv` (`84,633` đoạn đường một chiều). Riêng bảng `train.csv` (`33,441` dòng) được tách hoàn toàn khỏi dây chuyền xử lý và chỉ dùng làm **bảng đối chứng độc lập** để kiểm tra độ khớp khóa `100.0%`.
- **Bốn nhóm kết quả đầu ra chính thức (`System Outputs`):**
  1. **`Medallion Lakehouse` (`data/lakehouse/`):** Gồm tầng `Bronze`, `Silver`, `Gold` chia theo `10` phân vùng tháng (`2020-07` đến `2021-04`), cùng bảng đặc trưng `data/dataset.csv` (`33,441` dòng × `59` cột).
  2. **`Frozen Model & Static Feature Registry`:** Gồm `model.joblib`, `vmax_lookup.json` và `segment_static_features.parquet` phủ đủ `84,633` đoạn đường.
  3. **`Quarantine & WAL Checkpoints`:** Lưu đầy đủ mọi bản ghi vi phạm quy tắc kiểm định và trạng thái hồi phục luồng.
  4. **`MongoDB 8.0.34 Serving Store` & `Streamlit 5-Tab UI` (`:8501`):** Phục vụ `33,441` bản ghi trạng thái và dự báo với độ trễ truy vấn dưới `1 ms`, làm mới trực tiếp trên trình duyệt.

*Hình 1.2. Sơ đồ System Boundary: 2 nguồn dữ liệu thực tế đi qua Unified Pipeline (P1 → P4) tạo ra 4 nhóm kết quả phục vụ điều hành trực tuyến và phân tích lịch sử.*

### 1.5. Tổng Quan Nghiên Cứu Quản Lý – Dự Báo Giao Thông Đô Thị (Literature Review) & Khoảng Trống Thực Tiễn

Theo quy trình nghiên cứu thuận chiều (**Xác định bài toán thực tế → Khảo sát giải pháp hiện có → Xác định khoảng trống → Đề xuất nền tảng tích hợp → Thực nghiệm kiểm chứng**), nhóm khảo sát bốn hướng tiếp cận chính trong quản lý và dự báo giao thông đô thị:

| Hướng Tiếp Cận Trong Thực Tế & Học Thuật | Hệ Thống Điển Hình & Công Trình Tiêu Biểu | Đóng Góp Trong Quản Lý Giao Thông | Hạn Chế Khi Áp Dụng Cho Đô Thị Như TP.HCM |
|---|---|---|---|
| **1. Quan trắc Hạ tầng Cố định & Đèn Thích ứng (`Fixed-Sensor ITS`)** | Cảm biến vòng từ, Camera CCTV; hệ thống đèn `SCATS` (*Sims & Dobinson, 1980*), `SCOOT` (*Hunt et al., 1981*). | Đo đạc lưu lượng liên tục, chính xác cao tại các nút giao có lắp thiết bị để điều chỉnh chu kỳ đèn cục bộ. | Chi phí đầu tư và bảo trì rất lớn; chỉ bao phủ số ít trục chính, để trống gần `90%` mạng lưới đường nhánh và khu dân cư (*vùng mù quan trắc*). |
| **2. Giám sát Từ Dữ liệu Hành trình (`Floating Car / Probe GPS`)** | Định vị GPS từ xe buýt, taxi, bản đồ số (*Herrera et al., 2010; Zheng, 2015*). | Mở rộng độ phủ giám sát diện rộng không cần đào đường lắp cảm biến, phản ánh trực tiếp vận tốc dòng xe. | Phân bố lệch mạnh về đại lộ, tín hiệu ngắt quãng dài theo thời gian, khuyết thông tin tốc độ chuẩn và mới dừng ở bản đồ hiện trạng. |
| **3. Nghiên cứu Dự báo Ùn tắc Ngắn hạn (`Short-Term Forecasting`)** | Chuỗi thời gian `ARIMA` / Quán tính (*Vlahogianni et al., 2014*); mạng đồ thị sâu `DCRNN`, `Graph WaveNet` (*Li et al., 2018*). | Dự báo vận tốc hoặc mức độ ùn tắc trước `15–60 phút` dựa trên tương quan không gian – thời gian. | Phần lớn thử nghiệm trên cao tốc có cảm biến dày đặc, liên tục (`PeMS`, `METR-LA`); suy giảm mạnh khi dữ liệu đô thị thưa (`88.15%` đoạn trống chuỗi GPS) và đứt quãng (`66.27%` cặp cách `≥ 60 phút`). |
| **4. Nền tảng Hỗ trợ Quyết định Điều hành (`Urban Traffic DSS`)** | Trung tâm điều hành `TOC` & nền tảng dữ liệu lớn đô thị (*Lv et al., 2015; Amini et al., 2017*). | Hợp nhất lưu trữ lịch sử phục vụ quy hoạch với bảng điều khiển giám sát trực tuyến cho điều hành viên. | Mô hình dự báo học thuật thường tách rời khỏi luồng vận hành thực tế (bản tin GPS di động trễ/trùng); thiếu sự gắn kết giữa cảnh báo tức thời và quy hoạch điểm nghẽn dài hạn. |

> **Khoảng trống nghiên cứu & Lý do cần nền tảng tích hợp `HCMC Smart Traffic`:** Các công trình dự báo ngắn hạn thường giả định dữ liệu cảm biến sạch và liên tục, trong khi hệ thống giám sát đô thị thực tế lại thiếu dự báo sớm `+30 phút` và bỏ trống vùng đường nhánh không có GPS. Việc triển khai rời rạc từng công nghệ đơn lẻ không giải quyết được bài toán: một mô hình học máy độc lập không thể tự làm sạch luồng GPS di động nhiễu/đến trễ, không thể suy diễn trạng thái cho `74,606` đoạn đường vùng mù nếu tách rời phân tích cấu trúc đồ thị mạng lưới đường, và không thể phục vụ đồng thời cả trực ban `TOC` lẫn quy hoạch `Sở GTVT`. Đây chính là cơ sở khoa học để đồ án đề xuất kiến trúc nền tảng tích hợp ở các chương tiếp theo.

---

## Chương 2. P1A — Khảo Sát 5 Bảng Dữ Liệu Gốc (EDA), 10 Edge Cases Thực Nghiệm & Ordered Quality Funnel

### 2.1. Kiểm Kê Chi Tiết Năm Bảng Dữ Liệu Nguồn (`raw_data/` · Giao thông TP.HCM)

| Bảng Dữ liệu | Số Bản ghi | Số Cột | Dung lượng | Khóa Chính / Khóa Ngoại | Đặc điểm Phân bố Khi Khảo sát Thực tế |
|---|---:|---:|---:|---|---|
| **`nodes.csv`** | `577,967` | `3` | `19.44 MB` | PK: `_id` · Cột: `_id, long, lat` | Có tới **`525,542` điểm (`90.93%`) chỉ là `intermediate shape points`** (điểm uốn cong trung gian trên OpenStreetMap, không phải giao lộ); chỉ có **`52,425` giao lộ thực sự** (`52,181` giao lộ sau khi tách lối đi bộ). |
| **`streets.csv`** | `5,553` | `5` | `0.20 MB` | PK: `_id` · Cột: `_id, level, max_velocity, name, type` | Cấp đường từ `1` đến `4`; **`82.23%` (`4,566` tuyến) khuyết tốc độ tối đa**; tồn tại `695` tên đường thô lệch chuẩn dấu tiếng Việt cần gộp về `649` tên chuẩn `Unicode NFC`. |
| **`segments.csv`** | `84,633` | `11` | `10.29 MB` | PK: `_id`; FK: `s_node_id, e_node_id, street_id` | **`88.34%` (`74,762` đoạn) thiếu tốc độ tối đa**; **`154` đoạn chiều dài `≤ 0m`**; **`427` đoạn lối đi bộ/nội bộ** (`580` đoạn lỗi địa lý). Chỉ `10,027` đoạn (`11.85%`) có GPS, còn **`74,606` đoạn (`88.15% Cold-Start`) không có dữ liệu GPS**. |
| **`segment_status.csv`** | `90,938` | `4` | `3.58 MB` | PK: `_id`; FK: `segment_id` · Cột: `updated_at, velocity` | Chuỗi bản tin vận tốc GPS theo giờ `UTC` (`07/2020–04/2021`); chứa `151` bản tin thuộc lối đi bộ (`90,787` bản tin trên đường cơ giới); giờ cao điểm lệch tải (`Data Skew`) lên tới **`687.43` lần** trung vị. |
| **`train.csv`** | `33,441` | `18` | `4.99 MB` | Khóa cửa sổ `(segment_id, date, period)` kèm nhãn `LOS` | *Bảng đối chứng độc lập* · Khớp `100.0%` khóa cửa sổ với kết quả tổng hợp từ `segment_status.csv`; **`85.21%` (`28,495` dòng) thiếu tốc độ tối đa**; `631` dòng mang tên `Phường/Quận`; `4,320` dòng đổi ngày khi chuyển sang giờ Việt Nam `UTC+7`. |

![Hình 2.1. Hồ sơ khuyết thiếu và đặc thù dữ liệu trên 5 bảng gốc](../../outputs/slide_charts/chart_eda_anomalies.png)

*Hình 2.1. Tỷ lệ khuyết thiếu tốc độ tối đa, intermediate shape points, đoạn đường Cold-Start thiếu GPS và tín hiệu ngắt quãng trên 5 bảng dữ liệu gốc.*

### 2.2. Danh Mục 10 Edge Cases Thực Nghiệm & Cơ Chế Phòng Vệ Kỹ Thuật (`EC-1–EC-10`)

| Edge Case | Hiện tượng Dữ liệu Thực tế | Quy mô Ghi nhận Định lượng | Giải pháp Kỹ thuật Đã Triển khai & Kiểm chứng |
|---|---|---|---|
| **`EC-1`** | Thiếu thông tin tốc độ tối đa diện rộng (`Missing max_velocity`) | `28,495 / 33,441` cửa sổ (`85.21%`) & `74,762 / 84,633` đoạn (`88.34%`) | **Train-Only 3-Tier Imputation (`vmax_lookup.json`):** Giữ tốc độ gốc hợp lệ (`4,946` cửa sổ) → Phân vị `P85` lịch sử đoạn đường trên `TRAIN` (`8,164` cửa sổ) → Trung vị theo `(level, street_type)` trên `TRAIN` (`20,124` cửa sổ) → Chuẩn đô thị `{80, 60, 50, 40} km/h` (`207` cửa sổ). |
| **`EC-2`** | Đoạn đường chiều dài `≤ 0m` hoặc thuộc lối đi bộ (`Non-road OSM tags`) | `154` đoạn `≤ 0m` & `427` đoạn phi cơ giới (`580` đoạn lỗi địa lý) | Lưu vết vào `quarantine_events.parquet` theo mã quy tắc vi phạm đầu tiên; điền chiều dài trung vị (`27.0m`); luồng Spark độc lập lọc tách `151` bản tin lối đi bộ (`33,394` cửa sổ). |
| **`EC-3`** | Tên đường chứa nhãn `Phường/Quận` & lệch chuẩn `Unicode NFD/NFC` | `631` cửa sổ `Phường/Quận` & `2,071` cửa sổ biến thể dấu/khoảng trắng | Chuẩn hóa dấu `Unicode NFC`, đồng nhất ký tự `Ð → Đ` (`695` tên thô gộp về `649` tên chuẩn, hợp nhất `46` biến thể); loại bỏ nhãn `Phường/Quận` khỏi giỏ `SON / FPGrowth`. |
| **`EC-4`** | Đổi ngày qua nửa đêm khi chuyển giờ `UTC` sang giờ Việt Nam (`UTC+7`) | `4,320 / 33,441` cửa sổ (`12.92%`) rơi vào khung `17:00–23:30 UTC` | Giữ nguyên ngày `UTC` làm khóa chuẩn, đồng thời tính chính xác giờ Việt Nam `vn_hour = (hour_utc + minute/60 + 7) mod 24` và cộng `+1 ngày` cho `ict_date` khi `hour_utc ≥ 17`. |
| **`EC-5`** | Tín hiệu GPS ngắt quãng trên `30 phút` (`Non-contiguous temporal gaps`) | `15,516 / 23,414` cặp liên tiếp (`66.27%`) cách từ `60 phút` đến nhiều ngày | Ghép nhãn dự báo bằng phép nối chính xác trên khóa `(segment_id, slot_ts + 30 phút)`, giữ đúng `7,898` cặp chuẩn xác `exact t+30m`; không dùng lệnh trượt dòng `shift(-1)` ngây thơ. |
| **`EC-6`** | `88.15%` đoạn đường nhánh không có xe gửi GPS (`Cold-Start Segments`) | `74,606 / 84,633` đoạn đường không có bản tin GPS nào | Xây dựng kho đặc trưng bản đồ tĩnh (`segment_static_features.parquet`) phủ kín `84,633` đoạn đường nhờ điểm `Line-Graph PageRank`, số nhánh vào/ra và tâm cụm `7D K-Means`. |
| **`EC-7`** | Điểm uốn cong trung gian (`Intermediate Shape Points`) & `Dangling Sinks` | `525,542 / 577,967` shape points (`90.93%`) & `48` đoạn cụt | Lọc bỏ toàn bộ `525,542` shape points trước khi dựng đồ thị; chia đều lại xác suất của `48` đoạn cụt ở mỗi vòng lặp `PageRank`, bảo toàn tổng điểm `84,633.0`. |
| **`EC-8`** | Lệch tải giờ cao điểm (`Data Skew`) & bẫy trung bình từng phần | Lệch tải `687.43x`; trung bình từng phần gây sai số `11.11 km/h` | Gộp song song bằng `5-Tuple Commutative Monoid Combiner` kết hợp `2-Stage Key Salting` (`8` buckets) kéo độ lệch tải giờ cao điểm về **`1.023x` (`CV = 0.0057`)**, sai số bằng `0.0`. |
| **`EC-9`** | Bản tin mạng di động gửi trùng (`1%`) và đến trễ (`> 30m Watermark`) | `334` cửa sổ trùng lặp & `501` cửa sổ đến trễ trải trên `219` khung giờ | Chặn `334/334` tin trùng bằng `BitPackedBloomFilter` (`32.0 KiB`); lưu `100%` tin thô vào `Medallion Lakehouse`; cuối ngày backfill đủ `501` cửa sổ trễ, cập nhật lịch sử cho `3,953` cửa sổ kế tiếp và re-score `377` cửa sổ. |
| **`EC-10`** | Sập tiến trình luồng đột ngột & tin cũ đến muộn ghi đè (`Stale Overwrite`) | Ngắt cưỡng bức giữa chừng hoặc tin tạm thời đến muộn sau đối soát | Hồi phục chính xác vị trí đọc từ `WAL Checkpoint` với `0` bản ghi nhân đôi; `Monotonic Version Guard` (`status_version`) trong `MongoDB` chặn đứng `25/25` bản tin cũ ghi đè kết quả v2. |

### 2.3. Ordered Quality Funnel (Bảo Toàn 100% Quân Số Bản Ghi)

Toàn bộ dữ liệu thô đi vào hệ thống đều được ép kiểu an toàn `try_cast` dưới chế độ `spark.sql.ansi.enabled = true`. Sáu quy tắc kiểm định được thực thi theo thứ tự cố định (`R1` → `R6`) sao cho mỗi bản ghi lỗi chỉ nhận đúng một mã lỗi đầu tiên, bảo đảm tổng đầu vào luôn bằng tổng bản trùng cộng bản hợp lệ cộng bản lưu vết cách ly:

| Thứ tự | Mã Quy tắc Kiểm định | Nguồn Kiểm tra | Điều kiện Phát hiện (`ANSI try_cast`) | Quy mô Ghi nhận (`quarantine_summary.csv`) | Hành động Chuẩn hóa |
|---:|---|---|---|---:|---|
| `1` | **`R1_NONPOSITIVE_SEGMENT_LENGTH`** | `segments.csv` | `try_cast(length AS DOUBLE) <= 0 OR NULL` | `154` đoạn (`57` cửa sổ liên quan) | Lưu vết lỗi chiều dài và điền trung vị `27.0m`. |
| `2` | **`R2_NON_ROAD_OSM_TAG`** | `segments.csv` | `street_type IN ('service','footway','path','steps','pedestrian','track')` | `427` đoạn tổng (`580` đoạn lỗi địa lý) | Lưu vết địa lý; trong `spark_batch.py` lọc tách `151` bản tin lối đi bộ (`33,394` cửa sổ). |
| `3` | **`R3_ADMIN_POLYGON_STREET_NAME`** | `streets / windows` | `street_name` khớp mẫu hành chính `^(Phường\|Quận)\b` | `631` cửa sổ (`1.89%`) | Giữ cho bài toán vận tốc đoạn đường nhưng loại khỏi giỏ khai phá cặp đường hay kẹt cùng lúc. |
| `4` | **`R4_UNICODE_NFD_AND_PREFIX_DRIFT`** | Chuẩn hóa tên | `street_name != NFC(collapse_spaces(name))` | `2,071` cửa sổ (`46` tên đường được hợp nhất) | Chuẩn hóa về dạng chính tắc `Unicode NFC` (`695` tên thô → `649` tên chuẩn). |
| `5` | **`R5_MISSING_MAX_VELOCITY`** | `segments / windows` | `max_velocity IS NULL OR max_velocity <= 0` | `28,495` cửa sổ (`85.21%`) & `74,762` đoạn (`88.34%`) | Điền khuyết không rò rỉ dữ liệu bằng quy tắc 3 bậc học duy nhất từ tập `TRAIN`. |
| `6` | **`R6_STATIONARY_OR_LATE_EVENT`** | `status / luồng` | `velocity < 1.0 km/h` (`517` bản tin) hoặc trễ quá `30 phút` (`501` cửa sổ) | `517` bản tin dừng xe & `501` cửa sổ đến muộn | Giữ bản tin dừng xe (phản ánh kẹt cứng `LOS F`); lưu bản tin trễ vào hồ dữ liệu thô chờ bù cuối ngày. |

> **Giải trình sự thống nhất toán học giữa `33,441` và `33,394` cửa sổ:** Trên toàn bộ `90,938` bản tin của `segment_status.csv`, luồng chuẩn hóa chính `pipeline.py` tạo ra đúng **`33,441` cửa sổ 30 phút** (khớp `100.0%` khóa với bảng đối chứng `train.csv`). Trong khi đó, tác vụ `spark_batch.py` lọc tách riêng `427` đoạn lối đi bộ/nội bộ trước khi gộp, qua đó tách đúng `151` bản tin thuộc các đoạn này (`90,938 - 151 = 90,787` bản tin đường cơ giới) và tạo ra **`33,394` cửa sổ** (chênh đúng `47` cửa sổ lối đi bộ, đạt sai lệch `0.0` trên toàn bộ mạng đường cơ giới).

### 2.4. Ước Lượng Tốc Độ Tối Đa Chỉ Học Từ Tập TRAIN & Quy Tắc Xác Định Mức LOS A–F

Để điền khuyết tốc độ tối đa mà tuyệt đối không nhìn trộm dữ liệu kiểm định, hàm `fit_train_vmax_lookup` chỉ trích xuất thống kê trên các cửa sổ thuộc tập `TRAIN` ($\tau \le \text{2020-12-12T08:00:00Z}$) và khóa cố định vào `outputs/vmax_lookup.json`:

![Công thức ước lượng tốc độ tối đa và quy đổi mức LOS](../../outputs/slide_charts/eq_vmax_los.png)

- **Phân bổ thực nghiệm 3 bậc ước lượng tốc độ tối đa:** Bậc 1a giữ tốc độ gốc hợp lệ cho **`4,946` cửa sổ** (`9,871` đoạn); Bậc 1b điền bằng phân vị `P85` lịch sử đoạn đường trên `TRAIN` ($n \ge 5$) cho **`8,164` cửa sổ** (`519` đoạn); Bậc 2 điền bằng trung vị cùng cấp và loại đường trên `TRAIN` cho **`20,124` cửa sổ** (`72,726` đoạn); Bậc 3 áp dụng chuẩn đô thị theo cấp đường `{1: 80, 2: 60, 3: 50, 4: 40} km/h` cho **`207` cửa sổ** (`1,517` đoạn).
- **Phân bố 6 mức phục vụ giao thông (`LOS A–F`) tính từ tỷ lệ vận tốc $r_{s,w} = \bar{v}_{s,w} / \hat{v}_{\max}(s)$:**
  - **`LOS A` ($r \ge 0.80$ — Thông thoáng tự do):** `13,405` cửa sổ (đối chứng `train.csv`: `13,278`).
  - **`LOS B` ($0.65 \le r < 0.80$ — Lưu thông ổn định tốc độ cao):** `2,571` cửa sổ (đối chứng: `4,700`).
  - **`LOS C` ($0.50 \le r < 0.65$ — Lưu lượng trung bình ổn định):** `4,858` cửa sổ (đối chứng: `3,839`).
  - **`LOS D` ($0.35 \le r < 0.50$ — Đông đúc giờ cao điểm):** `6,098` cửa sổ (đối chứng: `3,707`).
  - **`LOS E` ($0.20 \le r < 0.35$ — Ùn ứ nghiêm trọng):** `3,463` cửa sổ (đối chứng: `3,828`).
  - **`LOS F` ($r < 0.20$ hoặc $v = 0$ — Kẹt cứng tê liệt):** `3,046` cửa sổ (đối chứng: `4,089`). Nhãn nhị phân kẹt cứng `is_severe = 1` khi $\text{LOS} \in \{E, F\}$.

---

## Chương 3. Kiến Trúc Tổng Thể 4 Bước, Quy Trình Khóa Mô Hình Dùng Chung & Bước 1B Trên Apache Spark 3.5.7

### 3.1. Sơ Đồ Kiến Trúc Tổng Quan Dễ Đọc & Bảng Tham Chiếu 17 Mô-đun Kỹ Thuật

Để người đọc hình dung ngay luồng chảy dữ liệu xuyên suốt 4 bước chỉ trong vài giây, Hình 3.1 trình bày **Sơ đồ Kiến trúc Tổng quan** vận hành trên cụm `Docker Compose` gọn nhẹ **`10.5 / 16.0 GB RAM`** (thống nhất `Apache Kafka 4.1.0 KRaft`, **`Apache Spark 3.5.7 (Java 17)`**, hồ dữ liệu `Parquet`, `MongoDB 8.0.34` và `Streamlit :8501`):

![Hình 3.1. Sơ đồ kiến trúc tổng thể hệ thống theo luồng 4 bước](../../outputs/system_architecture.png)

*Hình 3.1. Sơ đồ kiến trúc tổng thể hệ thống theo luồng 4 bước nối tiếp nhau từ bản tin GPS thô và bản đồ đường bộ TP.HCM đến cảnh báo trực tuyến dưới 1 ms và đối soát cuối ngày.*

#### Bảng 3.1a — Bảng Tham Chiếu Chi Tiết 17 Mô-đun Kỹ Thuật Trong Mã Nguồn (`[C1]–[C17]`)

| Mã Mô-đun | Zone | Tên Thành phần Kỹ thuật | Cơ chế Thực thi Cốt lõi | Tệp Mã nguồn Chính | Kết quả Đầu ra |
|---|---|---|---|---|---|
| **`[C1]`** | Zone A | `GPS Telemetry Producer` | Phát lại sự kiện (`seed=42`, `1%` trùng, `5%` trễ) | `producer.py` | Topic `hcmc-traffic-events`, `replay_stream_summary.json` |
| **`[C2]`** | Zone A | `HCMC GIS Topology` | 3 bảng địa lý gốc (`nodes`, `streets`, `segments`) | `raw_data/*.csv` | Mạng lưới `52,425` giao lộ & `84,633` đoạn đường một chiều |
| **`[C3]`** | Zone A | `Traffic Consumers` | Trung tâm `TOC`, Kỹ sư đèn, Chuyên viên `Sở GTVT` | Trình duyệt Web | Truy cập giao diện điều hành tại `http://localhost:8501` |
| **`[C4]`** | Zone B | `Kafka 4.1.0 KRaft Broker` | Chế độ `KRaft` không cần ZooKeeper, `3` partitions | `docker-compose.yml` | Chia đều tải theo khóa `segment_id` |
| **`[C5]`** | Zone B | `Quality Funnel & Validator` | Cổng kiểm tra chung Batch/Stream: `ANSI try_cast`, `NFC`, `UTC+7` | `traffic_data/spark_etl.py`, `starter_stream.py`, `pipeline.py` | Chuyển bản ghi sạch sang `[C6]`/`[C9]` & cách ly lỗi vào `[C15]` |
| **`[C6]`** | Zone C | `Watermark (30m) & Dual Sink` | Ngưỡng `Watermark 30m`, tách song song lưu thô & chấm nhanh | `starter_stream.py`, `spark_engine.py` | Nhánh 1 lưu thô `100%` & Nhánh 2 chấm điểm tức thời |
| **`[C7]`** | Zone C | `O(1) Stream Sketches (33.0 KiB)` | `BitPackedBloomFilter` (`32.0 KiB`), `HLL++` (`1.0 KiB`), `EWMA` $\alpha=0.15$ | `starter_stream.py`, `pipeline.py` | Chặn tin trùng, đếm đường hoạt động & xuất `hotspot_alerts.csv` |
| **`[C8]`** | Zone C | `Online Scoring Engine` | Chỉ đọc `[C13a]/[C13b]` kèm kiểm tra `SHA-256` mỗi micro-batch | `starter_stream.py` | Xuất cảnh báo tạm thời `STREAM_PROVISIONAL (v1)` |
| **`[C9]`** | Zone D | `5-Tuple Monoid & 2-Stage Salting` | `5-Tuple Monoid Combiner`, `8` salt buckets, `BroadcastHashJoin` | `spark_batch.py`, `spark_engine.py`, `baseline.py` | `33,441` cửa sổ chuẩn, `monoid_baseline_verification.json` |
| **`[C10]`** | Zone D | `Line-Graph PageRank & 7D K-Means` | `Line-Graph PageRank`, `Spark FPGrowth`, `2-Pass SON`, `7D K-Means` | `pipeline.py`, `spark_engine.py` | `road_pagerank_top.csv`, `co/lagged_congestion_rules.csv` |
| **`[C11]`** | Zone D | `Causal Priors & +30m Forecast` | Ghép đúng `exact t+30m`, cắt mẫu biên, `Random Forest` (`23D`) | `pipeline.py`, `spark_engine.py` | `ablation_results.csv`, `transition_evaluation.csv`, khóa `[C13a]` |
| **`[C12]`** | Zone D | `Two-Tier Reconciliation` | Khớp đúng hạn `100%`, backfill `501` cửa sổ trễ & re-score `377` cửa sổ | `pipeline.py`, `starter_stream.py` | `reconciliation_audit.csv`, nâng cấp bản chuẩn `v2` |
| **`[C13a]`** | Zone E | `Frozen Model Registry` | Chỉ học `.fit()` đúng một lần duy nhất ở `Phase A` | `pipeline.py` | `model.joblib`, `spark_pipeline_model/`, `vmax_lookup.json` |
| **`[C13b]`** | Zone E | `Static Feature Store` | Điểm `PageRank` và cụm `7D K-Means` cho đủ `84,633` đoạn đường | `pipeline.py` | `data/lakehouse/segment_static_features.parquet` |
| **`[C14]`** | Zone F | `Medallion Lakehouse (Parquet)` | Lưu trữ cột nén `Snappy` chia `10` phân vùng tháng `year_month` | `pipeline.py`, `spark_batch.py` | `bronze_raw_events`, `silver_cleaned_windows`, `curated_gold` |
| **`[C15]`** | Zone F | `Quarantine & WAL Checkpoints` | Lưu vết mã lỗi đầu tiên, `offset_log.json`, `state_wal.json` | `starter_stream.py`, `spark_engine.py` | `quarantine_events.parquet`, `query_progress.json` |
| **`[C16]`** | Zone F | `MongoDB 8.0.34 Serving Store` | `Idempotent Upsert`, `Monotonic Version Guard` & `Compound Index IXSCAN` | `pipeline.py`, `starter_stream.py` | Bảng `bda501.traffic_scores` (`33,441` bản ghi), `mongo_verify.json` |
| **`[C17]`** | Zone F | `Streamlit 5-Tab UI (:8501)` | Kết nối trực tiếp `MongoDB` làm mới `5 giây` | `app.py` | Cổng điều hành trực tuyến tại `http://localhost:8501` |

### 3.2. Quy Trình Ba Pha Không Xung Đột & Khóa Chữ Ký Toàn Vẹn SHA-256

Để ngăn xung đột ghi giữa luồng trực tuyến và xử lý lô cuối ngày, hệ thống tách bạch rõ 3 pha vận hành:
1. **`Pha A — Huấn luyện một lần trên lịch sử`:** Chỉ duy nhất `[C10]` và `[C11]` được gọi `.fit()` trên tập `TRAIN` (`slot_ts <= 2020-12-12T08:00:00Z` và `target_ts <= 2020-12-12T08:00:00Z`, gồm `5,494` mẫu hợp lệ), sau đó khóa chữ ký `SHA-256` vào `outputs/model_registry_manifest.json`:
   - `outputs/model.joblib` (`sha256 = 97a5a63e4e347092894e270f32aa0cb74b4fe565364657c76cbf3b3b15c7142c`)
   - `data/lakehouse/segment_static_features.parquet` (`sha256 = 2877a4dd11022e353c6d7277e6e7e87a049e6e585c33a0ece7d2d430308b0251`, phủ đủ `84,633` đoạn đường)
   - `outputs/vmax_lookup.json` (`sha256 = b4c29c687368d64aa5015b7865f66bfd634cf1cf9154bbf93a4d58296379f3a4`)
2. **`Pha B — Chấm điểm luồng trực tuyến chỉ đọc`:** Tiến trình `starter_stream.py` kiểm tra chữ ký `SHA-256` trước và sau mỗi lô nhỏ, chỉ gọi `.predict_proba()` để xuất cảnh báo tạm thời (`status_version = 1`).
3. **`Pha C — Đối soát cuối ngày & bù dữ liệu trễ`:** Quét lại hồ dữ liệu thô, bù đủ `501` cửa sổ đến trễ, cập nhật lại biến lịch sử cho `3,953` cửa sổ kế tiếp, chấm điểm lại `377` cửa sổ bằng chính mô hình đã khóa ở `Pha A` và nâng cấp lên bản chuẩn (`status_version = 2`).

*Hình 3.2. Tách biệt rạch ròi quyền huấn luyện tại Pha A và quyền chấm điểm chỉ đọc tại Pha B, Pha C.*

### 3.3. Bước 1B — Gộp Song Song Chính Xác Bằng Bộ 5 Đại Lượng & Chia 8 Giỏ Chống Lệch Tải

Khi gộp dữ liệu trên nhiều máy tính song song, nếu mỗi máy tự tính trung bình vận tốc cục bộ rồi lấy trung bình cộng lần hai, sự chênh lệch số bản tin giữa các phân vùng sẽ gây sai số lớn (thử nghiệm trên 3 phân vùng lệch kích thước `[1, 2, 3]` gây sai số tới **`11.111111 km/h`**). Để gộp song song chính xác tuyệt đối, hệ thống nâng mỗi bản tin vận tốc hợp lệ $v > 0$ lên bộ `5` đại lượng giao hoán và kết hợp (`5-Tuple Commutative Monoid`: số bản tin, tổng vận tốc, vận tốc nhỏ nhất, vận tốc lớn nhất, số bản tin kẹt):

![Bộ 5 đại lượng giao hoán và kết hợp](../../outputs/slide_charts/eq_monoid.png)

- **Kiểm chứng đối chiếu độc lập (`baseline.py`):** Trên toàn bộ `90,938` bản tin GPS (`33,441` cửa sổ, tích lũy `42,170` bản tin kẹt), kết quả gộp song song khớp `100%` với `baseline.py` (**sai số bằng `0.0`** trên cả 5 đại lượng và vận tốc trung bình).
- **Cân bằng tải giờ cao điểm bằng chia `8` giỏ khóa (`2-Stage Key Salting`):** Khung giờ cao điểm nhất chứa `2,062` bản tin trong khi khung trung vị chỉ có `3.0` bản tin (lệch tải **`687.43` lần**). Ở Bước 1, hệ thống chia ngẫu nhiên tất định vào `8` giỏ $b \in \{0,\dots,7\}$ tạo `59,895` nhóm trung gian, kéo tỷ số lệch tải xuống chỉ còn **`1.023` lần (`CV = 0.0057`)**. Ở Bước 2, hệ thống bỏ giỏ và cộng gộp bộ 5 đại lượng về đúng `33,441` cửa sổ (`exceptAll == 0`).

### 3.4. Tối Ưu Hóa Kế Hoạch Thực Thi Trên Apache Spark 3.5.7 & Hồ Dữ Liệu Parquet 10 Tháng

| Hạng mục Kiểm chứng trên `Apache Spark 3.5.7` | Kết quả trên `spark_engine.py` (`spark_runtime_evidence.json`) | Kết quả trên `spark_batch.py` (`quyen_validation/manifest.json`) |
|---|---|---|
| **Ghép bảng bản đồ tĩnh (`BroadcastHashJoin`)** | Chuyển từ `SortMergeJoin` (`10` bước truyền mạng `Shuffle`, `0.946s`) sang **`BroadcastHashJoin` (`2` bước `Exchange`, `0` `SortMergeJoin`, `0.506s`)** | Chuyển từ `SortMergeJoin` (`5` bước `Shuffle`, `0.932s`) sang **`3` nút `BroadcastHashJoin` (`0` bước `Shuffle`, `0.666s`)** |
| **Đối chiếu tương đương tập hợp (`exceptAll`)** | `dsl_except_sql_count = 0` và `sql_except_dsl_count = 0` (`33,441` dòng) | `join_difference = {left_only: 0, right_only: 0}` và `monoid_difference = 0` (`33,394` dòng) |
| **Cắt tỉa phân vùng theo tháng** | `PartitionFilters: [year_month = 2020-08]` — chỉ đọc đúng **`7,294` dòng** của tháng `2020-08` thay vì quét `33,441` dòng | Xuất chuẩn `10` phân vùng tháng `year_month=2020-07..2021-04` định dạng `Parquet Snappy` |
| **Chỉ đọc đúng cột cần thiết** | Chỉ nạp `4 / 59` cột từ đĩa (`segment_id, date, period, LOS`) | Chỉ nạp `2` cột từ đĩa (`segment_id, mean_velocity`) |

![Hình 3.3. Cân bằng tải giờ cao điểm và loại bỏ truyền mạng dư thừa](../../outputs/slide_charts/chart_spark_skew_catalyst.png)

*Hình 3.3. Kết quả cân bằng tải giờ cao điểm bằng chia 8 giỏ khóa (trên) và loại bỏ truyền mạng dư thừa nhờ `BroadcastHashJoin` trên các bảng bản đồ tĩnh (dưới).*

---

## Chương 4. Bước 2 — Lấp Đầy "Vùng Mù" 88.15% Đường Thiếu GPS Bằng Đồ Thị PageRank & Phân Cụm K-Means

> **Sợi dây liên kết nhân quả từ Chương 3 sang Chương 5:**
> Sau khi hoàn tất Bước 1 ở Chương 2–3, hệ thống mới chỉ có số đo vận tốc trên `10,027` đoạn đường có xe gửi GPS, còn **`74,606` đoạn đường nhánh (`88.15%` mạng lưới TP.HCM) hoàn toàn không có dữ liệu cảm biến**. Chương 4 không phải là các thống kê rời rạc mà thực hiện **hai nhiệm vụ nối tiếp trực tiếp**:
> 1. **Cung cấp bức tranh quy hoạch cho Sở GTVT:** Tìm các "nút cổ chai" tự nhiên do cấu trúc đấu nối giao lộ (`PageRank`), nhận diện các cặp đường hay kẹt dây chuyền (`SON / FPGrowth`), và phân loại hành vi cho toàn bộ `74,606` đoạn đường thiếu GPS (`K-Means`).
> 2. **Tạo biến đầu vào không gian cho mô hình dự báo ở Chương 5:** Điểm `pagerank_score`, số nhánh vào/ra (`in_degree`, `out_degree`) và mã nhóm hành vi `cluster_id` từ Chương 4 được đóng gói vào kho đặc trưng bản đồ tĩnh và **ghép trực tiếp vào vector `23` biến đầu vào của mô hình `Random Forest` ở Chương 5**.

### 4.1. Lọc Bỏ Điểm Uốn Cong Trung Gian & Xếp Hạng Nút Cổ Chai Bằng Đồ Thị PageRank

- **Lọc bỏ điểm uốn cong trung gian trên bản đồ:** Trong `577,967` điểm tọa độ của `nodes.csv` trích xuất từ OpenStreetMap, có tới **`525,542` điểm (`90.93%`) chỉ là điểm hình học trung gian nằm dọc thân đường để vẽ độ uốn cong** (*shape points*), hoàn toàn không phải ngã rẽ giao lộ. Hệ thống lọc bỏ toàn bộ các điểm vẽ cong này và giữ lại đúng **`52,425` giao lộ thực sự**.
- **Dựng đồ thị nối đoạn đường một chiều:** Mỗi đoạn đường trong **`84,633` đoạn đường một chiều** trở thành một đỉnh của đồ thị; một cung có hướng $(u \to s)$ được nối khi điểm cuối đoạn $u$ trùng với điểm đầu đoạn $s$, tạo thành **`151,929` hướng rẽ hợp lệ** và **`48` đoạn đường cụt** không có lối ra. Thuật toán `PageRank` chạy `25` vòng lặp ($\beta = 0.85$) có chia đều lại xác suất của `48` đoạn cụt để bảo toàn tổng điểm toàn mạng đúng bằng **`84,633.0`**:

![Công thức đệ quy PageRank bảo toàn khối lượng](../../outputs/slide_charts/eq_pagerank.png)

| Hạng | `segment_id` | Tuyến Đường | Cấp Đường | Nhánh Vào / Ra | Điểm `PageRank` | Ý nghĩa Giao thông Thực tế tại TP.HCM (`UC-3`) |
|---:|---:|---|---:|---:|---:|---|
| `#1` | `61781` | **Phú Định** | Cấp `4` | `3 → 2` | **`3.7515`** | Cửa ngõ hội tụ luồng xe tải và kho bãi ven kênh Quận 6 – Quận 8 |
| `#2` | `50017` | **Lê Lai** | Cấp `4` | `2 → 2` | **`3.4650`** | Trục gom lưu lượng hướng thẳng về chợ Bến Thành – trung tâm Quận 1 |
| `#3` | `62682` | **Châu Văn Liêm** | Cấp `3` | `3 → 2` | **`2.9246`** | Hành lang thương mại huyết mạch kết nối khu vực Chợ Lớn – Quận 5 |
| `#4` | `21969` | **Nguyễn Kiệm** | Cấp `2` | `3 → 1` | **`2.7313`** | Nút thắt cổ chai chụm 3 nhánh vào 1 hướng về vòng xoay Tân Sơn Nhất |
| `#5` | `34723` | **Quốc lộ 1** | Cấp `1` | `2 → 1` | **`2.6430`** | Vành đai vận tải hàng hóa liên tỉnh chịu tải trọng xe đầu kéo lớn |
| `#6` | `69417` | **Lê Văn Chí** | Cấp `4` | `3 → 2` | **`2.6031`** | Tuyến kết nối khu công nghệ cao và cụm đại học cửa ngõ Thủ Đức |
| `#7` | `17428` | **Lê Văn Chí** | Cấp `4` | `3 → 2` | **`2.6031`** | Đoạn đối xứng phân luồng tại giao lộ Lê Văn Chí – Linh Trung |
| `#8` | `50018` | **Lê Lai** | Cấp `4` | `3 → 1` | **`2.5843`** | Nhánh tiếp cận trực tiếp giao lộ Phạm Hồng Thái – Ngã sáu Phù Đổng |
| `#9` | `21160` | **Phạm Hồng Thái** | Cấp `4` | `3 → 1` | **`2.5843`** | Nút chuyển tiếp dòng xe nội đô Quận 1 từ hành lang Lê Lai |
| `#10` | `77224` | **Kha Vạn Cân** | Cấp `4` | `3 → 3` | **`2.5599`** | Giao điểm trung chuyển mật độ cao dọc hành lang đường sắt Thủ Đức |

### 4.2. Khai Phá Các Cặp Đường Hay Kẹt Cùng Lúc (`SON / FPGrowth`) & Lưu Ý Trung Thực Về Cỡ Mẫu Nhỏ

- Mỗi khung `30 phút` tạo thành một giỏ gồm tất cả các tuyến đường đã chuẩn hóa dấu `Unicode NFC` (loại bỏ tên `Phường/Quận`) có ít nhất một đoạn đang kẹt cứng (`LOS E/F`), giữ nguyên toàn bộ giỏ thực tế (tối đa **`57` tuyến đường/khung**).
- Trên `Apache Spark 3.5.7`, thuật toán `FPGrowth` tìm ra **`286` tập phổ biến** và **`335` luật kết hợp**; thuật toán phân tán `2-Pass SON` trích xuất `30` luật đồng kẹt xe hai chiều (`outputs/co_congestion_rules.csv`) và `20` luật kẹt nối tiếp sau `30 phút` (`outputs/lagged_congestion_rules.csv`):

| Loại Tương tác | Cặp Tuyến Đường Điển hình | Số Cửa sổ Chung | Tần suất | Độ tin cậy | Hệ số `Lift` | Ý nghĩa Tham khảo Điều tiết (`UC-4`) |
|---|---|---:|---:|---:|---:|---|
| **Đồng thời (`t ↔ t`)** | `Lý Chính Thắng ↔ Nguyễn Thông` *(chiều ngược: `66.67%`)* | `8` cửa sổ | `0.0233` | `100.00%` | **`28.5833x`** | Hai trục giao cắt hành lang kênh Nhiêu Lộc – Quận 3 kẹt cùng pha |
| **Đồng thời (`t ↔ t`)** | `Cầu vượt Lý Thái Tổ - NTP ↔ Lý Thái Tổ` *(ngược: `50.0%`)* | `8` cửa sổ | `0.0233` | `80.00%` | **`17.1500x`** | Ùn ứ lan tỏa trực tiếp giữa mặt cầu vượt và nhánh dẫn bên dưới |
| **Đồng thời (`t ↔ t`)** | `Cao Thắng ↔ Cống Quỳnh` *(chiều ngược: `55.56%`)* | `10` cửa sổ | `0.0292` | `83.33%` | **`15.8796x`** | Liên thông cụm giao lộ Quận 3 – Quận 1 qua Ngã sáu Cộng Hòa |
| **Đồng thời (`t ↔ t`)** | `Hải Thượng Lãn Ông ↔ 3 Tháng 2` *(chiều ngược: `50.0%`)* | `12` cửa sổ | `0.0350` | `85.71%` | **`12.2500x`** | Tương tác lưu lượng thương mại Chợ Lớn sang trục Quận 10 |
| **Đồng thời (`t ↔ t`)** | `Cộng Hòa ↔ Trường Chinh` *(chiều ngược: `64.00%`)* | `16` cửa sổ | `0.0466` | `88.89%` | **`12.1956x`** | Cặp trục xương sống cửa ngõ sân bay Tân Sơn Nhất – Mũi Tàu |
| **Trễ (`t → t+30p`)** | `Nguyễn Ngọc Lộc (t) → Hải Thượng Lãn Ông (t+30p)` | `5` cửa sổ | `0.0127` | `83.33%` | **`32.9167x`** | Giả thuyết sóng ùn tắc lan từ Quận 10 xuống trục Quận 5 |
| **Trễ (`t → t+30p`)** | `Lê Quang Định (t) → Trần Quốc Toản (t+30p)` | `5` cửa sổ | `0.0127` | `41.67%` | **`32.9167x`** | Giả thuyết dòng xe từ Bình Thạnh đổ vào nội đô Quận 3 |
| **Trễ (`t → t+30p`)** | `Nguyễn Văn Đậu (t) → Trần Hưng Đạo (t+30p)` | `5` cửa sổ | `0.0127` | `100.00%` | **`24.6875x`** | Giả thuyết tín hiệu báo trước 30 phút trên hành lang xuyên tâm |
| **Trễ (`t → t+30p`)** | `Hải Thượng Lãn Ông (t) → 3 Tháng 2 (t+30p)` | `7` cửa sổ | `0.0177` | `77.78%` | **`18.0719x`** | Giả thuyết dịch chuyển ùn tắc liên quận khu vực Tây Nam |

> **Nhận định khoa học thẳng thắn về hạn chế cỡ mẫu nhỏ:**
> Dù các luật lan truyền sau `+30 phút` đạt hệ số `Lift` rất cao (`18.07x` đến `32.92x`), mỗi cặp thực chất mới cùng xuất hiện trong **`5` đến `8` cửa sổ 30 phút** (tương đương **`2.5` đến `4.0` giờ quan sát** trong 10 tháng) do độ phủ GPS gốc thưa (`11.85%`). Với cỡ mẫu nhỏ như vậy, chưa thể loại trừ sự trùng hợp ngẫu nhiên vào giờ cao điểm chung. Vì vậy, báo cáo chỉ sử dụng các luật trễ này như **giả thuyết cảnh báo thăm dò** kết hợp đối chiếu trên đồ thị đường bộ chứ không khẳng định là quy luật nhân quả cứng.

### 4.3. Phân 6 Nhóm Hành Vi Đường Phố Bằng K-Means (Học Trên TRAIN) Phủ Kín 84,633 Đoạn Đường

Pipeline `StandardScaler + KMeans(k=6, random_state=42)` chỉ học trên **`6,926` đoạn đường thuộc tập `TRAIN`** trong không gian 7 đặc trưng chuẩn hóa `(mean_los_ordinal, severe_rate, mean_velocity, velocity_ratio, length_m, max_velocity, pagerank_score)`, đạt **`Silhouette = 0.2853`**, **`Davies-Bouldin = 1.1181`** và **`Calinski-Harabasz = 2,044.85`**. Nhờ đó, toàn bộ **`84,633` đoạn đường** (gồm `10,027` đoạn có GPS và **`74,606` đoạn thiếu GPS**) được gán nhóm hành vi đầy đủ tại `data/lakehouse/segment_static_features.parquet`:

![Hình 4.1. Bản đồ 6 nhóm hành vi đường phố K-Means phủ kín 84,633 đoạn đường TP.HCM](../../outputs/slide_charts/map_hcmc_clusters_7d.png)

*Hình 4.1. Bản đồ không gian 6 nhóm hành vi đường phố (`K-Means`, $k=6$) phủ kín `100%` mạng lưới `84,633` đoạn đường TP.HCM trên nền sông Sài Gòn và các quận, tách bạch rõ giao lộ nội đô kẹt mãn tính (đỏ), đại lộ tốc độ cao (xanh đậm), hành lang thông thoáng (xanh lá) và đường nhánh dân cư (xám).*

| Nhóm Đường | Cửa sổ Quan sát | Vận tốc TB | Tốc độ Tối đa TB | Tỷ lệ Kẹt `E/F` | `PageRank` TB | Đoạn Có / Thiếu GPS | Đặc tính Hành vi Giao thông Thực tế (`UC-5`) |
|---|---:|---:|---:|---:|---:|---:|---|
| **Nhóm 0** | `1,130` | `19.12 km/h` | `52.68 km/h` | `54.16%` | `0.6372` | `2,803` / `56,846` | Đường nhánh và hẻm dân cư tốc độ trung bình thấp, dễ nghẽn cục bộ |
| **Nhóm 1** | `2,420` | `10.52 km/h` | `36.36 km/h` | **`96.98%`** | `1.0219` | `946` / `1,406` | Giao lộ nội đô kẹt mãn tính (đoạn ngắn `~26.8m` sát ngã tư lớn) |
| **Nhóm 2** | `19,006` | **`44.25 km/h`** | `50.14 km/h` | `3.54%` | `1.0041` | `349` / `3,448` | Hành lang lưu thông thông thoáng ổn định (`LOS A/B` chiếm ưu thế) |
| **Nhóm 3** | `300` | `14.86 km/h` | `44.31 km/h` | `81.33%` | `0.9796` | `2,672` / `4,123` | Đoạn đường dài (`~188.3m`) chịu tải dồn ứ kéo dài giờ cao điểm |
| **Nhóm 4** | `4,460` | `24.68 km/h` | **`79.26 km/h`** | `33.59%` | **`1.1077`** | `2,842` / `7,276` | Trục đại lộ / quốc lộ cấp 1 tốc độ thiết kế cao, `PageRank` lớn |
| **Nhóm 5** | `6,125` | `19.10 km/h` | `38.88 km/h` | `41.53%` | `0.9967` | `415` / `1,507` | Tuyến phố thương mại cấp 3–4 dao động mạnh theo khung giờ |

---

## Chương 5. Bước 3 — Mô Hình Random Forest Dự Báo Trước 30 Phút & Đối Chứng Trực Diện Với Baseline

### 5.1. Ghép Cặp Quan Sát Cách Đúng 30 Phút & Cắt Sạch Biên Thời Gian

1. **Ghép cặp đúng `+30 phút` thay vì trượt dòng tuần tự:** Lệnh trượt dòng `shift(-1)` tạo ra `23,414` cặp nhưng có tới `15,516` cặp (`66.27%`) bị ngắt quãng từ `60 phút` đến nhiều ngày. Hệ thống chỉ ghép nhãn khi cùng một `segment_id` có quan sát ở đúng thời điểm `target_ts = slot_ts + 30 phút`, thu được **`7,898` cặp chuẩn xác `+30 phút`**.
2. **Chia tập theo mốc thời gian toàn cục `70 / 15 / 15` kèm cắt sạch mẫu biên (`target_ts <= cutoff_ts`):**
   - **Giai đoạn khởi tạo:** `slot_ts <= 2020-08-02T23:30:00Z` (cố định tâm cụm `K-Means` và tỷ lệ kẹt nền ban đầu).
   - **Tập `TRAIN` (`70%`):** `slot_ts <= 2020-12-12T08:00:00Z` và `target_ts <= 2020-12-12T08:00:00Z` → **`5,494` mẫu hợp lệ** (đã cắt `50` mẫu sát biên có nhãn vắt sang `VAL`).
   - **Tập `VAL` (`15%`):** `2020-12-12T08:00:00Z < slot_ts <= 2020-12-25T03:30:00Z` và `target_ts <= 2020-12-25T03:30:00Z` → **`1,167` mẫu hợp lệ** (đã cắt `5` mẫu sát biên có nhãn vắt sang `TEST`).
   - **Tập `TEST` độc lập (`15%`):** `slot_ts > 2020-12-25T03:30:00Z` → **`1,182` mẫu kiểm thử** (`185` mẫu kẹt cứng `LOS E/F`).

### 5.2. Tỷ Lệ Kẹt Tích Lũy Quá Khứ & Tổ Hợp 23 Biến Đầu Vào Của Mô Hình

Tại mỗi thời điểm dự báo $t$, tỷ lệ kẹt lịch sử của đoạn đường $s$ và tuyến đường $r(s)$ chỉ cộng dồn trên các khung giờ quá khứ chặt $\tau < t$:

![Công thức tỷ lệ kẹt tích lũy quá khứ](../../outputs/slide_charts/eq_prior.png)

Kiểm thử tự động đảo trộn ngẫu nhiên toàn bộ dữ liệu tương lai $\tau > t$ xác nhận cả `23` biến đầu vào tại thời điểm $t$ giữ nguyên tuyệt đối (`max_abs_diff = 0.0`):

| Nhóm Biến Đầu vào (`Tổng = 23 biến`) | Danh sách Biến & Mức Đóng góp | Tổng Trọng số |
|---|---|---:|
| **1. Trạng thái hiện tại & lịch sử gần (`7` biến)** | `current_los_ordinal` (`0.5051`), `mean_velocity` (`0.2236`), `trailing_severe_rate` (`0.0623`), `lag1_los_ordinal` (`0.0035`), `velocity_ratio` (`0.0033`), `lag2_los_ordinal` (`0.0004`), `obs_count` (`0.0000`) | **`79.82%`** |
| **2. Chu kỳ giờ Việt Nam & hình học đường (`7` biến)** | `period_sin` (`0.1018`), `period_cos` (`0.0156`), `length_m` (`0.0035`), `weekday` (`0.0004`), `street_level` (`0.0003`), `max_velocity` (`0.0001`), `gap_minutes` (`0.0001`) | **`12.18%`** |
| **3. Đặc trưng bản đồ từ Bước 2 & tỷ lệ quá khứ (`9` biến)** | `segment_train_severe_prior` (`0.0492`), `segment_train_prior_los` (`0.0197`), `pagerank_score` (`0.0033`), `street_causal_prior_los` (`0.0023`), `street_causal_prior_sev` (`0.0022`), `neighbor_los_ordinal` (`0.0016`), `in_degree` (`0.0005`), `out_degree` (`0.0005`), `cluster_id` (`0.0004`) | **`8.00%`** |

### 5.3. Lựa Chọn Mô Hình Duy Nhất Qua Tập Kiểm Định VAL, Bảng Thử Nghiệm 7 Cấu Hình & Ma Trận 6 Mức

Trên tập kiểm định `VAL` (`1,167` mẫu), hệ thống đối chiếu 7 cấu hình ứng viên và chọn ra **một mô hình đa lớp duy nhất để triển khai xuyên suốt hệ thống**: cấu hình **`#5 Random Forest (+PageRank, Tỷ lệ quá khứ & K-Means, 23 biến)`** đạt kết quả tốt nhất trên tập `VAL` (**`VAL ROC-AUC = 0.9593`**, **`VAL PR-AUC = 0.8704`**, độ chính xác 6 mức `= 0.9529`) và được khóa cố định. Bảng dưới đây trình bày kết quả chấm một lần trên `1,182` mẫu `TEST` độc lập (`outputs/ablation_results.csv`):

| Cấu hình Thử nghiệm (Mô hình vận hành chính: `#5 Random Forest 23 biến`) | Số biến | Đúng 6 mức | `Macro-F1` | `Prec Kẹt` | `Rec Kẹt` | `F1 Kẹt` | `ROC-AUC` | `PR-AUC` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **1. Baseline (`Đoán LOS(t+30p) = LOS(t) hiện tại`)** | `1 biến` | `0.9484` | `0.5654` | `0.9365` | `0.9568` | `0.9465` | `0.9867` | `0.9367` |
| **2. Cây quyết định `Decision Tree` (`Chỉ biến cục bộ`)** | `14 biến` | `0.9425` | `0.3794` | `0.9362` | `0.9514` | `0.9437` | `0.9856` | `0.9482` |
| **3. Cây quyết định `Decision Tree` (`+PageRank & K-Means`)** | `23 biến` | `0.9425` | `0.3772` | `0.9362` | `0.9514` | `0.9437` | `0.9844` | `0.9327` |
| **4. `Random Forest` (`Chỉ biến cục bộ & giờ Việt Nam`)** | `14 biến` | `0.9425` | `0.5455` | `0.9409` | `0.9459` | `0.9434` | `0.9866` | `0.9630` |
| **5. `Random Forest` (`+PageRank, Tỷ lệ quá khứ & K-Means`) [Mô hình chính]** | **`23 biến`** | **`0.9450`** | **`0.5543`** | **`0.9358`** | **`0.9459`** | **`0.9409`** | **`0.9882`** | **`0.9471`** |
| **6. `GBT` Nhị phân (`Chỉ báo kẹt cứng E/F, biến cục bộ`)** | `14 biến` | `N/A` | `N/A` | `0.6667` | `0.9514` | `0.7840` | `0.9812` | `0.9328` |
| **7. `GBT` Nhị phân (`+PageRank, Tỷ lệ quá khứ & K-Means`)** | `23 biến` | `N/A` | `N/A` | `0.9365` | `0.9568` | `0.9465` | `0.9843` | `0.9419` |

#### Bảng 5.4 — Ma trận phân loại 6 mức của mô hình chính `Random Forest (23 biến)` trên `1,182` mẫu `TEST`

| Nhãn Thực tế \ Dự báo | Dự báo `LOS A` | Dự báo `LOS B` | Dự báo `LOS C` | Dự báo `LOS D` | Dự báo `LOS E` | Dự báo `LOS F` | Tổng Thực tế |
|---|---:|---:|---:|---:|---:|---:|---:|
| **Thực tế `LOS A` (Thông thoáng)** | **`944`** | `9` | `2` | `0` | `0` | `4` | **`959`** (`81.13%`) |
| **Thực tế `LOS B` (Tốc độ cao)** | `3` | **`4`** | `1` | `0` | `1` | `1` | **`10`** (`0.85%`) |
| **Thực tế `LOS C` (Ổn định)** | `6` | `0` | **`3`** | `1` | `1` | `4` | **`15`** (`1.27%`) |
| **Thực tế `LOS D` (Đông đúc)** | `2` | `1` | `2` | **`7`** | `1` | `0` | **`13`** (`1.10%`) |
| **Thực tế `LOS E` (Ùn ứ nặng)** | `1` | `1` | `0` | `3` | **`6`** | `9` | **`20`** (`1.69%`) |
| **Thực tế `LOS F` (Kẹt cứng)** | `1` | `1` | `1` | `2` | `7` | **`153`** | **`165`** (`13.96%`) |

![Hình 5.1. So sánh hiệu năng cảnh báo kẹt cứng LOS E/F và phân loại 6 mức LOS A–F](../../outputs/slide_charts/chart_ablation_t30m.png)

*Hình 5.1. So sánh trực quan hiệu năng cảnh báo kẹt cứng (`ROC-AUC`, `PR-AUC`, `F1`) và phân loại 6 mức `LOS A–F` giữa các ứng viên trên tập `TEST`.*

### 5.4. So Sánh Trực Diện Giữa Baseline Và Random Forest (23 Biến) Khi Giao Thông Thay Đổi Trạng Thái

Nhìn vào bảng tổng gộp `1,182` mẫu `TEST`, cách đoán đơn giản **`Baseline`** (hiện tại đường ở mức nào thì đoán `30 phút` nữa giữ nguyên mức đó) đạt độ chính xác 6 mức tới **`94.84%`** (nhỉnh hơn `94.50%` của `Random Forest` đúng `4` mẫu). Lý do rất tự nhiên: **`94.84%` thời gian (`1,121 / 1,182` mẫu) đoạn đường giữ nguyên mức `LOS` cũ sau 30 phút** (trong đó riêng đường thông thoáng `LOS A` đã chiếm `81.13%`, còn các mức trung gian `B, C, D, E` chỉ có `10–20` mẫu mỗi mức).

Tuy nhiên, ở **`5.16%` thời điểm giao thông thực sự bắt đầu chuyển trạng thái** (`61` trường hợp từ thoáng sang đông/kẹt hoặc ngược lại), ở những đoạn gửi GPS ngắt quãng, và trên **`88.15%` đoạn đường thiếu GPS**, bảng đối chứng trực diện 1-đối-1 dưới đây (không pha trộn bất kỳ mô hình phụ nào khác) cho thấy trung thực cả điểm mạnh lẫn giới hạn thực nghiệm của **`Random Forest (23 biến)`** so với **`Baseline`**:

#### Bảng 5.5 — Đối chứng trực diện 1-đối-1 giữa `Baseline` và mô hình chính `Random Forest (23 biến)`

| Tình huống Kiểm định Thực tế | `Baseline` (Giữ nguyên trạng thái hiện tại) | Mô hình chính: `Random Forest (23 biến)` | Kết luận Thực tiễn & Ghi nhận Khoa học Trung thực |
|---|---|---|---|
| **1. Nhóm giao thông đổi trạng thái** (`LOS(t) ≠ LOS(t+30p)`, **`61 / 1,182` mẫu TEST**) | **Đoán đúng mức mới: `0 / 61` (`0.00%`)** · Khả năng phát hiện biến động `ROC-AUC = 0.5000` · `PR-AUC` kẹt cứng `= 0.5408` · Sai số `Brier = 0.3279` | **Phát hiện biến động: `ROC-AUC = 0.7664`** · Đoán đúng mức mới: **`9 / 61` (`14.75%`** với ngưỡng xác suất `τ = 0.30` chọn trên `VAL`; mặc định thận trọng đạt `2 / 61 = 3.28%`) · Xếp đúng hướng mới ở vị trí `#1` đạt **`24 / 61` (`39.34%`)** · `PR-AUC = 0.5708` · `Brier = 0.2948` | `Baseline` mù hoàn toàn (đoán sai `100%`) mỗi khi giao thông đổi pha; `Random Forest` phát hiện sớm tín hiệu rủi ro (`ROC-AUC = 0.7664`) và giảm `10.1%` sai số xác suất. |
| **2. Đường đang thoáng (`A–D`) đột ngột hóa kẹt cứng (`E–F`)** (chỉ có **`8 / 993` mẫu TEST**) | **Bắt trúng ở ngưỡng cứng: `0 / 8` (`0.00%`)** · Gán xác suất kẹt cứng `= 0%` như nhau cho mọi đoạn đường đang mức `A–D` | **Bắt trúng ở ngưỡng cứng mặc định: `0 / 8` (`0.00%`** do cỡ mẫu quá hiếm `0.81%`, xác suất cao nhất đạt `12.2%`) · Xét về xếp hạng xác suất: đạt **`ROC-AUC = 0.8136`** và đẩy xác suất rủi ro trung bình trên 8 ca sắp kẹt **cao gấp `9.35x`** đường thoáng ổn định (`3.46%` so với `0.37%`) | *Ghi nhận trung thực:* Với vỏn vẹn `8` ca chuyển đột ngột trong tập `TEST`, ngưỡng phân lớp cứng chưa bắt kịp, nhưng tín hiệu xác suất của `Random Forest` vẫn tăng gấp `9.35` lần để cảnh báo sớm. |
| **3. Tín hiệu GPS ngắt quãng trên `30 phút`** (**`3,513` mẫu TEST**, `47.45%` đổi mức) | `PR-AUC` kẹt cứng `= 0.6369` · `ROC-AUC = 0.7850` (quán tính cũ sụp đổ khi mất tín hiệu lâu) | **`PR-AUC` kẹt cứng `= 0.7082` (`+7.13%`)** · **`ROC-AUC = 0.8427` (`+5.77%`)** (đối chiếu chuẩn lịch sử: `PR-AUC = 0.7628`) | Trên `66.27%` trường hợp xe gửi tín hiệu thưa, chu kỳ giờ Việt Nam + `PageRank` + `K-Means` giúp `Random Forest` vượt xa `Baseline`. |
| **4. Mạng lưới đường thiếu cảm biến GPS** (`74,606 / 84,633` đoạn = **`88.15%`** TP.HCM) | **Độ phủ dự báo `= 0.0%`** (bất lực hoàn toàn vì không có mức `LOS` hiện tại trên đường) | **Độ phủ dự báo `= 100.0%` (`84,633 / 84,633` đoạn)** nhờ ghép trực tiếp kho đặc trưng bản đồ tĩnh từ Bước 2 | Chỉ duy nhất `Random Forest (23 biến)` mới chấm điểm rủi ro được cho `88.15%` mạng lưới đường nhánh chưa có xe gửi GPS. |

![Hình 5.2. So sánh trực diện Baseline và Random Forest ở các tình huống khó nhất](../../outputs/slide_charts/chart_transition_analysis.png)

*Hình 5.2. So sánh trực diện 1-đối-1 giữa `Baseline` và `Random Forest (23 biến)` ở các tình huống khó nhất (trên), đối chiếu với phân bố mẫu kiểm thử giải thích vì sao `94.84%` đường giữ nguyên trạng thái tĩnh (dưới).*

---

## Chương 6. Bước 4 — Cảnh Báo Trực Tuyến Gọn Nhẹ (33.0 KiB), Tự Động Bù Dữ Liệu Trễ & Giao Diện Điều Hành

### 6.1. Tiếp Nhận Luồng Kafka 4.1.0 KRaft, Chia Hai Nhánh Không Mất Tin Trễ & Tự Hồi Phục Sự Cố

- **Tái hiện luồng sự kiện thực tế (`producer.py`, `seed = 42`):** Phát `33,775` sự kiện cửa sổ (`33,441` gốc + `334` bản trùng, gồm `1,170` tin trễ dưới `30 phút` và `501` tin trễ quá `30 phút` trải trên `219` khung giờ) cùng `91,847` sự kiện bản tin GPS thô qua `3` phân vùng của `apache/kafka:4.1.0` (đã kiểm chứng truyền nhận thực tế `3,000` bản tin JSON).
- **Chia hai nhánh không mất dữ liệu:** Nhánh 1 ghi nguyên vẹn `100%` sự kiện từ `Kafka` vào hồ dữ liệu `Parquet` thô trước khi xét hạn trễ; Nhánh 2 áp dụng ngưỡng chờ `30 phút` để lọc trùng, chấm điểm tức thời và phát cảnh báo tạm thời (`v1`).
- **Tự hồi phục sau sự cố (`WAL`):** Khi ngắt cưỡng bức tiến trình `starter_stream.py --kill-after-batch 1`, hệ thống đọc lại `offset_log.json` và `state_wal.json`, tự động bỏ qua lô `1` đã xử lý và chạy tiếp lô `2`, lô `3` đạt đúng `32,940` sự kiện trong hạn với **`0` bản ghi bị nhân đôi**.

### 6.2. Vì Sao Cần Thêm Bộ Đếm Gọn 33.0 KiB & Điểm Nóng EWMA Bên Cạnh Random Forest?

Một câu hỏi rất thiết thực khi nối từ Chương 5 sang Chương 6 là: *Ở Chương 5 đã có mô hình `Random Forest` dự báo kẹt xe rồi, sao sang Chương 6 lại có thêm bộ lọc `Bloom`, `HyperLogLog++` và `Điểm nóng EWMA` để bắt đường Lý Thường Kiệt? Hai công cụ này khác gì nhau?*
- **`Random Forest` (Chương 5 — Dự báo tương lai `t + 30 phút`):** Trả lời câu hỏi *"30 phút nữa đoạn đường nào SẮP chuyển sang kẹt cứng?"* giúp kỹ sư tín hiệu đèn chủ động kéo dài đèn xanh phòng ngừa từ trước.
- **`Điểm nóng EWMA` ($\alpha = 0.15$ — Giám sát độ lì hiện tại):** Trả lời câu hỏi *"Ngay lúc này, đoạn đường nào ĐANG kẹt cứng dai dẳng suốt nhiều khung 30 phút liên tiếp chưa giải tỏa được?"* (ví dụ đoạn `47407` Lý Thường Kiệt bị kẹt lặp đi lặp lại nhiều giờ khiến điểm tích lũy vọt lên `6.6652`, phân biệt rõ với một đoạn chỉ thoáng chậm lại 1 khung rồi thông ngay).
- **`BitPackedBloomFilter` (`32.0 KiB`) & `HyperLogLog++` (`1.0 KiB`):** Bảo vệ luồng trực tuyến khỏi bản tin gửi trùng và đếm nhanh số đoạn đường đang có xe chạy chỉ trong **`33.0 KiB` bộ nhớ cố định**, không làm phình RAM khi hệ thống chạy liên tục nhiều tháng:

![Công thức ba cấu trúc luồng gọn nhẹ 33.0 KiB](../../outputs/slide_charts/eq_sketches.png)

| Công cụ Luồng Trực tuyến | Cấp phát Bộ nhớ Vật lý Gọn nhẹ | Kết quả Kiểm chứng Thực tế (`metrics.json` & `hotspot_alerts.csv`) |
|---|---|---|
| **1. Bộ lọc trùng `BitPackedBloomFilter`** | $m = 262{,}144$ bit đóng gói trong mảng **`uint8[32768]` = đúng `32,768 Bytes (32.0 KiB)`**, $k = 4$ hàm băm | Chặn chính xác **`334 / 334` (`100.0%`)** bản tin trùng; tỷ lệ báo trùng nhầm thực tế chỉ **`0.616%`** (`< 2.55%` lý thuyết). |
| **2. Bộ đếm đường `HyperLogLogPlusPlus`** | $p = 10 \Rightarrow 1{,}024$ thanh ghi trong mảng **`uint8[1024]` = đúng `1,024 Bytes (1.0 KiB)`**, sai số chuẩn `3.25%` | Ước lượng **`9,627.9` đoạn đường hoạt động** so với `10,027` đoạn thực tế, đạt **sai số tương đối `3.98% < 5.0%`**. |
| **3. Bộ theo dõi điểm nóng `EWMA`** | Hệ số suy giảm $\alpha = 0.15$ mỗi khung `30 phút`, tự xóa khóa khi điểm $< \varepsilon = 10^{-4}$ (`4,556` khóa đang mở) | Phát hiện chính xác các điểm kẹt dai dẳng nhất TP.HCM: **#1 Đoạn `47407` Lý Thường Kiệt (`6.6652`)**, **#2 Đoạn `21263` Trương Định (`6.6635`)**, **#3 Đoạn `22684` Nguyễn Đình Chiểu (`6.6300`)**. |

### 6.3. Đối Soát Hai Tầng Cuối Ngày, Tự Động Bù Tin Trễ & Khóa Phiên Bản Chống Ghi Đè

1. **Tầng 1 (Đối chiếu dữ liệu đúng hạn `≤ 30 phút`):** Phép trừ tập hợp hai chiều `exceptAll` giữa luồng nhanh và xử lý lô trên toàn bộ **`32,940 / 32,940` cửa sổ đúng hạn** đạt **`0` dòng sai lệch (`100.0%` khớp tuyệt đối)**.
2. **Tầng 2 (Khôi phục tin trễ & cập nhật chuỗi lịch sử):** Cuối ngày, hệ thống quét lại hồ dữ liệu thô bù đủ **`501` cửa sổ đến trễ quá `30 phút`** trải trên **`219` khung giờ**, nâng độ đầy đủ từ **`98.50%` lên `100.00%`**; đồng thời cập nhật lại các biến lịch sử (`lag1_los_ordinal`, `lag2_los_ordinal`, `trailing_severe_rate`) cho **`3,953` cửa sổ kế tiếp** và **chấm điểm lại `377` cửa sổ dự báo** (điều chỉnh xác suất của `100` cửa sổ chính xác hơn).
3. **Khóa phiên bản tăng dần (`status_version`):** Kho phục vụ duy trì rõ **`488` bản tạm thời (`v1`)**, **`501` bản đã bù tin trễ (`v2`)** và **`32,452` bản đã xác thực (`v2`)**; cơ chế khóa phiên bản đã chặn đứng **`25 / 25` bản tin luồng cũ đến muộn** không cho ghi đè lùi lên kết quả chuẩn `v2`.

### 6.4. Kho Phục Vụ MongoDB 8.0.34 (< 1 ms) & Ảnh Cận Cảnh Giao Diện Điều Hành (:8501)

- **Truy vấn cảnh báo dưới `1 ms` trên `MongoDB 8.0.34` (`outputs/mongo_verify.json`):** Toàn bộ `33,441` bản ghi được ghi cập nhật an toàn qua `4` phân vùng (`69` lô × `500` lệnh `UpdateOne(upsert=True)`). Truy vấn `UC-2` lọc các trục chính cấp `1–2` có xác suất kẹt cứng `≥ 70%` khai thác chỉ mục kết hợp **`level_prob_segment_idx` (`{street_level: 1, severe_prob: -1, segment_id: 1}`)**, khớp `1,669` cửa sổ trục chính và trả `25` đoạn đầu qua `winning_stages = ["LIMIT", "PROJECTION_SIMPLE", "FETCH", "SORT_MERGE", "IXSCAN", "IXSCAN"]` mà không cần sắp xếp trong RAM, đạt độ trễ **`warm_median = 0.530 ms`** và **`warm_p95 = 0.636 ms`**.

![Hình 6.1. Vòng đời đối soát cuối ngày và độ trễ truy vấn MongoDB 8.0.34](../../outputs/slide_charts/chart_streaming_serving.png)

*Hình 6.1. Phân bố `33,441` cửa sổ theo vòng đời đối soát cuối ngày (trên) và độ trễ phản hồi dưới `1 ms` trên `MongoDB 8.0.34` (dưới).*

- **Ảnh chụp cận cảnh Giao diện Điều hành `Streamlit` 5 tab (`app.py`, chạy trực tiếp tại `http://localhost:8501`):** Thay vì thu nhỏ toàn trang làm mờ chữ, các hình dưới đây phóng to trực tiếp vào từng khung nghiệp vụ quan trọng nhất trên màn hình thực tế:

![Hình 6.2. Ảnh chụp cận cảnh Tab 1 trên giao diện điều hành: Bản đồ TP.HCM và bảng điểm nóng kẹt dai dẳng](../../outputs/slide_charts/ui_tab1_overview_hotspots.png)

*Hình 6.2. Ảnh chụp cận cảnh **Tab 1** của giao diện điều hành (`:8501`): hiển thị bản đồ giao thông TP.HCM có sông Sài Gòn và tên quận cạnh bảng xếp hạng các đoạn đường đang kẹt dai dẳng nhất (dẫn đầu là Lý Thường Kiệt).*

![Hình 6.3. Ảnh chụp cận cảnh Tab 2 kết nối trực tiếp MongoDB 8.0: Dự báo đúng pha chuyển E sang F tại Công Trường Quốc Tế](../../outputs/slide_charts/ui_tab2_live_forecast_mongo.png)

*Hình 6.3. Ảnh chụp cận cảnh khung dự báo trên **Tab 2** kết nối trực tiếp **`MongoDB 8.0` (`33,441` bản ghi)**: tại đoạn đường `21064 — Công Trường Quốc Tế` lúc `11:30` trưa đang ở mức `E`, cách đoán theo hiện tại giữ nguyên mức `E` (sai), trong khi `Random Forest (23 biến)` dự báo chính xác sau `30 phút` chuyển sang kẹt cứng **mức `F` (xác suất kẹt cứng `59.6%`, xác suất đổi trạng thái `73.0%`)** khớp hoàn toàn thực tế.*

![Hình 6.4. Ảnh chụp cận cảnh Tab 4: Tra cứu đoạn đường tương đồng cho vùng thiếu GPS](../../outputs/slide_charts/ui_tab4_kmeans_coldstart.png)

*Hình 6.4. Ảnh chụp cận cảnh công cụ tra cứu vùng thiếu GPS trên **Tab 4**: khi chọn đoạn `61781 — Phú Định` (chưa từng có dữ liệu GPS), hệ thống tìm ngay các đoạn có cảm biến mang hành vi tương đồng nhất (`Châu Văn Liêm`, `Nguyễn Kiệm`...). *

---

## Chương 7. Phân Công Đóng Góp Nhóm, Kiểm Thử Tự Động, Ngân Sách Tài Nguyên & Hướng Dẫn Chạy Lại

### 7.1. Phân Công Chuyên Trách & Kết Quả Bàn Giao Của Ba Thành Viên (`100% Hoàn thành`)

| Thành viên Thực hiện | Mảng Chuyên trách | Tệp Mã nguồn Chính | Kết quả Thực nghiệm Bàn giao |
|---|---|---|---|
| **1. Nguyễn Tạ Minh Quyền** (`MSSV: 25MS23280`) | **Bước 1: Làm sạch dữ liệu & Xử lý phân tán Spark** | `spark_batch.py`, `traffic_data/spark_etl.py`, `baseline.py`, `compose.quyen.yml`, `tests/test_quyen_spark.py` | Phễu kiểm định 5 bước với `ANSI try_cast`, chuẩn hóa dấu `Unicode NFC` & giờ Việt Nam (`UTC+7`), ước lượng tốc độ tối đa từ `TRAIN`, bộ gộp 5 đại lượng khớp `100%` `baseline.py`, chia `8` giỏ khóa (`687.43x → 1.023x`), `BroadcastHashJoin` (`0 SortMergeJoin`) và `10` phân vùng tháng `Parquet`. |
| **2. Nguyễn Thành Đạt** (`MSSV: 25MS23306`) | **Bước 2 & Bước 3: Đồ thị, Phân cụm & Dự báo `+30 phút`** | `pipeline.py`, `spark_engine.py`, bộ kiểm thử tự động `tests/` | Lọc `90.93%` điểm uốn cong trung gian, tính `PageRank` `84,633` đoạn đường, khai phá `Spark FPGrowth` (`335` luật) & `2-Pass SON`, phân `6` nhóm `K-Means` ($k=6$, `Silhouette = 0.2853`) phủ `74,606` đoạn thiếu GPS, ghép đúng `t+30p` (`7,898` cặp) + cắt mẫu biên, huấn luyện `Random Forest` (`ROC-AUC = 0.9882`), đối chứng trực diện `Baseline` – `Random Forest` và khóa chữ ký `SHA-256`. |
| **3. Phạm Công Minh** (`MSSV: 25MS23310`) | **Bước 4: Luồng trực tuyến, Đối soát tin trễ, MongoDB & Giao diện** | `producer.py`, `starter_stream.py`, `app.py`, `docker-compose.yml`, `scripts/*` | Cụm hạ tầng `Docker` gói gọn trong `10.5 / 16.0 GB RAM`, trình phát sự kiện `Kafka 4.1.0 KRaft` (`3` phân vùng), bộ chia hai nhánh chờ `30 phút`, lọc trùng `Bloom` `32.0 KiB` & `HLL++` `1.0 KiB`, hồi phục sự cố `WAL` (`0` bản nhân đôi), bù `501` cửa sổ trễ / cập nhật `3,953` cửa sổ kế tiếp / chấm lại `377` cửa sổ, `MongoDB 8.0.34` (`0.636 ms`) & giao diện `Streamlit` 5 tab. |

### 7.2. Hệ Thống Kiểm Thử Tự Động Toàn Diện & Tiêu Chuẩn Chất Lượng Mã Nguồn

Toàn bộ các bất biến toán học, điều kiện chống rò rỉ dữ liệu tương lai và giao thức vận hành đều được kiểm chứng tự động qua bộ kiểm thử **`pytest`** và **`Spark unittest`**:
1. Kiểm thử bảo toàn quân số phễu kiểm định chất lượng, ranh giới 6 mức `derive_los`, đổi ngày qua nửa đêm sang giờ Việt Nam và tính bất biến của `vmax_lookup.json` khi thay đổi tập `TEST`.
2. Kiểm thử tính kết hợp của bộ gộp 5 đại lượng trên các phân vùng lệch, tiêm lỗi vận tốc và xác thực kế hoạch thực thi `BroadcastHashJoin` + cắt tỉa phân vùng `Parquet`.
3. Kiểm thử bảo toàn tổng điểm `PageRank` có đoạn cụt (`84,633.0`), chuẩn hóa dấu `Unicode NFC`, giữ nguyên giỏ tuyến đường `SON` không cắt ngọn và tìm đoạn tương đồng cho vùng thiếu GPS trong cùng nhóm `K-Means`.
4. Kiểm thử từ chối bước nhảy `60 phút / 9 giờ` khi ghép cặp `t + 30 phút`, cắt mẫu biên `target_ts <= cutoff_ts`, bất biến biến đầu vào khi đảo trộn tương lai (`max_abs_diff = 0.0`), đối chứng `evaluate_transition_regimes` và chữ ký `SHA-256` của 3 tệp khóa cố định.
5. Kiểm thử kích thước đúng `32,768 Bytes` của bộ lọc `Bloom`, khóa phiên bản `status_version` chống ghi đè, hồi phục sự cố `WAL` (`--kill-after-batch 1`), và báo cáo trung thực `not_verified` khi tắt `MongoDB`. Mã nguồn đạt chuẩn **`Ruff`** (`0` lỗi, độ phức tạp hàm `McCabe <= 14`).

### 7.3. Ngân Sách Tài Nguyên Cụm Docker (`10.5 / 16.0 GB RAM`) & Từ Điển 59 Cột `data/dataset.csv`

| Dịch vụ Container (`docker-compose.yml`) | Ảnh Container Chuẩn hóa | Cổng Kết nối | Giới hạn RAM | Vai trò Vận hành trong Hệ thống |
|---|---|---|---:|---|
| **`kafka`** | `apache/kafka:4.1.0` (`KRaft Mode`) | `9092:9092` | `1.5 GB` | Hàng đợi tiếp nhận sự kiện `3` phân vùng cho chủ đề `hcmc-traffic-events` (`[C4]`) |
| **`mongo`** | `mongo:8.0` (`MongoDB 8.0.34`) | `27017:27017` | `2.0 GB` | Kho phục vụ trực tuyến `bda501.traffic_scores` với chỉ mục kết hợp `IXSCAN` (`[C16]`) |
| **`spark`** | `apache/spark:3.5.7-java17` (`PySpark 3.5.7`) | Nội bộ `bda501-net` | `6.0 GB` | Xử lý phân tán cho làm sạch, đồ thị, học máy & đối soát cuối ngày (`[C5]–[C12]`) *(tương thích chéo `PySpark 4.2.0 / Java 21`)* |
| **`app`** | `python:3.12-slim` (`Streamlit`) | `8501:8501` | `1.0 GB` | Giao diện điều hành trực tuyến 5 tab kết nối trực tiếp `MongoDB` (`[C17]`) |
| **Tổng cộng** | **4 Dịch vụ Container Cốt lõi** | — | **`10.5 GB`** | **Vận hành ổn định trên máy cá nhân `16.0 GB RAM` (dư `5.5 GB` cho Hệ điều hành)** |

- **Cấu trúc 59 cột của bảng dữ liệu chuẩn hóa `data/dataset.csv` (`33,441` dòng):**
  - *Nhóm Định danh & Tọa độ không gian (`10` cột):* `event_id, segment_id, street_id, street_name, street_type, street_level, long_snode, lat_snode, long_enode, lat_enode`.
  - *Nhóm Thời gian `UTC / Giờ VN` & Phân tập (`14` cột):* `date, ict_date, weekday, is_weekend, period, ict_period, period_idx, ict_period_idx, vn_hour, period_sin, period_cos, slot_ts, target_ts, split_set`.
  - *Nhóm Tổng hợp vận tốc & mức `LOS` (`13` cột):* `LOS, derived_los, los_ordinal, is_severe, mean_velocity, sum_velocity, min_velocity, max_velocity_obs, severe_ping_count, max_velocity, vmax_impute_tier, velocity_ratio, obs_count`.
  - *Nhóm Đồ thị, Phân cụm & Biến quá khứ (`16` cột):* `length_m, pagerank_score, in_degree, out_degree, lag1_los_ordinal, lag2_los_ordinal, trailing_severe_rate, neighbor_los_ordinal, segment_train_prior_los, segment_train_severe_prior, street_causal_prior_los, street_causal_prior_sev, gap_minutes, is_contiguous_30m, cluster_id, cluster`.
  - *Nhóm Nhãn dự báo đúng `+30 phút` & đối chiếu phụ (`6` cột):* `target_next_los_ordinal, target_next_severe, target_t30m_los_ordinal, target_t30m_severe, next_obs_los_ordinal, next_obs_severe`.

### 7.4. Hướng Dẫn Khởi Chạy & Tái Lập Toàn Bộ Kết Quả Thực Nghiệm (`PowerShell / Docker CLI`)

```powershell
# 1. Khởi chạy cụm dịch vụ Docker (Kafka 4.1.0 KRaft, MongoDB 8.0.34, Spark 3.5.7, Streamlit UI :8501)
docker compose up -d kafka mongo spark app

# 2. Thực thi trọn vẹn dây chuyền xử lý 4 bước & kiểm chứng đối chiếu độc lập bộ gộp 5 đại lượng
python pipeline.py
python baseline.py

# 3. Thực thi tác vụ Spark Batch độc lập từ dữ liệu thô trên container Apache Spark 3.5.7
$env:TRAFFIC_FIT_CUTOFF = '2020-08-01T00:00:00Z'
docker compose -f compose.quyen.yml run --rm batch

# 4. Phát luồng sự kiện Kafka & kiểm chứng tự hồi phục từ nhật ký WAL (--kill-after-batch 1)
python producer.py --mode both --bootstrap-servers localhost:9092
python starter_stream.py --kill-after-batch 1

# 5. Xuất bản Sơ đồ Kiến trúc, Bản đồ TP.HCM, Báo cáo PDF & Bộ Slide Thuyết trình (.pptx + .pdf)
python scripts/render_arch_diagram.py
python scripts/render_visual_assets.py
python scripts/build_proposal_pdf.py
python scripts/build_slides.py

# 6. Chạy bộ kiểm thử tự động toàn diện & kiểm tra chất lượng mã nguồn Ruff (max-complexity=14)
pytest -v
ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
```

### 7.5. Tổng Hợp `Limitations & Risks` Theo Từng Component, 4 Rủi Ro Chung Toàn Hệ Thống (Mục 11) & Lộ Trình 3 Giai Đoạn (Mục 12 — `Plan`)

| Component Kỹ thuật (Mục 1–10) | Hạn chế & Rủi ro Kỹ thuật Đặc thù (`Component-Level Limitations & Risks`) | Cơ chế Kiểm soát Đã Áp dụng & Kế hoạch Nâng cấp (`Plan`) |
|---|---|---|
| **1–2. Problem Scope & Literature Review** | Độ phủ cảm biến GPS cộng đồng chỉ đạt `11.85%` (`10,027 / 84,633` đoạn); `66.27%` cặp quan sát ngắt quãng $\ge 60$ phút khiến các mạng `Deep Spatio-Temporal GNN (DCRNN / ST-GCN)` bị suy biến chuỗi thời gian. | Sử dụng `Random Forest (23D)` kết hợp `Static Feature Store` (`Line-Graph PageRank` + `7D K-Means`) để vừa xử lý dữ liệu bảng đứt quãng, vừa phủ kín `100%` mạng lưới trong `10.5 GB RAM`. |
| **3. System Architecture (`Zones A–F`)** | Chạy chung 4 dịch vụ (`Kafka 1.5G`, `Spark 6.0G`, `Mongo 2.0G`, `UI 1.0G`) trên 1 máy chủ đơn tiềm ẩn tranh chấp `Disk I/O` khi Shuffle lớn; có độ trễ cập nhật biến trễ từ các gói tin đến muộn `> 30m` giữa `Phase B` và `Phase C`. | Giới hạn cứng `mem_limit` từng container; tách biệt quyền đọc/ghi `Zero-Conflict 3-Phase MLOps`; **Phase 3 Plan** nâng cấp lên cụm `Kubernetes Multi-Node`. |
| **4–5. Data, EDA & Data Pipeline** | `88.15%` đoạn đường `Cold-Start`; `60.79%` cửa sổ dùng tốc độ tối đa suy diễn tĩnh (`Tier 2 & Tier 3`) chưa phản ánh rào chắn thi công hay mưa ngập; rủi ro tăng đột biến `Quarantine [C15]` nếu thiết bị GPS đổi schema. | Áp dụng `Train-Only 3-Tier Imputation` chống rò rỉ dữ liệu; `ANSI try_cast` bảo toàn `100%` quân số bản ghi; **Phase 2 Plan** tích hợp API Thời tiết/Mưa ngập và cảnh báo tự động tỷ lệ `Quarantine`. |
| **6 & 9. Modeling & Experiments** | Ngưỡng `argmax` mặc định bảo thủ trên các pha chuyển trạng thái (`2/61` mặc định so với `9/61` ở $\tau = 0.30$); `Hard Recall = 0 / 8` ở ca hiếm đang thoáng (`A–D`) đột ngột hóa kẹt cứng (`E–F`, chỉ chiếm `0.81%` mẫu `TEST`), dù xác suất rủi ro tăng `9.35x` (`ROC-AUC = 0.8136`). | Báo cáo minh bạch cả kết quả ngưỡng mặc định và ngưỡng hiệu chỉnh trên `VAL`; **Phase 1 Plan** bổ sung hàm phạt nhạy chi phí (`Cost-Sensitive Focal Loss`) và ngưỡng động theo cấp đường. |
| **7. Batch Processing (`Spark & Graph`)** | Luật lan truyền trễ `+30m` của `SON / FPGrowth` chỉ xuất hiện trong `5–8` cửa sổ (`2.5–4.0` giờ quan sát); phân cụm `7D K-Means` cho `74,606` đoạn `Cold-Start` dựa trên hình thái tĩnh. | Gắn nhãn `Exploratory Hypothesis` rõ ràng trên giao diện; kết hợp `Line-Graph PageRank` bảo toàn khối lượng `84,633.0`. |
| **8 & 10. Stream Processing & Demo (`:8501`)** | Bộ lọc `BitPackedBloomFilter (32.0 KiB)` sẽ tăng `FPR` nếu chạy liên tục nhiều ngày không xoay vòng bit; bản tin trễ `> 30m` phải chờ `Tier-2 Reconciliation` cuối ngày; giao diện `:8501` mới dừng ở hỗ trợ quyết định (`Human-in-the-Loop`). | **Phase 1 Plan** xoay vòng `Ring-Buffer Sliding Bloom Filter` mỗi 30 phút; **Phase 3 Plan** kết nối trực tiếp API dự báo `+30m` vào tủ điều khiển đèn tín hiệu thích ứng `SCATS`. |

- **Bốn Rủi ro & Hạn chế Chung Toàn Hệ thống (`System-Wide Limitations & Risks` — Mục 11):**
  1. **Thiên lệch đại diện phương tiện (`Fleet Representation Bias`):** Nguồn GPS cộng đồng nghiêng về ô tô/xe thương mại trên các trục chính (`11.85%`), chưa đại diện đầy đủ cho luồng xe máy (`> 80%` lưu lượng TP.HCM) và mạng lưới hẻm dân cư.
  2. **Trôi dạt phân phối mở (`Open-World Concept Drift`):** Trọng số mô hình và `Static Feature Store` đóng băng ở `Phase A` sẽ suy giảm khi có lô cốt thi công mới, phân luồng giao thông dịp lễ hoặc mưa ngập diện rộng.
  3. **Khan hiếm mẫu chuyển pha đột ngột (`Rare Sudden-Onset Scarcity`):** Chỉ có `8 / 993` mẫu `TEST` (`0.81%`) chuyển đột ngột từ thoáng (`A–D`) sang kẹt cứng (`E–F`), đòi hỏi thu thập thêm luồng dữ liệu liên tục nhiều mùa.
  4. **Trần hạ tầng máy chủ đơn (`Single-Host Deployment Ceiling`):** Cụm Docker `10.5 / 16.0 GB RAM` đáp ứng trọn vẹn mẫu chuẩn `Proof-of-Scale` (`38.5 MB`), nhưng khi mở rộng lên `1.5–3.0 TB/ngày` toàn đô thị cần chuyển sang kiến trúc `Kubernetes Multi-Node` và phân mảnh (`Sharding`) kho `MongoDB 8.0`.

