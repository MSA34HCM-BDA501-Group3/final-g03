# Hướng Dẫn Chạy Live & Kiểm Chứng 100% Tiêu Chí Session 9 (BDA501 — Nhóm 3)

> **Mục đích:** Sổ tay thực chiến để nhóm **mở sẵn cạnh cửa sổ PowerShell + trình duyệt (`http://localhost:8000`)** khi bảo vệ **Session 10**. Toàn bộ câu lệnh bên dưới đã được **chạy kiểm chứng trực tiếp trên máy này (`Windows PowerShell` + `Docker Compose` + `Spark 3.5.7` + `MongoDB 8.0` + `Kafka 4.1`)**.

---

## 0. Kiểm Tra Trước Giờ Trình Bày (Pre-Flight — 60 Giây)

Mở PowerShell tại `d:\Dat\FSB\big_data\groupexe-knowledgetest` và chạy:

```powershell
cd d:\Dat\FSB\big_data\groupexe-knowledgetest
docker compose ps
```

**Kết quả chuẩn bắt buộc thấy đủ 5 container đang `Up` (`healthy`):**
1. `groupexe-knowledgetest-kafka-1` (`127.0.0.1:9092`, `healthy`)
2. `groupexe-knowledgetest-mongo-1` (`127.0.0.1:27017`, `healthy`)
3. `groupexe-knowledgetest-spark-1` (`hcmc-traffic-spark:3.5.7`, `Up`)
4. `groupexe-knowledgetest-toc-web-1` (`127.0.0.1:8000`, `healthy`)
5. `groupexe-knowledgetest-live-consumer-1` (`Up`)

> **⚠️ Nếu cụm chưa chạy:** Chạy `docker compose up -d` và chờ ~15 giây rồi mở trình duyệt tại **`http://localhost:8000`**.

> **🚨 2 BẪY LỆNH TUYỆT ĐỐI TRÁNH KHI DEMO LIVE TRÊN WINDOWS:**
> 1. **KHÔNG chạy `python spark_batch.py` trực tiếp trên Windows host** (máy host không cài `pyspark` — `HOST PYSPARK = False`). Mọi lệnh Spark **BẮT BUỘC** chạy qua `docker compose exec -T spark python3 ...`.
> 2. **KHÔNG gõ `mongosh --eval "... $in ..."` bằng dấu ngoặc kép `"..."` trên PowerShell** vì PowerShell sẽ hiểu `$in` là biến và báo lỗi cú pháp. Hãy **copy nguyên khối `@' ... '@ | docker compose exec -T mongo mongosh ...`** ở Mục 6 bên dưới.

---

## 1. Bảng Đối Chiếu Nhanh Tiêu Chí Session 9 (Deliverables A–I & Card B)

| Mục Session 9 | Tiêu chí Giảng viên chấm | Trạng thái | Mở chỗ nào khi Thầy hỏi? |
| :--- | :--- | :---: | :--- |
| **Deliverable A** | Sơ đồ kiến trúc phân tách rõ `[I] Implemented` vs `[P] Proposed` + Trace 1 bản ghi đầu–cuối | ✅ PASS | File [`evidence/session9/A_architecture.png`](./evidence/session9/A_architecture.png) + **UI Tab 06** (Kiến trúc Lambda 2 làn) |
| **Deliverable B** | Thực nghiệm kiểm soát hiệu năng Spark (3 cấu hình: Baseline / V1 BHJ / V2 Salting, Median ± IQR, `exceptAll = 0`) | ✅ PASS | **UI Tab 05** (Bảng 3A & 3B) + [`evidence/session9/B_benchmark.csv`](./evidence/session9/B_benchmark.csv) |
| **Deliverable C** | Spark Query Plan (`explain`) chứng minh `SortMergeJoin ➔ BroadcastHashJoin (5 ➔ 1 Exchange)` & `Parquet PartitionFilters` | ✅ PASS | Chạy **Lệnh 4A** (8 giây) hoặc mở [`evidence/session9/C_query_plan.txt`](./evidence/session9/C_query_plan.txt) |
| **Deliverable D** | Kiểm định dữ liệu (`R1–R6`, `0 Silent Drops`, khớp `baseline.py` sai số `0.0`, đọc lại Parquet + SHA-256) | ✅ PASS | Chạy **Lệnh 3** (`python baseline.py`, 1 giây) + **UI Tab 05** (Bảng 4) |
| **Deliverable E** | Kiểm định ML 7 mô hình (Ablation + Giải mã Nghịch lý Persistence trên 61 ca đổi pha + McNemar `p = 0.001953`) | ✅ PASS | **UI Tab 05** (Bảng 1 & 2) + [`outputs/ablation_results.csv`](./outputs/ablation_results.csv) |
| **Deliverable F** | NoSQL MongoDB 8.0 (Access pattern, Compound ESR `IXSCAN`, `0 SORT`, `$status_version` Guard) + Streaming (`Kafka`, `33 KiB Sketches`, `Watermark 30p`, `WAL 0 dupes`) | ✅ PASS | Chạy **Lệnh 6** (`mongosh`, 2 giây) + **UI Tab 02** (`⚡ Stress-Test 200 tin`) + **Lệnh 7** (`kafka-topics.sh`) |
| **Deliverable G** | Kiến trúc triển khai Docker (`5` services, `11 GB RAM`) ➔ Cloud K8s `[P]` + Reproducibility (`seed=42`, `44/44` tests) | ✅ PASS | [`evidence/session9/G_deployment.md`](./evidence/session9/G_deployment.md) + **Lệnh 8** (`pytest`) |
| **Deliverable H (Card B)** | **B1:** Ma trận Point-in-Time 23 biến `τ ≤ t` (`0` leakage)<br>**B2:** Phản biện PageRank (`#21969` thoáng vs `#34019` kẹt) & 7D K-Means phi liền kề<br>**B3:** Độ thưa `88.13%` Cold-Start & sai số Sketch (`HLL++ 3.75%`, `Bloom 0.0389%`) | ✅ PASS | **B1:** UI Tab 05 (Bảng 6)<br>**B2:** UI Tab 03 (Bảng trên cùng) & Tab 04<br>**B3:** UI Tab 05 (Bảng 7) & [`evidence/session9/H_project_card.md`](./evidence/session9/H_project_card.md) |
| **Deliverable I** | Slide tổng kết Session 9 Readiness (đủ 5 dòng bắt buộc) + Điểm tự đánh giá `95/100` | ✅ PASS | **Slide 17** trong [`HCMC_Smart_Traffic_Final_Presentation.pptx`](./HCMC_Smart_Traffic_Final_Presentation.pptx) & [`HCMC_Smart_Traffic_Session9_Readiness_Slide.pptx`](./HCMC_Smart_Traffic_Session9_Readiness_Slide.pptx) |

---

## 2. Kịch Bản 8 Bước Chạy Live Theo Đúng Trình Tự Section 10 Lab Manual

> Khi thầy yêu cầu **"Chạy trực tiếp kiểm chứng giúp tôi"**, chỉ cần bám đúng 8 bước ngắn dưới đây (tổng thời gian nếu chạy đủ cả 8 bước chỉ **~2–3 phút**; nếu thầy hỏi riêng phần nào thì nhảy thẳng vào đúng bước đó).

### Bước 1 — Kiến Trúc `[I]` vs `[P]` & 5 Container Đang Chạy (~10 giây)
* **Mở UI:** Tab **`⚡ 06 · Luồng Thực Thi Pipeline (Live)`** tại `http://localhost:8000` (chỉ rõ làn trên màu tím **Speed/Stream `v1`**, làn dưới màu xanh **Batch `v2`**, 2 mũi tên nét đứt **MLOps Read-Only**, và điểm hội tụ **MongoDB 8.0**).
* **Mở file bằng chứng `[I]` vs `[P]`:** [`evidence/session9/A_architecture.png`](./evidence/session9/A_architecture.png).
* **Chạy kiểm chứng cụm sống:**
```powershell
docker compose ps
```

---

### Bước 2 — Trace 1 Đoạn Đường Thực Tế Từ Input ➔ Output (~20 giây)
* **Mở UI Tab 06:** Chọn thẻ mẫu **`#50026 Điện Biên Phủ (TEST · Trúng thẳng E ➔ F)`**, bấm **`▶ Chạy Từng Bước`** hoặc click lần lượt `Stage 1 ➔ 3 ➔ 5 ➔ 6 ➔ 7`.
* **Chuyển sang kiểm chứng mù trên Màn 1:** Bấm **`🔍 Mở trên Bản đồ Tab 01`** ➔ bấm **`🙈 Ẩn Thực Tế (Chế độ Kiểm Chứng Mù)`** ➔ cho thầy xem dự báo `E ➔ F (P = 87.2%)` ➔ bấm mở lại **`👁️ Hiện Thực Tế`** để đối chiếu khớp `F`.

---

### Bước 3 — Chứng Minh MapReduce / Pure 5-Tuple Commutative Monoid (~1 giây)
Chạy đối chứng thuật toán gộp Monoid `(cnt, sum_v, min_v, max_v, severe_cnt)` với bộ tham chiếu thuần Python (chứng minh 0 sai lệch & bẫy *Average-of-Averages*):

```powershell
python baseline.py
```

**Các dòng vàng cần chỉ cho Thầy trên Terminal:**
* `"raw_distinct_utc_windows": 33394` và `"medallion_matched_windows": 33394` (`100.0%` khớp).
* `"obs_count_discrepancies": 0`, `"mean_velocity_discrepancies": 0`, `"max_abs_velocity_diff": 0.0`.
* `"naive_average_of_averages_abs_error": 11.111111` (chứng minh gộp trung bình ngây thơ bị sai tới `11.11 km/h`, còn 5-Tuple Monoid sai số `0.0`).

---

### Bước 4 — Chạy Live Apache Spark 3.5.7: Query Plan, Parquet Pruning & 5 Bài Test Spark (~8–33 giây)

#### 4A. Chạy Live Spark 3.5.7 Physical Plan (`Parquet Partition Pruning` — ~8 giây)
Chứng minh Spark chỉ đọc đúng `1/10` thư mục tháng (`year_month = 2020-08`) và cắt cột (`ReadSchema` chỉ 4 cột):

```powershell
@'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Session9LiveCheck").master("local[4]").config("spark.sql.shuffle.partitions", "8").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet("/workspace/data/lakehouse/curated_gold").filter("year_month = '2020-08'").select("segment_id", "date", "period", "LOS")
print("GOLD_2020_08_ROWS:", df.count())
df.explain("formatted")
spark.stop()
'@ | docker compose exec -T spark python3 -
```

**Chỉ cho Thầy 3 dòng kết quả in ra:**
* `GOLD_2020_08_ROWS: 7268`
* `PartitionFilters: [isnotnull(year_month), (year_month = 2020-08)]` (bỏ qua 9/10 phân vùng tháng)
* `ReadSchema: struct<segment_id:bigint,date:string,period:string,LOS:string>` (columnar projection)

#### 4B. Chạy Live Bộ 5 Kiểm Thử Spark Data Engineering (`Monoid`, `Vmax`, `Query Plans`, `Quarantine` — ~33 giây)
Nếu Thầy muốn xem Spark chạy thực thi test thật cả `BroadcastHashJoin`, `Salted Monoid`, `Quality Funnel`:

```powershell
docker compose exec -T spark python3 -m pytest -v tests/test_quyen_spark.py
```
**Kết quả mong đợi:** `5 passed in ~33s`.

> *Xem nhanh full Physical Plan `SortMergeJoin (5 Exchanges)` vs `BroadcastHashJoin (1 Exchange)` đã lưu sẵn:* mở [`evidence/session9/C_query_plan.txt`](./evidence/session9/C_query_plan.txt) hoặc **UI Tab 05 (Bảng 3A & 3B)**.

---

### Bước 5 — Chứng Minh ML Ablation, Nghịch Lý Persistence & Card B1/B2/B3 (~15 giây)
* **Trên UI Tab 05 (`🛡️ 05 · Spark 3.5.7 & Ablation`):**
  * **Bảng 1:** 7 mô hình trên `1,182` cặp TEST độc lập (`RF 23D`: `ROC-AUC = 0.9894`, `PR-AUC = 0.9543`, `Acc = 92.55%`, `Severe-F1 = 0.9362`).
  * **Bảng 2:** Giải mã Nghịch lý Persistence — trên **`61` ca thực sự đổi pha**, Persistence đoán trúng **`0.00%`**, còn `RF 23D` đạt **`16.39%` (raw) / `22.95%` (hiệu chuẩn VAL)** (`McNemar p = 0.001953`).
  * **Bảng 6 (Card B1):** `23/23` biến đầu vào `PASS` Point-in-Time `τ ≤ t` (`Future-Shuffle diff = 0.0`).
  * **Bảng 7 (Card B3):** Sai số `1 KiB HLL++ = 3.75%`, `32 KiB Bloom FPR = 0.0389%`.
* **Chạy kiểm thử ML chống rò rỉ & chuyển pha bằng CLI (~6 giây):**
```powershell
python -m pytest -v -k "d01_d02 or transition_regime_evaluation or session9_evidence"
```

---

### Bước 6 — Chứng Minh Live NoSQL MongoDB 8.0 (`33,394` Docs + `IXSCAN` + `0 SORT` — ~2 giây)
Chạy truy vấn trực tiếp vào container `mongo:8.0` kiểm chứng Access Pattern `UC-2` (Top-25 cảnh báo trục chính `level ∈ {1, 2}`, `severe_prob >= 0.70`):

```powershell
@'
const ex = db.traffic_scores.find({street_level:{$in:[1,2]}, severe_prob:{$gte:0.70}}).sort({severe_prob:-1, segment_id:1}).limit(25).explain("executionStats");
const planStr = JSON.stringify(ex.queryPlanner.winningPlan);
printjson({
  total_docs: db.traffic_scores.countDocuments(),
  arterial_severe_docs: db.traffic_scores.countDocuments({street_level:{$in:[1,2]}, severe_prob:{$gte:0.70}}),
  indexes: db.traffic_scores.getIndexes().map(i => i.name),
  uses_IXSCAN: planStr.includes("IXSCAN"),
  uses_ESR_index: planStr.includes("level_prob_segment_idx"),
  in_memory_SORT: planStr.includes('"SORT"'),
  nReturned: ex.executionStats.nReturned,
  totalKeysExamined: ex.executionStats.totalKeysExamined,
  totalDocsExamined: ex.executionStats.totalDocsExamined,
  executionTimeMillis: ex.executionStats.executionTimeMillis
});
'@ | docker compose exec -T mongo mongosh bda501 --quiet
```

**Kết quả vàng cần chỉ cho Thầy:**
* `total_docs: 33394`, `arterial_severe_docs: 1234`
* `uses_IXSCAN: true`, `uses_ESR_index: true`, `in_memory_SORT: false`
* `nReturned: 25`, `totalKeysExamined: 26`, `totalDocsExamined: 25`, `executionTimeMillis: 0` (< 1 ms!).

---

### Bước 7 — Chứng Minh Live Streaming (`Kafka 4.1` 3 Partitions + `33 KiB Sketches` + `WAL Recovery` — ~15 giây)

#### 7A. Kiểm chứng Topic & 3 Partitions trên Kafka 4.1 KRaft (~2 giây)
```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hcmc-traffic-events
```
**Kết quả mong đợi:** `Topic: hcmc-traffic-events`, `PartitionCount: 3` (`Partition 0, 1, 2` đều `Leader: 1`, `Isr: 1`).

#### 7B. Bơm 200 bản tin chạy live trên UI (Kafka ➔ Consumer Sketches 33 KiB ➔ MongoDB v1 ➔ Bản đồ)
* Mở **UI Tab `📡 02 · Live Kafka & Sketches 33 KiB`** tại `http://localhost:8000`.
* Nhấn **`⚡ Stress-Test 200 tin`** — cho Thầy thấy trực tiếp:
  * Chia tải đều 3 partition (`P0 / P1 / P2`).
  * **32 KiB Bloom Filter** chặn bản tin trùng, **Watermark 30p** giữ lại tin trễ cho Tier-2, **1 KiB HLL++** ước lượng số đoạn hoạt động khớp sát Exact.
  * **Bản đồ Live Streaming** tự chấm tối đa 45 đoạn `v1` ngay tại Tab 02 mà không đè sang Tab 01.
  * *(Lưu ý giải thích nếu Thầy hỏi về con số `6,000` tin trong báo cáo Session 9: `6,000` là kết quả benchmark tĩnh đã lưu trong `F_serving_stream.md`, còn nút trên UI chạy nhanh `200` tin live để kịp thời lượng thuyết trình).*

#### 7C. Nếu Thầy hỏi thêm về Crash Recovery WAL (`0 Dupes`) (~4 giây)
```powershell
python starter_stream.py --kill-after-batch 1
```
*(Chứng minh giả lập kill sau lô 1 ➔ khởi động lại đọc `WAL`, phục hồi đúng offset với `duplicate_side_effects_on_restart = 0`).*

---

### Bước 8 — Kiểm Thử Tái Lập Toàn Bộ Hệ Thống & Chốt Giới Hạn Trung Thực

* **Chạy nhanh kiểm thử chất lượng code + bộ test chính (~6 giây cho kiểm thử trọng yếu, hoặc ~100 giây nếu chạy full cả 44 test):**
```powershell
# Kiểm tra lint + độ phức tạp McCabe <= 14 (~2 giây):
python -m ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"

# Chạy nhanh cụm test trọng yếu TOC + Session 9 + Leakage (~6 giây):
python -m pytest -v -k "toc_fastapi or regression_toc or session9_evidence or d01_d02"

# (Chỉ chạy khi Thầy yêu cầu full suite — mất ~100 giây):
python -m pytest -v
```
* **3 giới hạn trung thực luôn sẵn sàng trả lời (Card B + Slide 15 & 17):**
  1. **Vùng mù Cold-Start (`88.13%` = `74,211/84,206` đoạn có 0 GPS):** Hệ thống tuyệt đối không bịa vận tốc GPS, chỉ trả về **Structural Prior** gắn nhãn `[COLD_START_STRUCTURAL_PRIOR]` + Top-8 đường có GPS tương đồng từ 7D K-Means (`Masked-GPS Proxy`: `Acc 58.63%`, `ROC-AUC 0.7000`, `PR-AUC 0.5152`).
  2. **Mất cân bằng lớp trung gian & khởi phát kẹt hiếm:** `TEST Macro-F1 = 0.4253` do lớp `B–E` chỉ có `10–20` mẫu; khởi phát đột ngột từ thông thoáng sang kẹt nặng chỉ có `8/993` ca TEST (`Onset ROC-AUC = 0.8438`, `3.26x` risk lift, ngưỡng cứng VAL `τ = 0.25` bắt `0/8` ca).
  3. **Dữ liệu phát luồng là Historical Replay (Proof-of-Scale `38.50 MB`):** Không giả danh là luồng GPS trực tiếp từ Sở GTVT TP.HCM; phạm vi luồng viễn thông `8–17` tỷ tin/ngày và điều khiển đèn `SCATS` được tách rõ nhãn **`[P] Proposed`**.

---

## 3. Phân Công Chạy Demo Nhanh Theo 3 Thành Viên

| Thành viên | Mảng cầm máy / trả lời khi Thầy hỏi | Lệnh / Tab phụ trách |
| :--- | :--- | :--- |
| **Nguyễn Tạ Minh Quyền** (`25MS23280`) | Spark 3.5.7 ETL, Phễu `R1–R6`, Monoid 5-Tuple, Salting, Parquet Pruning | **Bước 3** (`baseline.py`), **Bước 4A–4B** (`docker compose exec spark ...`), **Tab 05 (Bảng 3A, 3B, 4)** |
| **Nguyễn Thành Đạt** (`25MS23306`) | PageRank, SON/FP-Growth, 7D K-Means Cold-Start, Random Forest 23D, Card B1–B2 | **Bước 2** (Tab 06 & Tab 01 Blind Mode), **Bước 5** (Tab 03, Tab 04, Tab 05 Bảng 1–2–6) |
| **Phạm Công Minh** (`25MS23310`) | Docker 5 services, Kafka 4.1, 33 KiB Sketches, WAL Recovery, MongoDB 8.0 IXSCAN, Card B3 | **Bước 1** (`docker compose ps`), **Bước 6** (`mongosh`), **Bước 7** (`kafka-topics.sh` + Tab 02 Live Map + Tab 05 Bảng 3C, 5, 7) |
