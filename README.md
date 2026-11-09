# HCMC Smart Traffic Analytics & Congestion Prediction Platform (V3)

> **Nhánh Data Engineering của Quyền (PR #1 — Integrated):** Pipeline Spark độc lập từ dữ liệu thô nằm ở
> [`spark_batch.py`](./spark_batch.py), dùng chung transformations tại
> [`traffic_data/spark_etl.py`](./traffic_data/spark_etl.py) (tương thích cả **Spark 3.5.7 / Java 17** và **Spark 4.2.0 / Java 21**).
> Xem [hướng dẫn chạy, data contract và checklist](./docs/QUYEN_DATA_ENGINEERING.md),
> **[Hướng dẫn chi tiết bằng tiếng Việt, checklist có bằng chứng và xác nhận đã chốt của Đạt/Minh](./docs/HUONG_DAN_BRANCH_QUYEN_VI.md)**,
> và [bảng đối chiếu 28 nhóm yêu cầu Proposal V3](./PROPOSAL_V3_AUDIT_CHECKLIST.md).

## 1. Tổng quan Đề tài & Kiến trúc Giải pháp V3 (Zones A–F, Components `[C1]`–`[C17]`)

Dự án xây dựng nền tảng phân tích dữ liệu lớn và dự báo ùn tắc giao thông đô thị TP. Hồ Chí Minh (**HCMC Smart Traffic Big Data Platform — V3**) trên bộ dữ liệu thực tế 5 bảng liên kết (**Kaggle HCMC Traffic Flow Dataset**), bám sát khung lý thuyết của hai giáo trình cốt lõi và hệ thống 4 bài Lab thực hành của học phần **BDA501**:
- **Big Data Systems:** *Big Data Analytics: A Hands-On Approach* (Arshdeep Bahga & Vijay Madisetti, 2019 — 4-Tier Analytics Value Chain, Gamma/Delta Pattern, Medallion Lakehouse, Load Leveling, NoSQL CAP).
- **Massive Dataset Mining:** *Mining of Massive Datasets, 3rd Ed.* (Jure Leskovec, Anand Rajaraman & Jeffrey D. Ullman, 2020 — Stream Sketching Ch. 4, Link Analysis PageRank Ch. 5, Frequent Itemsets SON/FP-Growth Ch. 6, Clustering Ch. 7, Decision Trees & Ensembles Ch. 12).
- **4 Trụ cột Lab BDA501:**
  - **Pillar I1 (`Hadoop MapReduce & Commutative Monoid`):** Khởi tạo cửa sổ 30 phút trực tiếp từ 3 bảng GIS (`nodes.csv`, `streets.csv`, `segments.csv`) và `segment_status.csv` (`90,938` bản tin GPS thô, khử trùng theo `raw_event_id`) bằng **5-Tuple Commutative Monoid Combiner** $(\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt})$ tại `[C9]`, đối chứng độc lập `0` sai lệch trên cả 5 thành phần tuple với [`baseline.py`](./baseline.py) (`compare_window_aggregates` & `verify_against_medallion`, kèm kiểm chứng luật kết hợp trên các phân vùng lệch kích thước).
  - **Pillar I2 (`Spark SQL, Catalyst & Skew`):** Phễu kiểm định chất lượng có thứ tự (**All-String Ordered `try_cast` Quality Funnel**) tại `[C5]` ([`traffic_data/spark_etl.py`](./traffic_data/spark_etl.py) & [`pipeline.py`](./pipeline.py)) gán đúng một `first_failing_rule` duy nhất cho mỗi bản ghi lỗi vào `[C15]` `quarantine_events.parquet` (`input = dedup_dropped + accepted + rejected`); thực thi và đo lường trực tiếp trên **Apache Spark 3.5.7** ([`spark_engine.py`](./spark_engine.py) & [`spark_batch.py`](./spark_batch.py)) giảm `Exchange` từ `SortMergeJoin` xuống `BroadcastHashJoin`, kiểm chứng hai chiều `exceptAll() == 0`, pruning phân vùng Parquet `year_month`, và **2-Stage Key Salting (`8 buckets`)** chống lệch phân vùng (`Data Skew`).
  - **Pillar I3 (`Spark MLlib & Zero-Conflict MLOps`):** Quy trình **Zero-Conflict 3-Phase MLOps Lifecycle** phân tách tuyệt đối quyền huấn luyện (`.fit()` duy nhất tại **Phase A** bởi `[C11]` trên tập `TRAIN` đã cắt theo mốc thời gian và lọc bỏ `target_ts > cutoff_ts` $\to$ đóng băng 3 artifact kèm kiểm chứng bất biến `SHA-256`: `model.joblib`, `segment_static_features.parquet`, `vmax_lookup.json`) với quyền suy luận chỉ đọc (`score_feature_snapshot` tại **Phase B Stream `[C8]`** và **Phase C Reconciliation `[C12]`**).
  - **Pillar S7 (`Kafka + Spark Structured Streaming + MongoDB 8.0`):** Bộ phát sự kiện [`producer.py`](./producer.py) (`[C1]`) hỗ trợ cả 2 chế độ phát lại (`raw_ping` 90,938 bản tin & `window` 33,441 cửa sổ) với `event_ts` / `ingest_ts`; bộ xử lý luồng [`starter_stream.py`](./starter_stream.py) & [`spark_engine.py`](./spark_engine.py) (`[C6]`–`[C8]`) vận hành **Bit-Packed Bloom Filter (`32.0 KiB = uint8[32768]`)**, **HyperLogLog++ (`1.0 KiB = uint8[1024]`)**, **Bounded Exponentially Decaying State ($\alpha=0.15$)**, hồi phục sự cố từ **WAL Checkpoints (`offset_log.json`, `state_wal.json`)**, và ghi idempotent **Bulk `UpdateOne(upsert=True)`** (`33,441` bản ghi chia 4 phân vùng, `69` lô $\times$ `500` docs, có bảo vệ phiên bản `status_version` chống ghi đè lùi `BATCH_RECONCILED -> STREAM_PROVISIONAL`) vào `[C16]` **MongoDB 8.0** kèm kiểm chứng thực thi **`IXSCAN`** (`0` in-memory `SORT`, độ trễ `p95 < 1 ms`).

Đặc tả toán học `P1–P4`, `UC-1–UC-6`, `EC-1–EC-10`, các quyết định thiết kế `G01–G05`, kiến trúc 6 phân vùng (`Zones A–F`, `[C1]`–`[C17]`) và kết quả thực nghiệm chi tiết xem tại **[TECHNICAL_NOTE.md](./TECHNICAL_NOTE.md)** và cây tài liệu quản trị sản phẩm **[product/](./product/)**.

---

## 2. Cấu trúc 5 Bảng Dữ liệu Đầu vào (`raw_data/`)

| Tệp nguồn | Số bản ghi | Số cột | Dung lượng | Vai trò trong Kiến trúc V3 (`System Boundary`) |
|---|---:|---:|---:|---|
| `nodes.csv` | 577,967 | 3 | 19.44 MB | `[C2]` Tọa độ `(long, lat)`; cắt tỉa `525,542` đỉnh hình học OSM mồ côi (`EC-7`), giữ `52,425` giao lộ thực (`52,181` khi lọc `S3_NON_ROAD_TYPE`) |
| `streets.csv` | 5,553 | 5 | 0.20 MB | `[C2]` Danh mục đường phố (`name`, `level` 1–4, `type`, `max_velocity`) dùng cho `BroadcastHashJoin` |
| `segments.csv` | 84,633 | 11 | 10.29 MB | `[C2]` Đồ thị cạnh có hướng (`s_node_id -> e_node_id`, `151,929` cạnh chuyển tiếp Line-Graph, `48` nút cụt) |
| `segment_status.csv` | 90,938 | 4 | 3.58 MB | `[C1]` Bản tin vận tốc GPS thực tế (`velocity`, `updated_at`) dùng để dựng độc lập `33,441` cửa sổ 30 phút (`33,394` sau lọc `S3_NON_ROAD_TYPE`) & Replay |
| `train.csv` | 33,441 | 18 | 4.99 MB | `Reference View` tùy chọn (khớp `100.0%` tập khóa `(segment_id, date, period)` với kết quả gộp từ `segment_status.csv`) |

---

## 3. Năm Quyết định Chuẩn hóa Kỹ thuật (`Group Decisions G01–G05`)

1. **`G01` — Canonical Event Schema & Key Hierarchy:** Phân tách rõ khóa bản tin GPS thô `raw_event_id = sha256(_id | segment_id | event_ts | velocity)` (`90,938` bản tin) với khóa cửa sổ 30 phút `event_id = sha256(segment_id | date | period)` (`33,441` cửa sổ), đồng thời gắn cả `event_ts` và `ingest_ts = event_ts + arrival_delay_sec`.
2. **`G02` — Dual Replay Modes (`Raw Ping` & `Window`) Không Gán Tay:** [`producer.py`](./producer.py) hỗ trợ `--mode raw` (`90,938` pings + `1%` duplicate + `5%` late) và `--mode window` (`33,441` windows + `1%` duplicate = `334` + `5%` late = `1,170` in-watermark + `501` out-of-watermark trên `219` khung `(date, period)` hoàn toàn bằng RNG có seed `42`, bỏ hẳn việc chọn tay chỉ số dòng).
3. **`G03` — Suy dẫn `LOS` (`derive_los`) & Nội suy `max_velocity` 3 Tầng Chỉ Học Trên `TRAIN`:** Cung cấp hàm tường minh `derive_los(velocity_ratio)` theo ngưỡng `0.80 / 0.65 / 0.50 / 0.35 / 0.20` đối chứng song song với nhãn tham chiếu (`52.37%` khớp tuyệt đối); `fit_train_vmax_lookup` & `fit_velocity_lookup` chỉ học trên `TRAIN` (`Tier 1 Segment Observed = 4,946 / TRAIN P85 = 8,164` $\to$ `Tier 2 TRAIN Median(level, type) = 20,124` $\to$ `Tier 3 Hierarchical Prior {1: 80, 2: 60, 3: 50, 4: 40} km/h = 207`).
4. **`G04` — Dự báo Đúng Chân trời `t + 30m` & Cắt Tập Theo Timestamp Có Purge:** Nhãn mục tiêu chính thức `target_t30m_los_ordinal` được ghép bằng self-join trên `(segment_id, slot_ts + 30m)` (`7,898` cặp liên tiếp đúng `+30m`; các khoảng trống `60m` hoặc `9h` không bao giờ bị gán nhầm làm nhãn `+30m`). Chia tách `70/15/15` theo mốc thời gian toàn cục và loại bỏ (`purge`) mọi mẫu ở biên có `target_ts > cutoff_ts` (`5,494` TRAIN, `1,167` VAL, `1,182` TEST), đồng thời giữ bảng đối chiếu phụ cho bài toán `next_observed` (`23,414` cặp).
5. **`G05` — Runtime Lock & Kiểm chứng Thật (`Kafka 4.1.0 KRaft` + `Spark 3.5.7 / 4.2.0` + `MongoDB 8.0.34`):** Thực thi trực tiếp `apache/kafka:4.1.0`, PySpark `3.5.7` (Java 17) / `4.2.0` (Java 21) và MongoDB `8.0.34` qua container; khi dịch vụ ngoài tắt, hệ thống trả về `status = "not_verified"` trung thực, tuyệt đối không giả lập kết quả `IXSCAN` hay `Exchange`.

---

## 4. Hướng dẫn Chạy Dự án & Kiểm thử Tự động

### 4.1. Chạy trực tiếp trên Host Python & Container Runtime (Windows PowerShell)

```powershell
python -m pip install -r requirements.txt
python pipeline.py
python baseline.py
python producer.py --mode both --bootstrap-servers localhost:9092
python starter_stream.py --kill-after-batch 1
pytest -v
ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
streamlit run app.py
```

Sau khi khởi chạy, mở trình duyệt tại <http://localhost:8501>.

### 4.2. Chạy bằng Docker Compose (4 Core Services: `kafka`, `mongo`, `spark`, `app` & `compose.quyen.yml`)

```powershell
docker compose up -d kafka mongo spark app
docker compose --profile batch run --rm pipeline
docker compose --profile stream run --rm stream

# Hoặc chạy snapshot Spark batch độc lập của Quyền:
$env:TRAFFIC_FIT_CUTOFF = '2020-08-01T00:00:00Z'
docker compose -f compose.quyen.yml run --build --rm batch
docker compose -f compose.quyen.yml run --rm --entrypoint python batch -m unittest discover -s tests -p test_quyen_spark.py -v
```

---

## 5. Danh mục Tệp Đầu ra Chính (`data/` & `outputs/`)

| Tệp kết quả | Thành phần Kiến trúc | Nội dung |
|---|---|---|
| `data/dataset.csv` | `[C14]` Medallion Gold | Bảng dữ liệu hợp nhất (`33,441` dòng $\times$ `54` cột) dựng từ GIS + `segment_status.csv`, gắn PageRank, 7D K-Means và biến nhân quả |
| `data/lakehouse/bronze_raw_events/events.parquet` | `[C14]` Bronze Layer | Luồng sự kiện GPS Replay (`33,775` bản tin gồm `1%` trùng lặp & `5%` sự kiện trễ) |
| `data/lakehouse/silver_cleaned_windows/windows.parquet` | `[C14]` Silver Layer | Cửa sổ 30 phút đã chuẩn hóa Unicode `NFC`, đồng bộ `ICT (UTC+7)` và nội suy vận tốc 3 tầng từ `TRAIN` |
| `data/lakehouse/curated_gold/year_month=*` | `[C14]` Gold Partitioned | `10` phân vùng Parquet theo tháng (`2020-07` đến `2021-04`, định dạng `TIMESTAMP(MICROS)` tương thích Spark 3.5) |
| `data/lakehouse/segment_static_features.parquet` | `[C13b]` Static Feature Store | Hồ sơ tĩnh cho toàn bộ `84,633` đoạn đường (`10,027` có GPS + `74,606` vùng mù `Cold-Start`) |
| `data/lakehouse/quarantine_events.parquet` | `[C15]` Quarantine Store | Bản ghi vi phạm quy tắc chất lượng kèm đúng một `first_failing_rule` và `raw_payload` |
| `data/lakehouse/serving_traffic_scores.parquet` | `[C16]` Serving Mirror | Toàn bộ `33,441` bản ghi dự báo kèm trạng thái `STREAM_PROVISIONAL (v1)` / `BATCH_VERIFIED (v2)` / `BATCH_RECONCILED (v2)` |
| `outputs/quyen_validation/*` | `[C5]` & `[C9]` Spark Batch | Bằng chứng thực thi [`spark_batch.py`](./spark_batch.py) (`manifest.json`, `broadcast_plan.txt` 3 `BroadcastHashJoin` / `0` `SortMergeJoin`, `sort_merge_plan.txt`, `pruning_plan.txt`, `task_metrics.json` 1-stage vs 2-stage salted monoid) |
| `outputs/vmax_lookup.json` & `golden_predictions.parquet` | `[C13a]` & `[C13b]` | Bảng tra cứu `max_velocity` đóng băng từ `TRAIN` và tập dự báo chuẩn kiểm chứng đồng nhất Batch/Stream/Recon |
| `outputs/spark_runtime_evidence.json` & `join_plan_evidence.json` | `[C9]` Pillar I1 & I2 | Bằng chứng thực thi PySpark 3.5.7 (`BroadcastHashJoin` giảm `10 -> 2` Exchanges, `exceptAll == 0`, Parquet pruning, `FPGrowth`, `PipelineModel`, Streaming WAL) |
| `outputs/monoid_baseline_verification.json` | `[C9]` Pillar I1 | Kiểm chứng đối chiếu Monoid 5-tuple khớp tuyệt đối (`0` sai lệch trên cả 5 thành phần) so với [`baseline.py`](./baseline.py) |
| `outputs/model.joblib` & `model_registry_manifest.json` | `[C13a]` Frozen Registry | Mô hình đóng băng từ Phase A (`[C11]`) kèm mã băm bất biến `SHA-256` của cả 3 artifact Phase A |
| `outputs/mongo_verify.json` | `[C16]` MongoDB 8.0 | Kết quả ghi Bulk `UpdateOne` đủ `33,441` bản ghi qua 4 phân vùng vào `mongo:8.0.34`, kiểm chứng idempotent replay, độ trễ `p95 < 1 ms` và `IXSCAN` (`0` SORT) |
| `outputs/road_pagerank_top.csv` | `[C10]` MMDS Ch. 5 | Top 50 nút cổ chai cấu trúc theo Directed Line-Graph PageRank ($\beta = 0.85$) |
| `outputs/co_congestion_rules.csv` & `lagged_congestion_rules.csv` | `[C10]` MMDS Ch. 6 | Top 30 luật đồng kẹt xe có hướng 2 chiều (`2-Pass SON`) và Top 20 luật lan truyền trễ 30 phút (`Street_A(t) => Street_B(t+30m)`) |
| `outputs/hotspot_alerts.csv` | `[C7]` MMDS Ch. 4.7 | Top 25 điểm nóng ùn tắc kéo dài tính bằng cửa sổ suy giảm mũ ($\alpha = 0.15$) có cắt tỉa ngưỡng $\epsilon$ |
| `outputs/reconciliation_audit.csv` | `[C12]` Two-Tier Recon | Bảng kiểm toán đối soát Batch–Streaming (`100%` in-watermark match, bù `501` bản tin trễ trên `219` khung giờ & hàn gắn + chấm điểm lại `3,953` cửa sổ trễ bậc xuôi dòng) |
| `outputs/ablation_results.csv` | `[C11]` MMDS Ch. 12 | Bảng đối chứng 7 cấu hình ML trên chân trời đúng `t + 30m` (tách bạch trung thực 6-class multiclass và binary GBT) |
| `outputs/checkpoints/` & `query_progress.json` | `[C6]` & `[C15]` | WAL offsets (`raw/offset_log.json`), state store log (`agg/state_wal.json`) và bằng chứng kill/restart recovery `0` duplicate side effects |
| `outputs/metrics.json` | `[C17]` UI Telemetry | Tổng hợp toàn bộ chỉ số vận hành của 17 thành phần `[C1]`–`[C17]` |


