# Báo cáo tổng kết và đặc tả Session 9

## Hệ thống giao thông TP Hồ Chí Minh

Chương 1. Bài toán và phạm vi sử dụng

Trong ví dụ batch lịch sử, một GPS trên Lý Thường Kiệt, đoạn 47407, báo 4 km/h. Một bản tin riêng lẻ chưa mô tả toàn bộ dòng xe. Hệ thống gom quan sát thành cửa sổ, bổ sung thông tin đường, dự báo slot kế tiếp và trình bày mức độ đầy đủ của dữ liệu.

Bước 1 chuẩn hóa GPS; Bước 2 tạo đặc trưng đồ thị và nhóm đường; Bước 3 dùng mô hình đóng băng; Bước 4 lưu các phiên bản dự báo để phục vụ UI. Đây là historical replay PoC, chưa phải nguồn GPS thành phố trực tiếp.

Nguyễn Tạ Minh Quyền (25MS23280) · Nguyễn Thành Đạt (25MS23306) · Phạm Công Minh (25MS23310)

Giảng viên hướng dẫn: Đặng Trần Khánh · Fall 2026

Hình 1.1. Bốn bước của hệ thống và ranh giới replay.

## 1.4. Cửa sổ đích và thời điểm phát

Một cửa sổ nguồn [09:30,10:00) có thể cung cấp đầu vào khi đóng lúc 10:00. Dự báo khi đó nhắm vào [10:00,10:30). Nhãn slot cách nhau 30 phút không tạo ra 30 phút cảnh báo trước khi đích bắt đầu.

Bản tin tới muộn tạo revision mới, kể cả khi khoảng đích đã bắt đầu hoặc kết thúc. UI hiển thị issued_at thực tế, run ID, revision, freshness và target interval. Kết quả batch trên các tab khác vẫn được gắn nhãn snapshot.

Proof-of-Scale là mục đích đối chiếu thuật toán trên mẫu dữ liệu. Nó không phải kết quả đo cluster scaling hoặc bằng chứng đáp ứng lưu lượng toàn thành phố.

Hình 1.2. Phân biệt nhãn thời gian và thời gian báo trước.

## Chương 2. Dữ liệu và validation

Năm CSV có hash khớp manifest rerun. Có 577,967 nodes, 5,553 streets, 84,633 segments và 90,938 GPS pings. Aggregation toàn nguồn tạo 33,441 cửa sổ 30 phút. train.csv là bảng đối chiếu độc lập, không phải nhãn triển khai trực tiếp.

Các bảng khác nhau có denominator khác nhau. Số cửa sổ không thể dùng thay cho số bản tin; số đoạn đường không thể dùng thay cho số quan sát GPS theo thời gian.

Hình 2.1. Quy mô các bảng đầu vào và đầu ra aggregation.

## 2.4. Khóa, chất lượng và population

Contract dùng raw_event_id cho từng ping và event_id cho (segment_id,date,period) UTC. Hai ping hợp lệ trong cùng slot đều đóng góp aggregate. Duplicate delivery cùng raw_event_id không được tăng count.

Đường engineering lọc motor roads có 90,787 pings và 33,394 windows. Chênh 151 pings và 47 windows với đường toàn nguồn là do filter, không phải mất dữ liệu ngầm.

NFC, timezone UTC/ICT, null/range/key checks, quarantine và parity được kiểm thử theo source. Khẳng định monoid baseline toàn nguồn thuộc evidence/session9_rerun; full-source benchmark mới có two-way exceptAll = 0.

Raw sự kiện ngoài watermark vẫn được giữ trong Bronze. Invalid schema, định danh rỗng và payload xung đột được ghi lý do cách ly trong ingress.

| Population | Pings | Windows |
|---|---|---|
| Toàn nguồn | 90,938 | 33,441 |
| Motor roads | 90,787 | 33,394 |
| Khác do filter | 151 | 47 |

## Chương 3. Kiến trúc và runtime

[I] Producer phát raw GPS vào Kafka, consumer Python ghi durable ingress, exact raw events, feature snapshots, forecast history và latest traffic_scores trong MongoDB. UI đọc đúng database/run. Spark đảm nhiệm batch ETL và analytics.

[P] Live city feed, cloud nhiều nút, online learning và điều khiển đèn vẫn là thiết kế. Spark file-stream, local replay cũ và kiểm tra publish Kafka cũ là các phép thử riêng.

Runtime build từ repo: Python 3.13.9, Java 17, PySpark 3.5.7, Kafka 4.1.0, MongoDB 8.0.38. Không phụ thuộc image/network/container của project khác.

Hình 3.1. Luồng triển khai thực; phần cloud vẫn design-only.

## 3.4. Monoid và thực thi Spark

Tuple gồm count, sum_velocity, min_velocity, max_velocity và severe_ping_count. Partial aggregates kết hợp theo phép cộng/min/max; mean cuối cùng là sum/count. Không lấy trung bình không trọng số của các trung bình con.

Key salting tạo partial tuple theo salt rồi merge theo khóa gốc. Chỉ diễn giải kết quả sau khi xác nhận parity. Hai implementation trả 33,441 windows và không có dòng sai khác hai chiều.

Hai thống kê cũ 687.43x và 1.023x có population khác nhau nên không được dùng để kết luận giảm task skew. Exchange nodes cũng không bằng số runtime stages.

Hình 3.2. Monoid kết hợp kết quả partial aggregation.

## 3.4. Benchmark và analytical queries

Mỗi cặp thử chỉ đổi join strategy hoặc salting; cùng input, partitions=8, AQE=false, input cache và action collect. Có 2 warmups và 5 lượt đo xen kẽ. Raw timings, full plans và 335 task records được giữ lại.

Median SMJ 0.310514s, BHJ 0.261904s; unsalted 0.124255s, salted 0.116502s. Đây là phép đo warm local[2], không phải đo city throughput hoặc cluster scaling.

Query thứ nhất JOIN GPS/segments và GROUP BY street_id. Query thứ hai lọc year_month thực trong Gold, chỉ project segment_id và mean_velocity. Full plans chỉ ra PartitionFilters/ReadSchema và shuffle. Ví dụ 7,294 windows tháng 08 thuộc dữ liệu lịch sử, không phải partition vừa benchmark.

Hình 3.3. Median của hai cặp thí nghiệm kiểm soát.

## Chương 4. Đồ thị và nhóm đường

PageRank mô tả tầm quan trọng cấu trúc. B2 dùng Nguyễn Kiệm, segment 21969: PageRank 2.7313 nhưng tốc độ TRAIN 45.41 km/h, ratio khoảng 0.9082. Không suy ra nguyên nhân từ số làn hoặc gán PageRank thành tình trạng ùn tắc.

K-Means dùng feature list thực trong registry/code: mean_los_ordinal, severe_rate, mean_velocity, velocity_ratio, length_m, max_velocity, pagerank_score. Không có ràng buộc liền vùng địa lý. Gọi kết quả là behavioral/morphological clusters.

Độ phủ mô hình cho 74,606 đoạn thiếu GPS không chứng minh accuracy cold-start. Luật đồng kẹt hoặc trễ có ít cửa sổ chỉ là exploratory hypotheses.

Hình 4.1. Đối chiếu cấu trúc với quan sát TRAIN ở B2.

## Chương 5. Dự báo và availability

Dữ liệu có 7,898 cặp exact t+30m; split sau purge gồm 5,494 TRAIN, 1,167 VAL và 1,182 TEST. Cutoffs: 2020-12-12T08:00:00Z và 2020-12-25T03:30:00Z. 55 cặp biên bị purge.

Online dùng imputer, Vmax, cluster và priors đóng băng; không fit lại. Run pin model, static store, Vmax, source code chia sẻ và runtime versions. Scorer dùng chung API batch, nhưng neighbor feature online là frozen street prior; accuracy cho policy này chưa tái đánh giá.

B1 gồm 2 origins × 23 features = 46 dòng. Replay availability có cutoff/sequence; vận hành lịch sử luôn UNVERIFIED vì thiếu arrival log và issuance history gốc.

| Feature | RF importance |
|---|---|
| current_los_ordinal | 0.5051 |
| lag1_los_ordinal | 0.0035 |
| lag2_los_ordinal | 0.0004 |
| trailing_severe_rate | 0.0623 |
| mean_velocity | 0.2236 |
| velocity_ratio | 0.0033 |
| obs_count | 0.0000 |
| length_m | 0.0035 |
| max_velocity | 0.0001 |
| street_level | 0.0003 |
| weekday | 0.0004 |
| period_sin | 0.1018 |
| period_cos | 0.0156 |
| gap_minutes | 0.0001 |
| pagerank_score | 0.0033 |
| in_degree | 0.0005 |
| out_degree | 0.0005 |
| neighbor_los_ordinal | 0.0016 |
| cluster_id | 0.0004 |
| segment_train_prior_los | 0.0197 |
| segment_train_severe_prior | 0.0492 |
| street_causal_prior_los | 0.0023 |
| street_causal_prior_sev | 0.0022 |

## 5.4. Đánh giá batch và baseline

Các metric dưới đây thuộc rerun batch lưu sẵn, không phải accuracy online vừa đo. RF có accuracy 0.9450, macro-F1 0.5543, weighted-F1 0.9439, severe F1 0.9409, ROC-AUC 0.9882 và severe PR-AUC 0.9471. Persistence accuracy 0.9484 cao hơn RF trên TEST đầy mẫu giữ nguyên trạng thái.

Bảng 5.4. RF và Persistence trên cùng TEST.

| Mô hình | Accuracy | Severe PR-AUC |
|---|---|---|
| Persistence | 0.9484 | 0.9367 |
| RF 23D | 0.9450 | 0.9471 |
| Decision Tree 23D | 0.9425 | 0.9327 |
Bảng 5.5. Decision Tree 23D có precision 0.9362 và recall 0.9514. Metric này khác RF; không trộn các hàng model.
Ranking tốt chưa chứng minh cảnh báo sớm đủ lead time. Target phải khác dữ liệu cửa sổ nguồn và được đánh giá theo đúng forecast issuance contract.
Hình 5.1. ROC batch lịch sử; không đánh giá policy online mới.

## 5.4. Khi trạng thái thay đổi

Trên 61 mẫu đổi LOS, RF mặc định đúng 2 / 61; ngưỡng hiệu chỉnh VAL đúng 9 / 61. Trong 8 ca từ không severe sang severe, hard recall là 0 / 8. Mô hình chưa chứng minh phát hiện tốt các ca onset hiếm.

Tỷ lệ risk score 9.35x và AUC onset 0.8136 là thống kê ranking lịch sử. Chúng không thay thế recall 0 / 8 và không tạo một cam kết vận hành.

3,513 cặp GPS ngắt quãng là regime khác exact t+30m. Không dùng accuracy của regime này để diễn giải accuracy cho 74,606 đoạn cold-start.

Hình 5.2. Persistence và RF theo regime cùng denominator.

## Chương 6. Streaming bền vững

Consumer tắt auto-commit. Mỗi delivery có khóa (run_id,topic,partition,offset). Exact raw identity quyết định duplicate; nhãn is_duplicate/lateness_bucket không đi vào logic.

Ingress/raw/snapshot/history được ghi acknowledged journaled w=1,j=true. Worker drain pending ledger trước khi poll Kafka. Offset từng partition chỉ commit sau durable writes và xử lý của record đó.

Retry dùng dữ liệu tới sequence cutoff cố định. Snapshot và history insert-once được đọc lại làm nội dung canonical. Latest cập nhật revision bằng atomic conditional pipeline.

Hình 6.1. Các ranh giới durable trước broker commit.

## 6.2. Raw replay và phiên bản

Raw smoke dùng 20 GPS thực từ segment 33246; producer thêm 1 duplicate và 1 replay-clock control. Run: raw-c8c7fa7014bd4b54a1e03fe153b8397b. Có 3 latest source windows trong MongoDB.

Watermark là max event time quan sát trừ 30 phút. Replay clock đóng cửa sổ; beyond-watermark events vẫn vào Bronze và reconciliation cập nhật cả các lag bị ảnh hưởng.

B3 trên đúng stream này: Bloom false positives 0, exact distinct segments 1, HLL khoảng 1.00049. Bloom 32.0 KiB và HLL 1.0 KiB chỉ là evaluator/KPI; trạng thái exact online không có giới hạn bộ nhớ 33 KiB.

Số 501 cửa sổ backfill, 3,953 downstream candidate windows và 377 cửa sổ rescored thuộc local replay cũ. Số candidate không đếm feature cells thực sự đổi. Không dùng chúng làm kết quả raw-smoke Kafka mới.

Hình 6.2. Đếm delivery, raw identity và latest windows trong raw smoke.

## 6.3. Crash, outage và độ trễ

Năm vị trí kill thật: before ingress, after ingress, after raw, after forecast và before commit. Mỗi fixture hội tụ về offset 6, 4 raw identities, 5 forecast revisions. History trước crash vẫn nguyên vẹn. Concurrent stale writes không kéo latest lùi revision.

Outage Mongo và Kafka thực tế ở startup: process lỗi khi service dừng, sau restore health và restart thì hội tụ. Không tuyên bố uninterrupted service hoặc mid-request outage coverage.

Độ trễ ingress-to-forecast trong F_trace khác broker-to-ingress và UI freshness. Warm Mongo query median/p95 rerun là 0.432/0.565 ms. Số 0.636 ms thuộc phép đo query cũ; không phải latency toàn pipeline.

Hình 6.3. Broker offset sau restart ở năm failpoints.
Hình 6.4. Stop/restore/restart trên hai service thực.

## Chương 7. Đóng gói và tái lập

7.2. Validation: 32 passed; lint E/F/W/C901 theo README. Integration Kafka/Mongo không skip. Build/runtime/image logs, full plans, raw timings và read-back được lưu trong evidence/session9.

7.3. Trách nhiệm: Minh Quyền phụ trách dữ liệu/Spark; Thành Đạt phụ trách graph/ML; Công Minh phụ trách Kafka/Mongo/UI. Reviewer độc lập đối chiếu correctness, crash và claim consistency.

Model/static/Vmax digest prefixes: 97a5a63e4e34, 2877a4dd1102, b4c29c687368. Các hash đầy đủ và source snapshot nằm trong manifest; restart cùng run không cho đổi registry/code/runtime.

Runbook: docker compose build spark app; docker compose up -d --wait kafka mongo; python -m scripts.session9_validate. Dùng run/database namespace riêng; không xóa checkpoint để khởi động lại. Mọi lệnh chạy từ repository root.

Mongo một nút và khóa worker trên volume chung chỉ phù hợp PoC một host. Không có bằng chứng HA, partition tolerance hay exactly-once xuyên Kafka/Mongo.


## 7.4. Session 9 Technical Readiness Summary

G7 là kiểm tra gói bàn giao, không phải tự chấm toàn hệ thống Ready. Trạng thái cuối được xác lập bằng artifact_review.json, test/lint logs và manifest, sau khi render và rà từng trang/slide.

Limitations còn lại được công khai: availability vận hành lịch sử UNVERIFIED, accuracy policy online chưa đo, crash oracle chưa phủ cross-partition, chưa có full replay throughput và chưa có cluster scaling.

Ba bước tiếp theo: ML/data kiểm tra accuracy policy online trước claim accuracy; streaming đo cross-partition/full replay trước SLO; cả nhóm diễn tập giải thích raw-to-serve trước buổi bảo vệ.

| Chủ đề | Kết luận có evidence |
|---|---|
| Architecture truth | Raw GPS → Kafka → consumer → model → Mongo → UI |
| Performance | Common-input parity; 2 warmups + 5 alternating runs |
| Validation | 32 tests; 46 B1 rows; historical UNVERIFIED |
| Operations | Idempotent durable processing, immutable history, restart convergence |
| Limitation | Single-host replay PoC; no accuracy/scaling overclaim |
