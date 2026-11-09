# Đối chiếu Proposal V3 và checklist cho nhóm 3 người

Ngày kiểm tra: 08/11/2026. Branch: `feat/platform-v3-complete`; HEAD: `2233f25`.

## 1. Phạm vi và cách tính

Đã đọc nội dung đủ 8 trang `HCMC_Smart_Traffic_Final_Project_Proposal_V3.pdf`, đối chiếu trực tiếp `pipeline.py`, `producer.py`, `baseline.py`, `app.py`, Docker, requirements, tests và các artifact hiện có. Phân công dưới đây dựa trên ma trận DRI đang có trong `product/00-proposal/project-proposal.md` và `product/06-engineering/sprint-plans/sprint-plan-v3.md`; PDF xác nhận tên 3 thành viên nhưng không cung cấp ma trận giao việc cá nhân tương đương.

Working tree đã có thay đổi ở 7 artifact: ablation_results.csv, metrics.json, model.joblib, model_registry_manifest.json, mongo_verify.json, roc_curve.csv, roc_curve.png. Đánh giá phản ánh code và artifact đang có tại thời điểm kiểm tra, không chỉ snapshot commit HEAD. Không sửa các artifact này.

Proposal không có một checklist checkbox duy nhất. Báo cáo chuẩn hóa thành **28 nhóm yêu cầu**, không coi đây là số hạng mục chính thức hay trọng số chấm điểm của giảng viên.

- **Đạt: 4/28 (14,3%)**: có hiện thực phù hợp cho phạm vi nhóm yêu cầu, với code/artifact đối chiếu được; không đồng nghĩa đã kiểm chứng toàn hệ thống.
- **Một phần: 20/28 (71,4%)**: có nền tảng nhưng còn sai khác thuật toán, hợp đồng, runtime hoặc bằng chứng.
- **Chưa có: 4/28 (14,3%)**: thiếu hiện thực runtime cốt lõi.
- Có nền tảng ở 24/28 nhóm (85,7%), nhưng **không nên nói hoàn thành 85,7% proposal**. Chưa có trụ cột I1/I2/I3/S7 nào được chứng minh đầy đủ end-to-end theo đúng Spark/Kafka/MongoDB của PDF.

Kết luận: branch là một prototype offline pandas/scikit-learn có nhiều output hữu ích; chưa phải platform phân tán vận hành đúng blueprint.

## 2. Checklist yêu cầu và mức đáp ứng

| # | Yêu cầu trong PDF | Trang | Trạng thái | Bằng chứng hiện tại và khoảng trống |
|---|---|---|---|---|
| 01 | Kiểm kê đủ 5 bảng và quy mô dữ liệu | 1 | Đạt | `inspect_raw_tables`, `outputs/file_inventory.csv`, raw_data có sẵn. |
| 02 | System boundary chỉ GIS và raw GPS; train.csv là view dẫn xuất | 1,3 | Một phần | `build_medallion_frame` khởi đầu từ train.csv; producer lấy dataset.csv, chưa độc lập xây hệ thống từ raw GPS. |
| 03 | All-string schema, ordered try_cast, first failing rule, quarantine | 2,5,8 | Một phần | Có thống kê và quarantine segment; chưa có funnel Spark cho malformed GPS. Các mask độc lập chưa đảm bảo chỉ rule lỗi đầu tiên. |
| 04 | Impute TRAIN-only: segment P85 → median(level,type) → level prior | 2 | Một phần | Code dùng median theo level trên toàn frame; nhãn tier(level,type) không phản ánh phép fill thực tế; prior 60/50/40/30 khác PDF 80/60/50/40. |
| 05 | Suy LOS A–F từ mean velocity/vmax sau aggregate | 2 | Một phần | LOS lấy trực tiếp từ train.csv; chưa có hàm derive LOS theo 0.80/0.65/0.50/0.35/0.20 như hợp đồng. |
| 06 | Chuẩn ICT, cyclical encoding, xử lý period lạ | 1,2 | Một phần | Có vn_hour +7; sin/cos vẫn theo period UTC, chưa thống nhất local date/weekday. Chưa có Spark handleInvalid. |
| 07 | Monoid 5-tuple và 2-stage salt 8 buckets | 6,8 | Một phần | Có pandas aggregate và salting thật, chưa là Spark distributed; skew hiện so các cách gom khác nhau, chưa benchmark task partition. |
| 08 | Independent baseline chứng minh aggregate khớp | 1,8 | Một phần | baseline.py độc lập, so count/mean; chưa so đủ min/max/severe, missing/extra keys; chưa kiểm luật kết hợp qua nhiều cách partition. |
| 09 | Catalyst BroadcastHashJoin, exceptAll, Exchange/pruning evidence | 6,8 | Chưa có | `verify_salted_monoid_and_join_plan` ghi literal strategy, 7/3 Exchanges, diff=0; không thực thi Spark explain/exceptAll. |
| 10 | Directed line-graph PageRank, dangling redistribution | 2,6,8 | Đạt | `compute_road_pagerank` dựng cạnh, chạy 25 vòng beta=.85 và bù dangling mass thật bằng numpy/pandas. Cần port/benchmark khi tích hợp Spark. |
| 11 | SON / Spark FPGrowth, luật có hướng | 2,6,8 | Một phần | Đếm cặp Python, cắt basket[:25], chỉ sinh một chiều từ cặp đã sắp xếp; không SON/FPGrowth. |
| 12 | TRAIN-only 7D KMeans k=6, peer search | 2,6,8 | Một phần | Có sklearn KMeans; fit cutoff khác supervised split, profile toàn TRAIN gắn ngược cho hàng TRAIN sớm; chưa chứng minh feature cluster không lộ tương lai. Peer search chỉ trên dữ liệu quan sát. |
| 13 | Static feature store toàn bộ 84.633 segments/cold-start | 2,6 | Đạt | `build_static_feature_store`, parquet store có nhánh cold-start. Chưa tính phần live join vào tiêu chí này. |
| 14 | Causal priors chỉ dùng thông tin đến t | 1,3,6 | Một phần | Cumcount/cumsum có nhân quả theo hàng; global mean toàn TRAIN ảnh hưởng hàng TRAIN sớm. Cần xử lý ties theo timestamp và chứng minh bằng future perturbation. |
| 15 | Target đúng t+30m, gap và contiguous chính xác | 2,3 | Một phần | `shift(-1)` là quan sát kế tiếp bất kể gap; contiguous dùng previous gap <=60 phút, không phải next gap ==30 phút. Flag cũng chưa ở FULL_ML_FEATURES. |
| 16 | Chronological 70/15/15 và validation AUC gate | 6,8 | Một phần | Có split nhưng val_df không được dùng chọn model; chọn cố định rf_full. Cần split theo timestamp, purge target vượt cutoff, đồng bộ cutoff clustering/imputation. |
| 17 | Spark ML Pipeline DT/RF/GBT và ablation 14D/23D | 6,8 | Một phần | Có sklearn và bảng 7 cấu hình; chưa MLlib PipelineModel. Các metric GBT 6-class lấy RF predictions, cần ghi rõ hoặc tách báo cáo. |
| 18 | Phase A freeze; B/C load read-only artifacts | 3,6,7 | Một phần | Có joblib/hash check; B/C chưa là tiến trình riêng, chưa chứng minh chỉ score sau T_train, chưa kiểm hash static store. |
| 19 | Kafka KRaft 3 partitions và producer raw event-time replay | 1,3,5,7 | Chưa có | producer chỉ tạo DataFrame/JSON summary, không gửi broker, không event_ts/arrival delay thực tế. |
| 20 | readStream Kafka, watermark 30m, 2 sinks, trigger 5s | 5,7 | Chưa có | Không có starter_stream.py, readStream/writeStream/foreachBatch thực thi. |
| 21 | Bloom 32KB, HLL++, decay state hữu hạn | 2,5,7 | Một phần | Có thuật toán mô phỏng; Bloom numpy uint8[262144] thực tế 256KiB, không 32KiB; exact_seen tăng theo stream; HLL tự viết chưa là Spark HLL++. |
| 22 | Recon Tier 1 exceptAll và Tier 2 raw backfill/heal lag | 6,7 | Một phần | Tính thống kê pandas theo date/period; match=100% và diff=0 gán cố định; chưa raw recompute theo segment/window, heal lag và rescore thực. |
| 23 | WAL checkpoint thực và restart recovery | 3,6,7 | Chưa có | `_write_wal_and_manifest` ghi offsets cố định, không checkpoint của Spark, chưa có kill/restart test. |
| 24 | Mongo bulk upsert, unique/compound index, IXSCAN 0 SORT <10ms | 2,6,8 | Một phần | Có mongosh tùy chọn chỉ ghi top-25 sample vào container hard-code khác project; chưa full foreachPartition. live=false nhưng fallback vẫn báo IXSCAN/1ms. |
| 25 | Streamlit 5 tabs, live serving/query status | 6 | Một phần | UI có nhiều chức năng nhưng đọc CSV/JSON/joblib; chưa query Mongo live, refresh 5s và status thật. |
| 26 | Bronze/Silver/Gold Parquet partition year_month | 6,8 | Đạt | `persist_medallion_lakehouse`, Parquet layers/partition đã có; live append/batch_id và pruning evidence thuộc nhóm khác còn thiếu. |
| 27 | Test thực thi P1–P4/UC1–6/EC1–10 | 2,3 | Một phần | 11 hàm test hiện có pass khi gọi trực tiếp; nhiều assertion kiểm artifact/literal, thiếu integration/failure injection. |
| 28 | Compose Kafka/Spark/Mongo/UI và ngân sách RAM | 8 | Một phần | Chỉ app và pipeline Python; thiếu Kafka, Spark, Mongo, healthchecks, resource budgets và lệnh start pipeline live. |

## 3. Các lỗi cần giải quyết trước khi tin metric

1. **Forecast horizon sai:** label hiện là next observed, không đảm bảo +30m. Báo cáo F1=.6670, PR-AUC=.7568 hiện chỉ là artifact của protocol cũ; cần đánh giá lại sau khi sửa target. Không bắt model mới tái tạo số cũ bằng mọi giá.
2. **Leakage chưa đóng:** imputation lấy toàn frame; global prior biết toàn TRAIN; KMeans fit profile toàn TRAIN rồi gắn lại vào hàng sớm. Split supervised và cutoff cluster còn khác nhau. Test cumsum hiện tại không phủ các đường rò rỉ này.
3. **Evidence không phải kết quả đo:** join plan/exceptAll, WAL offsets, recon match và fallback IXSCAN có literal. Chỉ báo “verified” khi truy vấn thật thành công; lỗi/missing runtime phải trả `not_verified` kèm lý do.
4. **Không có stream thực:** lateness hiện chỉ là category, không event-time/arrival-time làm watermark tiến lên. Lifecycle hiện gán trên full offline dataset, chưa phản ánh transition giữa hai lần ghi thực.
5. **Memory claim sai:** Bloom phải pack bit; oracle exact sets chỉ dùng ở test/benchmark. Trạng thái decay là O(số segment) toàn hệ thống, O(1) mỗi segment, không phải tổng RAM không phụ thuộc network size.
6. **Luật đồng xuất hiện không tự chứng minh lan truyền nhân quả:** cần trình bày đúng là association; nếu muốn dự báo đường kẹt tiếp theo thì cần basket có time lag và đánh giá riêng.

## 4. Các quyết định chung cần chốt trước khi code tiếp

- [ ] **G01 — Cả nhóm, P0:** Chốt một canonical event schema: raw_event_id, segment_id, velocity, event_ts UTC, ingest_ts, replay metadata; phân biệt raw ping ID với window ID. Hai ping hợp lệ cùng segment/slot không được bị dedup mất.
- [ ] **G02 — Quyền + Minh, P0:** Chốt raw replay và window replay là hai chế độ khác nhau. PDF dùng 90.938 pings nhưng 334 duplicates/1.170/501 late theo 33.441 windows, nên số lượng không cùng mẫu số. Ghi rõ event-level/window-level và seed; không ép dữ liệu thật đạt số 319 bằng cách chọn tay cửa sổ.
- [ ] **G03 — Đạt + Quyền, P0:** Chốt label LOS từ ratio hay nhãn gốc làm ground truth; báo cả mức lệch nếu khác. Chốt vmax fallback, đơn vị, timezone, schema 23D. PDF nói thêm contiguous nhưng liệt kê feature dimensions chưa hoàn toàn nhất quán; cập nhật feature contract rõ ràng.
- [ ] **G04 — Đạt, P0; nhóm review:** Chốt chính thức horizon +30m; next-observed chỉ là thử nghiệm riêng. Chốt train/validation/test boundaries theo timestamp và chính sách label vượt cutoff.
- [ ] **G05 — Minh, P0:** Xác minh runtime version thực sự build được theo yêu cầu môn; khóa image/package/connector/Java tương thích trong lock/config, ghi deviation nếu blueprint version không khả dụng. Đây là việc cần kiểm tra khi triển khai, báo cáo này không xác nhận availability online.

## 5. Checklist của Nguyễn Tạ Minh Quyền — Data Engineering, I1/I2

Quyền phụ trách C2, C5, C9, C14, C15; phối hợp Đạt về feature và Minh về stream input.

- [ ] **Q01 — P0, sau G01/G03:** Tạo Spark batch entrypoint từ GIS + segment_status, không lấy train.csv làm nguồn sản xuất; train.csv chỉ làm reference. **Nghiệm thu:** bỏ train.csv khỏi input sản xuất vẫn sinh cửa sổ; so key/count với reference.
- [ ] **Q02 — P0:** Đọc schema toàn string, try_cast/try_to_timestamp, validate required fields, foreign key, velocity [0,200], length. **Nghiệm thu:** fixture N/A/null/bad timestamp/unknown segment không làm crash; mỗi dòng có đúng first failing rule.
- [ ] **Q03 — P0:** Ghi quarantine raw payload + source/raw_event_id + rule + timestamp; tránh đếm trùng khi vi phạm nhiều rule. **Nghiệm thu:** accepted + rejected = input sau chính sách dedup đã ghi rõ.
- [ ] **Q04 — P0, cùng Đạt:** Fit vmax segment P85 → median(level,type) → prior trên TRAIN; lưu lookup bất biến dùng cả B/C. **Nghiệm thu:** thay đổi VAL/TEST không đổi lookup; tier label khớp giá trị và đủ coverage.
- [ ] **Q05 — P0:** Implement derive_los từ aggregate ratio, chuẩn ICT gồm cả ngày qua nửa đêm, weekday và sin/cos. **Nghiệm thu:** test đúng 5 ranh giới LOS, velocity 0, UTC 17:00 đổi ngày ICT, period chưa từng thấy.
- [ ] **Q06 — P1:** Port monoid và salt 8 buckets sang Spark; exact dedup theo raw_event_id. **Nghiệm thu:** đủ 5 thành phần khớp baseline; unequal partition sizes không gây average-of-averages.
- [ ] **Q07 — P1:** Chạy baseline/optimized joins thật; lưu explain formatted/executed plan và hai chiều exceptAll sau chuẩn schema. **Nghiệm thu:** thấy BroadcastHashJoin từ plan thật, diff=0; Exchanges ghi số đo, không ép 7→3 nếu runtime khác.
- [ ] **Q08 — P1:** Đo skew cùng partition key/workload trước/sau salt, task time và shuffle bytes. **Nghiệm thu:** có bảng benchmark tái lập, seed và config.
- [ ] **Q09 — P1:** Tăng baseline.py so min/max/severe/count/sum/mean và missing/extra keys. **Nghiệm thu:** cố ý sửa một aggregate hoặc thêm/mất key phải fail.
- [ ] **Q10 — P1:** Chuẩn Bronze raw immutable, Silver cleaned windows, Gold year_month và window_metrics/batch_id; prune nodes thực tế khi load. **Nghiệm thu:** manifest/count/schema, rerun không nhân đôi, plan có PartitionFilters/ReadSchema phù hợp.
- [ ] **Q11 — P1:** Handoff schema/quality functions cho Minh; tạo fixtures nhỏ và data dictionary cho Đạt. **Nghiệm thu:** batch và streaming dùng cùng rule/LOS/window definitions.

## 6. Checklist của Nguyễn Thành Đạt — Mining, ML, I3

Đạt phụ trách C10, C11, C13a/b; cùng Quyền xác minh dữ liệu và cùng Minh đóng gói scorer.

- [ ] **D01 — P0, sau G04:** Xây label bằng join segment và timestamp+30m; giữ next_ts/target_ts/gap audit. **Nghiệm thu:** cặp cách 60m/9h/3 ngày không trở thành target +30m; contiguous là ==30m đúng chiều.
- [ ] **D02 — P0:** Chia theo timestamp chung, không cắt giữa timestamp bằng index; purge TRAIN/VAL có target qua cutoff. **Nghiệm thu:** max(train target_ts) không vượt train boundary; lưu counts và boundaries.
- [ ] **D03 — P0:** Sửa prior global/ties và imputation leakage; xây feature tại t từ prefix hợp lệ. **Nghiệm thu:** perturb mọi quan sát sau t không làm đổi feature ở t theo contract; test cả intra-TRAIN, VAL và TEST.
- [ ] **D04 — P0:** Ngăn cluster profile toàn TRAIN tiết lộ tương lai cho hàng TRAIN sớm; dùng warmup freeze hoặc temporal out-of-fold/expanding profiles được mô tả rõ. **Nghiệm thu:** test future perturbation trên cluster-derived feature và cutoff khớp D02.
- [ ] **D05 — P1:** Port PageRank sang cách thực thi Spark phù hợp, giữ mass correction và topology chuẩn. **Nghiệm thu:** V/E/dangling counts, mass invariant và so numpy reference trên toy graph/toàn mạng.
- [ ] **D06 — P1:** Implement Spark FPGrowth và SON nếu rubric yêu cầu cả hai; bỏ basket[:25], sinh đủ hai hướng. **Nghiệm thu:** support/confidence/lift khớp brute-force toy baskets, không self-rule, lưu params và số baskets.
- [ ] **D07 — P1:** Port 7D KMeans TRAIN-only k=6 sang MLlib, lưu imputer/scaler/cluster model; peer search phủ cả cold-start theo chính sách. **Nghiệm thu:** 6 clusters, metrics tái lập và 84.633 IDs có static features không trùng.
- [ ] **D08 — P1:** Tạo Spark ML Pipeline DT/RF multiclass, GBT binary, unknown-category handling và explicit feature schema. **Nghiệm thu:** save/load PipelineModel ở process mới ra cùng prediction, không fit trong scorer.
- [ ] **D09 — P1:** Dùng VAL cho AUC gate/model/threshold; TEST chỉ báo cáo cuối. **Nghiệm thu:** selection report có mọi candidate/VAL metric; perturb TEST không đổi selected model/threshold.
- [ ] **D10 — P1:** Chạy lại ablation persistence/DT/RF/GBT local/full trên target đúng; tách metrics binary GBT và multiclass RF. **Nghiệm thu:** F1/precision/recall/ROC-AUC/PR-AUC/confusion matrix cùng TEST IDs; báo chênh với proposal và lý do.
- [ ] **D11 — P1:** Freeze model + static store + preprocessing lookup; manifest có hash/version/cutoffs/features. **Nghiệm thu:** hash tất cả artifacts không đổi sau B/C và load trong container scorer riêng.
- [ ] **D12 — P1:** Handoff scorer API/schema/golden predictions cho Minh. **Nghiệm thu:** cùng input trong batch, stream, recon ra cùng prediction khi feature snapshot giống nhau.

## 7. Checklist của Phạm Công Minh — Infra, Streaming, Serving, UI

Minh phụ trách C1, C4, C6–C8, C12, C16, C17. Khối lượng lớn nhất ở đường live; Quyền hỗ trợ recon aggregate, Đạt hỗ trợ feature/heal lag/scoring.

- [ ] **M01 — P0, sau G05:** Compose Kafka KRaft, Spark, Mongo8, UI cùng network/volumes/healthchecks; resource budget 1,5/6/2/1GB và CPU theo PDF. **Nghiệm thu:** clean startup theo runbook, ghi docker stats và không vượt máy 16GB trong demo.
- [ ] **M02 — P0, sau G01/G02:** Producer gửi raw GPS JSON thật vào topic 3 partitions, key segment_id, có replay speed/seed/late-every/late-seconds; duplicate giữ raw_event_id. **Nghiệm thu:** consumer đo partition/count/event_ts/arrival_ts; injected duplicates/late có ground truth.
- [ ] **M03 — P0:** starter_stream.py dùng readStream Kafka + parse schema + shared quality rules. **Nghiệm thu:** lấy sự kiện thật từ broker, bad records vào quarantine, có lastProgress thật.
- [ ] **M04 — P0:** Sink 1 append 100% raw events vào Bronze trước watermark; Sink 2 window 30m + watermark30m + foreachBatch trigger5s. **Nghiệm thu:** sự kiện quá watermark vẫn ở Bronze và không vào closed online aggregate; hai checkpointLocation riêng.
- [ ] **M05 — P1:** Pack Bloom đúng 32KiB/k=4; triển khai HLL++/Spark approx_count_distinct; decay state có lifecycle/reset. **Nghiệm thu:** đo bytes thực và FP/cardinality error; exact oracle chỉ trong validation, không tăng RAM production theo tổng events.
- [ ] **M06 — P1, sau D11/D12:** C8 load frozen model/static store, join và maintain lag/prior state, score chỉ sau T_train. **Nghiệm thu:** không fit, prediction parity với golden dataset và ghi STREAM_PROVISIONAL thật.
- [ ] **M07 — P0:** Mongo writer cấu hình URI/service name thay hard-code container; repartition4/foreachPartition, 1 client/partition, bulk500 UpdateOne; unique event/window key đúng tầng. **Nghiệm thu:** full dataset thật có count và acknowledged writes; replay batch không tăng duplicates.
- [ ] **M08 — P0:** Loại fallback evidence giả; lưu nguyên winningPlan/executionStats từ server và parse stages thật. **Nghiệm thu:** Mongo unavailable ⇒ not_verified; không tự báo IXSCAN/0 SORT/1ms.
- [ ] **M09 — P1:** Đo UC2 query với index đầy đủ, sort thực, warm/cold và latency distribution. **Nghiệm thu:** evidence query/index/version/dataset size, IXSCAN và không blocking SORT; báo median/p95 để đánh giá <10ms theo protocol đã chốt.
- [ ] **M10 — P1, cùng Quyền/Đạt:** Recon Tier1 từ hai kết quả độc lập cùng accepted-event set, hai chiều exceptAll. Tier2 recompute late windows từ Bronze, heal downstream lag/prior, rescore. **Nghiệm thu:** sai dữ liệu phải làm audit fail; không gán literal100%; completeness đo từ raw IDs.
- [ ] **M11 — P1:** Version/ordering guard khi provisional và recon ghi cùng key. **Nghiệm thu:** provisional retry không ghi đè kết quả reconciled mới hơn; lifecycle transition có audit.
- [ ] **M12 — P1:** Dùng Spark WAL/checkpoints thật, bỏ JSON offsets cố định khỏi evidence production. **Nghiệm thu:** kill/restart giữa microbatch giữ offsets/state, không mất accepted events hoặc nhân đôi serving; kèm logs trước/sau.
- [ ] **M13 — P1:** UI query Mongo live và Gold/quarantine, hiển thị status/last update/failure thật; refresh 5s ở phần live. **Nghiệm thu:** quan sát cùng window từ provisional→reconciled trên UI; không dùng JSON sample thay query.
- [ ] **M14 — P2:** Hoàn thiện 5 tabs theo PDF, cold-start lookup và UI unknown/empty data states. **Nghiệm thu:** demo đủ UC1–6, có screenshots/video theo scenario.

## 8. Checklist tích hợp và bàn giao chung

- [ ] **T01 — Quyền chủ trì, P0:** Fixture E2E nhỏ có raw duplicates, invalid values, late-in/late-out, UTC date rollover, sparse/cold-start và dangling topology. Expected output tính độc lập.
- [ ] **T02 — Đạt chủ trì, P0:** Regression cho horizon, split boundaries, preprocessing/cluster leakage và validation selection; không chỉ assert lại công thức hiện thực.
- [ ] **T03 — Minh chủ trì, P1:** E2E producer→Kafka→Spark→Mongo→UI và recon; lưu raw logs, progress, explain, hashes, counts trong evidence/run_id.
- [ ] **T04 — Minh + Quyền, P1:** Restart/failure/race/idempotency tests; thử broker/service unavailable và malformed event. Fail phải hiển thị fail, không xuất bằng chứng thành công mặc định.
- [ ] **T05 — Đạt + Quyền, P1:** Thêm pytest/ruff vào dev requirements, khóa dependencies; chạy pytest chính thức và lint trong clean container/CI, không phụ thuộc image cá nhân hoặc artifact cũ.
- [ ] **T06 — Cả nhóm, P1:** Regenerate toàn bộ outputs từ run sạch; manifest chứa commit/config/input hashes/runtime/model version. Báo cáo metric và architecture phản ánh đúng implementation cuối.
- [ ] **T07 — Cả nhóm, P2:** Cập nhật README, TECHNICAL_NOTE, test matrix, deck và runbook; sửa các khẳng định 100%/IXSCAN/32KB/+30m chưa có bằng chứng.
- [ ] **T08 — Cả nhóm, P1:** Rehearsal từ checkout sạch trên máy16GB. Demo: raw ingestion → prediction → late event → recon → restart → query explain. Mỗi người trình bày phần mình và dẫn evidence.

## 9. Thứ tự thực hiện đề xuất

1. **Đợt 1 — Chốt hợp đồng và sửa tính đúng:** G01–05, Q01–05, D01–04, M01–04/M07–08, T01–02. Quyền/Đạt làm dữ liệu và target trong lúc Minh dựng hạ tầng; tích hợp sau schema chung.
2. **Đợt 2 — Hoàn thiện engines:** Q06–11, D05–12, M05–06/M09–12. C8 và recon chỉ nghiệm thu sau preprocessing/model contract ổn định.
3. **Đợt 3 — Chứng minh và bàn giao:** M13–14, T03–08. Chỉ chốt số liệu báo cáo sau run sạch và kiểm tra live.

P0 = chặn tính đúng hoặc tích hợp; P1 = bắt buộc để đáp ứng proposal; P2 = hoàn thiện trải nghiệm/tài liệu sau core. Không có deadline hay số giờ làm mỗi ngày nên đây là thứ tự phụ thuộc, không phải cam kết thời lượng.

Nếu Minh bị nghẽn, Quyền nhận phần recompute/exceptAll của M10 và Đạt nhận heal lag/scorer parity; Minh giữ orchestration, Mongo concurrency và restart tests. Tách PR theo module/contracts để giảm conflict ở pipeline.py.

## 10. Kiểm chứng đã thực hiện và giới hạn

- Python host hiện trỏ WindowsApps alias không chạy được. Đã dùng Docker image sẵn có `final-g03-pipeline:latest`, mount repository chỉ đọc, network none, không sửa output.
- `python -m pytest tests/test_platform_v3.py ...` không chạy vì image thiếu pytest; không báo pytest pass.
- Đã dùng runpy gọi trực tiếp **11 hàm test không fixture: 11 pass, 0 fail**. Đây là kiểm tra source hiện tại trên artifact hiện tại, không rebuild pipeline hay Kafka/Spark/Mongo integration.
- Tìm thấy docker images lab và Mongo/Kafka trên máy không đồng nghĩa branch Compose đã triển khai chúng. Không gọi hàm ghi Mongo trong audit.
- `outputs/mongo_verify.json` ghi `verified_against_mongo_8_0=false`. Không có bằng chứng live latency/index đầy đủ được xác minh trong lần audit này.
- Chưa chạy lại training/full pipeline, UI tương tác, benchmark16GB, Spark explain hay crash-recovery. Các hạng mục đó nằm trong checklist nghiệm thu thay vì được suy ra từ tên file hoặc lời mô tả README.

Các source anchors để review nhanh: `pipeline.py:202` quality, `:342` join evidence, `:488` imputation/LOS, `:559` target/gap, `:587` priors, `:623` KMeans, `:820` rules, `:915` sketches, `:983` recon, `:1105` ML split, `:1381` serving, `:1429` WAL, `:1496` Mongo. `producer.py` là replay offline; `docker-compose.yml` chỉ app/pipeline; `tests/test_platform_v3.py` là 11 tests hiện có.
