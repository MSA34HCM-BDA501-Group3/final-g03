# Handoff session ngày 18 tháng 11 năm 2026

## 1. Chốt trạng thái

**Đã triển khai đường historical replay qua Kafka → consumer Python → features/scoring → MongoDB → UI. G7 bàn giao tài liệu đã PASS.**

Full suite cuối: **32 passed / 135.05 giây**, không skip integration; lint PASS.
Manual, report và slides đã dựng lại, render và rà đủ **44 trang/slide**.
Hash của cả bốn binary hiện tại đã được đối chiếu lại với biên bản G7 khi viết handoff này và đều khớp.

Điều này **không có nghĩa toàn bộ hệ thống operationally Ready**, không xác nhận điểm 95/100 và không đóng các giới hạn về availability lịch sử, accuracy online, throughput hay HA.

## 2. Workspace và phạm vi đã được yêu cầu

- Repository: `C:\Users\P16S G4\Documents\final-g03`.
- Branch: `feat/platform-v3-complete`.
- HEAD hiện tại: `782f02cd2a077ab988c68662769506c5786744e6`.
- **Chưa tạo commit trong session này.** Working tree có nhiều thay đổi tracked/untracked.
- Plan được người dùng duyệt: `C:\Users\P16S G4\.codex\attachments\e7c21e12-cd20-4848-9976-7c7d29c42019\approved-plan.md`.
- Người dùng tiếp tục yêu cầu xử lý G7 và đã chọn **“Cho phép dùng script hiện có (khuyến nghị)”** để dùng Python của repo cùng LibreOffice/Poppler thay runtime riêng của presentation skill.

**Lưu ý working tree:** trước khi triển khai đã có thay đổi từ lần rerun/report-sync trước đó, đặc biệt `outputs/`, `pipeline.py`, PDF và một số Markdown. Không coi tất cả diff là thay đổi mới của session này; không reset hoặc stage toàn bộ một cách máy móc.

## 3. Đã làm gì trong session

### 3.1. Consumer và xử lý bền vững

File chính: [traffic_online.py](../traffic_online.py), [producer.py](../producer.py), [tests/test_online.py](../tests/test_online.py).

- Event contract có `schema_version`, `run_id`; tách event time, simulated arrival và actual receive time.
- Producer phát raw events theo key `segment_id`; có replay-clock control để đóng slot cuối của replay hữu hạn.
- Consumer Kafka tắt auto-commit; ledger delivery dùng `(run_id, topic, partition, offset)`.
- Raw events dedup theo exact durable identity; không dựa vào nhãn mô phỏng `is_duplicate`/`lateness_bucket`.
- Durable writes dùng Mongo `w=1,j=true`; restart drain pending ledger trước khi poll broker.
- Features chỉ dùng raw events tới sequence/cutoff đã lưu; scorer nạp frozen registry, không fit online.
- Snapshot/history insert-once và read-back làm nội dung canonical khi retry.
- Forecast history append-only; latest score dùng atomic revision guard chống stale/concurrent writes.
- Run pin model/static/Vmax, consumer/shared code và runtime versions; đổi contract phải dùng run ID mới.
- Cửa sổ UTC `[start,end)`; watermark dựa trên max event time quan sát trừ 30 phút.
- Beyond-watermark events vẫn vào Bronze; reconciliation cập nhật cả downstream lags bị ảnh hưởng.
- Dùng khóa OS trên shared checkpoint volume: **một host, một worker/run**, chưa phải distributed fencing.

### 3.2. Runtime và UI

- Loại bỏ phụ thuộc image/network/container của project `song-recommender` khỏi đường chạy chính.
- Chuẩn hóa runtime: Python **3.13.9**, Java **17**, PySpark **3.5.7**, Kafka **4.1.0**, MongoDB **8.0.38**.
- Broker nội bộ: `kafka:29092`; URI, database/run, topic, registry và checkpoint qua environment/CLI.
- Namespace riêng: `session9_online`, `session9_tests`, `session9_outages`.
- UI chọn đúng run/database, hiển thị revision, issued_at, target interval và freshness.
- Khi selected run unavailable, không lẫn dữ liệu snapshot cũ vào run đó. Các tab/ví dụ batch khác vẫn được ghi rõ là snapshot.

### 3.3. Evidence, benchmark và Card B

- Giữ nguyên hai bundle `evidence/session9_original/` và `evidence/session9_rerun/`.
- Tạo [gói A–I hiện tại](../evidence/session9/README.md), có source/input/artifact hashes và provenance.
- Controlled Spark benchmark: hai cặp SMJ/BHJ và unsalted/salted; cùng input/settings/action; **2 warmups + 5 lượt đo xen kẽ**.
- Lưu full plans, hai analytical queries, raw timings và **335 task records**.
- B1: **46 dòng = 2 origins × 23 features**, có source/availability/artifact cutoff và verdict.
- B2 nêu segment 21969, đúng feature list và số liệu TRAIN; bỏ diễn giải nguyên nhân về số làn.
- B3 so exact/sketch trên đúng stream raw smoke; sketches không quyết định aggregate/alert online.

### 3.4. G7 và bộ tài liệu

- Presentation/documents skills đã bundled; phiên bản được phát hiện khi render cuối: `26.1117.11355`.
- `@oai/artifact-tool` không có sẵn, kiểm tra npm trả 404. Không tuyên bố đã cài được thư viện này.
- Theo lựa chọn của người dùng, đã cài môi trường **Docker riêng** qua [Dockerfile.artifacts](../Dockerfile.artifacts): LibreOffice, Poppler, python-docx, reportlab và công cụ render.
- Dựng manual/report/deck từ cùng nguồn evidence; cập nhật Markdown và script thuyết trình tương ứng.
- PDF slides được xuất từ **PPTX thật bằng LibreOffice**; manual dùng renderer `render_docx.py` của documents skill.
- Reviewer độc lập mở từng trang manual/report; agent chính mở từng slide. Tổng cộng **44 trang/slide**, không có lỗi clipping/overlap/mất dấu được ghi nhận.
- Manual có checklist **16 items**, **9 red flags**, **25 ô hyperlink**, không có link thiếu.
- Kiểm tra cấu trúc PPTX: không có finding, không có shape ngoài slide.
- Sửa nhãn metric: RF weighted F1 **0.9439**, severe F1 **0.9409**; DT 23D PR-AUC **0.9327**.
- Sửa denominator **3,953** thành downstream candidate windows của local replay lịch sử; không gọi là số feature cells hoặc số features thực sự đổi.
- Bản trước sửa được giữ tại [g7_archive](../evidence/session9/g7_archive/), kèm `archive_hashes.json`.

## 4. Đạt được gì và bằng chứng ở đâu

| Kết quả | Phạm vi đã kiểm chứng | Evidence |
|---|---|---|
| Full suite và lint | 32 tests pass; lint E/F/W/C901 pass; không skip integration | [full_suite.log](../evidence/session9/full_suite.log), [lint.log](../evidence/session9/lint.log), [validation.json](../evidence/session9/validation.json) |
| Dedup/late/quarantine | Đảo nhãn mô phỏng không đổi kết quả; duplicate thật không tăng aggregate; watermark equality và một microsecond trước boundary | [test_online.py](../tests/test_online.py), full suite log |
| Crash/restart | Kill thật ở 5 vị trí; fixture hội tụ về offset 6, 4 unique raw, 5 revisions; history cũ giữ nguyên | [crash evidence](../evidence/session9/tests/) |
| Outage recovery | Dừng Mongo/Kafka thật tại startup; process lỗi khi unavailable; restore health rồi restart hội tụ | [outages](../evidence/session9/outages/) |
| End-to-end raw replay | 20 GPS thật + 1 duplicate + 1 clock control; 3 latest source windows; trace qua broker/model/Mongo/UI | [raw_smoke.json](../evidence/session9/raw_smoke.json), [F_trace.json](../evidence/session9/F_trace.json), [F_ui.png](../evidence/session9/F_ui.png) |
| Full-source aggregation/join parity | 90,938 source pings; 33,441 windows; zero two-way differences trong benchmark | [benchmark/results.json](../evidence/session9/benchmark/results.json) |
| Feature availability audit | 46 replay rows; vận hành lịch sử vẫn UNVERIFIED | [H_B1_availability.csv](../evidence/session9/H_B1_availability.csv) |
| G7 PASS | 4 binary artifacts, 44 render items, content/structure/link/visual checks, exact hashes | [artifact_review.json](../evidence/session9/artifact_review.json), [g7_artifact_checks.json](../evidence/session9/g7_artifact_checks.json), [g7_validation.json](../evidence/session9/g7_validation.json) |

Median benchmark mới, đơn vị giây: SMJ **0.310514**, BHJ **0.261904**, unsalted **0.124255**, salted **0.116502**. Đây là warm local measurements, chưa phải cluster scaling.

Raw smoke dùng run `raw-c8c7fa7014bd4b54a1e03fe153b8397b`, database `session9_online`; xem `raw_smoke.json` để lấy topic và danh sách source IDs.

## 5. File bàn giao hiện hành

| Artifact | Quy mô | File |
|---|---:|---|
| Manual Session 9 | 12 trang | [BDA501_Session9_Lab_Manual_G03.docx](../BDA501_Session9_Lab_Manual_G03.docx) |
| Final report | 16 trang, 7 chương | [HCMC_Smart_Traffic_Final_Report.pdf](../HCMC_Smart_Traffic_Final_Report.pdf) |
| PowerPoint | 16 slides | [HCMC_Smart_Traffic_Final_Presentation.pptx](../HCMC_Smart_Traffic_Final_Presentation.pptx) |
| Slides PDF | 16 trang | [HCMC_Smart_Traffic_Final_Slides.pdf](../HCMC_Smart_Traffic_Final_Slides.pdf) |

Slide 16 là **Session 9 Technical Readiness Summary** với đủ 5 chủ đề.
Nguồn đi kèm: [README](../README.md), [PRESENTATION_SCRIPT.md](../PRESENTATION_SCRIPT.md), [TECHNICAL_NOTE.md](../TECHNICAL_NOTE.md), [report Markdown](../product/00-proposal/project-proposal.md).

Snapshot chốt G7 trong [manifest.json](../evidence/session9/manifest.json):

- Thời điểm: `2026-11-18T06:55:36.923518+00:00`, tương ứng khoảng **13:55 ICT**.
- Source snapshot SHA-256: `cac31f71ad41f46248758331dd193414287813576033e81e9e4e3cdbed271983`.
- Đã kiểm tra 87 source hashes, 55 evidence checksums và hash của 4 binary được review khi chốt G7.
- File handoff này được bổ sung **sau snapshot đó**; không dựng lại binary hoặc chạy lại pipeline chỉ để viết bản tổng kết này.

## 6. Cần làm gì nữa

### Ưu tiên bàn giao/source control

1. Review working tree và phân biệt thay đổi trước session với phần triển khai mới; chia nhóm code/runtime, tests, evidence và deliverables trước khi commit.
2. Chọn phần cần đưa vào repo/submission theo chính sách nhóm. `raw_data/` và `data/` đang là dữ liệu ignored; không giả định clean checkout tự có input/model/static store.
3. Diễn tập demo từ raw source → broker offset → feature snapshot → model → history/latest → UI; chuẩn bị fallback screenshot có nhãn replay.
4. Kiểm tra đường dẫn bộ tài liệu thật khi trình chiếu; không mở nhầm bản archived hoặc bản copy ngoài repository.

### Những giới hạn còn mở, không phải việc đã hoàn thành

| Ưu tiên | Việc cần làm | Điều kiện trước khi nâng claim |
|---|---|---|
| ML/data | Đánh giá accuracy của feature policy online, đặc biệt frozen-neighbor prior khác policy batch | Chưa dùng metric batch làm accuracy online |
| Availability | Tìm arrival logs, artifact availability và issuance history thực nếu có | Nếu không có, giữ historical operation UNVERIFIED; replay không thay thế được |
| Streaming | Kiểm thử crash/recovery nhiều partition, rebalance và nhiều luồng delivery | Fixture crash hiện chỉ một partition |
| Performance | Chạy full online replay, đo throughput/latency và giới hạn state; cải thiện cách rebuild toàn bộ closed windows mỗi delivery | Chưa đặt SLO hoặc claim city-scale từ smoke 20 pings |
| Resilience | Bổ sung mid-request outage, broker recreation/storage migration và host-power-loss tests nếu cần | Startup outage/restart không chứng minh uptime/HA |
| Deployment | Distributed coordination/fencing, retention/compaction và multi-node deployment khi mở rộng | Khóa OS/shared volume và Mongo một nút chỉ là PoC |

Các claim phải tiếp tục giữ đúng: coverage không là cold-start accuracy; query latency không là end-to-end latency; slot +30m không là 30 phút advance warning; 687.43x/1.023x khác population không chứng minh giảm task skew; không tuyên bố exactly-once Kafka/Mongo.

## 7. Cách nối tiếp ở session sau

Đọc theo thứ tự: **file này → [I_readiness.md](../evidence/session9/I_readiness.md) → [artifact_review.json](../evidence/session9/artifact_review.json) → [README](../README.md)**. Chỉ mở [audit TODO cũ](SESSION9_AUDIT_TODO_VI.md) để hiểu lịch sử; không coi các nhận định thiếu input/evidence hoặc G7 chưa render trong phần audit ban đầu là trạng thái hiện tại.

Kiểm tra trạng thái trước khi sửa:

```powershell
git status --short
git branch --show-current
git rev-parse HEAD
```

Khi cần chạy lại validation:

```powershell
docker compose build spark app
docker compose up -d --wait kafka mongo
python -m scripts.session9_validate
```

Khi thực sự cần dựng lại tài liệu:

```powershell
docker build -f Dockerfile.artifacts -t final-g03-artifacts .
docker run --rm --mount "type=bind,source=$PWD,target=/workspace" final-g03-artifacts python -m scripts.session9_build_artifacts
python -m scripts.session9_g7_validate
```

Sau rebuild, phải rà lại hình, cập nhật `artifact_review.json` theo hash file mới rồi mới refresh manifest. `session9_g7_validate` chỉ làm render và structural/content checks, không tự thay cho visual review. Không chạy lại authoring sau khi seal mà vẫn giữ review/hash cũ.

Các entrypoint build cũ `scripts/build_proposal_pdf.py` và `scripts/build_slides.py` đã chuyển sang bộ dựng evidence-driven mới. Tooling/ủy quyền thay runtime được ghi tại [g7_tooling.json](../evidence/session9/g7_tooling.json). Renderer được tìm theo skill đang cài, tránh hardcode cache version cũ.

`tmp/g7/` chứa PNG render và helper tạm, có đường dẫn/hash được tham chiếu trong biên bản review. Không dọn thư mục này trước khi quyết định cách lưu QA evidence; cũng không stage toàn bộ `tmp/` chỉ vì muốn đóng gói.
