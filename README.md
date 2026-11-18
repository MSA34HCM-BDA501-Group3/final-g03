# HCMC Smart Traffic · BDA501

Historical GPS replay PoC: raw pings → Kafka → Python consumer → cutoff-bounded
features/frozen scoring → MongoDB history/latest → run-scoped Streamlit UI.
Spark handles batch ETL and analytics. This is not a live city feed.

Nhóm: **Nguyễn Tạ Minh Quyền (25MS23280)** — dữ liệu/Spark;
**Nguyễn Thành Đạt (25MS23306)** — graph/ML;
**Phạm Công Minh (25MS23310)** — Kafka/Mongo/UI.
Giảng viên: **Đặng Trần Khánh**, FSB · FPT University · BDA501, Fall 2026.

## Tài liệu bàn giao hiện hành

- [Manual Session 9](BDA501_Session9_Lab_Manual_G03.docx): 12 trang, đủ 10 mục,
  Card B, checklist 16 items và 9 red flags có liên kết evidence.
- [Báo cáo PDF](HCMC_Smart_Traffic_Final_Report.pdf): 16 trang, 7 chương;
  [nguồn Markdown](product/00-proposal/project-proposal.md).
- [PowerPoint](HCMC_Smart_Traffic_Final_Presentation.pptx) và
  [Slides PDF](HCMC_Smart_Traffic_Final_Slides.pdf): 16 slides; slide 16 là
  **Session 9 Technical Readiness Summary**.
- [PRESENTATION_SCRIPT.md](PRESENTATION_SCRIPT.md): kịch bản, Live Demo và Q&A;
  [TECHNICAL_NOTE.md](TECHNICAL_NOTE.md): số liệu và phạm vi cùng nguồn báo cáo.
- [Evidence A–I](evidence/session9/README.md),
  [gate matrix](evidence/session9/I_readiness.md),
  [artifact review](evidence/session9/artifact_review.json),
  [manifest](evidence/session9/manifest.json).

Bản trước sửa nằm trong `evidence/session9/g7_archive/`. Hai bundle
`session9_original` và `session9_rerun` giữ nguyên. Bản PreExperiment vẫn là tài liệu
trước thí nghiệm; không trộn với kết quả đo hiện hành.

## Phạm vi kết quả

Năm CSV có hash khớp rerun. Toàn nguồn 90,938 pings → 33,441 windows; motor-road
path 90,787 → 33,394 có filter khác. Benchmark mới xác nhận salted/unsalted và
SMJ/BHJ parity trước khi đo 2 warmups + 5 lượt xen kẽ, cùng input/settings/action.
Đây là đo warm local[2], không phải cluster scaling.

Metric batch RF: accuracy 0.9450, weighted F1 0.9439, severe F1 0.9409,
ROC-AUC 0.9882, severe PR-AUC 0.9471. Persistence accuracy 0.9484. Các metric này
không đánh giá accuracy cho feature policy online mới. Card B1 có 46 dòng cho
2 replay origins × 23 features; availability vận hành lịch sử vẫn **UNVERIFIED**.

Đường online dùng at-least-once và idempotent durable processing. Simulation labels
không quyết định dedup/late; exact Mongo identity quyết định. Năm vị trí process kill,
stale/concurrent latest writes, startup Mongo/Kafka outages và 20 GPS thực có evidence.
Test logs là nguồn chính thức cho passed/failed/skipped; không bỏ qua integration.

Proof-of-Scale là mục đích đối chiếu thuật toán, chưa chứng minh city throughput.
Không suy giảm task skew từ 687.43x/1.023x khác population; warm query latency không
phải end-to-end latency; slot +30m không bảo đảm 30 phút báo trước target start;
cold-start coverage không chứng minh accuracy. Cross-partition crash parity, full online
throughput, HA và scaling vẫn chưa được xác minh.

## Chạy và kiểm thử

```powershell
docker compose build spark app
docker compose up -d --wait kafka mongo
python -m scripts.session9_validate
docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 spark python -m scripts.session9_replay_smoke
python -m scripts.session9_outages
```

Consumer: `traffic_online.py --help`. Cấu hình brokers, URI, database/run, topic,
registry và checkpoint bằng environment/CLI. Dùng namespace `session9_` riêng;
một worker/run trên cùng volume checkpoint. Không xóa checkpoints để restart.
Đổi artifact/code/runtime thì dùng run ID mới.

```powershell
$env:TRAFFIC_RUN_ID = 'demo-20261118'
docker compose --profile stream up -d stream app
```

Để xem raw smoke, dùng run trong `evidence/session9/raw_smoke.json` và cùng database
`session9_online`. Bảng vận hành Tab 2 hiển thị revision, issued_at, target và freshness;
các tab/ví dụ batch còn lại là snapshot lịch sử. F_ui.png là fallback replay có nhãn.

## Dựng và kiểm tra G7

Presentation skill đã bundled. Runtime riêng không có sẵn; người dùng đã cho phép
dùng bộ dựng Python của repo với LibreOffice/Poppler. Môi trường bổ sung tách biệt
trong Docker, được mô tả tại [g7_tooling.json](evidence/session9/g7_tooling.json).

```powershell
docker build -f Dockerfile.artifacts -t final-g03-artifacts .
docker run --rm --mount "type=bind,source=$PWD,target=/workspace" final-g03-artifacts python -m scripts.session9_build_artifacts
python -m scripts.session9_g7_validate
```

Sau rebuild, cần rà hình từng trang/slide và cập nhật artifact_review/manifest.
G7 được chốt theo review và hash của chính file xuất, không tự PASS chỉ vì build xong.
`session9_g7_validate` tự tìm renderer trong phiên bản skill đang cài; không hardcode
đường dẫn plugin cũ. PDF slide được xuất từ PPTX thật bằng LibreOffice.

Lint theo cấu hình repo:

```powershell
ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
```
