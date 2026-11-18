# Kết quả chạy lại Session 9

> **Cập nhật riêng sau rerun (18/11/2026):** Đã sửa lỗi đồng bộ chữ ký artifact trong `product/00-proposal/project-proposal.md`, `TECHNICAL_NOTE.md` và dựng lại `HCMC_Smart_Traffic_Final_Report.pdf` bằng script hiện có. Toàn bộ suite sau sửa: **24 passed, 44.87s** (`post_fix_tests.log`). Sau khi bổ sung ghi chú nguồn số đo vào tài liệu và PDF cuối cùng, test đồng bộ tiếp tục **1 passed, 2.33s** (`report_sync_tests.log`); lint script dựng PDF pass. PDF đã render và kiểm tra bố cục. Các dòng “23 passed, 1 failed” bên dưới là lịch sử của lần chạy trước bản sửa, được giữ để đối chiếu. Không thay đổi test; các TODO về hành vi streaming/B1 vẫn còn. PDF/Markdown ghi rõ số đo nào thuộc lần chạy trước; slide và lab manual chưa được sửa trong lần cập nhật riêng này.

Branch gốc `feat/platform-v3-complete`, HEAD `782f02c`; bổ sung `TRAFFIC_LOCAL_VERIFIER=1` trong pipeline.py để gọi Spark/Mongo verifier bằng cùng Python runtime, không phụ thuộc image song-recommender ngoài repo.

CSV đầu vào lấy từ `C:/Users/P16S G4/Downloads/session9.zip`. Bộ evidence trong ZIP được giữ riêng ở `evidence/session9_original/`; đó là bằng chứng bàn giao cũ, không phải số đo của lần chạy này. Các hash input khớp manifest Quyen đã lưu. Raw/data vẫn bị gitignore.

## Kết quả đã thực thi

| Hạng mục | Kết quả mới | Đối chiếu DOCX |
|---|---|---|
| Pipeline GIS/GPS | 577,967 nodes; 84,633 segments; 90,938 pings; 33,441 windows | Khớp |
| Exact t+30m | 7,898 cặp; TEST 1,182 | Khớp |
| Coverage | 10,027 observed; 74,606 không có GPS | Khớp |
| Independent baseline | 33,441 matched; không thiếu/thừa key; mọi component diff=0; max_abs_diff=0 | Khớp |
| RF 23D | Accuracy 0.9450; ROC-AUC 0.9882; severe AP 0.9471 | Khớp |
| Persistence | Accuracy 0.9484 | Khớp |
| LOS transitions | 61; RF default 2/61, VAL-calibrated 9/61 | Khớp |
| Spark tests | 5 passed, 21.38s | Repo hiện có 5, khác claim cũ 4 |
| Spark physical plans | Exchanges 10 → 2; exceptAll diff=0 | Khớp cấu trúc |
| Spark salted monoid | 59,828 groups → 33,441 windows; diff=0 | Khớp |
| Runtime aggregation | Unsalted 1181.79ms; salted 1099.71ms | Khác số cũ 1054.17/1073.37; một lần đo local, không kết luận speedup tổng quát |
| Mongo | 33,441 docs; IXSCAN; không in-memory SORT | Khớp chức năng |
| Mongo warm query | median 0.432ms; p95 0.565ms | Khác số cũ 0.530/0.636ms; không phải end-to-end latency |
| Mongo version | 8.0.38 | DOCX dùng 8.0.34; tag mongo:8.0 đã đổi patch |
| Kafka | 4.1.0; publish check 3,000 messages, 3 partitions | Xem kafka_replay.log; không chứng minh consumer scoring xuyên suốt |
| Local restart replay | WAL replay skip=true; duplicate side effects=0; 501 late windows | Khớp phạm vi mô phỏng, không phải process-kill test |
| Lint | All checks passed | Theo cấu hình README |
| Full pytest | 23 passed, 1 failed, 43.51s | Fail: test_proposal_pdf_and_pptx_deliverables_synchronized |
| Streamlit | HTTP /_stcore/health trả ok | Smoke test server; chưa kiểm tra mọi tương tác UI |

Runtime: Python 3.13, Java 17, PySpark 3.5.7. Xem environment.txt và java.txt. Đồng hồ Docker ghi 2026-11-17 UTC; ngày phiên làm việc là 18/11/2026. Giữ timestamps nguyên bản trong log.

## Những việc chạy lại không tự giải quyết

- Test đồng bộ report đã fail ngay tại model hash `97a5a63e4e34` không có trong project-proposal.md; các kiểm tra phía sau assertion này chưa được thực hiện. Xem final_tests.log. Không sửa test hoặc thay số để ép pass.
- `model.joblib` chứa serialization metadata/timing nên hash mới không chứng minh prediction khác. CSV ablation/transition đã đối chiếu được các metric chính.
- B1 arrival-time availability, append-only forecast history, oracle dedup, cách so skew và phạm vi claim RAM/recovery vẫn là các việc code/design cần sửa trong docs/SESSION9_AUDIT_TODO_VI.md.
- Bước pipeline tích hợp dùng Kafka mặc định localhost:9092 trong container nên broker check của bước đó không kết nối. Chạy producer riêng sau khi Kafka ready với kafka:29092 thành công; log riêng phân biệt hai lần. Khi chạy lại toàn bộ, đặt KAFKA_BOOTSTRAP_SERVERS=kafka:29092 ngay từ đầu.
- Chưa cập nhật DOCX/PDF/PPTX theo số mới. Bằng chứng cũ trong ZIP không được coi là bằng chứng mới hoặc tự động xác nhận hết checkbox.

## Lệnh tái lập

Dockerfile.session9 dùng dependency lock thu được từ lần chạy này. Tạo image, chạy trên network có Mongo và Kafka, mount repo vào /workspace, đặt MONGO_URI và KAFKA_BOOTSTRAP_SERVERS đúng DNS services. Các lệnh đã thực thi trong container final-g03-audit:

```sh
TRAFFIC_LOCAL_VERIFIER=1 MONGO_URI=mongodb://final-g03-audit-mongo:27017 python pipeline.py
python baseline.py
python spark_engine.py --mode all --mongo-uri mongodb://final-g03-audit-mongo:27017
python spark_batch.py --raw-dir raw_data --output-root data/session9_rerun_quyen --train-end 2020-08-01T00:00:00Z
python producer.py --mode both --bootstrap-servers kafka:29092 --max-messages 3000
python starter_stream.py --kill-after-batch 1
python -m pytest -q
ruff check . --select E,F,W,C901 --config lint.mccabe.max-complexity=14 --config line-length=120
```

spark_batch yêu cầu output-root mới mỗi lần, không ghi đè snapshot cũ. Log lần thử đầu và lần chạy tích hợp được giữ riêng. outputs/ là kết quả tái tạo mới, có thay đổi tracked artifacts/checkpoints và model stage UUID theo Spark; chưa commit.
