# Sổ Tay Vận Hành Hệ Thống & Xử Lý Sự Cố (Operations Runbook — SRE & MLOps)

## 1. Quy Trình Khởi Chạy & Kiểm Tra Sức Khỏe Hệ Thống (`Standard Operating Procedures`)

### 1.1. Chạy Toàn Bộ Pipeline & Kiểm Chứng `Artifacts` (`Host / Container`)
```powershell
# Chạy Pipeline V3 ([C1]-[C16]) tạo đầy đủ Medallion Lakehouse, Frozen Model & MongoDB IXSCAN
python pipeline.py

# Chạy kiểm chứng luồng chịu lỗi WAL Checkpoint Kill/Restart & Frozen Registry SHA-256
python starter_stream.py --kill-after-batch 1

# Chạy đối chiếu độc lập Pillar I1 (Monoid vs Baseline)
python baseline.py

# Chạy giả lập Replay Producer ([C1])
python producer.py --limit 1000

# Chạy kiểm thử tự động toàn diện
pytest -v
```

### 1.2. Khởi Động Dịch Vụ Dashboard & Kiểm Tra Cổng `:8501`
```powershell
docker compose up -d app
docker ps --filter "name=groupexe-knowledgetest"
```

---

## 2. Xử Lý Sự Cố Thường Gặp (`Incident Response Playbook`)

| Kịch bản Sự cố (`Incident`) | Triệu chứng (`Symptoms`) | Quy trình Khắc phục Chuẩn (`Resolution Steps`) |
| :--- | :--- | :--- |
| **1. Lệch Phiên bản `scikit-learn` Khi Nạp `model.joblib`** | `ModuleNotFoundError: _loss` hoặc cảnh báo không tương thích khi nạp `outputs/model.joblib`. | Chạy lại `python pipeline.py` (hoặc `docker compose run --rm pipeline`) trong đúng môi trường mục tiêu để đồng bộ hóa `outputs/model.joblib` và `outputs/model_registry_manifest.json`. |
| **2. Sự kiện Đến Trễ Quá Ngưỡng `Watermark` (`EC-9`)** | `pre_reconciliation_completeness_pct < 100%` trong `outputs/reconciliation_audit.csv`. | Không cần can thiệp thủ công: `[C6]` `Sink 1` đã lưu `100%` sự kiện thô vào `data/lakehouse/bronze_raw_events/`; `[C12]` ở `Phase C` tự động tính lại `501` cửa sổ đoạn đường trễ trên `219` khung `(date, period)`, thực hiện `Lag Feature Healing` cho `3,953` `downstream windows` (`377` cửa sổ được `re-score`, `100` cửa sổ đổi xác suất) và nâng cấp trạng thái lên `BATCH_RECONCILED` (`status_version = 2`). |
| **3. Khởi động lại Container Đột ngột (`EC-10`)** | Tiến trình xử lý luồng hoặc ghi `MongoDB` bị ngắt giữa chừng. | Hệ thống tự động đọc lại offset từ `outputs/checkpoints/{raw,agg}` và thực thi `Idempotent Upsert` `UpdateOne({'event_id': eid}, {'$set': doc}, upsert=True)` trên `Unique Index` `{event_id: 1}` mà không tạo bản ghi trùng. |
| **4. Dịch vụ Treo Quá 60 Giây (`Service Timeout Rule`)** | Container hoặc tiến trình đầu cuối không phản hồi sau `60s`. | Xác định đúng `Container ID` hoặc `PID` gắn với cổng `:8501` / `:27019` và chỉ khởi động lại đúng tiến trình mục tiêu (`docker restart groupexe-knowledgetest-app-1`), tuyệt đối không dùng lệnh dừng hàng loạt. |

