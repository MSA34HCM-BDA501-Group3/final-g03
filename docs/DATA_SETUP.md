# Thiết lập dữ liệu để tái lập dự án

## Nguồn và quyền sử dụng

Dataset được ghi nhận trong `outputs/metrics.json` là [HCMC Traffic Flow Data](https://www.kaggle.com/datasets/thanhnguyen2612/traffic-flow-data-in-ho-chi-minh-city-viet-nam). Repository không chứa dữ liệu nguồn; hãy tải trực tiếp từ trang dataset, đọc điều khoản và giữ thông tin ghi công theo yêu cầu của chủ sở hữu. Không commit raw data hoặc thông tin xác thực Kaggle.

## Chuẩn bị trên Windows

Tạo thư mục `raw_data/` ở thư mục gốc repository, tải ZIP từ Kaggle và giải nén sao cho năm tệp nằm trực tiếp trong thư mục đó:

```powershell
New-Item -ItemType Directory -Force raw_data
Expand-Archive -Path "$env:USERPROFILE\Downloads\<ten-file-dataset>.zip" -DestinationPath raw_data -Force

$required = @("nodes.csv", "streets.csv", "segments.csv", "segment_status.csv", "train.csv")
$missing = @($required | Where-Object { -not (Test-Path (Join-Path "raw_data" $_)) })
if ($missing.Count -gt 0) {
    throw "Thiếu tệp nguồn trong raw_data/: $($missing -join ', ')"
}
```

Kiểm kê kỳ vọng của bản dữ liệu dùng trong báo cáo:

| Tệp | Số dòng dữ liệu | Kích thước xấp xỉ |
|---|---:|---:|
| `nodes.csv` | 577,967 | 19.44 MB |
| `streets.csv` | 5,553 | 0.20 MB |
| `segments.csv` | 84,633 | 10.29 MB |
| `segment_status.csv` | 90,938 | 3.58 MB |
| `train.csv` | 33,394 cơ giới (+47 phi đường bộ) | 4.99 MB |

Các con số kiểm kê trong `outputs/file_inventory.csv` là kết quả đã ghi nhận; cần chạy lại pipeline trên bộ dữ liệu đã tải để xác nhận phiên bản dữ liệu cục bộ.

## Chạy và kiểm tra đầu ra

```powershell
python -m pip install -r requirements.txt
docker compose up -d --build
python pipeline.py
python baseline.py
python starter_stream.py --kill-after-batch 1
python -m ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
```

Sau khi chạy thành công, cần có `data/dataset.csv` (`33,394` dòng × `60` cột), `data/lakehouse/segment_static_features.parquet` (`84,206` đoạn đường cơ giới), `data/lakehouse/bronze_raw_events/events.parquet` (`90,787` bản tin GPS sạch), `data/lakehouse/stream_replay_windows/replay.parquet`, `data/lakehouse/curated_gold/year_month=*` (`10` phân vùng tháng đọc sạch bởi cả PyArrow/Pandas và Spark 3.5.7), và các báo cáo trong `outputs/`, bao gồm `outputs/metrics.json`, `outputs/ablation_results.csv`, `outputs/transition_evaluation.csv`, `outputs/mongo_verify.json`, `outputs/spark_runtime_evidence.json`.

Giao diện Web hợp nhất duy nhất đọc dữ liệu đã sinh sau khi chạy pipeline:
- **ReactJS + FastAPI TOC Command Center tại <http://localhost:8000>:** dịch vụ `toc-web` (`toc_backend/main.py` + `toc_frontend/dist/`) trong cụm `5` dịch vụ mặc định (`11.0 GB RAM`) của `docker-compose.yml`, hoặc chạy cục bộ bằng `uvicorn toc_backend.main:app --host 127.0.0.1 --port 8000`.

`raw_data/` và `data/` nằm trong `.gitignore` có chủ ý. Bản dựng Spark 3.5.7 dùng `Dockerfile.spark35` (`hcmc-traffic-spark:3.5.7`); `Dockerfile.spark`/`requirements-spark.txt` là môi trường riêng cho `compose.quyen.yml`.
*(Bộ kiểm thử `tests/test_platform_v3.py` và `tests/test_quyen_spark.py` đã đạt `Gate 3 PASS`, đồng bộ hoàn toàn với chuẩn thực nghiệm sạch không rò rỉ dữ liệu trên cả full data lẫn checkout mới dùng `data/demo_sample/`.)*
